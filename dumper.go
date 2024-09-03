package pgdump

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

type Dumper struct {
	ConnectionString string
	Parallels        int
	DumpVersion      string
}

func NewDumper(connectionString string, threads int) *Dumper {
	// fetch the DumpVersion from the GitHub repository
	dumpVersion, err := fetchDumpVersion("JCoupalK", "go-pgdump")
	if err != nil {
		dumpVersion = "unknown"
	}

	// set a default value for Parallels if it is zero or less
	if threads <= 0 {
		threads = 50
	}
	return &Dumper{ConnectionString: connectionString, Parallels: threads, DumpVersion: dumpVersion}
}

func fetchDumpVersion(owner, repo string) (string, error) {
	// GitHub API URL to get the latest tags
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/tags", owner, repo)

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch tags: status code %d", resp.StatusCode)
	}

	var tags []struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return "", err
	}

	if len(tags) == 0 {
		return "", fmt.Errorf("no tags found in repository")
	}

	// Return the latest tag
	return tags[0].Name, nil
}

func (d *Dumper) DumpDatabase(outputFile string, opts *TableOptions) error {
	db, err := sql.Open("postgres", d.ConnectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	file, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Template variables
	info := DumpInfo{
		DumpVersion:   d.DumpVersion,
		ServerVersion: getServerVersion(db),
		CompleteTime:  time.Now().Format("2006-01-02 15:04:05 -0700 MST"),
		ThreadsNumber: d.Parallels,
	}

	if err := writeHeader(file, info); err != nil {
		return err
	}

	tables, err := getTables(db, opts)
	if err != nil {
		return err
	}

	var (
		wg sync.WaitGroup
		mx sync.Mutex
	)

	chunks := slices.Chunk(tables, d.Parallels)
	for chunk := range chunks {
		wg.Add(len(chunk))
		for _, table := range chunk {
			//we can add the switch here for export and add a go func here.
			go func(table string) {
				defer wg.Done()
				str, err := scriptTable(db, table)
				if err != nil {
					return
				}
				mx.Lock()
				file.WriteString(str)
				mx.Unlock()
			}(table)
		}
		wg.Wait()
	}
	if err := writeFooter(file, info); err != nil {
		return err
	}

	return nil
}

func (d *Dumper) DumpToCSV(outputDIR string, tablename ...string) ([]string, error) {
	db, err := sql.Open("postgres", d.ConnectionString)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	chunks := slices.Chunk(tablename, d.Parallels)
	var (
		paths []string
	)
	g, _ := errgroup.WithContext(context.Background())
	for chunk := range chunks {
		g.SetLimit(len(chunk))
		for _, table := range chunk {
			table := table // capture the current value of table for use in goroutine
			g.Go(func() error {
				records, err := getTableDataAsCSV(db, table)
				if err != nil {
					return err
				}

				// Correctly open (or create) the file for writing
				f, err := os.Create(path.Join(outputDIR, table+".csv"))
				if err != nil {
					return err
				}
				defer f.Close()

				csvWriter := csv.NewWriter(f)
				if err := csvWriter.WriteAll(records); err != nil {
					return err
				}
				csvWriter.Flush()

				paths = append(paths, path.Join(outputDIR, table+".csv"))
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func scriptTable(db *sql.DB, tableName string) (string, error) {
	var buffer string
	// Script CREATE TABLE statement
	createStmt, err := getCreateTableStatement(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error creating table statement for %s: %v", tableName, err)
	}
	buffer = buffer + createStmt + "\n\n"

	// Script associated sequences (if any)
	seqStmts, err := scriptSequences(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting sequences for table %s: %v", tableName, err)
	}
	buffer = buffer + seqStmts + "\n\n"

	// Script primary keys
	pkStmt, err := scriptPrimaryKeys(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting primary keys for table %s: %v", tableName, err)
	}
	buffer = buffer + pkStmt + "\n\n"

	// Dump table data
	copyStmt, err := getTableDataCopyFormat(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error generating COPY statement for table %s: %v", tableName, err)
	}
	buffer = buffer + copyStmt + "\n\n"

	return buffer, nil
}

func scriptSequences(db *sql.DB, tableName string) (string, error) {
	var sequencesSQL strings.Builder

	// Query to identify sequences linked to the table's columns and fetch sequence definitions
	query := `
SELECT 'CREATE SEQUENCE ' || n.nspname || '.' || c.relname || ';' as seq_creation,
       pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(t.relname), quote_ident(a.attname)) as seq_owned,
       'ALTER TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(t.relname) ||
       ' ALTER COLUMN ' || quote_ident(a.attname) ||
       ' SET DEFAULT nextval(''' || n.nspname || '.' || c.relname || '''::regclass);' as col_default
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a' AND d.classid = 'pg_class'::regclass
JOIN pg_attrdef ad ON ad.adrelid = d.refobjid AND ad.adnum = d.refobjsubid
JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
JOIN pg_class t ON t.oid = d.refobjid AND t.relkind = 'r'
WHERE c.relkind = 'S' AND t.relname = $1 AND n.nspname = 'public';
`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying sequences for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var seqCreation, seqOwned, colDefault string
		if err := rows.Scan(&seqCreation, &seqOwned, &colDefault); err != nil {
			return "", fmt.Errorf("error scanning sequence information: %v", err)
		}

		// Here we directly use the sequence creation script.
		// The seqOwned might not be necessary if we're focusing on creation and default value setting.
		sequencesSQL.WriteString(seqCreation + "\n" + colDefault + "\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating over sequences: %v", err)
	}

	return sequencesSQL.String(), nil
}

func scriptPrimaryKeys(db *sql.DB, tableName string) (string, error) {
	var pksSQL strings.Builder

	// Query to find primary key constraints for the specified table.
	query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = connamespace
WHERE con.contype = 'p' 
AND rel.relname = $1
AND nsp.nspname = 'public';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying primary keys for table %s: %v", tableName, err)
	}
	defer rows.Close()

	// Iterate through each primary key constraint found and script it.
	for rows.Next() {
		var constraintName, constraintDef string
		if err := rows.Scan(&constraintName, &constraintDef); err != nil {
			return "", fmt.Errorf("error scanning primary key information: %v", err)
		}

		// Construct the ALTER TABLE statement to add the primary key constraint.
		pksSQL.WriteString(fmt.Sprintf("ALTER TABLE public.%s ADD CONSTRAINT %s %s;\n",
			tableName, constraintName, constraintDef))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating over primary keys: %v", err)
	}

	return pksSQL.String(), nil
}
