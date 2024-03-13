package pgdump

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Dumper struct {
	ConnectionString string
}

func NewDumper(connectionString string) *Dumper {
	return &Dumper{ConnectionString: connectionString}
}

func (d *Dumper) DumpDatabase(outputFile string) error {
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
	serverVersion, err := getServerVersion(db)
	if err != nil {
		fmt.Printf("failed to get PostgreSQL server version: %v", err)
	}

	info := DumpInfo{
		DumpVersion:   "1.0.2",
		ServerVersion: serverVersion,
		CompleteTime:  time.Now().Format("2006-01-02 15:04:05 -0700 MST"),
	}

	if err := writeHeader(file, info); err != nil {
		return err
	}

	tables, err := getTables(db)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if err := scriptTable(db, file, table); err != nil {
			return err
		}
	}

	if err := writeFooter(file, info); err != nil {
		return err
	}

	return nil
}

// New function to encapsulate table scripting, including sequences and primary keys.
func scriptTable(db *sql.DB, file *os.File, tableName string) error {
	// Example of creating table statement.
	createStmt, err := getCreateTableStatement(db, tableName)
	if err != nil {
		return err
	}
	file.WriteString(createStmt + "\n\n")

	// Handle sequences for auto-increment columns.
	seqStmts, err := scriptSequences(db, tableName)
	if err != nil {
		return err
	}
	file.WriteString(seqStmts)

	// Handle primary keys.
	pkStmt, err := scriptPrimaryKeys(db, tableName)
	if err != nil {
		return err
	}
	file.WriteString(pkStmt)

	// Example of dumping table data.
	copyStmt, err := getTableDataCopyFormat(db, tableName)
	if err != nil {
		return err
	}
	file.WriteString(copyStmt + "\n\n")

	return nil
}

func scriptSequences(db *sql.DB, tableName string) (string, error) {
	var sequencesSQL strings.Builder

	query := `
SELECT n.nspname AS sequence_schema,
       s.relname AS sequence_name,
       seq.sequence_schema,
       seq.sequence_name,
       seq.start_value,
       seq.minimum_value,
       seq.maximum_value,
       seq.increment_by,
       a.attname AS related_column
FROM pg_class s
JOIN pg_depend d ON d.objid = s.oid
JOIN pg_class t ON t.oid = d.refobjid AND t.relkind = 'r'
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
JOIN pg_namespace n ON n.oid = s.relnamespace
JOIN pg_sequences seq ON seq.sequencename = s.relname AND seq.schemaname = n.nspname
WHERE t.relname = $1 AND n.nspname = 'public';
`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying sequences for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			sequenceSchema, sequenceName, relatedColumn         string
			startValue, minimumValue, maximumValue, incrementBy string
		)
		if err := rows.Scan(&sequenceSchema, &sequenceName, &sequenceSchema, &sequenceName, &startValue, &minimumValue, &maximumValue, &incrementBy, &relatedColumn); err != nil {
			return "", fmt.Errorf("error scanning sequence information: %v", err)
		}

		// Script the CREATE SEQUENCE statement with fetched properties.
		sequencesSQL.WriteString(fmt.Sprintf("CREATE SEQUENCE %s.%s START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s;\n", sequenceSchema, sequenceName, startValue, incrementBy, minimumValue, maximumValue))
		sequencesSQL.WriteString(fmt.Sprintf("ALTER SEQUENCE %s.%s OWNED BY %s.%s;\n\n", sequenceSchema, sequenceName, tableName, relatedColumn))
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
