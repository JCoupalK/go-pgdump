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
	info := DumpInfo{
		DumpVersion:   "1.0.2",
		ServerVersion: getServerVersion(db),
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

func scriptTable(db *sql.DB, file *os.File, tableName string) error {
	// Script CREATE TABLE statement
	createStmt, err := getCreateTableStatement(db, tableName)
	if err != nil {
		return fmt.Errorf("error creating table statement for %s: %v", tableName, err)
	}
	file.WriteString(createStmt + "\n\n")

	// Script associated sequences (if any)
	seqStmts, err := scriptSequences(db, tableName)
	if err != nil {
		return fmt.Errorf("error scripting sequences for table %s: %v", tableName, err)
	}
	file.WriteString(seqStmts)

	// Script primary keys
	pkStmt, err := scriptPrimaryKeys(db, tableName)
	if err != nil {
		return fmt.Errorf("error scripting primary keys for table %s: %v", tableName, err)
	}
	file.WriteString(pkStmt)

	// Dump table data
	copyStmt, err := getTableDataCopyFormat(db, tableName)
	if err != nil {
		return fmt.Errorf("error generating COPY statement for table %s: %v", tableName, err)
	}
	file.WriteString(copyStmt + "\n\n")

	return nil
}

func scriptSequences(db *sql.DB, tableName string) (string, error) {
	var sequencesSQL strings.Builder

	query := `
SELECT 'CREATE SEQUENCE ' || n.nspname || '.' || c.relname || 
       ' INCREMENT BY ' || s.increment_by || 
       ' MINVALUE ' || s.min_value || 
       ' MAXVALUE ' || s.max_value || 
       ' START WITH ' || s.start_value || 
       ' CACHE ' || s.cache_value || 
       CASE WHEN s.cycle_option = 'YES' THEN ' CYCLE' ELSE '' END || ';' AS sequence_definition,
       'ALTER SEQUENCE ' || n.nspname || '.' || c.relname ||
       ' OWNED BY ' || t.relname || '.' || a.attname || ';' AS ownership,
       'ALTER TABLE ' || t.relname || 
       ' ALTER COLUMN ' || a.attname || 
       ' SET DEFAULT nextval(''' || n.nspname || '.' || c.relname || '''::regclass);' AS default_value
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
JOIN pg_class t ON t.oid = d.refobjid
JOIN (
    SELECT sequence_schema, sequence_name, increment_by, min_value, max_value, start_value, cache_value, cycle_option
    FROM information_schema.sequences
    WHERE sequence_catalog = current_database()
) s ON s.sequence_schema = n.nspname AND s.sequence_name = c.relname
WHERE t.relname = $1 AND n.nspname = 'public';
`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying sequences for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var sequenceDefinition, ownership, defaultValue string
		if err := rows.Scan(&sequenceDefinition, &ownership, &defaultValue); err != nil {
			return "", fmt.Errorf("error scanning sequence information: %v", err)
		}

		sequencesSQL.WriteString(sequenceDefinition + "\n" + ownership + "\n" + defaultValue + "\n")
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
