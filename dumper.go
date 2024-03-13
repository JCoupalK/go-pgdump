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

	// Adjusted query to leverage pg_sequences for PostgreSQL 16.1
	query := `
SELECT 
    format('CREATE SEQUENCE %I.%I INCREMENT BY %s MINVALUE %s MAXVALUE %s START WITH %s CACHE %s %s;',
        s.schemaname, 
        s.sequencename, 
        s.increment_by, 
        s.min_value, 
        s.max_value, 
        s.start_value, 
        s.cache_size,
        CASE WHEN s.cycle THEN 'CYCLE' ELSE '' END) AS sequence_creation,
    format('ALTER SEQUENCE %I.%I OWNED BY %I.%I;', 
        s.schemaname, 
        s.sequencename, 
        t.table_name, 
        c.column_name) AS sequence_ownership,
    format('ALTER TABLE %I ALTER COLUMN %I SET DEFAULT nextval(''%I.%I''::regclass);', 
        t.table_name, 
        c.column_name, 
        s.schemaname, 
        s.sequencename) AS column_default
FROM pg_sequences s
JOIN information_schema.columns c ON s.sequencename = replace(c.column_default, 'nextval(''''', '')::regclass::text', '') AND c.column_default LIKE 'nextval%'
JOIN information_schema.tables t ON t.table_name = c.table_name
WHERE t.table_name = $1 AND t.table_schema = 'public';
`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying sequences for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var sequenceCreation, sequenceOwnership, columnDefault string
		if err := rows.Scan(&sequenceCreation, &sequenceOwnership, &columnDefault); err != nil {
			return "", fmt.Errorf("error scanning sequence information: %v", err)
		}

		sequencesSQL.WriteString(sequenceCreation + "\n" + sequenceOwnership + "\n" + columnDefault + "\n")
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
