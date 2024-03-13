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
	serverVersion := getServerVersion(db)

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
SELECT
    'CREATE SEQUENCE ' || sequence_schema || '.' || sequence_name ||
    ' INCREMENT BY ' || increment_by ||
    ' MINVALUE ' || min_value ||
    ' MAXVALUE ' || max_value ||
    ' START WITH ' || start_value ||
    (CASE WHEN is_cycled = 'YES' THEN ' CYCLE' ELSE '' END) || ';' AS sequence_creation,
    'ALTER SEQUENCE ' || sequence_schema || '.' || sequence_name ||
    ' OWNED BY ' || t.relname || '.' || a.attname || ';' AS sequence_ownership,
    'ALTER TABLE ' || t.relname ||
    ' ALTER COLUMN ' || a.attname || 
    ' SET DEFAULT ' || 'nextval(''' || sequence_schema || '.' || sequence_name || '''::regclass);' AS column_default
FROM pg_sequences
JOIN pg_class t ON t.relname = replace(sequence_name, '_id_seq', '')
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = replace(sequence_name, '_id_seq', 'id')
WHERE sequence_schema = 'public' AND t.relname = $1;
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
