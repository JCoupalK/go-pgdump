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

	// Query to find sequences associated with a table's columns
	query := `
SELECT n.nspname AS sequence_schema,
       seq.relname AS sequence_name,
       seq_att.attname AS column_name,
       format_type(seq_typ.typnamespace, seq_typ.typtypmod) AS data_type,
       seq_start.start_value AS start_value,
       seq_inc.increment AS increment_by,
       seq_min.minimum_value AS min_value,
       seq_max.maximum_value AS max_value
FROM pg_class tbl
JOIN pg_namespace n ON n.oid = tbl.relnamespace
JOIN pg_depend dep ON dep.refobjid = tbl.oid AND dep.deptype = 'a'
JOIN pg_class seq ON seq.oid = dep.objid AND seq.relkind = 'S'
JOIN pg_attribute seq_att ON seq_att.attrelid = seq.oid AND seq_att.attnum = -1
JOIN pg_attrdef ad ON ad.adrelid = dep.refobjid AND ad.adnum = dep.refobjsubid
JOIN pg_type seq_typ ON seq_typ.oid = seq_att.atttypid,
LATERAL (SELECT pg_get_expr(ad.adbin, ad.adrelid) AS start_value) seq_start,
LATERAL (SELECT increment_by FROM pg_sequences WHERE sequencename = seq.relname) seq_inc,
LATERAL (SELECT min_value FROM pg_sequences WHERE sequencename = seq.relname) seq_min,
LATERAL (SELECT max_value FROM pg_sequences WHERE sequencename = seq.relname) seq_max
WHERE tbl.relname = $1 AND n.nspname = 'public';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying sequences for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			sequenceSchema, sequenceName, columnName, dataType string
			startValue, incrementBy, minValue, maxValue        int64
		)
		if err := rows.Scan(&sequenceSchema, &sequenceName, &columnName, &dataType, &startValue, &incrementBy, &minValue, &maxValue); err != nil {
			return "", fmt.Errorf("error scanning sequence information: %v", err)
		}

		sequenceSQL := fmt.Sprintf("CREATE SEQUENCE %s.%s AS %s START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d;\n",
			sequenceSchema, sequenceName, dataType, startValue, incrementBy, minValue, maxValue)
		sequenceSQL += fmt.Sprintf("ALTER SEQUENCE %s.%s OWNED BY %s.%s;\n",
			sequenceSchema, sequenceName, tableName, columnName)

		sequencesSQL.WriteString(sequenceSQL)
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
