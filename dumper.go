package pgdump

import (
	"database/sql"
	"log"
	"os"
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
		log.Fatalf("Failed to get PostgreSQL server version: %v", err)
	}

	info := DumpInfo{
		DumpVersion:   "1.0.0",
		ServerVersion: serverVersion,
		CompleteTime:  time.Now().Format(time.RFC1123),
	}

	if err := writeHeader(file, info); err != nil {
		return err
	}
	// Dump schema: Iterate through each table to write CREATE TABLE statements
	tables, err := getTables(db)
	if err != nil {
		return err
	}
	for _, table := range tables {
		createStmt, err := getCreateTableStatement(db, table)
		if err != nil {
			return err
		}
		file.WriteString(createStmt + "\n\n")
	}

	// Dump data: Iterate through each table to write data using COPY
	for _, table := range tables {
		copyStmt, err := getTableDataCopyFormat(db, table)
		if err != nil {
			return err
		}
		file.WriteString(copyStmt + "\n\n")
	}

	if err := writeFooter(file, info); err != nil {
		return err
	}

	return nil
}
