# Go PostgreSQL Dump

Create PostgreSQL dumps in Go without the pg_dump CLI as a dependancy.

## Simple example

```go
package main

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq" // PostgreSQL driver
	"yourproject/pgdump" // Adjust the import path according to your project structure
)

func main() {
	// PostgreSQL database connection string
	// Replace the placeholder values with your actual database connection details
	connStr := "host=your_host user=your_user password=your_password dbname=your_dbname sslmode=disable"

	// Open the database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Specify the directory where the dump file should be stored and the file name format
	dumpDirectory := "./dumps"
	fileNameFormat := "2006-01-02_15-04-05"

	// Register the dumper with the database connection, directory, and file format
	dumper, err := pgdump.Register(db, dumpDirectory, fileNameFormat)
	if err != nil {
		log.Fatalf("Failed to register dumper: %v", err)
	}

	// Perform the dump
	if err := dumper.Dump(); err != nil {
		log.Fatalf("Failed to dump database: %v", err)
	}

	// Close the dumper (and the file it writes to)
	if err := dumper.Close(); err != nil {
		log.Fatalf("Failed to close dumper: %v", err)
	}

	log.Println("Database dump completed successfully.")
}
```
