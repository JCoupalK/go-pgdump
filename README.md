# Go PostgreSQL Dump

Create PostgreSQL dumps in Go without the pg_dump CLI as a dependancy.

Doesn't feature all of pg_dump features just yet (mainly around sequences) so it is still a work in progress.

## Simple example using the library

```go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JCoupalK/go-pgdump"
)

func BackupPostgreSQL(username, password, hostname, dbname, outputDir string, port int) {
	// PostgreSQL connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		hostname, port, username, password, dbname)

	currentTime := time.Now()
	dumpFilename := filepath.Join(outputDir, fmt.Sprintf("%s-%s.sql", dbname, currentTime.Format("20060102T150405")))

	// Create a new dumper instance
	dumper := pgdump.NewDumper(psqlInfo)

	if err := dumper.DumpDatabase(dumpFilename); err != nil {
		fmt.Printf("Error dumping database: %v", err)
		os.Remove(dumpFilename) // Cleanup on failure
		return
	}

	fmt.Println("Backup successfully saved to", dumpFilename)
}

func main(){

	username := "user"
	password := "example"
	hostname := "examplehost"
	db := "dbname"
	outputDir := "path/to/example"
	port := 5432

	BackupPostgreSQL(username, password, hostname, db, outputDir, port)
}
```
