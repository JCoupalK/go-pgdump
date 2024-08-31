# Go PostgreSQL Dump

Create PostgreSQL dumps in Go without the pg_dump CLI as a dependancy.

Inspired by [go-mysqldump](https://github.com/jamf/go-mysqldump) which does that but for MySQL/MariaDB.

Doesn't feature all of pg_dump features just yet (mainly around sequences) so it is still a work in progress.

## Simple example using the library

```go
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hakaitech/go-pgdump"
)

var (
	outputDIR = flag.String("o", "", "path to output directory")
	suffix    = flag.String("sx", "", "suffix of tablen names for dump")
	prefix    = flag.String("px", "", "prefix of tablen names for dump")
	schema    = flag.String("s", "", "schema filter for dump")
)

func BackupPostgreSQL(username, password, hostname, dbname, outputDir string, port int) {
	// PostgreSQL connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		hostname, port, username, password, dbname)

	currentTime := time.Now()
	dumpFilename := filepath.Join(outputDir, fmt.Sprintf("%s-%s.sql", dbname, currentTime.Format("20060102T150405")))

	// Create a new dumper instance
	dumper := pgdump.NewDumper(psqlInfo)

	if err := dumper.DumpDatabase(dumpFileName, &pgdump.TableOptions{
		TableSuffix: *suffix,
		TablePrefix: *prefix,
		Schema:      *schema,
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Backup successfully saved to", dumpFilename)
}

func main(){
	flag.Parse()
	username := "user"
	password := "example"
	hostname := "examplehost"
	db := "dbname"
	outputDir := *outputDIR
	port := 5432

	BackupPostgreSQL(username, password, hostname, db, outputDir, port)
}
```
