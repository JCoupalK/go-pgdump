# Go PostgreSQL Dump

Create PostgreSQL dumps in Go without the pg_dump CLI as a dependancy.

Inspired by [go-mysqldump](https://github.com/jamf/go-mysqldump) which does that but for MySQL/MariaDB.

Doesn't feature all of pg_dump features just yet (mainly around sequences) so it is still a work in progress.

## Simple example for a CLI tool using the library

```go
package main

import (
 "flag"
 "fmt"
 "log"
 "path/filepath"
 "time"

 "github.com/JCoupalK/go-pgdump"
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
 dumpFilename := filepath.Join(
  outputDir,
  fmt.Sprintf("%s-%s.sql", dbname, currentTime.Format("20060102T150405")),
 )

 // Create a new dumper instance with connection string and number of threads
 dumper := pgdump.NewDumper(psqlInfo, 100)

 if err := dumper.DumpDatabase(dumpFilename, &pgdump.TableOptions{
  TableSuffix: *suffix,
  TablePrefix: *prefix,
  Schema:      *schema,
 }); err != nil {
  log.Fatal(err)
 }

 fmt.Println("Backup successfully saved to", dumpFilename)
}

func main() {
 flag.Parse()
 username := "user"
 password := "example"
 hostname := "localhost"
 db := "test"
 outputDir := *outputDIR
 port := 5432

 BackupPostgreSQL(username, password, hostname, db, outputDir, port)
}
```

## Usage of the example

```bash
./go-pgdump-test -o test -sx example -px test -s myschema
```
