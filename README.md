# GoPGDump - Go PostgreSQL Dump

![License](https://img.shields.io/github/license/JCoupalK/go-pgdump)
![GitHub issues](https://img.shields.io/github/issues-raw/JCoupalK/go-pgdump)
![GitHub go.mod Go version (branch & subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/JCoupalK/go-pgdump/main)

Create PostgreSQL or CSV dumps in Go without the pg_dump CLI as a dependancy.

Inspired by [go-mysqldump](https://github.com/jamf/go-mysqldump) which does that but for MySQL/MariaDB.

Doesn't feature all of pg_dump features just yet so it is still a work in progress.

## Simple example for a CLI tool using the library

```go
package main

import (
 "flag"
 "fmt"
 "log"
 "path/filepath"
 "strings"
 "time"

 "github.com/JCoupalK/go-pgdump"
)

var (
 dumpCSV   = flag.Bool("csv", false, "dump to CSV")
 csvTables = flag.String("tables", "", "comma-separated list of table names to dump to CSV")
 outputDIR = flag.String("o", "", "path to output directory")
 suffix    = flag.String("sx", "", "suffix of tablen names for dump")
 prefix    = flag.String("px", "", "prefix of tablen names for dump")
 schema    = flag.String("s", "", "schema filter for dump")
)

func BackupPostgreSQL(username, password, hostname, dbname, outputDir string, port int) {
 // PostgreSQL connection string
 psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
  hostname, port, username, password, dbname)

 // Create a new dumper instance with connection string and number of threads
 dumper := pgdump.NewDumper(psqlInfo, 50)

 // Check if CSV dump is requested
 if *dumpCSV {
  tableList := strings.Split(*csvTables, ",")
  csvFiles, err := dumper.DumpToCSV(outputDir, tableList...)
  if err != nil {
   log.Fatal("Error dumping to CSV:", err)
  }
  fmt.Println("CSV files successfully saved in:", csvFiles)
 } else {
  // Regular SQL dump
  currentTime := time.Now()
  dumpFilename := filepath.Join(
   outputDir,
   fmt.Sprintf("%s-%s.sql", dbname, currentTime.Format("20060102T150405")),
  )

  if err := dumper.DumpDatabase(dumpFilename, &pgdump.TableOptions{
   TableSuffix: *suffix,
   TablePrefix: *prefix,
   Schema:      *schema,
  }); err != nil {
   log.Fatal("Error dumping database:", err)
  }

  fmt.Println("Dump successfully saved to:", dumpFilename)
 }
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

### Usage for a database dump

```bash
./go-pgdump-cli -o test -sx example -px test -s myschema
```

### Usage for a CSV dump

```bash
./go-pgdump-cli -o test -csv -tables employees,departments
```

See more about the CLI tool [here](https://github.com/JCoupalK/go-pgdump-cli).

## Contributing

Contributions are welcome. Please fork the repository and submit a pull request with your changes or improvements.

## License

This project is licensed under MIT - see the LICENSE file for details.