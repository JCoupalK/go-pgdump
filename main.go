package main

import (
	"log"
	"github.com/JCoupalK/go-pgdump/pgdump" // Replace with your module path
)

func main() {
	dumper := pgdump.NewDumper("host=localhost user=postgres dbname=yourdb sslmode=disable")
	err := dumper.DumpDatabase("schema.sql", "data.sql")
	if err != nil {
		log.Fatal(err)
	}
}
