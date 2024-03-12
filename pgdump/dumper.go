package pgdump

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Dumper struct {
	ConnectionString string
}

func NewDumper(connectionString string) *Dumper {
	return &Dumper{ConnectionString: connectionString}
}

func (d *Dumper) DumpDatabase(schemaFile, dataFile string) error {
	db, err := sql.Open("postgres", d.ConnectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := d.dumpSchema(db, schemaFile); err != nil {
		return fmt.Errorf("error dumping schema: %v", err)
	}

	if err := d.dumpData(db, dataFile); err != nil {
		return fmt.Errorf("error dumping data: %v", err)
	}

	return nil
}
