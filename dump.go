package pgdump

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"text/template"
	"time"

	_ "github.com/lib/pq"
)

type Data struct {
	Out              io.Writer
	Connection       *sql.DB
	IgnoreTables     []string
	MaxAllowedPacket int
	LockTables       bool
	DumpDir          string

	tx         *sql.Tx
	headerTmpl *template.Template
	footerTmpl *template.Template
	err        error
}

type table struct {
	Name   string
	Err    error
	isView bool

	cols   []string
	data   *Data
	rows   *sql.Rows
	values []interface{}
}

type metaData struct {
	DumpVersion   string
	ServerVersion string
	CompleteTime  string
}

const (
	Version                 = "1.0.0"
	defaultMaxAllowedPacket = 4194304
)

const headerTmpl = `-- Go PostgreSQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}
`

const footerTmpl = `-- Dump completed on {{ .CompleteTime }}
`

func (data *Data) Dump() error {
	meta := metaData{
		DumpVersion: Version,
	}

	if data.MaxAllowedPacket == 0 {
		data.MaxAllowedPacket = defaultMaxAllowedPacket
	}

	if err := data.prepareTemplates(); err != nil {
		return err
	}

	if err := data.begin(); err != nil {
		return err
	}
	defer data.rollback()

	if err := meta.updateServerVersion(data); err != nil {
		return err
	}

	if err := data.headerTmpl.Execute(data.Out, meta); err != nil {
		return err
	}

	tables, err := data.getTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if err := data.dumpTable(table); err != nil {
			return err
		}
	}

	if data.err != nil {
		return data.err
	}

	meta.CompleteTime = time.Now().Format(time.RFC3339)
	return data.footerTmpl.Execute(data.Out, meta)
}

func (data *Data) begin() (err error) {
	data.tx, err = data.Connection.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	return
}

func (data *Data) rollback() error {
	if data.tx != nil {
		return data.tx.Rollback()
	}
	return nil
}

func (data *Data) prepareTemplates() (err error) {
	data.headerTmpl, err = template.New("header").Parse(headerTmpl)
	if err != nil {
		return
	}

	data.footerTmpl, err = template.New("footer").Parse(footerTmpl)
	if err != nil {
		return
	}
	return
}

func (data *Data) getTables() ([]*table, error) {
	query := `
	SELECT tablename
	FROM pg_catalog.pg_tables
	WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
	`
	rows, err := data.tx.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []*table
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !data.isIgnoredTable(name) {
			tables = append(tables, &table{Name: name, data: data})
		}
	}

	return tables, rows.Err()
}

func (data *Data) isIgnoredTable(name string) bool {
	for _, ignored := range data.IgnoreTables {
		if ignored == name {
			return true
		}
	}
	return false
}

func (meta *metaData) updateServerVersion(data *Data) error {
	var version string
	if err := data.tx.QueryRow("SHOW server_version").Scan(&version); err != nil {
		return err
	}
	meta.ServerVersion = version
	return nil
}

func (data *Data) dumpTable(t *table) error {
	if t.isView {
		// Handle views differently if needed
		return nil
	}

	// Example for dumping table schema (simplified)
	_, err := fmt.Fprintf(data.Out, "-- Schema for table %s\n", t.Name)
	if err != nil {
		return err
	}

	// Example for dumping table data (simplified)
	err = data.dumpTableData(t)
	if err != nil {
		return err
	}

	return nil
}

func (data *Data) dumpTableData(t *table) error {
	// Use COPY TO STDOUT to dump table data, handling is simplified here
	query := fmt.Sprintf("COPY %s TO STDOUT WITH CSV HEADER", t.Name)
	_, err := data.tx.Exec(query)
	if err != nil {
		log.Printf("Error dumping table data for %s: %v", t.Name, err)
		return err
	}
	return nil
}
