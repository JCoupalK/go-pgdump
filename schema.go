package pgdump

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

func (d *Dumper) dumpSchema(db *sql.DB, filePath string) error {
	tables, err := getTables(db)
	if err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, table := range tables {
		createStmt, err := getCreateTableStatement(db, table)
		if err != nil {
			return err
		}
		file.WriteString(createStmt + "\n\n")
	}

	return nil
}

func getTables(db *sql.DB) ([]string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func getCreateTableStatement(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name='%s'", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName, dataType string
		var charMaxLength *int
		if err := rows.Scan(&columnName, &dataType, &charMaxLength); err != nil {
			return "", err
		}
		columnDef := fmt.Sprintf("%s %s", columnName, dataType)
		if charMaxLength != nil {
			columnDef += fmt.Sprintf("(%d)", *charMaxLength)
		}
		columns = append(columns, columnDef)
	}

	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n);", tableName, strings.Join(columns, ",\n  ")), nil
}
