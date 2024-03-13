package pgdump

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

func (d *Dumper) dumpData(db *sql.DB, filePath string) error {
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
		dataStmt, err := getTableData(db, table)
		if err != nil {
			return err
		}
		file.WriteString(dataStmt + "\n\n")
	}

	return nil
}

func getTableData(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var insertStatements []string
	for rows.Next() {
		rows.Scan(valuePtrs...)
		var valueStrings []string
		for _, value := range values {
			switch v := value.(type) {
			case nil:
				valueStrings = append(valueStrings, "NULL")
			case []byte:
				valueStrings = append(valueStrings, fmt.Sprintf("'%s'", string(v)))
			default:
				valueStrings = append(valueStrings, fmt.Sprintf("'%v'", v))
			}
		}
		insertStatements = append(insertStatements, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, strings.Join(columns, ", "), strings.Join(valueStrings, ", ")))
	}

	return strings.Join(insertStatements, "\n"), nil
}

func getServerVersion(db *sql.DB) string {
	var version string
	query := "SELECT version();"
	row := db.QueryRow(query)
	if err := row.Scan(&version); err != nil {
		return "Unknown"
	}
	return version
}
