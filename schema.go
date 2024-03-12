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

// getTables returns a slice of table names in the public schema.
func getTables(db *sql.DB) ([]string, error) {
    query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
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

// getCreateTableStatement generates the SQL for creating a table, including column definitions.
func getCreateTableStatement(db *sql.DB, tableName string) (string, error) {
    query := fmt.Sprintf("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '%s'", tableName)
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

    return fmt.Sprintf("CREATE TABLE %s (\n    %s\n);", tableName, strings.Join(columns, ",\n    ")), nil
}

// getTableDataCopyFormat generates the COPY command to import data for a table.
// This is a simplified version; handling all data types correctly is more complex.
func getTableDataCopyFormat(db *sql.DB, tableName string) (string, error) {
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
    values := make([]sql.RawBytes, len(columns))
    scanArgs := make([]interface{}, len(values))
    for i := range values {
        scanArgs[i] = &values[i]
    }

    var output strings.Builder
    output.WriteString(fmt.Sprintf("COPY %s (%s) FROM stdin;\n", tableName, strings.Join(columns, ", ")))
    for rows.Next() {
        err := rows.Scan(scanArgs...)
        if err != nil {
            return "", err
        }
        var valueStrings []string
        for _, value := range values {
            valueStrings = append(valueStrings, string(value))
        }
        output.WriteString(strings.Join(valueStrings, "\t") + "\n")
    }
    output.WriteString("\\.\n")

    return output.String(), nil
}
