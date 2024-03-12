// pgdump.go

package pgdump

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Register initializes a new database dump operation, preparing the output file and other resources.
func Register(db *sql.DB, dumpDir, filenameFormat string) (*Data, error) {
	// Ensure the dump directory exists
	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("specified dump directory does not exist: %s", dumpDir)
	}

	// Format the filename with the current time to ensure uniqueness
	filename := fmt.Sprintf("%s-%s.sql", filenameFormat, time.Now().Format("20060102-150405"))
	filePath := filepath.Join(dumpDir, filename)

	// Create the output file
	outFile, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file: %v", err)
	}

	return &Data{
		Out:        outFile,
		Connection: db,
		DumpDir:    dumpDir,
	}, nil
}
