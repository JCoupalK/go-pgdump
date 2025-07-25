package pgdump

import (
	"database/sql"
	"io"
	"text/template"
)

type DumpInfo struct {
	DumpVersion   string
	ServerVersion string
	CompleteTime  string
	ThreadsNumber int
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

func writeHeader(file io.Writer, info DumpInfo) error {
	const headerTemplate = `-- Go PostgreSQL Dump v{{ .DumpVersion }}
--
-- Server version:
--	 {{ .ServerVersion }}
-- Threads Used:
--   {{ .ThreadsNumber }}

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;
`
	tmpl, err := template.New("header").Parse(headerTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(file, info)
}

func writeFooter(file io.Writer, info DumpInfo) error {
	const footerTemplate = `--
-- Dump completed on {{ .CompleteTime }}
--`
	tmpl, err := template.New("footer").Parse(footerTemplate)
	if err != nil {
		return err
	}
	return tmpl.Execute(file, info)
}
