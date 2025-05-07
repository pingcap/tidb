package cloud

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, content string) {
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
}

func TestCreateSchemas_Success(t *testing.T) {
	// prepare temp dir with schema files
	tmp, err := os.MkdirTemp("", "schema")
	require.NoError(t, err)
	defer os.RemoveAll(tmp)
	writeFile(t, tmp, "testdb-schema-create.sql",
		"CREATE DATABASE IF NOT EXISTS testdb;")
	writeFile(t, tmp, "testdb.testtable-schema.sql",
		"CREATE TABLE IF NOT EXISTS testdb.testtable (id INT);")
	writeFile(t, tmp, "testdb.testview-view.sql",
		"CREATE VIEW IF NOT EXISTS testdb.testview AS SELECT 1;")

	// mock DB
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// expect Exec of DB, in any order
	// match backticks around the database name; semicolon is optional
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `testdb`") + ";?").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS testdb.testtable (id INT);")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE VIEW IF NOT EXISTS testdb.testview AS SELECT 1;")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// invoke CreateSchemas
	path := "file://" + tmp
	err = CreateSchemas(context.Background(), path, db, WithConcurrency(1))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateSchemas_DBError(t *testing.T) {
	tmp, err := os.MkdirTemp("", "schema")
	require.NoError(t, err)
	defer os.RemoveAll(tmp)
	writeFile(t, tmp, "d1-schema-create.sql", "CREATE DATABASE IF NOT EXISTS d1;")

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// simulate failure on CREATE DATABASE
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS d1;")).
		WillReturnError(sql.ErrConnDone)

	path := "file://" + tmp
	err = CreateSchemas(context.Background(), path, db, WithConcurrency(1))
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
