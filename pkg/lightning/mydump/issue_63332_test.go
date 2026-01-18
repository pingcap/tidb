package mydump

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIssue63332(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx := context.Background()
	store, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	logger := log.Logger{Logger: zap.NewExample()}
	importer := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 1)

	dbName := "testdb"
	viewName := "v"

	// The view schema from file.
	createViewSQL := "CREATE VIEW v AS SELECT * FROM t;"
	err = store.WriteFile(ctx, "v-schema-view.sql", []byte(createViewSQL))
	require.NoError(t, err)

	dbMeta := &MDDatabaseMeta{
		Name: dbName,
		Views: []*MDTableMeta{
			{
				DB:   dbName,
				Name: viewName,
				charSet: "utf8mb4", // Set charset to avoid decode errors
				SchemaFile: FileInfo{
					FileMeta: SourceFileMeta{
						Path: "v-schema-view.sql",
					},
				},
			},
		},
	}

	// Mock existing views check (returns empty)
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = .*").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}))

	// Expect USE statement before CREATE VIEW
	mock.ExpectExec("USE .*").WillReturnResult(sqlmock.NewResult(0, 0))

	// Expect CREATE VIEW statement
	mock.ExpectExec("CREATE.*VIEW.*`testdb`.*`v`.*").WillReturnResult(sqlmock.NewResult(0, 0))

	err = importer.importViews(ctx, []*MDDatabaseMeta{dbMeta})
	require.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
