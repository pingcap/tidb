// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/parser"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestParseViewSchemaSQL(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "test", Name: "v2"}
	sqlStr := `
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS v2;
DROP VIEW IF EXISTS v2;
SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;
SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;
SET character_set_client = utf8mb4;
SET character_set_results = utf8mb4;
SET collation_connection = utf8mb4_0900_ai_ci;
CREATE ALGORITHM=UNDEFINED DEFINER=` + "`root`@`%`" + ` SQL SECURITY DEFINER VIEW v2 (` + "`id`" + `) AS
	SELECT ` + "`id`" + ` FROM ` + "`test`.`v1`" + `;
SET character_set_client = @PREV_CHARACTER_SET_CLIENT;
SET character_set_results = @PREV_CHARACTER_SET_RESULTS;
SET collation_connection = @PREV_COLLATION_CONNECTION;
`

	parsed, err := parseViewSchemaSQL(p, currentView, sqlStr)
	require.NoError(t, err)
	require.Equal(t, currentView, parsed.key)
	require.Equal(t, []filter.Table{{Schema: "test", Name: "v1"}}, parsed.deps)
	require.NotContains(t, parsed.createSQL, "DROP TABLE")
	require.NotContains(t, parsed.createSQL, "DROP VIEW")
	require.Contains(t, parsed.createSQL, "SET NAMES 'binary'")
}

func TestParseViewSchemaSQLIgnoresCTEDependenciesInSetOperatorRoot(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "test", Name: "v_set_root_cte"}
	sqlStr := `
CREATE VIEW v_set_root_cte AS
WITH cte AS (
	SELECT id FROM t1
)
SELECT cte.id FROM cte
UNION
SELECT id FROM t2;
`

	parsed, err := parseViewSchemaSQL(p, currentView, sqlStr)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]filter.Table{
			{Schema: "test", Name: "t1"},
			{Schema: "test", Name: "t2"},
		},
		parsed.deps,
	)
}

func TestBuildViewImportPlanSupportsCrossSchemaDependencies(t *testing.T) {
	db1v1 := filter.Table{Schema: "db1", Name: "v1"}
	db2v2 := filter.Table{Schema: "db2", Name: "v2"}
	db2v3 := filter.Table{Schema: "db2", Name: "v3"}
	dumpTables := make(tableNameSet)
	dumpTables.add(filter.Table{Schema: "db1", Name: "t1"})
	dumpTables.add(filter.Table{Schema: "db2", Name: "t2"})
	dumpTables.add(filter.Table{Schema: "db2", Name: "t3"})

	plan, err := buildViewImportPlan([]*parsedViewSchema{
		{
			key:       db1v1,
			deps:      []filter.Table{{Schema: "db1", Name: "t1"}, {Schema: "db2", Name: "t2"}},
			createSQL: "CREATE VIEW `db1`.`v1` AS SELECT * FROM `db1`.`t1` JOIN `db2`.`t2`;",
		},
		{
			key:       db2v3,
			deps:      []filter.Table{{Schema: "db2", Name: "t3"}},
			createSQL: "CREATE VIEW `db2`.`v3` AS SELECT * FROM `db2`.`t3`;",
		},
		{
			key:       db2v2,
			deps:      []filter.Table{{Schema: "db1", Name: "v1"}, {Schema: "db2", Name: "v3"}},
			createSQL: "CREATE VIEW `db2`.`v2` AS SELECT * FROM `db1`.`v1` JOIN `db2`.`v3`;",
		},
	}, dumpTables)
	require.NoError(t, err)
	require.Len(t, plan.ordered, 3)
	require.Equal(t, []filter.Table{db1v1, db2v3, db2v2}, []filter.Table{
		plan.ordered[0].key,
		plan.ordered[1].key,
		plan.ordered[2].key,
	})
}

func TestRestoreSchemaWorkerEnqueueViewJobs(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	store, err := storage.NewLocalStorage(tempDir)
	require.NoError(t, err)

	view1File := "db1.v1-schema-view.sql"
	view2File := "db2.v2-schema-view.sql"
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, view1File), []byte("CREATE VIEW v1 AS SELECT id FROM t1;"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, view2File), []byte("CREATE VIEW v2 AS SELECT id FROM db1.v1;"), 0o644))

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name:   "db1",
			Tables: []*mydump.MDTableMeta{{DB: "db1", Name: "t1"}},
			Views: []*mydump.MDTableMeta{func() *mydump.MDTableMeta {
				viewMeta := mydump.NewMDTableMeta("auto")
				viewMeta.DB = "db1"
				viewMeta.Name = "v1"
				viewMeta.SchemaFile = mydump.FileInfo{FileMeta: mydump.SourceFileMeta{Path: view1File}}
				return viewMeta
			}()},
		},
		{
			Name: "db2",
			Views: []*mydump.MDTableMeta{func() *mydump.MDTableMeta {
				viewMeta := mydump.NewMDTableMeta("auto")
				viewMeta.DB = "db2"
				viewMeta.Name = "v2"
				viewMeta.SchemaFile = mydump.FileInfo{FileMeta: mydump.SourceFileMeta{Path: view2File}}
				return viewMeta
			}()},
		},
	}

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})

	mock.ExpectQuery("^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = \\?$").
		WithArgs("db1").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE"}).AddRow("t1", "BASE TABLE"))
	mock.ExpectQuery("^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = \\?$").
		WithArgs("db2").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE"}))
	mock.ExpectExec("VIEW `db1`.`v1` AS SELECT `id` FROM `t1`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("VIEW `db2`.`v2` AS SELECT `id` FROM `db1`.`v1`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	sqlMode, err := tmysql.GetSQLMode(tmysql.DefaultSQLMode)
	require.NoError(t, err)
	workerCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	worker := restoreSchemaWorker{
		ctx:    workerCtx,
		quit:   cancel,
		logger: log.FromContext(ctx),
		jobCh:  make(chan *schemaJob, 1),
		errCh:  make(chan error),
		glue:   glue.NewExternalTiDBGlue(db, sqlMode),
		store:  store,
	}
	go worker.doJob()
	t.Cleanup(func() {
		close(worker.jobCh)
	})

	require.NoError(t, worker.enqueueViewJobs(dbMetas))
}
