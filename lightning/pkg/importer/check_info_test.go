// Copyright 2021 PingCAP, Inc.
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

package importer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

const passed precheck.CheckType = "pass"

func TestCheckCSVHeader(t *testing.T) {
	dir := t.TempDir()

	ctx := context.Background()
	mockStore, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	type tableSource struct {
		Name    string
		SQL     string
		Sources []string
	}

	cases := []struct {
		ignoreColumns []*config.IgnoreColumns
		// empty msg means check pass
		level   precheck.CheckType
		Sources map[string][]*tableSource
	}{

		{
			nil,

			passed,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"aa,b\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			passed,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"aa,b\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			precheck.Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"a,b\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			precheck.Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\n",
						},
					},
				},
			},
		},
		{
			nil,

			precheck.Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			precheck.Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// ignore primary key, should still be warn
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			precheck.Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// ignore primary key, but has other unique key
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			precheck.Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`), unique key uk (`b`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// ignore primary key, non other unique key
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			precheck.Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`), KEY idx_b (`b`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// non unique key, but data type inconsistent
		{
			nil,
			precheck.Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a bigint, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// non unique key, but ignore inconsistent field
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			precheck.Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a bigint, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// multiple tables, test the choose priority
		{
			nil,
			precheck.Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(8), b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
						},
					},
					{
						"tbl2",
						"create table tbl1 (a varchar(8) primary key, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		{
			nil,
			precheck.Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(8), b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
						},
					},
				},
				"db2": {
					{
						"tbl2",
						"create table tbl1 (a bigint, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
	}

	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize: config.ReadBlockSize,
			CSV: config.CSVConfig{
				Separator:   ",",
				Delimiter:   `"`,
				Header:      false,
				NotNull:     false,
				Null:        []string{`\N`},
				EscapedBy:   `\`,
				TrimLastSep: false,
			},
		},
	}

	ioWorkers := worker.NewPool(context.Background(), 1, "io")
	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:        cfg,
		srcStorage: mockStore,
		ioWorkers:  ioWorkers,
	}
	rc := &Controller{
		cfg:           cfg,
		store:         mockStore,
		ioWorkers:     ioWorkers,
		preInfoGetter: preInfoGetter,
	}

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()

	for _, ca := range cases {
		rc.checkTemplate = NewSimpleTemplate()
		cfg.Mydumper.IgnoreColumns = ca.ignoreColumns
		rc.dbInfos = make(map[string]*checkpoints.TidbDBInfo)

		dbMetas := make([]*mydump.MDDatabaseMeta, 0)
		for db, tbls := range ca.Sources {
			tblMetas := make([]*mydump.MDTableMeta, 0, len(tbls))
			dbInfo := &checkpoints.TidbDBInfo{
				Name:   db,
				Tables: make(map[string]*checkpoints.TidbTableInfo),
			}
			rc.dbInfos[db] = dbInfo

			for _, tbl := range tbls {
				node, err := p.ParseOneStmt(tbl.SQL, "", "")
				require.NoError(t, err)
				core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
				require.NoError(t, err)
				core.State = model.StatePublic
				dbInfo.Tables[tbl.Name] = &checkpoints.TidbTableInfo{
					ID:   core.ID,
					DB:   db,
					Name: tbl.Name,
					Core: core,
				}

				fileInfos := make([]mydump.FileInfo, 0, len(tbl.Sources))
				for i, s := range tbl.Sources {
					fileName := fmt.Sprintf("%s.%s.%d.csv", db, tbl.Name, i)
					err = os.WriteFile(filepath.Join(dir, fileName), []byte(s), 0o644)
					require.NoError(t, err)
					fileInfos = append(fileInfos, mydump.FileInfo{
						FileMeta: mydump.SourceFileMeta{
							Path:     fileName,
							Type:     mydump.SourceTypeCSV,
							FileSize: int64(len(s)),
						},
					})
				}
				tblMetas = append(tblMetas, &mydump.MDTableMeta{
					DB:        db,
					Name:      tbl.Name,
					DataFiles: fileInfos,
				})
			}
			dbMetas = append(dbMetas, &mydump.MDDatabaseMeta{
				Name:   db,
				Tables: tblMetas,
			})
		}

		rc.dbMetas = dbMetas
		rc.precheckItemBuilder = NewPrecheckItemBuilder(
			cfg,
			dbMetas,
			preInfoGetter,
			nil,
			nil,
		)
		preInfoGetter.dbInfosCache = rc.dbInfos
		err = rc.checkCSVHeader(ctx)
		require.NoError(t, err)
		if ca.level != passed {
			require.Equal(t, 1, rc.checkTemplate.FailedCount(ca.level))
		}
	}
}

func TestCheckTableEmpty(t *testing.T) {
	dir := t.TempDir()

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = false
	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "test1",
			Tables: []*mydump.MDTableMeta{
				{
					DB:   "test1",
					Name: "tbl1",
				},
				{
					DB:   "test1",
					Name: "tbl2",
				},
			},
		},
		{
			Name: "test2",
			Tables: []*mydump.MDTableMeta{
				{
					DB:   "test2",
					Name: "tbl1",
				},
			},
		},
	}

	targetInfoGetter := &TargetInfoGetterImpl{
		cfg: cfg,
	}
	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:              cfg,
		dbMetas:          dbMetas,
		targetInfoGetter: targetInfoGetter,
	}
	theCheckBuilder := NewPrecheckItemBuilder(
		cfg,
		dbMetas,
		preInfoGetter,
		nil,
		nil,
	)

	rc := &Controller{
		cfg:                 cfg,
		dbMetas:             dbMetas,
		checkpointsDB:       checkpoints.NewNullCheckpointsDB(),
		preInfoGetter:       preInfoGetter,
		precheckItemBuilder: theCheckBuilder,
	}

	ctx := context.Background()

	// test tidb will do nothing
	rc.cfg.TikvImporter.Backend = config.BackendTiDB
	err := rc.checkTableEmpty(ctx)
	require.NoError(t, err)

	// test parallel mode
	rc.cfg.TikvImporter.Backend = config.BackendLocal
	rc.cfg.TikvImporter.ParallelImport = true
	err = rc.checkTableEmpty(ctx)
	require.NoError(t, err)

	rc.cfg.TikvImporter.ParallelImport = false
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.MatchExpectationsInOrder(false)
	targetInfoGetter.db = db
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl2` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("SELECT 1 FROM `test2`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.checkTableEmpty(ctx)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	// single table contains data
	db, mock, err = sqlmock.New()
	require.NoError(t, err)
	targetInfoGetter.db = db
	mock.MatchExpectationsInOrder(false)
	// test auto retry retryable error
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnError(&gmysql.MySQLError{Number: errno.ErrPDServerTimeout})
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl2` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("SELECT 1 FROM `test2`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.checkTableEmpty(ctx)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	tmpl := rc.checkTemplate.(*SimpleTemplate)
	require.Equal(t, 1, len(tmpl.criticalMsgs))
	require.Equal(t, "table(s) [`test2`.`tbl1`] are not empty", tmpl.criticalMsgs[0])

	// multi tables contains data
	db, mock, err = sqlmock.New()
	require.NoError(t, err)
	targetInfoGetter.db = db
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl2` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("SELECT 1 FROM `test2`.`tbl1` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.checkTableEmpty(ctx)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	tmpl = rc.checkTemplate.(*SimpleTemplate)
	require.Equal(t, 1, len(tmpl.criticalMsgs))
	require.Equal(t, "table(s) [`test1`.`tbl1`, `test2`.`tbl1`] are not empty", tmpl.criticalMsgs[0])

	// init checkpoint with only two of the three tables
	dbInfos := map[string]*checkpoints.TidbDBInfo{
		"test1": {
			Name: "test1",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"tbl1": {
					Name: "tbl1",
				},
			},
		},
		"test2": {
			Name: "test2",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"tbl1": {
					Name: "tbl1",
				},
			},
		},
	}
	rc.cfg.Checkpoint.Enable = true
	rc.checkpointsDB, err = checkpoints.NewFileCheckpointsDB(ctx, filepath.Join(dir, "cp.pb"))
	require.NoError(t, err)
	err = rc.checkpointsDB.Initialize(ctx, cfg, dbInfos)
	require.NoError(t, err)
	rc.precheckItemBuilder.checkpointsDB = rc.checkpointsDB
	db, mock, err = sqlmock.New()
	require.NoError(t, err)
	targetInfoGetter.db = db
	// only need to check the one that is not in checkpoint
	mock.ExpectQuery("SELECT 1 FROM `test1`.`tbl2` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	err = rc.checkTableEmpty(ctx)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	err = failpoint.Enable("github.com/pingcap/tidb/lightning/pkg/importer/CheckTableEmptyFailed", `return`)
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/lightning/pkg/importer/CheckTableEmptyFailed")
	}()

	// restrict the concurrency to ensure there are more tables than workers
	rc.cfg.App.RegionConcurrency = 1
	// test check tables not stuck but return the right error
	err = rc.checkTableEmpty(ctx)
	require.Regexp(t, ".*check table contains data failed: mock error.*", err.Error())
}

func TestLocalResource(t *testing.T) {
	dir := t.TempDir()

	mockStore, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	err = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/common/GetStorageSize", "return(2048)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/common/GetStorageSize")
	}()

	cfg := config.NewConfig()
	cfg.Mydumper.SourceDir = dir
	cfg.TikvImporter.SortedKVDir = dir
	cfg.TikvImporter.Backend = "local"
	ioWorkers := worker.NewPool(context.Background(), 1, "io")
	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:        cfg,
		srcStorage: mockStore,
		ioWorkers:  ioWorkers,
	}
	theCheckBuilder := NewPrecheckItemBuilder(
		cfg,
		nil,
		preInfoGetter,
		nil,
		nil,
	)
	rc := &Controller{
		cfg:                 cfg,
		store:               mockStore,
		ioWorkers:           ioWorkers,
		preInfoGetter:       preInfoGetter,
		precheckItemBuilder: theCheckBuilder,
	}

	estimatedSizeResult := new(EstimateSourceDataSizeResult)
	preInfoGetter.estimatedSizeCache = estimatedSizeResult
	ctx := context.Background()
	// 1. source-size is smaller than disk-size, won't trigger error information
	rc.checkTemplate = NewSimpleTemplate()
	estimatedSizeResult.SizeWithIndex = 1000
	err = rc.localResource(ctx)
	require.NoError(t, err)
	tmpl := rc.checkTemplate.(*SimpleTemplate)
	require.Equal(t, 1, tmpl.warnFailedCount)
	require.Equal(t, 0, tmpl.criticalFailedCount)
	require.Equal(t, "local disk resources are rich, estimate sorted data size 1000B, local available is 2KiB", tmpl.normalMsgs[1])

	// 2. source-size is bigger than disk-size, with default disk-quota will trigger a critical error
	rc.checkTemplate = NewSimpleTemplate()
	estimatedSizeResult.SizeWithIndex = 4096
	err = rc.localResource(ctx)
	require.NoError(t, err)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	require.Equal(t, 1, tmpl.warnFailedCount)
	require.Equal(t, 1, tmpl.criticalFailedCount)
	require.Equal(t, "local disk space may not enough to finish import, estimate sorted data size is 4KiB, but local available is 2KiB, please set `tikv-importer.disk-quota` to a smaller value than 2KiB or change `mydumper.sorted-kv-dir` to another disk with enough space to finish imports", tmpl.criticalMsgs[0])

	// 3. source-size is bigger than disk-size, with a vaild disk-quota will trigger a warning
	rc.checkTemplate = NewSimpleTemplate()
	rc.cfg.TikvImporter.DiskQuota = config.ByteSize(1024)
	estimatedSizeResult.SizeWithIndex = 4096
	err = rc.localResource(ctx)
	require.NoError(t, err)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	require.Equal(t, 1, tmpl.warnFailedCount)
	require.Equal(t, 0, tmpl.criticalFailedCount)
	require.Equal(t, "local disk space may not enough to finish import, estimate sorted data size is 4KiB, but local available is 2KiB,we will use disk-quota (size: 1KiB) to finish imports, which may slow down import", tmpl.normalMsgs[1])
}
