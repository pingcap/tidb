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

package restore

import (
	"context"
	"database/sql"
	"path/filepath"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/mysql"

	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
)

var _ = Suite(&checkInfoSuite{})

type checkInfoSuite struct{}

func (s *checkInfoSuite) TestCheckTableEmpty(c *C) {
	dir := c.MkDir()
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

	rc := &Controller{
		cfg:           cfg,
		dbMetas:       dbMetas,
		checkpointsDB: checkpoints.NewNullCheckpointsDB(),
	}

	ctx := context.Background()

	// test tidb will do nothing
	rc.cfg.TikvImporter.Backend = config.BackendTiDB
	err := rc.checkTableEmpty(ctx)
	c.Assert(err, IsNil)

	// test incremental mode
	rc.cfg.TikvImporter.Backend = config.BackendLocal
	rc.cfg.TikvImporter.IncrementalImport = true
	err = rc.checkTableEmpty(ctx)
	c.Assert(err, IsNil)

	rc.cfg.TikvImporter.IncrementalImport = false
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mock.MatchExpectationsInOrder(false)
	rc.tidbGlue = glue.NewExternalTiDBGlue(db, mysql.ModeNone)
	mock.ExpectQuery("select 1 from `test1`.`tbl1` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("select 1 from `test1`.`tbl2` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("select 1 from `test2`.`tbl1` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	// not error, need not to init check template
	err = rc.checkTableEmpty(ctx)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// single table contains data
	db, mock, err = sqlmock.New()
	c.Assert(err, IsNil)
	rc.tidbGlue = glue.NewExternalTiDBGlue(db, mysql.ModeNone)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("select 1 from `test1`.`tbl1` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("select 1 from `test1`.`tbl2` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("select 1 from `test2`.`tbl1` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.checkTableEmpty(ctx)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	tmpl := rc.checkTemplate.(*SimpleTemplate)
	c.Assert(len(tmpl.criticalMsgs), Equals, 1)
	c.Assert(tmpl.criticalMsgs[0], Matches, "table\\(s\\) \\[`test2`.`tbl1`\\] are not empty")

	// multi tables contains data
	db, mock, err = sqlmock.New()
	c.Assert(err, IsNil)
	rc.tidbGlue = glue.NewExternalTiDBGlue(db, mysql.ModeNone)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("select 1 from `test1`.`tbl1` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
	mock.ExpectQuery("select 1 from `test1`.`tbl2` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	mock.ExpectQuery("select 1 from `test2`.`tbl1` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.checkTableEmpty(ctx)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	c.Assert(len(tmpl.criticalMsgs), Equals, 1)
	c.Assert(tmpl.criticalMsgs[0], Matches, "table\\(s\\) \\[`test1`.`tbl1`, `test2`.`tbl1`\\] are not empty")

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
	rc.checkpointsDB = checkpoints.NewFileCheckpointsDB(filepath.Join(dir, "cp.pb"))
	err = rc.checkpointsDB.Initialize(ctx, cfg, dbInfos)
	c.Check(err, IsNil)
	db, mock, err = sqlmock.New()
	c.Assert(err, IsNil)
	rc.tidbGlue = glue.NewExternalTiDBGlue(db, mysql.ModeNone)
	// only need to check the one that is not in checkpoint
	mock.ExpectQuery("select 1 from `test1`.`tbl2` limit 1").
		WillReturnRows(sqlmock.NewRows([]string{""}).RowError(0, sql.ErrNoRows))
	err = rc.checkTableEmpty(ctx)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *checkInfoSuite) TestLocalResource(c *C) {
	dir := c.MkDir()
	mockStore, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/common/GetStorageSize", "return(2048)")
	c.Assert(err, IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/common/GetStorageSize")
	}()

	cfg := config.NewConfig()
	cfg.Mydumper.SourceDir = dir
	cfg.TikvImporter.SortedKVDir = dir
	cfg.TikvImporter.Backend = "local"
	rc := &Controller{
		cfg:       cfg,
		store:     mockStore,
		ioWorkers: worker.NewPool(context.Background(), 1, "io"),
	}

	// 1. source-size is smaller than disk-size, won't trigger error information
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.localResource(1000)
	c.Assert(err, IsNil)
	tmpl := rc.checkTemplate.(*SimpleTemplate)
	c.Assert(tmpl.warnFailedCount, Equals, 1)
	c.Assert(tmpl.criticalFailedCount, Equals, 0)
	c.Assert(tmpl.normalMsgs[1], Matches, "local disk resources are rich, estimate sorted data size 1000B, local available is 2KiB")

	// 2. source-size is bigger than disk-size, with default disk-quota will trigger a critical error
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.localResource(4096)
	c.Assert(err, IsNil)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	c.Assert(tmpl.warnFailedCount, Equals, 1)
	c.Assert(tmpl.criticalFailedCount, Equals, 1)
	c.Assert(tmpl.criticalMsgs[0], Matches, "local disk space may not enough to finish import, estimate sorted data size is 4KiB, but local available is 2KiB, please set `tikv-importer.disk-quota` to a smaller value than 2KiB or change `mydumper.sorted-kv-dir` to another disk with enough space to finish imports")

	// 3. source-size is bigger than disk-size, with a vaild disk-quota will trigger a warning
	rc.checkTemplate = NewSimpleTemplate()
	rc.cfg.TikvImporter.DiskQuota = config.ByteSize(1024)
	err = rc.localResource(4096)
	c.Assert(err, IsNil)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	c.Assert(tmpl.warnFailedCount, Equals, 1)
	c.Assert(tmpl.criticalFailedCount, Equals, 0)
	c.Assert(tmpl.normalMsgs[1], Matches, "local disk space may not enough to finish import, estimate sorted data size is 4KiB, but local available is 2KiB,we will use disk-quota \\(size: 1KiB\\) to finish imports, which may slow down import")
}
