// Copyright 2019 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/go-units"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/importer"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/noop"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/lightning/web"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/table/tables"
	tmock "github.com/pingcap/tidb/util/mock"
	"github.com/tikv/pd/server/api"
)

var _ = Suite(&restoreSuite{})

type restoreSuite struct{}

func (s *restoreSuite) TestNewTableRestore(c *C) {
	testCases := []struct {
		name       string
		createStmt string
	}{
		{"t1", "CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)"},
		// {"t2", "CREATE TABLE `t2` (`c1` varchar(30000) NOT NULL)"}, // no longer able to create this kind of table.
		{"t3", "CREATE TABLE `t3-a` (`c1-a` varchar(5) NOT NULL)"},
	}

	p := parser.New()
	se := tmock.NewContext()

	dbInfo := &checkpoints.TidbDBInfo{Name: "mockdb", Tables: map[string]*checkpoints.TidbTableInfo{}}
	for i, tc := range testCases {
		node, err := p.ParseOneStmt(tc.createStmt, "utf8mb4", "utf8mb4_bin")
		c.Assert(err, IsNil)
		tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(i+1))
		c.Assert(err, IsNil)
		tableInfo.State = model.StatePublic

		dbInfo.Tables[tc.name] = &checkpoints.TidbTableInfo{
			Name: tc.name,
			Core: tableInfo,
		}
	}

	for _, tc := range testCases {
		tableInfo := dbInfo.Tables[tc.name]
		tableName := common.UniqueTable("mockdb", tableInfo.Name)
		tr, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &checkpoints.TableCheckpoint{}, nil)
		c.Assert(tr, NotNil)
		c.Assert(err, IsNil)
	}
}

func (s *restoreSuite) TestNewTableRestoreFailure(c *C) {
	tableInfo := &checkpoints.TidbTableInfo{
		Name: "failure",
		Core: &model.TableInfo{},
	}
	dbInfo := &checkpoints.TidbDBInfo{Name: "mockdb", Tables: map[string]*checkpoints.TidbTableInfo{
		"failure": tableInfo,
	}}
	tableName := common.UniqueTable("mockdb", "failure")

	_, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &checkpoints.TableCheckpoint{}, nil)
	c.Assert(err, ErrorMatches, `failed to tables\.TableFromMeta.*`)
}

func (s *restoreSuite) TestErrorSummaries(c *C) {
	logger, buffer := log.MakeTestLogger()

	es := makeErrorSummaries(logger)
	es.record("first", errors.New("a1 error"), checkpoints.CheckpointStatusAnalyzed)
	es.record("second", errors.New("b2 error"), checkpoints.CheckpointStatusAllWritten)
	es.emitLog()

	lines := buffer.Lines()
	sort.Strings(lines[1:])
	c.Assert(lines, DeepEquals, []string{
		`{"$lvl":"ERROR","$msg":"tables failed to be imported","count":2}`,
		`{"$lvl":"ERROR","$msg":"-","table":"first","status":"analyzed","error":"a1 error"}`,
		`{"$lvl":"ERROR","$msg":"-","table":"second","status":"written","error":"b2 error"}`,
	})
}

func (s *restoreSuite) TestVerifyCheckpoint(c *C) {
	dir := c.MkDir()
	cpdb := checkpoints.NewFileCheckpointsDB(filepath.Join(dir, "cp.pb"))
	defer cpdb.Close()
	ctx := context.Background()

	actualReleaseVersion := build.ReleaseVersion
	defer func() {
		build.ReleaseVersion = actualReleaseVersion
	}()

	taskCp, err := cpdb.TaskCheckpoint(ctx)
	c.Assert(err, IsNil)
	c.Assert(taskCp, IsNil)

	newCfg := func() *config.Config {
		cfg := config.NewConfig()
		cfg.Mydumper.SourceDir = "/data"
		cfg.TaskID = 123
		cfg.TiDB.Port = 4000
		cfg.TiDB.PdAddr = "127.0.0.1:2379"
		cfg.TikvImporter.Backend = config.BackendImporter
		cfg.TikvImporter.Addr = "127.0.0.1:8287"
		cfg.TikvImporter.SortedKVDir = "/tmp/sorted-kv"

		return cfg
	}

	err = cpdb.Initialize(ctx, newCfg(), map[string]*checkpoints.TidbDBInfo{})
	c.Assert(err, IsNil)

	adjustFuncs := map[string]func(cfg *config.Config){
		"tikv-importer.backend": func(cfg *config.Config) {
			cfg.TikvImporter.Backend = config.BackendLocal
		},
		"tikv-importer.addr": func(cfg *config.Config) {
			cfg.TikvImporter.Addr = "128.0.0.1:8287"
		},
		"mydumper.data-source-dir": func(cfg *config.Config) {
			cfg.Mydumper.SourceDir = "/tmp/test"
		},
		"tidb.host": func(cfg *config.Config) {
			cfg.TiDB.Host = "192.168.0.1"
		},
		"tidb.port": func(cfg *config.Config) {
			cfg.TiDB.Port = 5000
		},
		"tidb.pd-addr": func(cfg *config.Config) {
			cfg.TiDB.PdAddr = "127.0.0.1:3379"
		},
		"version": func(cfg *config.Config) {
			build.ReleaseVersion = "some newer version"
		},
	}

	// default mode, will return error
	taskCp, err = cpdb.TaskCheckpoint(ctx)
	c.Assert(err, IsNil)
	for conf, fn := range adjustFuncs {
		cfg := newCfg()
		fn(cfg)
		err := verifyCheckpoint(cfg, taskCp)
		if conf == "version" {
			build.ReleaseVersion = actualReleaseVersion
			c.Assert(err, ErrorMatches, "lightning version is 'some newer version', but checkpoint was created at '"+actualReleaseVersion+"'.*")
		} else {
			c.Assert(err, ErrorMatches, fmt.Sprintf("config '%s' value '.*' different from checkpoint value .*", conf))
		}
	}

	for conf, fn := range adjustFuncs {
		if conf == "tikv-importer.backend" {
			continue
		}
		cfg := newCfg()
		cfg.App.CheckRequirements = false
		fn(cfg)
		err := cpdb.Initialize(context.Background(), cfg, map[string]*checkpoints.TidbDBInfo{})
		c.Assert(err, IsNil)
	}
}

func (s *restoreSuite) TestDiskQuotaLock(c *C) {
	lock := newDiskQuotaLock()

	lock.Lock()
	c.Assert(lock.TryRLock(), IsFalse)
	lock.Unlock()
	c.Assert(lock.TryRLock(), IsTrue)
	c.Assert(lock.TryRLock(), IsTrue)

	rLocked := 2
	lockHeld := make(chan struct{})
	go func() {
		lock.Lock()
		lockHeld <- struct{}{}
	}()
	for lock.TryRLock() {
		rLocked++
		time.Sleep(time.Millisecond)
	}
	select {
	case <-lockHeld:
		c.Fatal("write lock is held before all read locks are released")
	case <-time.NewTimer(10 * time.Millisecond).C:
	}
	for ; rLocked > 0; rLocked-- {
		lock.RUnlock()
	}
	<-lockHeld
	lock.Unlock()

	done := make(chan struct{})
	count := int32(0)
	reader := func() {
		for i := 0; i < 1000; i++ {
			if lock.TryRLock() {
				n := atomic.AddInt32(&count, 1)
				if n < 1 || n >= 10000 {
					lock.RUnlock()
					panic(fmt.Sprintf("unexpected count(%d)", n))
				}
				for i := 0; i < 100; i++ {
				}
				atomic.AddInt32(&count, -1)
				lock.RUnlock()
			}
			time.Sleep(time.Microsecond)
		}
		done <- struct{}{}
	}
	writer := func() {
		for i := 0; i < 1000; i++ {
			lock.Lock()
			n := atomic.AddInt32(&count, 10000)
			if n != 10000 {
				lock.RUnlock()
				panic(fmt.Sprintf("unexpected count(%d)", n))
			}
			for i := 0; i < 100; i++ {
			}
			atomic.AddInt32(&count, -10000)
			lock.Unlock()
			time.Sleep(time.Microsecond)
		}
		done <- struct{}{}
	}
	for i := 0; i < 5; i++ {
		go reader()
	}
	for i := 0; i < 2; i++ {
		go writer()
	}
	for i := 0; i < 5; i++ {
		go reader()
	}
	for i := 0; i < 12; i++ {
		<-done
	}
}

// failMetaMgrBuilder mocks meta manager init failure
type failMetaMgrBuilder struct {
	metaMgrBuilder
}

func (b failMetaMgrBuilder) Init(context.Context) error {
	return errors.New("mock init meta failure")
}

type panicCheckpointDB struct {
	checkpoints.DB
}

func (cp panicCheckpointDB) Initialize(context.Context, *config.Config, map[string]*checkpoints.TidbDBInfo) error {
	panic("should not reach here")
}

func (s *restoreSuite) TestPreCheckFailed(c *C) {
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendTiDB
	cfg.App.CheckRequirements = false

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	g := glue.NewExternalTiDBGlue(db, mysql.ModeNone)

	ctl := &Controller{
		cfg:            cfg,
		saveCpCh:       make(chan saveCp),
		checkpointsDB:  panicCheckpointDB{},
		metaMgrBuilder: failMetaMgrBuilder{},
		checkTemplate:  NewSimpleTemplate(),
		tidbGlue:       g,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN .*").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2"))
	mock.ExpectCommit()
	// precheck failed, will not do init checkpoint.
	err = ctl.Run(context.Background())
	c.Assert(err, ErrorMatches, ".*mock init meta failure")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	mock.ExpectBegin()
	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN .*").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2"))
	mock.ExpectCommit()
	ctl.saveCpCh = make(chan saveCp)
	// precheck failed, will not do init checkpoint.
	err1 := ctl.Run(context.Background())
	c.Assert(err1.Error(), Equals, err.Error())
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

var _ = Suite(&tableRestoreSuite{})

type tableRestoreSuiteBase struct {
	tr  *TableRestore
	cfg *config.Config

	tableInfo *checkpoints.TidbTableInfo
	dbInfo    *checkpoints.TidbDBInfo
	tableMeta *mydump.MDTableMeta

	store storage.ExternalStorage
}

type tableRestoreSuite struct {
	tableRestoreSuiteBase
}

func (s *tableRestoreSuiteBase) SetUpSuite(c *C) {
	// Produce a mock table info

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()
	node, err := p.ParseOneStmt(`
		CREATE TABLE "table" (
			a INT,
			b INT,
			c INT,
			KEY (b)
		)
	`, "", "")
	c.Assert(err, IsNil)
	core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
	c.Assert(err, IsNil)
	core.State = model.StatePublic

	s.tableInfo = &checkpoints.TidbTableInfo{Name: "table", DB: "db", Core: core}
	s.dbInfo = &checkpoints.TidbDBInfo{
		Name:   "db",
		Tables: map[string]*checkpoints.TidbTableInfo{"table": s.tableInfo},
	}

	// Write some sample SQL dump

	fakeDataDir := c.MkDir()

	store, err := storage.NewLocalStorage(fakeDataDir)
	c.Assert(err, IsNil)
	s.store = store

	fakeDataFilesCount := 6
	fakeDataFilesContent := []byte("INSERT INTO `table` VALUES (1, 2, 3);")
	c.Assert(len(fakeDataFilesContent), Equals, 37)
	fakeDataFiles := make([]mydump.FileInfo, 0, fakeDataFilesCount)
	for i := 1; i <= fakeDataFilesCount; i++ {
		fakeFileName := fmt.Sprintf("db.table.%d.sql", i)
		fakeDataPath := filepath.Join(fakeDataDir, fakeFileName)
		err = os.WriteFile(fakeDataPath, fakeDataFilesContent, 0o644)
		c.Assert(err, IsNil)
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta: mydump.SourceFileMeta{
				Path:     fakeFileName,
				Type:     mydump.SourceTypeSQL,
				SortKey:  fmt.Sprintf("%d", i),
				FileSize: 37,
			},
		})
	}

	fakeCsvContent := []byte("1,2,3\r\n4,5,6\r\n")
	csvName := "db.table.99.csv"
	err = os.WriteFile(filepath.Join(fakeDataDir, csvName), fakeCsvContent, 0o644)
	c.Assert(err, IsNil)
	fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
		TableName: filter.Table{Schema: "db", Name: "table"},
		FileMeta: mydump.SourceFileMeta{
			Path:     csvName,
			Type:     mydump.SourceTypeCSV,
			SortKey:  "99",
			FileSize: 14,
		},
	})

	s.tableMeta = &mydump.MDTableMeta{
		DB:        "db",
		Name:      "table",
		TotalSize: 222,
		SchemaFile: mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta: mydump.SourceFileMeta{
				Path: "db.table-schema.sql",
				Type: mydump.SourceTypeTableSchema,
			},
		},
		DataFiles: fakeDataFiles,
	}
}

func (s *tableRestoreSuiteBase) SetUpTest(c *C) {
	// Collect into the test TableRestore structure
	var err error
	s.tr, err = NewTableRestore("`db`.`table`", s.tableMeta, s.dbInfo, s.tableInfo, &checkpoints.TableCheckpoint{}, nil)
	c.Assert(err, IsNil)

	s.cfg = config.NewConfig()
	s.cfg.Mydumper.BatchSize = 111
	s.cfg.App.TableConcurrency = 2
}

func (s *tableRestoreSuite) TestPopulateChunks(c *C) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/restore/PopulateChunkTimestamp", "return(1234567897)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/restore/PopulateChunkTimestamp")
	}()

	cp := &checkpoints.TableCheckpoint{
		Engines: make(map[int32]*checkpoints.EngineCheckpoint),
	}

	rc := &Controller{cfg: s.cfg, ioWorkers: worker.NewPool(context.Background(), 1, "io"), store: s.store}
	err := s.tr.populateChunks(context.Background(), rc, cp)
	c.Assert(err, IsNil)
	//nolint:dupl // false positive.
	c.Assert(cp.Engines, DeepEquals, map[int32]*checkpoints.EngineCheckpoint{
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
		},
		0: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[0].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[0].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 0,
						RowIDMax:     7, // 37 bytes with 3 columns can store at most 7 rows.
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[1].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 7,
						RowIDMax:     14,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[2].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[2].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 14,
						RowIDMax:     21,
					},
					Timestamp: 1234567897,
				},
			},
		},
		1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[3].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[3].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 21,
						RowIDMax:     28,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[4].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[4].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 28,
						RowIDMax:     35,
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[5].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[5].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    37,
						PrevRowIDMax: 35,
						RowIDMax:     42,
					},
					Timestamp: 1234567897,
				},
			},
		},
		2: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[6].FileMeta.Path, Offset: 0},
					FileMeta: s.tr.tableMeta.DataFiles[6].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    14,
						PrevRowIDMax: 42,
						RowIDMax:     46,
					},
					Timestamp: 1234567897,
				},
			},
		},
	})

	// set csv header to true, this will cause check columns fail
	s.cfg.Mydumper.CSV.Header = true
	s.cfg.Mydumper.StrictFormat = true
	regionSize := s.cfg.Mydumper.MaxRegionSize
	s.cfg.Mydumper.MaxRegionSize = 5
	err = s.tr.populateChunks(context.Background(), rc, cp)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `.*unknown columns in header \[1 2 3\]`)
	s.cfg.Mydumper.MaxRegionSize = regionSize
	s.cfg.Mydumper.CSV.Header = false
}

type errorLocalWriter struct{}

func (w errorLocalWriter) AppendRows(context.Context, string, []string, kv.Rows) error {
	return errors.New("mock write rows failed")
}

func (w errorLocalWriter) IsSynced() bool {
	return true
}

func (w errorLocalWriter) Close(context.Context) (backend.ChunkFlushStatus, error) {
	return nil, nil
}

func (s *tableRestoreSuite) TestRestoreEngineFailed(c *C) {
	ctx := context.Background()
	ctrl := gomock.NewController(c)
	mockBackend := mock.NewMockBackend(ctrl)
	rc := &Controller{
		cfg:            s.cfg,
		pauser:         DeliverPauser,
		ioWorkers:      worker.NewPool(ctx, 1, "io"),
		regionWorkers:  worker.NewPool(ctx, 10, "region"),
		store:          s.store,
		backend:        backend.MakeBackend(mockBackend),
		errorSummaries: makeErrorSummaries(log.L()),
		saveCpCh:       make(chan saveCp, 1),
		diskQuotaLock:  newDiskQuotaLock(),
	}
	defer close(rc.saveCpCh)
	go func() {
		for cp := range rc.saveCpCh {
			cp.waitCh <- nil
		}
	}()

	cp := &checkpoints.TableCheckpoint{
		Engines: make(map[int32]*checkpoints.EngineCheckpoint),
	}
	err := s.tr.populateChunks(ctx, rc, cp)
	c.Assert(err, IsNil)

	tbl, err := tables.TableFromMeta(kv.NewPanickingAllocators(0), s.tableInfo.Core)
	c.Assert(err, IsNil)
	_, indexUUID := backend.MakeUUID("`db`.`table`", -1)
	_, dataUUID := backend.MakeUUID("`db`.`table`", 0)
	realBackend := tidb.NewTiDBBackend(nil, "replace", nil)
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().CloseEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockBackend.EXPECT().NewEncoder(gomock.Any(), gomock.Any()).
		Return(realBackend.NewEncoder(tbl, &kv.SessionOptions{})).
		AnyTimes()
	mockBackend.EXPECT().MakeEmptyRows().Return(realBackend.MakeEmptyRows()).AnyTimes()
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), dataUUID).Return(noop.Writer{}, nil)
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), indexUUID).
		Return(nil, errors.New("mock open index local writer failed"))
	openedIdxEngine, err := rc.backend.OpenEngine(ctx, nil, "`db`.`table`", -1)
	c.Assert(err, IsNil)

	// open the first engine meet error, should directly return the error
	_, err = s.tr.restoreEngine(ctx, rc, openedIdxEngine, 0, cp.Engines[0])
	c.Assert(err, ErrorMatches, "mock open index local writer failed")

	localWriter := func(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
		time.Sleep(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			return nil, errors.New("mock open index local writer failed after ctx.Done")
		default:
			return noop.Writer{}, nil
		}
	}
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), dataUUID).Return(errorLocalWriter{}, nil).AnyTimes()
	mockBackend.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), indexUUID).
		DoAndReturn(localWriter).AnyTimes()

	openedIdxEngine, err = rc.backend.OpenEngine(ctx, nil, "`db`.`table`", -1)
	c.Assert(err, IsNil)

	// open engine failed after write rows failed, should return write rows error
	_, err = s.tr.restoreEngine(ctx, rc, openedIdxEngine, 0, cp.Engines[0])
	c.Assert(err, ErrorMatches, "mock write rows failed")
}

func (s *tableRestoreSuite) TestPopulateChunksCSVHeader(c *C) {
	fakeDataDir := c.MkDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	c.Assert(err, IsNil)

	fakeDataFiles := make([]mydump.FileInfo, 0)

	fakeCsvContents := []string{
		// small full header
		"a,b,c\r\n1,2,3\r\n",
		// small partial header
		"b,c\r\n2,3\r\n",
		// big full header
		"a,b,c\r\n90000,80000,700000\r\n1000,2000,3000\r\n11,22,33\r\n3,4,5\r\n",
		// big full header unordered
		"c,a,b\r\n,1000,2000,3000\r\n11,22,33\r\n1000,2000,404\r\n3,4,5\r\n90000,80000,700000\r\n7999999,89999999,9999999\r\n",
		// big partial header
		"b,c\r\n2000001,30000001\r\n35231616,462424626\r\n62432,434898934\r\n",
	}
	total := 0
	for i, s := range fakeCsvContents {
		csvName := fmt.Sprintf("db.table.%02d.csv", i)
		err := os.WriteFile(filepath.Join(fakeDataDir, csvName), []byte(s), 0o644)
		c.Assert(err, IsNil)
		fakeDataFiles = append(fakeDataFiles, mydump.FileInfo{
			TableName: filter.Table{Schema: "db", Name: "table"},
			FileMeta:  mydump.SourceFileMeta{Path: csvName, Type: mydump.SourceTypeCSV, SortKey: fmt.Sprintf("%02d", i), FileSize: int64(len(s))},
		})
		total += len(s)
	}
	tableMeta := &mydump.MDTableMeta{
		DB:         "db",
		Name:       "table",
		TotalSize:  int64(total),
		SchemaFile: mydump.FileInfo{TableName: filter.Table{Schema: "db", Name: "table"}, FileMeta: mydump.SourceFileMeta{Path: "db.table-schema.sql", Type: mydump.SourceTypeTableSchema}},
		DataFiles:  fakeDataFiles,
	}

	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/restore/PopulateChunkTimestamp", "return(1234567897)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/restore/PopulateChunkTimestamp")
	}()

	cp := &checkpoints.TableCheckpoint{
		Engines: make(map[int32]*checkpoints.EngineCheckpoint),
	}

	cfg := config.NewConfig()
	cfg.Mydumper.BatchSize = 100
	cfg.Mydumper.MaxRegionSize = 40

	cfg.Mydumper.CSV.Header = true
	cfg.Mydumper.StrictFormat = true
	rc := &Controller{cfg: cfg, ioWorkers: worker.NewPool(context.Background(), 1, "io"), store: store}

	tr, err := NewTableRestore("`db`.`table`", tableMeta, s.dbInfo, s.tableInfo, &checkpoints.TableCheckpoint{}, nil)
	c.Assert(err, IsNil)
	c.Assert(tr.populateChunks(context.Background(), rc, cp), IsNil)

	c.Assert(cp.Engines, DeepEquals, map[int32]*checkpoints.EngineCheckpoint{
		-1: {
			Status: checkpoints.CheckpointStatusLoaded,
		},
		0: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[0].FileMeta.Path, Offset: 0},
					FileMeta: tableMeta.DataFiles[0].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    14,
						PrevRowIDMax: 0,
						RowIDMax:     4, // 37 bytes with 3 columns can store at most 7 rows.
					},
					Timestamp: 1234567897,
				},
				{
					Key:      checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
					FileMeta: tableMeta.DataFiles[1].FileMeta,
					Chunk: mydump.Chunk{
						Offset:       0,
						EndOffset:    10,
						PrevRowIDMax: 4,
						RowIDMax:     7,
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[2].FileMeta.Path, Offset: 6},
					FileMeta:          tableMeta.DataFiles[2].FileMeta,
					ColumnPermutation: []int{0, 1, 2, -1},
					Chunk: mydump.Chunk{
						Offset:       6,
						EndOffset:    52,
						PrevRowIDMax: 7,
						RowIDMax:     20,
						Columns:      []string{"a", "b", "c"},
					},

					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[2].FileMeta.Path, Offset: 52},
					FileMeta:          tableMeta.DataFiles[2].FileMeta,
					ColumnPermutation: []int{0, 1, 2, -1},
					Chunk: mydump.Chunk{
						Offset:       52,
						EndOffset:    60,
						PrevRowIDMax: 20,
						RowIDMax:     22,
						Columns:      []string{"a", "b", "c"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 6},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       6,
						EndOffset:    48,
						PrevRowIDMax: 22,
						RowIDMax:     35,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
			},
		},
		1: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 48},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       48,
						EndOffset:    101,
						PrevRowIDMax: 35,
						RowIDMax:     48,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[3].FileMeta.Path, Offset: 101},
					FileMeta:          tableMeta.DataFiles[3].FileMeta,
					ColumnPermutation: []int{1, 2, 0, -1},
					Chunk: mydump.Chunk{
						Offset:       101,
						EndOffset:    102,
						PrevRowIDMax: 48,
						RowIDMax:     48,
						Columns:      []string{"c", "a", "b"},
					},
					Timestamp: 1234567897,
				},
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[4].FileMeta.Path, Offset: 4},
					FileMeta:          tableMeta.DataFiles[4].FileMeta,
					ColumnPermutation: []int{-1, 0, 1, -1},
					Chunk: mydump.Chunk{
						Offset:       4,
						EndOffset:    59,
						PrevRowIDMax: 48,
						RowIDMax:     61,
						Columns:      []string{"b", "c"},
					},
					Timestamp: 1234567897,
				},
			},
		},
		2: {
			Status: checkpoints.CheckpointStatusLoaded,
			Chunks: []*checkpoints.ChunkCheckpoint{
				{
					Key:               checkpoints.ChunkCheckpointKey{Path: tableMeta.DataFiles[4].FileMeta.Path, Offset: 59},
					FileMeta:          tableMeta.DataFiles[4].FileMeta,
					ColumnPermutation: []int{-1, 0, 1, -1},
					Chunk: mydump.Chunk{
						Offset:       59,
						EndOffset:    60,
						PrevRowIDMax: 61,
						RowIDMax:     61,
						Columns:      []string{"b", "c"},
					},
					Timestamp: 1234567897,
				},
			},
		},
	})
}

func (s *tableRestoreSuite) TestGetColumnsNames(c *C) {
	c.Assert(getColumnNames(s.tableInfo.Core, []int{0, 1, 2, -1}), DeepEquals, []string{"a", "b", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, 0, 2, -1}), DeepEquals, []string{"b", "a", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 0, 1, -1}), DeepEquals, []string{"b", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{0, 1, -1, -1}), DeepEquals, []string{"a", "b"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, -1, 0, -1}), DeepEquals, []string{"c", "a"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 0, -1, -1}), DeepEquals, []string{"b"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, 2, 3, 0}), DeepEquals, []string{"_tidb_rowid", "a", "b", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{1, 0, 2, 3}), DeepEquals, []string{"b", "a", "c", "_tidb_rowid"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 0, 2, 1}), DeepEquals, []string{"b", "_tidb_rowid", "c"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{2, -1, 0, 1}), DeepEquals, []string{"c", "_tidb_rowid", "a"})
	c.Assert(getColumnNames(s.tableInfo.Core, []int{-1, 1, -1, 0}), DeepEquals, []string{"_tidb_rowid", "b"})
}

func (s *tableRestoreSuite) TestInitializeColumns(c *C) {
	ccp := &checkpoints.ChunkCheckpoint{}
	c.Assert(s.tr.initializeColumns(nil, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{0, 1, 2, -1})

	ccp.ColumnPermutation = nil
	c.Assert(s.tr.initializeColumns([]string{"b", "c", "a"}, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{2, 0, 1, -1})

	ccp.ColumnPermutation = nil
	c.Assert(s.tr.initializeColumns([]string{"b"}, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{-1, 0, -1, -1})

	ccp.ColumnPermutation = nil
	c.Assert(s.tr.initializeColumns([]string{"_tidb_rowid", "b", "a", "c"}, ccp), IsNil)
	c.Assert(ccp.ColumnPermutation, DeepEquals, []int{2, 1, 3, 0})

	ccp.ColumnPermutation = nil
	err := s.tr.initializeColumns([]string{"_tidb_rowid", "b", "a", "c", "d"}, ccp)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `unknown columns in header \[d\]`)

	ccp.ColumnPermutation = nil
	err = s.tr.initializeColumns([]string{"e", "b", "c", "d"}, ccp)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `unknown columns in header \[e d\]`)
}

func (s *tableRestoreSuite) TestCompareChecksumSuccess(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT.*tikv_gc_life_time.*").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("ADMIN CHECKSUM.*").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("db", "table", 1234567890, 12345, 1234567),
		)
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	remoteChecksum, err := DoChecksum(ctx, s.tr.tableInfo)
	c.Assert(err, IsNil)
	err = s.tr.compareChecksum(remoteChecksum, verification.MakeKVChecksum(1234567, 12345, 1234567890))
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *tableRestoreSuite) TestCompareChecksumFailure(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT.*tikv_gc_life_time.*").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("ADMIN CHECKSUM TABLE `db`\\.`table`").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("db", "table", 1234567890, 12345, 1234567),
		)
	mock.ExpectExec("UPDATE.*tikv_gc_life_time.*").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	remoteChecksum, err := DoChecksum(ctx, s.tr.tableInfo)
	c.Assert(err, IsNil)
	err = s.tr.compareChecksum(remoteChecksum, verification.MakeKVChecksum(9876543, 54321, 1357924680))
	c.Assert(err, ErrorMatches, "checksum mismatched.*")

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *tableRestoreSuite) TestAnalyzeTable(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectExec("ANALYZE TABLE `db`\\.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := context.Background()
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	c.Assert(err, IsNil)
	g := glue.NewExternalTiDBGlue(db, defaultSQLMode)
	err = s.tr.analyzeTable(ctx, g)
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *tableRestoreSuite) TestImportKVSuccess(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeBackend(mockBackend)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	rc := &Controller{saveCpCh: chptCh, cfg: config.NewConfig()}
	go func() {
		for scp := range chptCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()

	ctx := context.Background()
	engineUUID := uuid.New()

	mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID, gomock.Any()).
		Return(nil)
	mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil)

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, nil, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine, rc, 1)
	c.Assert(err, IsNil)
}

func (s *tableRestoreSuite) TestImportKVFailure(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeBackend(mockBackend)
	chptCh := make(chan saveCp)
	defer close(chptCh)
	rc := &Controller{saveCpCh: chptCh, cfg: config.NewConfig()}
	go func() {
		for scp := range chptCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()

	ctx := context.Background()
	engineUUID := uuid.New()

	mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil)
	mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID, gomock.Any()).
		Return(errors.Annotate(context.Canceled, "fake import error"))

	closedEngine, err := importer.UnsafeCloseEngineWithUUID(ctx, nil, "tag", engineUUID)
	c.Assert(err, IsNil)
	err = s.tr.importKV(ctx, closedEngine, rc, 1)
	c.Assert(err, ErrorMatches, "fake import error.*")
}

func (s *tableRestoreSuite) TestTableRestoreMetrics(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()

	chunkPendingBase := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	chunkFinishedBase := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	engineFinishedBase := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues("imported", metric.TableResultSuccess))
	tableFinishedBase := metric.ReadCounter(metric.TableCounter.WithLabelValues("index_imported", metric.TableResultSuccess))

	ctx := context.Background()
	chptCh := make(chan saveCp)
	defer close(chptCh)
	cfg := config.NewConfig()
	cfg.Mydumper.BatchSize = 1
	cfg.PostRestore.Checksum = config.OpLevelOff

	cfg.Checkpoint.Enable = false
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = 10080
	cfg.TiDB.Port = 4000
	cfg.TiDB.PdAddr = "127.0.0.1:2379"

	cfg.Mydumper.SourceDir = "."
	cfg.Mydumper.CSV.Header = false
	cfg.TikvImporter.Backend = config.BackendImporter
	tls, err := cfg.ToTLS()
	c.Assert(err, IsNil)

	err = cfg.Adjust(ctx)
	c.Assert(err, IsNil)

	cpDB := checkpoints.NewNullCheckpointsDB()
	g := mock.NewMockGlue(controller)
	rc := &Controller{
		cfg: cfg,
		dbMetas: []*mydump.MDDatabaseMeta{
			{
				Name:   s.tableInfo.DB,
				Tables: []*mydump.MDTableMeta{s.tableMeta},
			},
		},
		dbInfos: map[string]*checkpoints.TidbDBInfo{
			s.tableInfo.DB: s.dbInfo,
		},
		tableWorkers:      worker.NewPool(ctx, 6, "table"),
		ioWorkers:         worker.NewPool(ctx, 5, "io"),
		indexWorkers:      worker.NewPool(ctx, 2, "index"),
		regionWorkers:     worker.NewPool(ctx, 10, "region"),
		checksumWorks:     worker.NewPool(ctx, 2, "region"),
		saveCpCh:          chptCh,
		pauser:            DeliverPauser,
		backend:           noop.NewNoopBackend(),
		tidbGlue:          g,
		errorSummaries:    makeErrorSummaries(log.L()),
		tls:               tls,
		checkpointsDB:     cpDB,
		closedEngineLimit: worker.NewPool(ctx, 1, "closed_engine"),
		store:             s.store,
		metaMgrBuilder:    noopMetaMgrBuilder{},
		diskQuotaLock:     newDiskQuotaLock(),
	}
	go func() {
		for scp := range chptCh {
			if scp.waitCh != nil {
				scp.waitCh <- nil
			}
		}
	}()
	db, sqlMock, err := sqlmock.New()
	c.Assert(err, IsNil)
	g.EXPECT().GetDB().Return(db, nil).AnyTimes()
	sqlMock.ExpectQuery("SELECT version\\(\\);").WillReturnRows(sqlmock.NewRows([]string{"version()"}).
		AddRow("5.7.25-TiDB-v5.0.1"))

	web.BroadcastInitProgress(rc.dbMetas)

	err = rc.restoreTables(ctx)
	c.Assert(err, IsNil)

	chunkPending := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	chunkFinished := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
	c.Assert(chunkPending-chunkPendingBase, Equals, float64(7))
	c.Assert(chunkFinished-chunkFinishedBase, Equals, chunkPending-chunkPendingBase)

	engineFinished := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues("imported", metric.TableResultSuccess))
	c.Assert(engineFinished-engineFinishedBase, Equals, float64(8))

	tableFinished := metric.ReadCounter(metric.TableCounter.WithLabelValues("index_imported", metric.TableResultSuccess))
	c.Assert(tableFinished-tableFinishedBase, Equals, float64(1))
}

func (s *tableRestoreSuite) TestSaveStatusCheckpoint(c *C) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/restore/SlowDownCheckpointUpdate", "sleep(100)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/restore/SlowDownCheckpointUpdate")
	}()

	web.BroadcastInitProgress([]*mydump.MDDatabaseMeta{{
		Name:   "test",
		Tables: []*mydump.MDTableMeta{{DB: "test", Name: "tbl"}},
	}})
	web.BroadcastTableCheckpoint(common.UniqueTable("test", "tbl"), &checkpoints.TableCheckpoint{})

	saveCpCh := make(chan saveCp)

	rc := &Controller{
		saveCpCh:      saveCpCh,
		checkpointsDB: checkpoints.NewNullCheckpointsDB(),
	}
	go rc.listenCheckpointUpdates()

	start := time.Now()
	err := rc.saveStatusCheckpoint(context.Background(), common.UniqueTable("test", "tbl"), indexEngineID, nil, checkpoints.CheckpointStatusImported)
	c.Assert(err, IsNil)
	elapsed := time.Since(start)
	c.Assert(elapsed, GreaterEqual, time.Millisecond*100)

	close(saveCpCh)
	rc.checkpointsWg.Wait()
}

var _ = Suite(&chunkRestoreSuite{})

type chunkRestoreSuite struct {
	tableRestoreSuiteBase
	cr *chunkRestore
}

func (s *chunkRestoreSuite) SetUpTest(c *C) {
	s.tableRestoreSuiteBase.SetUpTest(c)

	ctx := context.Background()
	w := worker.NewPool(ctx, 5, "io")

	chunk := checkpoints.ChunkCheckpoint{
		Key:      checkpoints.ChunkCheckpointKey{Path: s.tr.tableMeta.DataFiles[1].FileMeta.Path, Offset: 0},
		FileMeta: s.tr.tableMeta.DataFiles[1].FileMeta,
		Chunk: mydump.Chunk{
			Offset:       0,
			EndOffset:    37,
			PrevRowIDMax: 18,
			RowIDMax:     36,
		},
	}

	var err error
	s.cr, err = newChunkRestore(context.Background(), 1, s.cfg, &chunk, w, s.store, nil)
	c.Assert(err, IsNil)
}

func (s *chunkRestoreSuite) TearDownTest(c *C) {
	s.cr.close()
}

func (s *chunkRestoreSuite) TestDeliverLoopCancel(c *C) {
	rc := &Controller{backend: importer.NewMockImporter(nil, "")}

	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	go cancel()
	_, err := s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, nil, nil, rc)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (s *chunkRestoreSuite) TestDeliverLoopEmptyData(c *C) {
	ctx := context.Background()

	// Open two mock engines.

	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	c.Assert(err, IsNil)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	c.Assert(err, IsNil)

	// Deliver nothing.

	cfg := &config.Config{}
	rc := &Controller{cfg: cfg, backend: importer, diskQuotaLock: newDiskQuotaLock()}

	kvsCh := make(chan []deliveredKVs, 1)
	kvsCh <- []deliveredKVs{}
	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataWriter, indexWriter, rc)
	c.Assert(err, IsNil)
}

func (s *chunkRestoreSuite) TestDeliverLoop(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	mockCols := []string{"c1", "c2"}

	// Open two mock engines.

	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	importer := backend.MakeBackend(mockBackend)

	mockBackend.EXPECT().OpenEngine(ctx, gomock.Any(), gomock.Any()).Return(nil).Times(2)
	// avoid return the same object at each call
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).Times(1)
	mockWriter := mock.NewMockEngineWriter(controller)
	mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().IsSynced().Return(true).AnyTimes()

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	c.Assert(err, IsNil)

	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	c.Assert(err, IsNil)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	c.Assert(err, IsNil)

	// Set up the expected API calls to the data engine...

	mockWriter.EXPECT().
		AppendRows(ctx, s.tr.tableName, mockCols, kv.MakeRowsFromKvPairs([]common.KvPair{
			{
				Key: []byte("txxxxxxxx_ryyyyyyyy"),
				Val: []byte("value1"),
			},
			{
				Key: []byte("txxxxxxxx_rwwwwwwww"),
				Val: []byte("value2"),
			},
		})).
		Return(nil)

	// ... and the index engine.
	//
	// Note: This test assumes data engine is written before the index engine.

	mockWriter.EXPECT().
		AppendRows(ctx, s.tr.tableName, mockCols, kv.MakeRowsFromKvPairs([]common.KvPair{
			{
				Key: []byte("txxxxxxxx_izzzzzzzz"),
				Val: []byte("index1"),
			},
		})).
		Return(nil)

	// Now actually start the delivery loop.

	saveCpCh := make(chan saveCp, 2)
	go func() {
		kvsCh <- []deliveredKVs{
			{
				kvs: kv.MakeRowFromKvPairs([]common.KvPair{
					{
						Key: []byte("txxxxxxxx_ryyyyyyyy"),
						Val: []byte("value1"),
					},
					{
						Key: []byte("txxxxxxxx_rwwwwwwww"),
						Val: []byte("value2"),
					},
					{
						Key: []byte("txxxxxxxx_izzzzzzzz"),
						Val: []byte("index1"),
					},
				}),
				columns: mockCols,
				offset:  12,
				rowID:   76,
			},
		}
		kvsCh <- []deliveredKVs{}
		close(kvsCh)
	}()

	cfg := &config.Config{}
	rc := &Controller{cfg: cfg, saveCpCh: saveCpCh, backend: importer, diskQuotaLock: newDiskQuotaLock()}

	_, err = s.cr.deliverLoop(ctx, kvsCh, s.tr, 0, dataWriter, indexWriter, rc)
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
	c.Assert(s.cr.chunk.Chunk.Offset, Equals, int64(12))
	c.Assert(s.cr.chunk.Chunk.PrevRowIDMax, Equals, int64(76))
	c.Assert(s.cr.chunk.Checksum.SumKVS(), Equals, uint64(3))
}

func (s *chunkRestoreSuite) TestEncodeLoop(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567895,
	})
	c.Assert(err, IsNil)
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, IsNil)
	c.Assert(kvsCh, HasLen, 2)

	kvs := <-kvsCh
	c.Assert(kvs, HasLen, 1)
	c.Assert(kvs[0].rowID, Equals, int64(19))
	c.Assert(kvs[0].offset, Equals, int64(36))

	kvs = <-kvsCh
	c.Assert(len(kvs), Equals, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopCanceled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567896,
	})
	c.Assert(err, IsNil)

	go cancel()
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopForcedError(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567897,
	})
	c.Assert(err, IsNil)

	// close the chunk so reading it will result in the "file already closed" error.
	s.cr.parser.Close()

	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, `in file .*[/\\]?db\.table\.2\.sql:0 at offset 0:.*file already closed`)
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverLimit(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs, 4)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567898,
	})
	c.Assert(err, IsNil)

	dir := c.MkDir()
	fileName := "db.limit.000.csv"
	err = os.WriteFile(filepath.Join(dir, fileName), []byte("1,2,3\r\n4,5,6\r\n7,8,9\r"), 0o644)
	c.Assert(err, IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	cfg := config.NewConfig()

	reader, err := store.Open(ctx, fileName)
	c.Assert(err, IsNil)
	w := worker.NewPool(ctx, 1, "io")
	p, err := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, 111, w, false, nil)
	c.Assert(err, IsNil)
	s.cr.parser = p

	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	c.Assert(failpoint.Enable(
		"github.com/pingcap/tidb/br/pkg/lightning/restore/mock-kv-size", "return(110000000)"), IsNil)
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, IsNil)

	// we have 3 kvs total. after the failpoint injected.
	// we will send one kv each time.
	count := 0
	for {
		kvs, ok := <-kvsCh
		if !ok {
			break
		}
		count += 1
		if count <= 3 {
			c.Assert(kvs, HasLen, 1)
		}
		if count == 4 {
			// we will send empty kvs before encodeLoop exists
			// so, we can receive 4 batch and 1 is empty
			c.Assert(kvs, HasLen, 0)
			break
		}
	}
}

func (s *chunkRestoreSuite) TestEncodeLoopDeliverErrored(c *C) {
	ctx := context.Background()
	kvsCh := make(chan []deliveredKVs)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := kv.NewTableKVEncoder(s.tr.encTable, &kv.SessionOptions{
		SQLMode:   s.cfg.TiDB.SQLMode,
		Timestamp: 1234567898,
	})
	c.Assert(err, IsNil)

	go func() {
		deliverCompleteCh <- deliverResult{
			err: errors.New("fake deliver error"),
		}
	}()
	cfg := config.NewConfig()
	rc := &Controller{pauser: DeliverPauser, cfg: cfg}
	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, "fake deliver error")
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestEncodeLoopColumnsMismatch(c *C) {
	dir := c.MkDir()
	fileName := "db.table.000.csv"
	err := os.WriteFile(filepath.Join(dir, fileName), []byte("1,2,3,4\r\n4,5,6,7\r\n"), 0o644)
	c.Assert(err, IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	ctx := context.Background()
	cfg := config.NewConfig()
	errorMgr := errormanager.New(nil, cfg)
	rc := &Controller{pauser: DeliverPauser, cfg: cfg, errorMgr: errorMgr}

	reader, err := store.Open(ctx, fileName)
	c.Assert(err, IsNil)
	w := worker.NewPool(ctx, 5, "io")
	p, err := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, 111, w, false, nil)
	c.Assert(err, IsNil)

	err = s.cr.parser.Close()
	c.Assert(err, IsNil)
	s.cr.parser = p

	kvsCh := make(chan []deliveredKVs, 2)
	deliverCompleteCh := make(chan deliverResult)
	kvEncoder, err := tidb.NewTiDBBackend(nil, config.ReplaceOnDup, errorMgr).NewEncoder(
		s.tr.encTable,
		&kv.SessionOptions{
			SQLMode:   s.cfg.TiDB.SQLMode,
			Timestamp: 1234567895,
		})
	c.Assert(err, IsNil)

	_, _, err = s.cr.encodeLoop(ctx, kvsCh, s.tr, s.tr.logger, kvEncoder, deliverCompleteCh, rc)
	c.Assert(err, ErrorMatches, "in file db.table.2.sql:0 at offset 8: column count mismatch, expected 3, got 4")
	c.Assert(kvsCh, HasLen, 0)
}

func (s *chunkRestoreSuite) TestRestore(c *C) {
	ctx := context.Background()

	// Open two mock engines

	controller := gomock.NewController(c)
	defer controller.Finish()
	mockClient := mock.NewMockImportKVClient(controller)
	mockDataWriter := mock.NewMockImportKV_WriteEngineClient(controller)
	mockIndexWriter := mock.NewMockImportKV_WriteEngineClient(controller)
	importer := importer.NewMockImporter(mockClient, "127.0.0.1:2379")

	mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)
	mockClient.EXPECT().OpenEngine(ctx, gomock.Any()).Return(nil, nil)

	dataEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, 0)
	c.Assert(err, IsNil)
	indexEngine, err := importer.OpenEngine(ctx, &backend.EngineConfig{}, s.tr.tableName, -1)
	c.Assert(err, IsNil)
	dataWriter, err := dataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	c.Assert(err, IsNil)
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	c.Assert(err, IsNil)

	// Expected API sequence
	// (we don't care about the actual content, this would be checked in the integrated tests)

	mockClient.EXPECT().WriteEngine(ctx).Return(mockDataWriter, nil)
	mockDataWriter.EXPECT().Send(gomock.Any()).Return(nil)
	mockDataWriter.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
		c.Assert(req.GetBatch().GetMutations(), HasLen, 1)
		return nil
	})
	mockDataWriter.EXPECT().CloseAndRecv().Return(nil, nil)

	mockClient.EXPECT().WriteEngine(ctx).Return(mockIndexWriter, nil)
	mockIndexWriter.EXPECT().Send(gomock.Any()).Return(nil)
	mockIndexWriter.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *import_kvpb.WriteEngineRequest) error {
		c.Assert(req.GetBatch().GetMutations(), HasLen, 1)
		return nil
	})
	mockIndexWriter.EXPECT().CloseAndRecv().Return(nil, nil)

	// Now actually start the restore loop.

	saveCpCh := make(chan saveCp, 2)
	err = s.cr.restore(ctx, s.tr, 0, dataWriter, indexWriter, &Controller{
		cfg:           s.cfg,
		saveCpCh:      saveCpCh,
		backend:       importer,
		pauser:        DeliverPauser,
		diskQuotaLock: newDiskQuotaLock(),
	})
	c.Assert(err, IsNil)
	c.Assert(saveCpCh, HasLen, 2)
}

var _ = Suite(&restoreSchemaSuite{})

type restoreSchemaSuite struct {
	ctx        context.Context
	rc         *Controller
	controller *gomock.Controller
	tableInfos []*model.TableInfo
}

func (s *restoreSchemaSuite) SetUpSuite(c *C) {
	ctx := context.Background()
	fakeDataDir := c.MkDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	c.Assert(err, IsNil)
	// restore database schema file
	fakeDBName := "fakedb"
	// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}-schema-create.sql'
	fakeFileName := fmt.Sprintf("%s-schema-create.sql", fakeDBName)
	err = store.WriteFile(ctx, fakeFileName, []byte(fmt.Sprintf("CREATE DATABASE %s;", fakeDBName)))
	c.Assert(err, IsNil)
	// restore table schema files
	fakeTableFilesCount := 8

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()

	tableInfos := make([]*model.TableInfo, 0, fakeTableFilesCount)
	for i := 1; i <= fakeTableFilesCount; i++ {
		fakeTableName := fmt.Sprintf("tbl%d", i)
		// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}.{table}-schema.sql'
		fakeFileName := fmt.Sprintf("%s.%s-schema.sql", fakeDBName, fakeTableName)
		fakeFileContent := fmt.Sprintf("CREATE TABLE %s(i TINYINT);", fakeTableName)
		err = store.WriteFile(ctx, fakeFileName, []byte(fakeFileContent))
		c.Assert(err, IsNil)

		node, err := p.ParseOneStmt(fakeFileContent, "", "")
		c.Assert(err, IsNil)
		core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
		c.Assert(err, IsNil)
		core.State = model.StatePublic
		tableInfos = append(tableInfos, core)
	}
	s.tableInfos = tableInfos
	// restore view schema files
	fakeViewFilesCount := 8
	for i := 1; i <= fakeViewFilesCount; i++ {
		fakeViewName := fmt.Sprintf("tbl%d", i)
		// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}.{table}-schema-view.sql'
		fakeFileName := fmt.Sprintf("%s.%s-schema-view.sql", fakeDBName, fakeViewName)
		fakeFileContent := []byte(fmt.Sprintf("CREATE ALGORITHM=UNDEFINED VIEW `%s` (`i`) AS SELECT `i` FROM `%s`.`%s`;", fakeViewName, fakeDBName, fmt.Sprintf("tbl%d", i)))
		err = store.WriteFile(ctx, fakeFileName, fakeFileContent)
		c.Assert(err, IsNil)
	}
	config := config.NewConfig()
	config.Mydumper.DefaultFileRules = true
	config.Mydumper.CharacterSet = "utf8mb4"
	config.App.RegionConcurrency = 8
	mydumpLoader, err := mydump.NewMyDumpLoaderWithStore(ctx, config, store)
	c.Assert(err, IsNil)
	s.rc = &Controller{
		checkTemplate: NewSimpleTemplate(),
		cfg:           config,
		store:         store,
		dbMetas:       mydumpLoader.GetDatabases(),
		checkpointsDB: &checkpoints.NullCheckpointsDB{},
	}
}

//nolint:interfacer // change test case signature might cause Check failed to find this test case?
func (s *restoreSchemaSuite) SetUpTest(c *C) {
	s.controller, s.ctx = gomock.WithContext(context.Background(), c)
	mockBackend := mock.NewMockBackend(s.controller)
	mockBackend.EXPECT().
		FetchRemoteTableModels(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(s.tableInfos, nil)
	mockBackend.EXPECT().Close()
	s.rc.backend = backend.MakeBackend(mockBackend)

	mockDB, sqlMock, err := sqlmock.New()
	c.Assert(err, IsNil)
	for i := 0; i < 17; i++ {
		sqlMock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(int64(i), 1))
	}
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().GetDB().AnyTimes().Return(mockDB, nil)
	mockTiDBGlue.EXPECT().
		OwnsSQLExecutor().
		AnyTimes().
		Return(true)
	parser := parser.New()
	mockTiDBGlue.EXPECT().
		GetParser().
		AnyTimes().
		Return(parser)
	s.rc.tidbGlue = mockTiDBGlue
}

func (s *restoreSchemaSuite) TearDownTest(c *C) {
	s.rc.Close()
	s.controller.Finish()
}

func (s *restoreSchemaSuite) TestRestoreSchemaSuccessful(c *C) {
	// before restore, if sysVars is initialized by other test, the time_zone should be default value
	if len(s.rc.sysVars) > 0 {
		tz, ok := s.rc.sysVars["time_zone"]
		c.Assert(ok, IsTrue)
		c.Assert(tz, Equals, "SYSTEM")
	}

	s.rc.cfg.TiDB.Vars = map[string]string{
		"time_zone": "UTC",
	}
	err := s.rc.restoreSchema(s.ctx)
	c.Assert(err, IsNil)

	// test after restore schema, sysVars has been updated
	tz, ok := s.rc.sysVars["time_zone"]
	c.Assert(ok, IsTrue)
	c.Assert(tz, Equals, "UTC")
}

func (s *restoreSchemaSuite) TestRestoreSchemaFailed(c *C) {
	injectErr := errors.New("Something wrong")
	mockSession := mock.NewMockSession(s.controller)
	mockSession.EXPECT().
		Close().
		AnyTimes().
		Return()
	mockSession.EXPECT().
		Execute(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(nil, injectErr)
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetSession(gomock.Any()).
		AnyTimes().
		Return(mockSession, nil)
	s.rc.tidbGlue = mockTiDBGlue
	err := s.rc.restoreSchema(s.ctx)
	c.Assert(err, NotNil)
	c.Assert(errors.ErrorEqual(err, injectErr), IsTrue)
}

// When restoring a CSV with `-no-schema` and the target table doesn't exist
// then we can't restore the schema as the `Path` is empty. This is to make
// sure this results in the correct error.
// https://github.com/pingcap/br/issues/1394
func (s *restoreSchemaSuite) TestNoSchemaPath(c *C) {
	fakeTable := mydump.MDTableMeta{
		DB:   "fakedb",
		Name: "fake1",
		SchemaFile: mydump.FileInfo{
			TableName: filter.Table{
				Schema: "fakedb",
				Name:   "fake1",
			},
			FileMeta: mydump.SourceFileMeta{
				Path: "",
			},
		},
		DataFiles: []mydump.FileInfo{},
		TotalSize: 0,
	}
	s.rc.dbMetas[0].Tables = append(s.rc.dbMetas[0].Tables, &fakeTable)
	err := s.rc.restoreSchema(s.ctx)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, `table .* schema not found`)
	s.rc.dbMetas[0].Tables = s.rc.dbMetas[0].Tables[:len(s.rc.dbMetas[0].Tables)-1]
}

func (s *restoreSchemaSuite) TestRestoreSchemaContextCancel(c *C) {
	childCtx, cancel := context.WithCancel(s.ctx)
	mockSession := mock.NewMockSession(s.controller)
	mockSession.EXPECT().
		Close().
		AnyTimes().
		Return()
	mockSession.EXPECT().
		Execute(gomock.Any(), gomock.Any()).
		AnyTimes().
		Do(func(context.Context, string) { cancel() }).
		Return(nil, nil)
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetSession(gomock.Any()).
		AnyTimes().
		Return(mockSession, nil)
	s.rc.tidbGlue = mockTiDBGlue
	err := s.rc.restoreSchema(childCtx)
	cancel()
	c.Assert(err, NotNil)
	c.Assert(err, Equals, childCtx.Err())
}

func (s *tableRestoreSuite) TestCheckClusterResource(c *C) {
	cases := []struct {
		mockStoreResponse   []byte
		mockReplicaResponse []byte
		expectMsg           string
		expectResult        bool
		expectErrorCount    int
	}{
		{
			[]byte(`{
				"count": 1,
				"stores": [
					{
						"store": {
							"id": 2
						},
						"status": {
							"available": "24"
						}
					}
				]
			}`),
			[]byte(`{
				"max-replicas": 1
			}`),
			"(.*)Cluster available is rich(.*)",
			true,
			0,
		},
		{
			[]byte(`{
				"count": 1,
				"stores": [
					{
						"store": {
							"id": 2
						},
						"status": {
							"available": "15"
						}
					}
				]
			}`),
			[]byte(`{
				"max-replicas": 1
			}`),
			"(.*)Cluster doesn't have enough space(.*)",
			false,
			1,
		},
	}

	ctx := context.Background()
	dir := c.MkDir()
	file := filepath.Join(dir, "tmp")
	f, err := os.Create(file)
	c.Assert(err, IsNil)
	buf := make([]byte, 16)
	// write 16 bytes file into local storage
	for i := range buf {
		buf[i] = byte('A' + i)
	}
	_, err = f.Write(buf)
	c.Assert(err, IsNil)
	mockStore, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	for _, ca := range cases {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var err error
			if strings.HasSuffix(req.URL.Path, "stores") {
				_, err = w.Write(ca.mockStoreResponse)
			} else {
				_, err = w.Write(ca.mockReplicaResponse)
			}
			c.Assert(err, IsNil)
		}))

		tls := common.NewTLSFromMockServer(server)
		template := NewSimpleTemplate()

		url := strings.TrimPrefix(server.URL, "https://")
		cfg := &config.Config{TiDB: config.DBStore{PdAddr: url}}
		rc := &Controller{cfg: cfg, tls: tls, store: mockStore, checkTemplate: template}
		var sourceSize int64
		err = rc.store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
			sourceSize += size
			return nil
		})
		err = rc.clusterResource(ctx, sourceSize)
		c.Assert(err, IsNil)

		c.Assert(template.FailedCount(Critical), Equals, ca.expectErrorCount)
		c.Assert(template.Success(), Equals, ca.expectResult)
		c.Assert(strings.ReplaceAll(template.Output(), "\n", ""), Matches, ca.expectMsg)

		server.Close()
	}
}

type mockTaskMetaMgr struct {
	taskMetaMgr
}

func (mockTaskMetaMgr) CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error {
	_, err := action([]taskMeta{{
		taskID: 1,
		pdCfgs: "",
		status: taskMetaStatusInitial,
		state:  taskStateNormal,
	}})
	return err
}

func (s *tableRestoreSuite) TestCheckClusterRegion(c *C) {
	type testCase struct {
		stores         api.StoresInfo
		emptyRegions   api.RegionsInfo
		expectMsgs     []string
		expectResult   bool
		expectErrorCnt int
	}

	makeRegions := func(regionCnt int, storeID uint64) []api.RegionInfo {
		var regions []api.RegionInfo
		for i := 0; i < regionCnt; i++ {
			regions = append(regions, api.RegionInfo{Peers: []api.MetaPeer{{Peer: &metapb.Peer{StoreId: storeID}}}})
		}
		return regions
	}

	testCases := []testCase{
		{
			stores: api.StoresInfo{Stores: []*api.StoreInfo{
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 1}}, Status: &api.StoreStatus{RegionCount: 200}},
			}},
			emptyRegions: api.RegionsInfo{
				Regions: append([]api.RegionInfo(nil), makeRegions(100, 1)...),
			},
			expectMsgs:     []string{".*Cluster doesn't have too many empty regions.*", ".*Cluster region distribution is balanced.*"},
			expectResult:   true,
			expectErrorCnt: 0,
		},
		{
			stores: api.StoresInfo{Stores: []*api.StoreInfo{
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 1}}, Status: &api.StoreStatus{RegionCount: 2000}},
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 2}}, Status: &api.StoreStatus{RegionCount: 3100}},
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 3}}, Status: &api.StoreStatus{RegionCount: 2500}},
			}},
			emptyRegions: api.RegionsInfo{
				Regions: append(append(append([]api.RegionInfo(nil),
					makeRegions(600, 1)...),
					makeRegions(300, 2)...),
					makeRegions(1200, 3)...),
			},
			expectMsgs: []string{
				".*TiKV stores \\(3\\) contains more than 1000 empty regions respectively.*",
				".*TiKV stores \\(1\\) contains more than 500 empty regions respectively.*",
				".*Region distribution is unbalanced.*but we expect it should not be less than 0.75.*",
			},
			expectResult:   false,
			expectErrorCnt: 1,
		},
		{
			stores: api.StoresInfo{Stores: []*api.StoreInfo{
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 1}}, Status: &api.StoreStatus{RegionCount: 1200}},
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 2}}, Status: &api.StoreStatus{RegionCount: 3000}},
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 3}}, Status: &api.StoreStatus{RegionCount: 2500}},
			}},
			expectMsgs:     []string{".*Region distribution is unbalanced.*but we expect it must not be less than 0.5.*"},
			expectResult:   false,
			expectErrorCnt: 1,
		},
		{
			stores: api.StoresInfo{Stores: []*api.StoreInfo{
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 1}}, Status: &api.StoreStatus{RegionCount: 0}},
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 2}}, Status: &api.StoreStatus{RegionCount: 2800}},
				{Store: &api.MetaStore{Store: &metapb.Store{Id: 3}}, Status: &api.StoreStatus{RegionCount: 2500}},
			}},
			expectMsgs:     []string{".*Region distribution is unbalanced.*but we expect it must not be less than 0.5.*"},
			expectResult:   false,
			expectErrorCnt: 1,
		},
	}

	mustMarshal := func(v interface{}) []byte {
		data, err := json.Marshal(v)
		c.Assert(err, IsNil)
		return data
	}

	for _, ca := range testCases {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var err error
			if req.URL.Path == pdStores {
				_, err = w.Write(mustMarshal(ca.stores))
			} else if req.URL.Path == pdEmptyRegions {
				_, err = w.Write(mustMarshal(ca.emptyRegions))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			c.Assert(err, IsNil)
		}))

		tls := common.NewTLSFromMockServer(server)
		template := NewSimpleTemplate()

		url := strings.TrimPrefix(server.URL, "https://")
		cfg := &config.Config{TiDB: config.DBStore{PdAddr: url}}
		rc := &Controller{cfg: cfg, tls: tls, taskMgr: mockTaskMetaMgr{}, checkTemplate: template}

		err := rc.checkClusterRegion(context.Background())
		c.Assert(err, IsNil)
		c.Assert(template.FailedCount(Critical), Equals, ca.expectErrorCnt)
		c.Assert(template.Success(), Equals, ca.expectResult)

		for _, expectMsg := range ca.expectMsgs {
			c.Assert(strings.ReplaceAll(template.Output(), "\n", ""), Matches, expectMsg)
		}

		server.Close()
	}
}

func (s *tableRestoreSuite) TestCheckHasLargeCSV(c *C) {
	cases := []struct {
		strictFormat    bool
		expectMsg       string
		expectResult    bool
		expectWarnCount int
		dbMetas         []*mydump.MDDatabaseMeta
	}{
		{
			true,
			"(.*)Skip the csv size check, because config.StrictFormat is true(.*)",
			true,
			0,
			nil,
		},
		{
			false,
			"(.*)Source csv files size is proper(.*)",
			true,
			0,
			[]*mydump.MDDatabaseMeta{
				{
					Tables: []*mydump.MDTableMeta{
						{
							DataFiles: []mydump.FileInfo{
								{
									FileMeta: mydump.SourceFileMeta{
										FileSize: 1 * units.KiB,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			false,
			"(.*)large csv: /testPath file exists(.*)",
			true,
			1,
			[]*mydump.MDDatabaseMeta{
				{
					Tables: []*mydump.MDTableMeta{
						{
							DataFiles: []mydump.FileInfo{
								{
									FileMeta: mydump.SourceFileMeta{
										FileSize: 1 * units.TiB,
										Path:     "/testPath",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	dir := c.MkDir()
	mockStore, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	for _, ca := range cases {
		template := NewSimpleTemplate()
		cfg := &config.Config{Mydumper: config.MydumperRuntime{StrictFormat: ca.strictFormat}}
		rc := &Controller{cfg: cfg, checkTemplate: template, store: mockStore}
		err := rc.HasLargeCSV(ca.dbMetas)
		c.Assert(err, IsNil)
		c.Assert(template.FailedCount(Warn), Equals, ca.expectWarnCount)
		c.Assert(template.Success(), Equals, ca.expectResult)
		c.Assert(strings.ReplaceAll(template.Output(), "\n", ""), Matches, ca.expectMsg)
	}
}
func (s *tableRestoreSuite) TestEstimate(c *C) {
	ctx := context.Background()
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockBackend := mock.NewMockBackend(controller)
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, s.tableInfo.Core)
	c.Assert(err, IsNil)

	mockBackend.EXPECT().MakeEmptyRows().Return(kv.MakeRowsFromKvPairs(nil)).AnyTimes()
	mockBackend.EXPECT().NewEncoder(gomock.Any(), gomock.Any()).Return(kv.NewTableKVEncoder(tbl, &kv.SessionOptions{
		SQLMode:        s.cfg.TiDB.SQLMode,
		Timestamp:      0,
		AutoRandomSeed: 0,
	})).AnyTimes()
	importer := backend.MakeBackend(mockBackend)
	s.cfg.TikvImporter.Backend = config.BackendLocal

	template := NewSimpleTemplate()
	rc := &Controller{
		cfg:           s.cfg,
		checkTemplate: template,
		store:         s.store,
		backend:       importer,
		dbMetas: []*mydump.MDDatabaseMeta{
			{
				Name:   "db1",
				Tables: []*mydump.MDTableMeta{s.tableMeta},
			},
		},
		dbInfos: map[string]*checkpoints.TidbDBInfo{
			"db1": s.dbInfo,
		},
		ioWorkers: worker.NewPool(context.Background(), 1, "io"),
	}
	source, err := rc.estimateSourceData(ctx)
	// Because this file is small than region split size so we does not sample it.
	c.Assert(err, IsNil)
	c.Assert(source, Equals, s.tableMeta.TotalSize)
	s.tableMeta.TotalSize = int64(config.SplitRegionSize)
	source, err = rc.estimateSourceData(ctx)
	c.Assert(err, IsNil)
	c.Assert(source, Greater, s.tableMeta.TotalSize)
	rc.cfg.TikvImporter.Backend = config.BackendTiDB
	source, err = rc.estimateSourceData(ctx)
	c.Assert(err, IsNil)
	c.Assert(source, Equals, s.tableMeta.TotalSize)
}

func (s *tableRestoreSuite) TestSchemaIsValid(c *C) {
	dir := c.MkDir()
	ctx := context.Background()

	case1File := "db1.table1.csv"
	mockStore, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	err = mockStore.WriteFile(ctx, case1File, []byte(`"a"`))
	c.Assert(err, IsNil)

	case2File := "db1.table2.csv"
	err = mockStore.WriteFile(ctx, case2File, []byte("\"colA\",\"colB\"\n\"a\",\"b\""))
	c.Assert(err, IsNil)

	cases := []struct {
		ignoreColumns []*config.IgnoreColumns
		expectMsg     string
		// MsgNum == 0 means the check passed.
		MsgNum    int
		hasHeader bool
		dbInfos   map[string]*checkpoints.TidbDBInfo
		tableMeta *mydump.MDTableMeta
	}{
		// Case 1:
		// csv has one column without header.
		// tidb has the two columns but the second column doesn't have the default value.
		// we expect the check failed.
		{
			nil,
			"TiDB schema `db1`.`table1` has 2 columns,and data file has 1 columns, but column colb are missing(.*)",
			1,
			false,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table1": {
							ID:   1,
							DB:   "db1",
							Name: "table1",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colA has the default value
										Name:          model.NewCIStr("colA"),
										DefaultIsExpr: true,
									},
									{
										// colB doesn't have the default value
										Name: model.NewCIStr("colB"),
										FieldType: types.FieldType{
											// not null flag
											Flag: 1,
										},
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table1",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case1File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.1:
		// csv has two columns(colA, colB) with the header.
		// tidb only has one column(colB).
		// we expect the check failed.
		{
			nil,
			"TiDB schema `db1`.`table2` doesn't have column cola,(.*)use tables.ignoreColumns to ignore(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.2:
		// csv has two columns(colA, colB) with the header.
		// tidb only has one column(colB).
		// we ignore colA by set config tables.IgnoreColumns
		// we expect the check success.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db1",
					Table:   "table2",
					Columns: []string{"cola"},
				},
			},
			"",
			0,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.3:
		// csv has two columns(colA, colB) with the header.
		// tidb has two columns(colB, colC).
		// we ignore colA by set config tables.IgnoreColumns
		// colC doesn't have the default value.
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db1",
					Table:   "table2",
					Columns: []string{"cola"},
				},
			},
			"TiDB schema `db1`.`table2` doesn't have the default value for colc(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
									{
										// colC doesn't have the default value
										Name: model.NewCIStr("colC"),
										FieldType: types.FieldType{
											Flag: 1,
										},
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 2.4:
		// csv has two columns(colA, colB) with the header.
		// tidb has two columns(colB, colC).
		// we ignore colB by set config tables.IgnoreColumns
		// colB doesn't have the default value.
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					TableFilter: []string{"`db1`.`table2`"},
					Columns:     []string{"colb"},
				},
			},
			"TiDB schema `db1`.`table2`'s column colb cannot be ignored(.*)",
			2,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB doesn't have the default value
										Name: model.NewCIStr("colB"),
										FieldType: types.FieldType{
											Flag: 1,
										},
									},
									{
										// colC has the default value
										Name:          model.NewCIStr("colC"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 3:
		// table3's schema file not found.
		// tidb has no table3.
		// we expect the check failed.
		{
			[]*config.IgnoreColumns{
				{
					TableFilter: []string{"`db1`.`table2`"},
					Columns:     []string{"colb"},
				},
			},
			"TiDB schema `db1`.`table3` doesn't exists(.*)",
			1,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"": {},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table3",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
				},
			},
		},
		// Case 4:
		// table4 has two datafiles for table. we only check the first file.
		// we expect the check success.
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db1",
					Table:   "table2",
					Columns: []string{"cola"},
				},
			},
			"",
			0,
			true,
			map[string]*checkpoints.TidbDBInfo{
				"db1": {
					Name: "db1",
					Tables: map[string]*checkpoints.TidbTableInfo{
						"table2": {
							ID:   1,
							DB:   "db1",
							Name: "table2",
							Core: &model.TableInfo{
								Columns: []*model.ColumnInfo{
									{
										// colB has the default value
										Name:          model.NewCIStr("colB"),
										DefaultIsExpr: true,
									},
								},
							},
						},
					},
				},
			},
			&mydump.MDTableMeta{
				DB:   "db1",
				Name: "table2",
				DataFiles: []mydump.FileInfo{
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							Type:     mydump.SourceTypeCSV,
						},
					},
					{
						FileMeta: mydump.SourceFileMeta{
							FileSize: 1 * units.TiB,
							Path:     case2File,
							// This type will make the check failed.
							// but it's the second file for table.
							// so it's unreachable so this case will success.
							Type: mydump.SourceTypeIgnore,
						},
					},
				},
			},
		},
	}

	for _, ca := range cases {
		template := NewSimpleTemplate()
		cfg := &config.Config{
			Mydumper: config.MydumperRuntime{
				ReadBlockSize: config.ReadBlockSize,
				CSV: config.CSVConfig{
					Separator:       ",",
					Delimiter:       `"`,
					Header:          ca.hasHeader,
					NotNull:         false,
					Null:            `\N`,
					BackslashEscape: true,
					TrimLastSep:     false,
				},
				IgnoreColumns: ca.ignoreColumns,
			},
		}
		rc := &Controller{
			cfg:           cfg,
			checkTemplate: template,
			store:         mockStore,
			dbInfos:       ca.dbInfos,
			ioWorkers:     worker.NewPool(context.Background(), 1, "io"),
		}
		msgs, err := rc.SchemaIsValid(ctx, ca.tableMeta)
		c.Assert(err, IsNil)
		c.Assert(msgs, HasLen, ca.MsgNum)
		if len(msgs) > 0 {
			c.Assert(msgs[0], Matches, ca.expectMsg)
		}
	}
}

func (s *tableRestoreSuite) TestGBKEncodedSchemaIsValid(c *C) {
	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize:          config.ReadBlockSize,
			DataCharacterSet:       "gb18030",
			DataInvalidCharReplace: string(utf8.RuneError),
			CSV: config.CSVConfig{
				Separator:       "，",
				Delimiter:       `"`,
				Header:          true,
				NotNull:         false,
				Null:            `\N`,
				BackslashEscape: true,
				TrimLastSep:     false,
			},
			IgnoreColumns: nil,
		},
	}
	charsetConvertor, err := mydump.NewCharsetConvertor(cfg.Mydumper.DataCharacterSet, cfg.Mydumper.DataInvalidCharReplace)
	c.Assert(err, IsNil)
	mockStore, err := storage.NewLocalStorage(c.MkDir())
	c.Assert(err, IsNil)
	csvContent, err := charsetConvertor.Encode(string([]byte("\"colA\"，\"colB\"\n\"a\"，\"b\"")))
	c.Assert(err, IsNil)
	ctx := context.Background()
	csvFile := "db1.gbk_table.csv"
	err = mockStore.WriteFile(ctx, csvFile, []byte(csvContent))
	c.Assert(err, IsNil)

	rc := &Controller{
		cfg:           cfg,
		checkTemplate: NewSimpleTemplate(),
		store:         mockStore,
		dbInfos: map[string]*checkpoints.TidbDBInfo{
			"db1": {
				Name: "db1",
				Tables: map[string]*checkpoints.TidbTableInfo{
					"gbk_table": {
						ID:   1,
						DB:   "db1",
						Name: "gbk_table",
						Core: &model.TableInfo{
							Columns: []*model.ColumnInfo{
								{
									Name: model.NewCIStr("colA"),
									FieldType: types.FieldType{
										Flag: 1,
									},
								},
								{
									Name: model.NewCIStr("colB"),
									FieldType: types.FieldType{
										Flag: 1,
									},
								},
							},
						},
					},
				},
			},
		},
		ioWorkers: worker.NewPool(ctx, 1, "io"),
	}
	msgs, err := rc.SchemaIsValid(ctx, &mydump.MDTableMeta{
		DB:   "db1",
		Name: "gbk_table",
		DataFiles: []mydump.FileInfo{
			{
				FileMeta: mydump.SourceFileMeta{
					FileSize: 1 * units.TiB,
					Path:     csvFile,
					Type:     mydump.SourceTypeCSV,
				},
			},
		},
	})
	c.Assert(err, IsNil)
	c.Assert(msgs, HasLen, 0)
}

type testChecksumMgr struct {
	checksum RemoteChecksum
	callCnt  int
}

func (t *testChecksumMgr) Checksum(ctx context.Context, tableInfo *checkpoints.TidbTableInfo) (*RemoteChecksum, error) {
	t.callCnt++
	return &t.checksum, nil
}
