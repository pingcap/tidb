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
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	tmock "github.com/pingcap/tidb/util/mock"
)

func TestNewTableRestore(t *testing.T) {
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
		require.NoError(t, err)
		tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(i+1))
		require.NoError(t, err)
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
		require.NotNil(t, tr)
		require.NoError(t, err)
	}
}

func TestNewTableRestoreFailure(t *testing.T) {
	tableInfo := &checkpoints.TidbTableInfo{
		Name: "failure",
		Core: &model.TableInfo{},
	}
	dbInfo := &checkpoints.TidbDBInfo{Name: "mockdb", Tables: map[string]*checkpoints.TidbTableInfo{
		"failure": tableInfo,
	}}
	tableName := common.UniqueTable("mockdb", "failure")

	_, err := NewTableRestore(tableName, nil, dbInfo, tableInfo, &checkpoints.TableCheckpoint{}, nil)
	require.Regexp(t, `failed to tables\.TableFromMeta.*`, err.Error())
}

func TestErrorSummaries(t *testing.T) {
	logger, buffer := log.MakeTestLogger()

	es := makeErrorSummaries(logger)
	es.record("first", errors.New("a1 error"), checkpoints.CheckpointStatusAnalyzed)
	es.record("second", errors.New("b2 error"), checkpoints.CheckpointStatusAllWritten)
	es.emitLog()

	lines := buffer.Lines()
	sort.Strings(lines[1:])
	require.Equal(t, []string{
		`{"$lvl":"ERROR","$msg":"tables failed to be imported","count":2}`,
		`{"$lvl":"ERROR","$msg":"-","table":"first","status":"analyzed","error":"a1 error"}`,
		`{"$lvl":"ERROR","$msg":"-","table":"second","status":"written","error":"b2 error"}`,
	}, lines)
}

func TestVerifyCheckpoint(t *testing.T) {
	dir := t.TempDir()

	cpdb := checkpoints.NewFileCheckpointsDB(filepath.Join(dir, "cp.pb"))
	defer cpdb.Close()
	ctx := context.Background()

	actualReleaseVersion := build.ReleaseVersion
	defer func() {
		build.ReleaseVersion = actualReleaseVersion
	}()

	taskCp, err := cpdb.TaskCheckpoint(ctx)
	require.NoError(t, err)
	require.Nil(t, taskCp)

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
	require.NoError(t, err)

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
	require.NoError(t, err)
	for conf, fn := range adjustFuncs {
		cfg := newCfg()
		fn(cfg)
		err := verifyCheckpoint(cfg, taskCp)
		if conf == "version" {
			build.ReleaseVersion = actualReleaseVersion
			require.Regexp(t, "lightning version is 'some newer version', but checkpoint was created at '"+actualReleaseVersion+"'.*", err.Error())
		} else {
			require.Regexp(t, fmt.Sprintf("config '%s' value '.*' different from checkpoint value .*", conf), err.Error())
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
		require.NoError(t, err)
	}
}

func TestDiskQuotaLock(t *testing.T) {
	lock := newDiskQuotaLock()

	lock.Lock()
	require.False(t, lock.TryRLock())
	lock.Unlock()
	require.True(t, lock.TryRLock())
	require.True(t, lock.TryRLock())

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
		t.Fatal("write lock is held before all read locks are released")
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

func TestPreCheckFailed(t *testing.T) {
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendTiDB
	cfg.App.CheckRequirements = false

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	g := glue.NewExternalTiDBGlue(db, mysql.ModeNone)

	ctl := &Controller{
		cfg:            cfg,
		saveCpCh:       make(chan saveCp),
		checkpointsDB:  panicCheckpointDB{},
		metaMgrBuilder: failMetaMgrBuilder{},
		checkTemplate:  NewSimpleTemplate(),
		tidbGlue:       g,
		errorMgr:       errormanager.New(nil, cfg),
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN .*").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2"))
	mock.ExpectCommit()
	// precheck failed, will not do init checkpoint.
	err = ctl.Run(context.Background())
	require.Regexp(t, ".*mock init meta failure", err.Error())
	require.NoError(t, mock.ExpectationsWereMet())

	mock.ExpectBegin()
	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN .*").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2"))
	mock.ExpectCommit()
	ctl.saveCpCh = make(chan saveCp)
	// precheck failed, will not do init checkpoint.
	err1 := ctl.Run(context.Background())
	require.Equal(t, err.Error(), err1.Error())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetTableInfoFromMeta(t *testing.T) {
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.Mydumper.DefaultFileRules = true
	cfg.Mydumper.CharacterSet = "utf8mb4"
	cfg.App.RegionConcurrency = 8

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	mockTiDBGlue := mock.NewMockGlue(gomock.NewController(t))
	mockTiDBGlue.EXPECT().GetParser().AnyTimes().Return(p)

	rc := &Controller{
		checkTemplate: NewSimpleTemplate(),
		cfg:           cfg,
		store:         store,
		tidbGlue:      mockTiDBGlue,
	}

	tableMeta := &mydump.MDTableMeta{
		DB:         "test",
		Name:       "test",
		SchemaFile: mydump.FileInfo{FileMeta: mydump.SourceFileMeta{}},
		DataFiles:  make([]mydump.FileInfo, 0),
		CharSet:    "binary",
	}

	ctx := context.Background()

	tableMeta.SchemaFile.FileMeta.Path = "not exist file"
	_, err = rc.getTableInfoFromMeta(ctx, tableMeta)
	require.Error(t, err)

	filePath := "invalid.sql"
	err = store.WriteFile(ctx, filePath, []byte("create t;"))
	require.NoError(t, err)
	tableMeta.SchemaFile.FileMeta.Path = filePath
	_, err = rc.getTableInfoFromMeta(ctx, tableMeta)
	require.Error(t, err)

	filePath = "multiple-create-table.sql"
	err = store.WriteFile(ctx, filePath, []byte("create table t(id int);create table t1(id int);"))
	require.NoError(t, err)
	tableMeta.SchemaFile.FileMeta.Path = filePath
	_, err = rc.getTableInfoFromMeta(ctx, tableMeta)
	require.Error(t, err)

	filePath = "not-create-table.sql"
	err = store.WriteFile(ctx, filePath, []byte("select 1 from t;"))
	require.NoError(t, err)
	tableMeta.SchemaFile.FileMeta.Path = filePath
	_, err = rc.getTableInfoFromMeta(ctx, tableMeta)
	require.Error(t, err)

	filePath = "create-table.sql"
	err = store.WriteFile(ctx, filePath, []byte("create table t(id int);"))
	require.NoError(t, err)
	tableMeta.SchemaFile.FileMeta.Path = filePath
	tableInfo, err := rc.getTableInfoFromMeta(ctx, tableMeta)
	require.NoError(t, err)
	require.Equal(t, tableInfo.Name.L, "t")
	require.Equal(t, tableInfo.State, model.StatePublic)
}

func TestLoadSchemaForCheckOnly(t *testing.T) {
	ctx := context.Background()
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.Mydumper.DefaultFileRules = true
	cfg.Mydumper.CharacterSet = "utf8mb4"
	cfg.App.RegionConcurrency = 8

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	mockCtrl := gomock.NewController(t)
	mockTiDBGlue := mock.NewMockGlue(mockCtrl)

	rc := &Controller{
		checkTemplate: NewSimpleTemplate(),
		cfg:           cfg,
		store:         store,
		tidbGlue:      mockTiDBGlue,
	}

	fakeDBName := "fakedb"
	fakeFileName := fmt.Sprintf("%s-schema-create.sql", fakeDBName)
	err = store.WriteFile(ctx, fakeFileName, []byte(fmt.Sprintf("CREATE DATABASE %s;", fakeDBName)))
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		fakeTableName := fmt.Sprintf("tbl%d", i)
		fakeFileName := fmt.Sprintf("%s.%s-schema.sql", fakeDBName, fakeTableName)
		fakeFileContent := fmt.Sprintf("CREATE TABLE %s(i int);", fakeTableName)
		err = store.WriteFile(ctx, fakeFileName, []byte(fakeFileContent))
		require.NoError(t, err)
	}

	mydumpLoader, err := mydump.NewMyDumpLoaderWithStore(ctx, rc.cfg, store)
	require.NoError(t, err)
	rc.dbMetas = mydumpLoader.GetDatabases()

	exec := mock.NewMockSQLExecutor(mockCtrl)
	exec.EXPECT().QueryStringsWithLog(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	mockTiDBGlue.EXPECT().GetParser().AnyTimes().Return(p)
	mockTiDBGlue.EXPECT().OwnsSQLExecutor().AnyTimes().Return(false)
	mockTiDBGlue.EXPECT().GetSQLExecutor().AnyTimes().Return(exec)

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return([]*model.TableInfo{}, nil)
	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rc.dbMetas))
	require.Equal(t, 2, len(rc.dbMetas[0].Tables))
	require.Equal(t, 0, len(rc.existedTblMap[fakeDBName]))

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return([]*model.TableInfo{
		{
			Name:  model.CIStr{O: "tbl1", L: "tbl1"},
			State: model.StatePublic,
		},
	}, nil)
	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rc.dbMetas))
	require.Equal(t, 2, len(rc.dbMetas[0].Tables))
	require.Equal(t, 1, len(rc.existedTblMap[fakeDBName]))
	require.NotNil(t, rc.existedTblMap[fakeDBName]["tbl1"])

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return([]*model.TableInfo{
		{
			Name:  model.CIStr{O: "tbl1", L: "tbl1"},
			State: model.StateNone,
		},
	}, nil)
	err = rc.loadSchemaForCheckOnly(ctx)
	require.Error(t, err)

	bak := rc.dbMetas[0].Tables[0].SchemaFile.FileMeta.Path
	rc.dbMetas[0].Tables[0].SchemaFile.FileMeta.Path = ""

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return([]*model.TableInfo{}, nil)
	err = rc.loadSchemaForCheckOnly(ctx)
	require.Error(t, err)

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return([]*model.TableInfo{}, nil)
	rc.dbMetas[0].Tables[0].SchemaFile.FileMeta.Path = "not-exist"
	err = rc.loadSchemaForCheckOnly(ctx)
	require.Error(t, err)

	rc.dbMetas[0].Tables[0].SchemaFile.FileMeta.Path = bak

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return(nil, errors.New("Unknown database "+fakeDBName))
	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rc.dbMetas))
	require.Equal(t, 2, len(rc.dbMetas[0].Tables))
	require.Equal(t, 0, len(rc.existedTblMap[fakeDBName]))

	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed"))
	err = rc.loadSchemaForCheckOnly(ctx)
	require.Error(t, err)
}
