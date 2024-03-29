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

package importer

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	tmock "github.com/pingcap/tidb/pkg/util/mock"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
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
			DB:   dbInfo.Name,
			Core: tableInfo,
		}
	}

	for _, tc := range testCases {
		tableInfo := dbInfo.Tables[tc.name]
		tableName := common.UniqueTable("mockdb", tableInfo.Name)
		tr, err := NewTableImporter(tableName, nil, dbInfo, tableInfo, &checkpoints.TableCheckpoint{}, nil, nil, nil, log.L())
		require.NotNil(t, tr)
		require.NoError(t, err)
	}
}

func TestNewTableRestoreFailure(t *testing.T) {
	tableInfo := &checkpoints.TidbTableInfo{
		Name: "failure",
		DB:   "mockdb",
		Core: &model.TableInfo{},
	}
	dbInfo := &checkpoints.TidbDBInfo{Name: "mockdb", Tables: map[string]*checkpoints.TidbTableInfo{
		"failure": tableInfo,
	}}
	tableName := common.UniqueTable("mockdb", "failure")

	_, err := NewTableImporter(tableName, nil, dbInfo, tableInfo, &checkpoints.TableCheckpoint{}, nil, nil, nil, log.L())
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
	ctx := context.Background()
	cpdb, err := checkpoints.NewFileCheckpointsDB(ctx, filepath.Join(dir, "cp.pb"))
	require.NoError(t, err)
	defer cpdb.Close()
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
		cfg.TikvImporter.Backend = config.BackendTiDB
		cfg.TikvImporter.SortedKVDir = "/tmp/sorted-kv"

		return cfg
	}

	err = cpdb.Initialize(ctx, newCfg(), map[string]*checkpoints.TidbDBInfo{})
	require.NoError(t, err)

	adjustFuncs := map[string]func(cfg *config.Config){
		"tikv-importer.backend": func(cfg *config.Config) {
			cfg.TikvImporter.Backend = config.BackendLocal
		},
		"mydumper.data-source-dir": func(cfg *config.Config) {
			cfg.Mydumper.SourceDir = "/tmp/test"
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
		require.Error(t, err)
		if conf == "version" {
			build.ReleaseVersion = actualReleaseVersion
			require.Regexp(t, "lightning version is 'some newer version', but checkpoint was created at '"+actualReleaseVersion+"'.*", err.Error())
		} else {
			require.Regexp(t, fmt.Sprintf("config '%s' value '.*' different from checkpoint value .*", conf), err.Error())
		}
	}

	// changing TiDB IP is OK
	cfg := newCfg()
	cfg.TiDB.Host = "192.168.0.1"
	err = verifyCheckpoint(cfg, taskCp)
	require.NoError(t, err)

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

	targetInfoGetter := &TargetInfoGetterImpl{
		cfg: cfg,
		db:  db,
	}
	preInfoGetter := &PreImportInfoGetterImpl{
		cfg:              cfg,
		targetInfoGetter: targetInfoGetter,
		dbMetas:          make([]*mydump.MDDatabaseMeta, 0),
	}
	cpdb := panicCheckpointDB{}
	theCheckBuilder := NewPrecheckItemBuilder(cfg, make([]*mydump.MDDatabaseMeta, 0), preInfoGetter, cpdb, nil)
	ctl := &Controller{
		cfg:                 cfg,
		saveCpCh:            make(chan saveCp),
		checkpointsDB:       cpdb,
		metaMgrBuilder:      failMetaMgrBuilder{},
		checkTemplate:       NewSimpleTemplate(),
		db:                  db,
		errorMgr:            errormanager.New(nil, cfg, log.L()),
		preInfoGetter:       preInfoGetter,
		precheckItemBuilder: theCheckBuilder,
	}

	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN .*").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2"))
	// precheck failed, will not do init checkpoint.
	err = ctl.Run(context.Background())
	require.Regexp(t, ".*mock init meta failure", err.Error())
	require.NoError(t, mock.ExpectationsWereMet())

	// clear the sys variable cache
	preInfoGetter.sysVarsCache = nil
	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN .*").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2"))
	ctl.saveCpCh = make(chan saveCp)
	// precheck failed, will not do init checkpoint.
	err1 := ctl.Run(context.Background())
	require.Equal(t, err.Error(), err1.Error())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAddExtendDataForCheckpoint(t *testing.T) {
	cfg := config.NewConfig()

	cfg.Mydumper.SourceID = "mysql-01"
	cfg.Routes = []*router.TableRule{
		{
			TableExtractor: &router.TableExtractor{
				TargetColumn: "c_table",
				TableRegexp:  "t(.*)",
			},
			SchemaExtractor: &router.SchemaExtractor{
				TargetColumn: "c_schema",
				SchemaRegexp: "test_(.*)",
			},
			SourceExtractor: &router.SourceExtractor{
				TargetColumn: "c_source",
				SourceRegexp: "mysql-(.*)",
			},
			SchemaPattern: "test_*",
			TablePattern:  "t*",
			TargetSchema:  "test",
			TargetTable:   "t",
		},
	}

	cp := &checkpoints.TableCheckpoint{
		Engines: map[int32]*checkpoints.EngineCheckpoint{
			-1: {
				Status: checkpoints.CheckpointStatusLoaded,
				Chunks: []*checkpoints.ChunkCheckpoint{},
			},
			0: {
				Status: checkpoints.CheckpointStatusImported,
				Chunks: []*checkpoints.ChunkCheckpoint{{
					FileMeta: mydump.SourceFileMeta{
						Path: "tmp/test_1.t1.000000000.sql",
					},
				}, {
					FileMeta: mydump.SourceFileMeta{
						Path: "./test/tmp/test_1.t2.000000000.sql",
					},
				}, {
					FileMeta: mydump.SourceFileMeta{
						Path: "test_2.t3.000000000.sql",
					},
				}},
			},
		},
	}
	require.NoError(t, addExtendDataForCheckpoint(context.Background(), cfg, cp))
	expectExtendCols := []string{"c_table", "c_schema", "c_source"}
	expectedExtendVals := [][]string{
		{"1", "1", "01"},
		{"2", "1", "01"},
		{"3", "2", "01"},
	}
	chunks := cp.Engines[0].Chunks
	require.Len(t, chunks, 3)
	for i, chunk := range chunks {
		require.Equal(t, expectExtendCols, chunk.FileMeta.ExtendData.Columns)
		require.Equal(t, expectedExtendVals[i], chunk.FileMeta.ExtendData.Values)
	}
}

func TestFilterColumns(t *testing.T) {
	p := parser.New()
	se := tmock.NewContext()

	testCases := []struct {
		columnNames    []string
		extendData     mydump.ExtendColumnData
		ignoreColsMap  map[string]struct{}
		createTableSQL string

		expectedFilteredColumns []string
		expectedExtendValues    []string
	}{
		{
			[]string{"a", "b"},
			mydump.ExtendColumnData{},
			nil,
			"CREATE TABLE t (a int primary key, b int)",
			[]string{"a", "b"},
			[]string{},
		},
		{
			[]string{},
			mydump.ExtendColumnData{},
			nil,
			"CREATE TABLE t (a int primary key, b int)",
			[]string{},
			[]string{},
		},
		{
			columnNames: []string{"a", "b"},
			extendData: mydump.ExtendColumnData{
				Columns: []string{"c_source", "c_schema", "c_table"},
				Values:  []string{"01", "1", "1"},
			},
			createTableSQL:          "CREATE TABLE t (a int primary key, b int, c_source varchar(11), c_schema varchar(11), c_table varchar(11))",
			expectedFilteredColumns: []string{"a", "b", "c_source", "c_schema", "c_table"},
			expectedExtendValues:    []string{"01", "1", "1"},
		},
		{
			columnNames: []string{},
			extendData: mydump.ExtendColumnData{
				Columns: []string{"c_source", "c_schema", "c_table"},
				Values:  []string{"01", "1", "1"},
			},
			createTableSQL:          "CREATE TABLE t (a int primary key, b int, c_source varchar(11), c_schema varchar(11), c_table varchar(11))",
			expectedFilteredColumns: []string{"a", "b", "c_source", "c_schema", "c_table"},
			expectedExtendValues:    []string{"01", "1", "1"},
		},
		{
			[]string{"a", "b"},
			mydump.ExtendColumnData{},
			map[string]struct{}{"a": {}},
			"CREATE TABLE t (a int primary key, b int)",
			[]string{"b"},
			[]string{},
		},
		{
			[]string{},
			mydump.ExtendColumnData{},
			map[string]struct{}{"a": {}},
			"CREATE TABLE t (a int primary key, b int)",
			[]string{"b"},
			[]string{},
		},
		{
			columnNames: []string{"a", "b"},
			extendData: mydump.ExtendColumnData{
				Columns: []string{"c_source", "c_schema", "c_table"},
				Values:  []string{"01", "1", "1"},
			},
			ignoreColsMap:           map[string]struct{}{"a": {}},
			createTableSQL:          "CREATE TABLE t (a int primary key, b int, c_source varchar(11), c_schema varchar(11), c_table varchar(11))",
			expectedFilteredColumns: []string{"b", "c_source", "c_schema", "c_table"},
			expectedExtendValues:    []string{"01", "1", "1"},
		},
	}
	for i, tc := range testCases {
		t.Logf("test case #%d", i)
		node, err := p.ParseOneStmt(tc.createTableSQL, "utf8mb4", "utf8mb4_bin")
		require.NoError(t, err)
		tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(i+1))
		require.NoError(t, err)
		tableInfo.State = model.StatePublic

		expectedDatums := make([]types.Datum, 0, len(tc.expectedExtendValues))
		for _, expectedValue := range tc.expectedExtendValues {
			expectedDatums = append(expectedDatums, types.NewStringDatum(expectedValue))
		}

		filteredColumns, extendDatums := filterColumns(tc.columnNames, tc.extendData, tc.ignoreColsMap, tableInfo)
		require.Equal(t, tc.expectedFilteredColumns, filteredColumns)
		require.Equal(t, expectedDatums, extendDatums)
	}
}

func TestInitGlobalConfig(t *testing.T) {
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCA)
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCert)
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLKey)
	initGlobalConfig(tikvconfig.Security{})
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCA)
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCert)
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLKey)

	initGlobalConfig(tikvconfig.Security{
		ClusterSSLCA: "ca",
	})
	require.NotEmpty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCA)
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCert)
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLKey)

	initGlobalConfig(tikvconfig.Security{})
	initGlobalConfig(tikvconfig.Security{
		ClusterSSLCert: "cert",
		ClusterSSLKey:  "key",
	})
	require.Empty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCA)
	require.NotEmpty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLCert)
	require.NotEmpty(t, tikvconfig.GetGlobalConfig().Security.ClusterSSLKey)
}
