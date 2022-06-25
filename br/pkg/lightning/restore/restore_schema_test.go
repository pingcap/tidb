// Copyright 2022 PingCAP, Inc.
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
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	tmock "github.com/pingcap/tidb/util/mock"
)

type restoreSchemaSuite struct {
	suite.Suite
	ctx        context.Context
	rc         *Controller
	controller *gomock.Controller
	tableInfos []*model.TableInfo
}

func TestRestoreSchemaSuite(t *testing.T) {
	suite.Run(t, new(restoreSchemaSuite))
}

func (s *restoreSchemaSuite) SetupSuite() {
	ctx := context.Background()
	fakeDataDir := s.T().TempDir()

	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(s.T(), err)
	// restore database schema file
	fakeDBName := "fakedb"
	// please follow the `mydump.defaultFileRouteRules`, matches files like '{schema}-schema-create.sql'
	fakeFileName := fmt.Sprintf("%s-schema-create.sql", fakeDBName)
	err = store.WriteFile(ctx, fakeFileName, []byte(fmt.Sprintf("CREATE DATABASE %s;", fakeDBName)))
	require.NoError(s.T(), err)
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
		require.NoError(s.T(), err)

		node, err := p.ParseOneStmt(fakeFileContent, "", "")
		require.NoError(s.T(), err)
		core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
		require.NoError(s.T(), err)
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
		require.NoError(s.T(), err)
	}
	config := config.NewConfig()
	config.Mydumper.DefaultFileRules = true
	config.Mydumper.CharacterSet = "utf8mb4"
	config.App.RegionConcurrency = 8
	mydumpLoader, err := mydump.NewMyDumpLoaderWithStore(ctx, config, store)
	require.NoError(s.T(), err)
	s.rc = &Controller{
		checkTemplate: NewSimpleTemplate(),
		cfg:           config,
		store:         store,
		dbMetas:       mydumpLoader.GetDatabases(),
		checkpointsDB: &checkpoints.NullCheckpointsDB{},
	}
}

//nolint:interfacer // change test case signature might cause Check failed to find this test case?
func (s *restoreSchemaSuite) SetupTest() {
	s.controller, s.ctx = gomock.WithContext(context.Background(), s.T())
	mockBackend := mock.NewMockBackend(s.controller)
	mockBackend.EXPECT().
		FetchRemoteTableModels(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(s.tableInfos, nil)
	mockBackend.EXPECT().Close()
	s.rc.backend = backend.MakeBackend(mockBackend)

	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(s.T(), err)
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

func (s *restoreSchemaSuite) TearDownTest() {
	exec := mock.NewMockSQLExecutor(s.controller)
	exec.EXPECT().Close()
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetSQLExecutor().
		AnyTimes().
		Return(exec)
	s.rc.tidbGlue = mockTiDBGlue

	s.rc.Close()
	s.controller.Finish()
}

func (s *restoreSchemaSuite) TestRestoreSchemaSuccessful() {
	// before restore, if sysVars is initialized by other test, the time_zone should be default value
	if len(s.rc.sysVars) > 0 {
		tz, ok := s.rc.sysVars["time_zone"]
		require.True(s.T(), ok)
		require.Equal(s.T(), "SYSTEM", tz)
	}

	exec := mock.NewMockSQLExecutor(s.controller)
	exec.EXPECT().
		QueryStringsWithLog(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return([][]string{{"time_zone", "SYSTEM"}}, nil)
	mockTiDBGlue := s.rc.tidbGlue.(*mock.MockGlue)
	mockTiDBGlue.EXPECT().
		GetSQLExecutor().
		AnyTimes().
		Return(exec)

	s.rc.cfg.TiDB.Vars = map[string]string{
		"time_zone": "UTC",
	}
	err := s.rc.restoreSchema(s.ctx)
	require.NoError(s.T(), err)

	// test after restore schema, sysVars has been updated
	tz, ok := s.rc.sysVars["time_zone"]
	require.True(s.T(), ok)
	require.Equal(s.T(), "UTC", tz)
}

func (s *restoreSchemaSuite) TestRestoreSchemaFailed() {
	// use injectErr which cannot be retried
	injectErr := stderrors.New("could not match actual sql")
	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(s.T(), err)
	sqlMock.ExpectExec(".*").WillReturnError(injectErr)
	for i := 0; i < 16; i++ {
		sqlMock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(int64(i), 1))
	}

	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetDB().
		AnyTimes().
		Return(mockDB, nil)
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
	err = s.rc.restoreSchema(s.ctx)
	require.Error(s.T(), err)
	require.True(s.T(), errors.ErrorEqual(err, injectErr))
}

// When restoring a CSV with `-no-schema` and the target table doesn't exist
// then we can't restore the schema as the `Path` is empty. This is to make
// sure this results in the correct error.
// https://github.com/pingcap/br/issues/1394
func (s *restoreSchemaSuite) TestNoSchemaPath() {
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
	require.Error(s.T(), err)
	require.Regexp(s.T(), `table .* schema not found`, err.Error())
	s.rc.dbMetas[0].Tables = s.rc.dbMetas[0].Tables[:len(s.rc.dbMetas[0].Tables)-1]
}

func (s *restoreSchemaSuite) TestRestoreSchemaContextCancel() {
	childCtx, cancel := context.WithCancel(s.ctx)
	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(s.T(), err)
	for i := 0; i < 17; i++ {
		sqlMock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(int64(i), 1))
	}
	mockTiDBGlue := mock.NewMockGlue(s.controller)
	mockTiDBGlue.EXPECT().
		GetDB().
		AnyTimes().
		Do(func() { cancel() }).
		Return(mockDB, nil)
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
	err = s.rc.restoreSchema(childCtx)
	cancel()
	require.Error(s.T(), err)
	require.Equal(s.T(), childCtx.Err(), err)
}
