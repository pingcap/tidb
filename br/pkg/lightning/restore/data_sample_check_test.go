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
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

func TestDataSampleCheckDoCheck(t *testing.T) {
	ctx := context.Background()
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.Mydumper.DefaultFileRules = true
	cfg.Mydumper.CharacterSet = "utf8mb4"
	cfg.Mydumper.ReadBlockSize = 1024
	cfg.App.RegionConcurrency = 8
	cfg.CheckOnly = &config.CheckOnly{
		Rate: 0.01,
		Rows: 1,
	}

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	mockCtrl := gomock.NewController(t)
	mockTiDBGlue := mock.NewMockGlue(mockCtrl)
	exec := mock.NewMockSQLExecutor(mockCtrl)
	exec.EXPECT().QueryStringsWithLog(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	mockTiDBGlue.EXPECT().GetParser().AnyTimes().Return(p)
	mockTiDBGlue.EXPECT().OwnsSQLExecutor().AnyTimes().Return(false)
	mockTiDBGlue.EXPECT().GetSQLExecutor().AnyTimes().Return(exec)
	mockTiDBGlue.EXPECT().GetTables(gomock.Any(), gomock.Any()).AnyTimes().Return([]*model.TableInfo{}, nil)

	rc := &Controller{
		checkTemplate: NewSimpleTemplate(),
		cfg:           cfg,
		store:         store,
		tidbGlue:      mockTiDBGlue,
		ioWorkers:     worker.NewPool(context.Background(), 1, "io"),
	}

	fakeDBName := "fakedb"
	fakeFileName := fmt.Sprintf("%s-schema-create.sql", fakeDBName)
	err = store.WriteFile(ctx, fakeFileName, []byte(fmt.Sprintf("CREATE DATABASE %s;", fakeDBName)))
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		fakeTableName := fmt.Sprintf("tbl%d", i)
		fakeFileName := fmt.Sprintf("%s.%s-schema.sql", fakeDBName, fakeTableName)
		fakeFileContent := fmt.Sprintf("CREATE TABLE %s(i varchar(100), j varbinary(100), k int);", fakeTableName)
		err = store.WriteFile(ctx, fakeFileName, []byte(fakeFileContent))
		require.NoError(t, err)
	}
	tbl1 := "tbl1"
	tbl1FileZero := fmt.Sprintf("%s.%s.000000000.sql", fakeDBName, tbl1)
	// 4 lines, 1 ok, 2 invalid utf-8, 3 more column, 4 less column
	fakeFileContent := fmt.Sprintf("insert into `%s` values\n('1', '1', 1),\n('\xd6\xd0', x'd6d0', 2),\n('2','2', 2, 2),\n('2','2');\n", tbl1)
	err = store.WriteFile(ctx, tbl1FileZero, []byte(fakeFileContent))
	require.NoError(t, err)

	mydumpLoader, err := mydump.NewMyDumpLoaderWithStore(ctx, rc.cfg, store)
	require.NoError(t, err)
	rc.dbMetas = mydumpLoader.GetDatabases()

	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)

	rc.checkTemplate = NewSimpleTemplate()
	check := newDataSampleCheck(rc)

	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.True(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(1), check.totalRows.Load())
	require.Equal(t, int64(0), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 2
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(2), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 3
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(3), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(1), check.totalColumnCountMismatchRows.Load())

	// more lines than file
	cfg.CheckOnly.Rows = 1000
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(4), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(2), check.totalColumnCountMismatchRows.Load())

	//
	// write a invalid sql file
	tbl1FileOne := fmt.Sprintf("%s.%s.000000001.sql", fakeDBName, tbl1)
	err = store.WriteFile(ctx, tbl1FileOne, []byte("invalid sql"))
	require.NoError(t, err)
	mydumpLoader, err = mydump.NewMyDumpLoaderWithStore(ctx, rc.cfg, store)
	require.NoError(t, err)
	rc.dbMetas = mydumpLoader.GetDatabases()
	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)

	rc.cfg.CheckOnly.Rate = 1
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.Error(t, err)

	//
	// check for csv, invalid gbk while it's actually utf-8
	require.NoError(t, os.Remove(path.Join(fakeDataDir, tbl1FileZero)))
	require.NoError(t, os.Remove(path.Join(fakeDataDir, tbl1FileOne)))

	cfg.Mydumper.CSV.Header = false
	cfg.Mydumper.DataCharacterSet = "gbk"

	tbl1FileZero = fmt.Sprintf("%s.%s.000000000.csv", fakeDBName, tbl1)
	// 4 lines, 1 ok, 2 invalid gbk, 3 binary string contains utf-8 encoded str, 4 more column
	err = store.WriteFile(ctx, tbl1FileZero, []byte("\"1\",\"1\",1\n\"中\",\"x\",1\n\"x\",\"中\",1\n\"2\",\"2\",2,2\n"))
	require.NoError(t, err)

	mydumpLoader, err = mydump.NewMyDumpLoaderWithStore(ctx, rc.cfg, store)
	require.NoError(t, err)
	rc.dbMetas = mydumpLoader.GetDatabases()
	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)

	cfg.CheckOnly.Rows = 1
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.True(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(1), check.totalRows.Load())
	require.Equal(t, int64(0), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 2
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(2), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 3
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(3), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 4
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(4), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(1), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = -1
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(4), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(1), check.totalColumnCountMismatchRows.Load())

	//
	// csv with header, fewer columns than ddl, columns out of order
	require.NoError(t, os.Remove(path.Join(fakeDataDir, tbl1FileZero)))
	cfg.Mydumper.CSV.Header = true
	tbl1FileZero = fmt.Sprintf("%s.%s.000000000.csv", fakeDBName, tbl1)
	// 4 lines, 1 ok, 2 invalid gbk, 3 binary string contains utf-8 encoded str, 4 more column
	err = store.WriteFile(ctx, tbl1FileZero, []byte("\"k\",\"i\"\n1,\"1\"\n2,\"中\"\n3,\"中\",1\n4\n"))
	require.NoError(t, err)

	mydumpLoader, err = mydump.NewMyDumpLoaderWithStore(ctx, rc.cfg, store)
	require.NoError(t, err)
	rc.dbMetas = mydumpLoader.GetDatabases()
	err = rc.loadSchemaForCheckOnly(ctx)
	require.NoError(t, err)

	cfg.CheckOnly.Rows = 1
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.True(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(1), check.totalRows.Load())
	require.Equal(t, int64(0), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 2
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(2), check.totalRows.Load())
	require.Equal(t, int64(1), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(0), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = 3
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(3), check.totalRows.Load())
	require.Equal(t, int64(2), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(1), check.totalColumnCountMismatchRows.Load())

	cfg.CheckOnly.Rows = -1
	rc.checkTemplate = NewSimpleTemplate()
	check = newDataSampleCheck(rc)
	err = check.doCheck(ctx)
	require.NoError(t, err)
	require.False(t, check.controller.checkTemplate.Success())
	require.Equal(t, int64(4), check.totalRows.Load())
	require.Equal(t, int64(2), check.totalInvalidCharRows.Load())
	require.Equal(t, int64(2), check.totalColumnCountMismatchRows.Load())
}

func TestDataSampleCheckGetRandomDataFiles(t *testing.T) {
	cfg := config.NewConfig()
	cfg.CheckOnly = &config.CheckOnly{
		Rate: 0.01,
		Rows: 100,
	}
	controller := &Controller{
		cfg: cfg,
	}
	check := newDataSampleCheck(controller)
	files := check.getRandomDataFiles()
	require.Equal(t, 0, len(files))

	controller.dbMetas = []*mydump.MDDatabaseMeta{
		{
			Name: "db1",
			Tables: []*mydump.MDTableMeta{
				{
					Name:      "tbl1",
					DataFiles: []mydump.FileInfo{{}, {}},
				},
				{
					Name:      "tbl2",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
	}
	files = check.getRandomDataFiles()
	require.Equal(t, 1, len(files))

	check.checkCfg.Rate = 1
	files = check.getRandomDataFiles()
	require.Equal(t, 3, len(files))
}
