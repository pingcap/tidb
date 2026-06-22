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

package brietest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func getBackupTempDir() string {
	if envDir := os.Getenv("BRIETEST_TMPDIR"); envDir != "" {
		return envDir
	}
	return os.TempDir()
}

func initTestKit(t *testing.T) *testkit.TestKit {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	cfg := config.GetGlobalConfig()
	cfg.Store = config.StoreTypeTiKV
	cfg.Path = "127.0.0.1:2379"
	config.StoreGlobalConfig(cfg)

	tk := testkit.NewTestKit(t, store)
	return tk
}

func TestBackupAndRestore(t *testing.T) {
	tk := initTestKit(t)
	tk.MustExec("create database if not exists br")
	tk.MustExec("use br")
	tk.MustExec("create table t1(v int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t1 values (2)")
	tk.MustExec("insert into t1 values (3)")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))

	tk.MustExec("create database if not exists br02")
	tk.MustExec("use br02")
	tk.MustExec("create table t1(v int)")

	tmpDir := path.Join(getBackupTempDir(), "bk1")
	require.NoError(t, os.RemoveAll(tmpDir))
	// backup database to tmp dir
	tk.MustQuery("backup database br to 'local://" + tmpDir + "'")

	// remove database for recovery
	tk.MustExec("drop database br")
	tk.MustExec("drop database br02")

	// restore database with backup data
	tk.MustQuery("restore database * from 'local://" + tmpDir + "'")
	tk.MustExec("use br")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))
	tk.MustExec("drop database br")
}

func TestRestoreTableModeProtectsTablesBeforePipeline(t *testing.T) {
	tk := initTestKit(t)
	executor.ResetGlobalBRIEQueueForTest()
	cleanupRestoreRegistry(tk)
	defer cleanupRestoreRegistry(tk)

	dbName := "br_restore_mode"
	tk.MustExec("drop database if exists " + dbName)
	tk.MustExec("create database " + dbName)
	defer tk.MustExec("drop database if exists " + dbName)
	tk.MustExec("create table " + dbName + ".t (id int primary key, v varchar(16))")
	tk.MustExec("insert into " + dbName + ".t values (1, 'before')")

	tmpDir := path.Join(makeTempDirForBackup(t), "backup")
	require.NoError(t, os.RemoveAll(tmpDir))
	tk.MustQuery("backup database " + dbName + " to 'local://" + tmpDir + "'")
	tk.MustExec("drop database " + dbName)

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/br/pkg/task/run-snapshot-restore-after-create-tables", func(errP *error) {
		checkTK := testkit.NewTestKit(t, tk.Session().GetStore())
		checkTK.MustGetErrCode("insert into "+dbName+".t values (2, 'during')", errno.ErrProtectedTableMode)
		checkTK.MustGetErrCode("drop table "+dbName+".t", errno.ErrProtectedTableMode)
		checkTK.MustGetErrCode("drop database "+dbName, errno.ErrProtectedTableMode)
		alterRestoredTableModeToNormal(t, checkTK, dbName, "t")
		*errP = errors.New("abort after restore creates tables")
	})

	err := tk.QueryToErr("restore database " + dbName + " from 'local://" + tmpDir + "'")
	require.ErrorContains(t, err, "abort after restore creates tables")
	tk.MustExec("drop database " + dbName)
}

func alterRestoredTableModeToNormal(t *testing.T, tk *testkit.TestKit, dbName, tableName string) {
	dom := domain.GetDomain(tk.Session())
	dbInfo, ok := dom.InfoCache().GetLatest().SchemaByName(ast.NewCIStr(dbName))
	require.True(t, ok)
	tbl, err := dom.InfoCache().GetLatest().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	require.NoError(t, err)
	require.NoError(t, ddl.AlterTableMode(dom.DDLExecutor(), tk.Session(), model.TableModeNormal, dbInfo.ID, tbl.Meta().ID))
}

func cleanupRestoreRegistry(tk *testkit.TestKit) {
	tk.MustExec(fmt.Sprintf("delete from %s.%s", registry.RestoreRegistryDBName, registry.RestoreRegistryTableName))
}

func TestRestoreMultiTables(t *testing.T) {
	tk := initTestKit(t)
	cleanupRestoreRegistry(tk)
	defer cleanupRestoreRegistry(tk)

	tk.MustExec("drop database if exists br")
	tk.MustExec("create database br")
	defer tk.MustExec("drop database if exists br")
	tk.MustExec("use br")

	tableNum := 100
	for i := 0; i < tableNum; i += 1 {
		tk.MustExec(fmt.Sprintf("create table table_%d (a int primary key, b json, c varchar(20))", i))
		tk.MustExec(fmt.Sprintf("insert into table_%d values (1, '{\"a\": 1, \"b\": 2}', '123')", i))
		tk.MustQuery(fmt.Sprintf("select count(*) from table_%d", i)).Check(testkit.Rows("1"))
	}

	tmpDir := path.Join(getBackupTempDir(), "bk1")
	require.NoError(t, os.RemoveAll(tmpDir))
	// backup database to tmp dir
	tk.MustQuery("backup database br to 'local://" + tmpDir + "'")

	// remove database for recovery
	tk.MustExec("drop database br")

	// restore database with backup data
	tk.MustQuery("restore database * from 'local://" + tmpDir + "'")
	tk.MustExec("use br")
	for i := 0; i < tableNum; i += 1 {
		tk.MustQuery(fmt.Sprintf("select count(*) from table_%d", i)).Check(testkit.Rows("1"))
	}
}
