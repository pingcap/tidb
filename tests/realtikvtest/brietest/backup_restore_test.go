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
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func initTestKit(t *testing.T) *testkit.TestKit {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("only run BR SQL integration test with tikv store")
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)

	cfg := config.GetGlobalConfig()
	cfg.Store = "tikv"
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

	tmpDir := path.Join(os.TempDir(), "bk1")
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

func TestRestoreMultiTables(t *testing.T) {
	tk := initTestKit(t)
	tk.MustExec("create database if not exists br")
	tk.MustExec("use br")

	tablesNameSet := make(map[string]struct{})
	tableNum := 100
	for i := 0; i < tableNum; i += 1 {
		tk.MustExec(fmt.Sprintf("create table table_%d (a int primary key, b json, c varchar(20))", i))
		tk.MustExec(fmt.Sprintf("insert into table_%d values (1, '{\"a\": 1, \"b\": 2}', '123')", i))
		tk.MustQuery(fmt.Sprintf("select count(*) from table_%d", i)).Check(testkit.Rows("1"))
		tablesNameSet[fmt.Sprintf("table_%d", i)] = struct{}{}
	}

	tmpDir := path.Join(os.TempDir(), "bk1")
	require.NoError(t, os.RemoveAll(tmpDir))
	// backup database to tmp dir
	tk.MustQuery("backup database br to 'local://" + tmpDir + "'")

	// remove database for recovery
	tk.MustExec("drop database br")

	// restore database with backup data
	tk.MustQuery("restore database * from 'local://" + tmpDir + "'")
	tk.MustExec("use br")
	ddlCreateTablesRows := tk.MustQuery("admin show ddl jobs where JOB_TYPE = 'create tables'").Rows()
	cnt := 0
	for _, row := range ddlCreateTablesRows {
		tables := row[2].(string)
		require.NotEqual(t, "", tables)
		for _, table := range strings.Split(tables, ",") {
			_, ok := tablesNameSet[table]
			require.True(t, ok)
			cnt += 1
		}
	}
	require.Equal(t, tableNum, cnt)
	for i := 0; i < tableNum; i += 1 {
		tk.MustQuery(fmt.Sprintf("select count(*) from table_%d", i)).Check(testkit.Rows("1"))
	}
	tk.MustExec("drop database br")
}
