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

package pipelineddmltest

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.Equal(t, tk.Session().GetSessionVars().BulkDMLEnabled, 0)
	tk.MustExec("set session tidb_dml_type = bulk")
	require.Equal(t, tk.Session().GetSessionVars().BulkDMLEnabled, 1)
	tk.MustExec("set session tidb_dml_type = standard")
	require.Equal(t, tk.Session().GetSessionVars().BulkDMLEnabled, 0)
	// not supportegod yet.
	tk.MustExecToErr("set session tidb_dml_type = bulk(10)")
}

func compareTables(t *testing.T, tk *testkit.TestKit, t1, t2 string) {
	t1Rows := tk.MustQuery("select * from " + t1).Sort().Rows()
	t2Rows := tk.MustQuery("select * from " + t2)
	require.Equal(t, len(t1Rows), len(t2Rows.Rows()))
	t2Rows.Sort().Check(t1Rows)
}

func prepareData(tk *testkit.TestKit) {
	tk.MustExec("drop table if exists t, _t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("create table _t like t")
	results := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
		results = append(results, fmt.Sprintf("%d %d", i, i))
	}
	tk.MustQuery("select * from t order by a asc").Check(testkit.Rows(results...))
}

func TestPipelinedDMLInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into _t select * from t")
	compareTables(t, tk, "t", "_t")

	tk.MustExec("insert into t select a + 10000, b from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(10000))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("20000"))

	// simulate multi regions by splitting table.
	tk.MustExec("truncate table _t")
	tk.MustQuery("split table _t between (0) and (10000) regions 10").Check(testkit.Rows("9 1"))
	tk.MustQuery("select count(1) from _t").Check(testkit.Rows("0"))
	tk.MustExec("insert into _t select * from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(20000))
	compareTables(t, tk, "t", "_t")
}

func TestPipelinedDMLDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("delete from t where a % 2 = 0")
	require.Equal(t, tk.Session().AffectedRows(), uint64(5000))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("5000"))

	// simulate multi regions by splitting table.
	tk.MustQuery("split table t between (0) and (10000) regions 10").Check(testkit.Rows("9 1"))
	tk.MustExec("delete from t where a % 2 = 1")
	require.Equal(t, tk.Session().AffectedRows(), uint64(5000))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("0"))
}

func TestPipelinedDMLUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("49995000"))
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("update t set b = b + 1")
	require.Equal(t, tk.Session().AffectedRows(), uint64(10000))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("50005000"))

	// simulate multi regions by splitting table.
	tk.MustQuery("split table t between (0) and (10000) regions 10").Check(testkit.Rows("9 1"))
	tk.MustExec("update t set b = b + 1")
	require.Equal(t, tk.Session().AffectedRows(), uint64(10000))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("50015000"))
}

func TestPipelinedDMLCommitFailed(t *testing.T) {
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedCommitFail", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/pipelinedCommitFail"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExecToErr("insert into _t select * from t")
	// TODO: fix the affected rows
	//require.Equal(t, tk.Session().AffectedRows(), uint64(0))
	tk1.MustQuery("select * from _t").Check(testkit.Rows())

	tk.MustExecToErr("insert into t select a + 10000, b from t")
	//require.Equal(t, tk.Session().AffectedRows(), uint64(0))
	tk1.MustQuery("select count(1) from t").Check(testkit.Rows("10000"))
}

func TestPipelinedDMLCommitSkipSecondaries(t *testing.T) {
	require.NoError(t, failpoint.Enable("tikvclient/pipelinedSkipResolveLock", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/pipelinedSkipResolveLock"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	prepareData(tk)
	tk.MustExec("set session tidb_dml_type = bulk")
	tk.MustExec("insert into _t select * from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(10000))
	compareTables(t, tk, "t", "_t")

	tk.MustExec("insert into t select a + 10000, b from t")
	require.Equal(t, tk.Session().AffectedRows(), uint64(10000))
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("20000"))
}
