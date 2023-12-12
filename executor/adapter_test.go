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

package executor_test

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestQueryTime(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	costTime := time.Since(tk.Session().GetSessionVars().StartTime)
	require.Less(t, costTime, time.Second)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (1), (1), (1), (1)")
	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")

	costTime = time.Since(tk.Session().GetSessionVars().StartTime)
	require.Less(t, costTime, time.Second)
}

func TestFormatSQL(t *testing.T) {
	val := executor.FormatSQL("aaaa")
	require.Equal(t, "aaaa", val.String())
	variable.QueryLogMaxLen.Store(0)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaa", val.String())
	variable.QueryLogMaxLen.Store(5)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaa(len:20)", val.String())
}

func TestRecordHistoryStats(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/fakeHistoryStatsNeedRecord", `return(true)`))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_optimizer_history_stats=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (10, 200), (20, 150), (30, 100), (40, 50)")
	tk.MustExec("analyze table t")

	// tidb_optimizer_history_stats is off, no recording
	require.Len(t, tk.MustQuery("select * from t where a >= 30 and b >= 100").Rows(), 1)
	testKit.MustQuery("select count(*) from mysql.predicate_stats").Check(testkit.Rows("0"))

	// Not in select statement, no recording
	tk.MustExec("set tidb_optimizer_history_stats=on")
	tk.MustExec("update t set b = 100 where a >= 30 and b >= 100")
	testKit.MustQuery("select count(*) from mysql.predicate_stats").Check(testkit.Rows("0"))

	// Normal record
	require.Len(t, tk.MustQuery("select * from t where a >= 30 and b >= 100").Rows(), 1)
	testKit.MustQuery("select count, predicate_selectivity from mysql.predicate_stats").Check(testkit.Rows("1 0.25"))

	// Update record
	tk.MustExec("insert into t values (60, 250)")
	require.Len(t, tk.MustQuery("select * from t where a >= 30 and b >= 100").Rows(), 2)
	testKit.MustQuery("select count, predicate_selectivity from mysql.predicate_stats").Check(testkit.Rows("2 0.4"))

	// RestrictedSQL, no recording
	testKit.MustQuery("select count(*) from mysql.predicate_stats where count > 0").Check(testkit.Rows("1"))
	testKit.MustQuery("select count, predicate_selectivity from mysql.predicate_stats where count > 0").Check(testkit.Rows("2 0.4"))

	failpoint.Disable("github.com/pingcap/tidb/executor/fakeHistoryStatsNeedRecord")
}
