// Copyright 2018 PingCAP, Inc.
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

package sessiontest

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestFailStatementCommitInRetry(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockCommitError8942", `return(true)`))
	_, err := tk.Exec("commit")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError8942"))

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}

func TestGetTSFailDirtyState(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (id int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockGetTSFail", "return"))
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/pingcap/tidb/pkg/session/mockGetTSFail"
	})
	_, err := tk.Session().Execute(ctx, "select * from t")
	if config.GetGlobalConfig().Store == "unistore" {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	// Fix a bug that active txn fail set TxnState.fail to error, and then the following write
	// affected by this fail flag.
	tk.MustExec("insert into t values (1)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockGetTSFail"))
}

func TestGetTSFailDirtyStateInretry(t *testing.T) {
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError"))
		require.NoError(t, failpoint.Disable("tikvclient/mockGetTSErrorInRetry"))
	}()

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (id int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockCommitError", `return(true)`))
	// This test will mock a PD timeout error, and recover then.
	// Just make mockGetTSErrorInRetry return true once, and then return false.
	require.NoError(t, failpoint.Enable("tikvclient/mockGetTSErrorInRetry",
		`1*return(true)->return(false)`))
	tk.MustExec("insert into t values (2)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2"))
}

func TestKillFlagInBackoff(t *testing.T) {
	// This test checks the `killed` flag is passed down to the backoffer through
	// session.KVVars.
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table kill_backoff (id int)")
	// Inject 1 time timeout. If `Killed` is not successfully passed, it will retry and complete query.
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `sleep(1000)->return("timeout")->return("")`))
	defer failpoint.Disable("tikvclient/tikvStoreSendReqResult")
	// Set kill flag and check its passed to backoffer.
	go func() {
		time.Sleep(300 * time.Millisecond)
		tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}()
	rs, err := tk.Exec("select * from kill_backoff")
	require.NoError(t, err)
	_, err = session.ResultSetToStringSlice(context.TODO(), tk.Session(), rs)
	// `interrupted` is returned when `Killed` is set.
	require.Regexp(t, ".*Query execution was interrupted.*", err.Error())
	rs.Close()
}

func TestClusterTableSendError(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("requestTiDBStoreError")`))
	defer func() { require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult")) }()
	tk.MustQuery("select * from information_schema.cluster_slow_query")
	require.Equal(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(1))
	require.Regexp(t, ".*TiDB server timeout, address is.*", tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error())
}

func TestAutoCommitNeedNotLinearizability(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t1;")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (c int)`)

	require.NoError(t, failpoint.Enable("tikvclient/getMinCommitTSFromTSO", `panic`))
	defer func() { require.NoError(t, failpoint.Disable("tikvclient/getMinCommitTSFromTSO")) }()

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_async_commit", "1"))
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_guarantee_linearizability", "1"))

	// Auto-commit transactions don't need to get minCommitTS from TSO
	tk.MustExec("INSERT INTO t1 VALUES (1)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (2)")
	// An explicit transaction needs to get minCommitTS from TSO
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (3)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	// Same for 1PC
	tk.MustExec("set autocommit = 1")
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_1pc", "1"))
	tk.MustExec("INSERT INTO t1 VALUES (4)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (5)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (6)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()
}

func TestKill(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("kill connection_id();")
}

func TestIssue42426(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `sbtest1` (" +
		"`id` bigint(20) NOT NULL AUTO_INCREMENT," +
		"`k` int(11) NOT NULL DEFAULT '0'," +
		"`c` char(120) NOT NULL DEFAULT ''," +
		"`pad` char(60) NOT NULL DEFAULT ''," +
		"PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */," +
		"KEY `k_1` (`k`)" +
		") PARTITION BY RANGE (`id`)" +
		"(PARTITION `pnew` VALUES LESS THAN (10000000)," +
		"PARTITION `p5` VALUES LESS THAN (MAXVALUE));")
	tk.MustExec(`INSERT INTO sbtest1 (id, k, c, pad) VALUES (502571, 499449, "init", "val");`)
	tk.MustExec(`BEGIN`)
	tk.MustExec(`DELETE FROM sbtest1 WHERE id=502571;`)
	tk.MustExec(`INSERT INTO sbtest1 (id, k, c, pad) VALUES (502571, 499449, "abc", "def");`)
	tk.MustExec(`COMMIT;`)
}

// for https://github.com/pingcap/tidb/issues/44123
func TestIndexLookUpWithStaticPrune(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint, b decimal(41,16), c set('a', 'b', 'c'), key idx_c(c)) partition by hash(a) partitions 4")
	tk.MustExec("insert into t values (1,2.0,'c')")
	tk.MustHavePlan("select * from t use index(idx_c) order by c limit 5", "Limit")
	tk.MustExec("select * from t use index(idx_c) order by c limit 5")
}

func TestTiKVClientReadTimeout(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("skip test since it's only work for tikv")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")

	rows := tk.MustQuery("select count(*) from information_schema.cluster_info where `type`='tikv';").Rows()
	require.Len(t, rows, 1)
	tikvCount, err := strconv.Atoi(rows[0][0].(string))
	require.NoError(t, err)
	if tikvCount < 3 {
		t.Skip("skip test since it's only work for tikv with at least 3 node")
	}

	require.NoError(t, failpoint.Enable("tikvclient/mockBatchClientSendDelay", "return(100)"))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/mockBatchClientSendDelay"))
	}()
	tk.MustExec("set @stale_read_ts_var=now(6);")

	// Test for point_get request
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t where a = 1").Rows()
	require.Len(t, rows, 1)
	explain := fmt.Sprintf("%v", rows[0])
	// num_rpc is 4 because there are 3 replica, and first try all 3 replicas with specified timeout will failed, then try again with default timeout will success.
	require.Regexp(t, ".*Point_Get.* Get:{num_rpc:4, total_time:.*", explain)

	// Test for batch_point_get request
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t where a in (1,2)").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*Batch_Point_Get.* BatchGet:{num_rpc:4, total_time:.*", explain)

	// Test for cop request
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:4.*", explain)

	// Test for stale read.
	tk.MustExec("insert into t values (1,1), (2,2);")
	tk.MustExec("set @@tidb_replica_read='closest-replicas';")
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t as of timestamp(@stale_read_ts_var) where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:(3|4|5).*", explain)

	// Test for tikv_client_read_timeout session variable.
	tk.MustExec("set @@tikv_client_read_timeout=1;")
	// Test for point_get request
	rows = tk.MustQuery("explain analyze select * from t where a = 1").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	// num_rpc is 4 because there are 3 replica, and first try all 3 replicas with specified timeout will failed, then try again with default timeout will success.
	require.Regexp(t, ".*Point_Get.* Get:{num_rpc:4, total_time:.*", explain)

	// Test for batch_point_get request
	rows = tk.MustQuery("explain analyze select * from t where a in (1,2)").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*Batch_Point_Get.* BatchGet:{num_rpc:4, total_time:.*", explain)

	// Test for cop request
	rows = tk.MustQuery("explain analyze select * from t where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:4.*", explain)

	// Test for stale read.
	tk.MustExec("set @@tidb_replica_read='closest-replicas';")
	rows = tk.MustQuery("explain analyze select * from t as of timestamp(@stale_read_ts_var) where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:(3|4|5).*", explain)
}
