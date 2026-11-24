// Copyright 2026 PingCAP, Inc.
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

package copr_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	copr_metrics "github.com/pingcap/tidb/pkg/store/copr/metrics"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	promtestutils "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const mockCopCacheFP = "github.com/pingcap/tidb/pkg/store/mockstore/unistore/cophandler/mockCopCacheInUnistore"

// newMaxKeysReadTestKit creates a unistore-backed session whose coprocessor
// cache admits every response, so cache behavior is deterministic.
func newMaxKeysReadTestKit(t *testing.T) (*testkit.TestKit, *domain.Domain, testutils.Cluster) {
	if kerneltype.IsNextGen() {
		t.Skip("tidb_max_keys_read is not supported in NextGen")
	}
	// Clone the current global config, so the settings TestMain applied
	// (e.g. the async-commit safe window) are kept.
	originConfig := config.GetGlobalConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.CoprCache.AdmissionMinProcessMs = 0
	})
	t.Cleanup(func() { config.StoreGlobalConfig(originConfig) })

	var cluster testutils.Cluster
	store, dom := testkit.CreateMockStoreAndDomain(t,
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cluster = c
		}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.cte_max_recursion_depth = 100000")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 1")
	tk.MustExec("set @@session.tidb_store_batch_size = 0")
	tk.MustExec("set session transaction isolation level repeatable read")
	return tk, dom, cluster
}

// createSeqTable creates table name with rows (id, v) = (i, i) for i in 1..n.
func createSeqTable(tk *testkit.TestKit, name string, n int) {
	tk.MustExec(fmt.Sprintf("create table %s (id bigint primary key clustered, v bigint not null)", name))
	tk.MustExec(fmt.Sprintf(
		"insert into %s with recursive s(n) as (select 1 union all select n+1 from s where n < %d) select n, n from s",
		name, n))
}

func requireMaxKeysReadExceeded(t *testing.T, tk *testkit.TestKit, sql string) {
	err := tk.QueryToErr(sql)
	require.Errorf(t, err, "sql: %s should fail with 8274", sql)
	require.Truef(t, exeerrors.ErrMaxKeysReadExceeded.Equal(err), "sql: %s, unexpected error: %v", sql, err)
}

func pagingModes() []string { return []string{"OFF", "ON"} }

// TestMaxKeysReadExactBudget checks that a scan around the budget either
// returns the complete result or 8274, never a successful prefix.
func TestMaxKeysReadExactBudget(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	cases := []struct {
		rows, budget int
		exceeded     bool
	}{
		{32, 32, false},
		{33, 32, true},
	}
	for _, c := range cases {
		name := fmt.Sprintf("t_%d_%d", c.rows, c.budget)
		createSeqTable(tk, name, c.rows)
		for _, paging := range pagingModes() {
			tk.MustExec("set @@session.tidb_enable_paging = " + paging)
			tk.MustExec(fmt.Sprintf("set @@session.tidb_max_keys_read = %d", c.budget))
			sql := fmt.Sprintf("select id, v from %s", name)
			if c.exceeded {
				requireMaxKeysReadExceeded(t, tk, sql)
			} else {
				require.Len(t, tk.MustQuery(sql).Rows(), c.rows, "paging=%s rows=%d budget=%d", paging, c.rows, c.budget)
			}
			tk.MustExec("set @@session.tidb_max_keys_read = 0")
		}
	}
}

// TestMaxKeysReadAggregates checks pushed-down aggregation at and above the
// budget.
func TestMaxKeysReadAggregates(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	createSeqTable(tk, "t1024", 1024)
	createSeqTable(tk, "t4096", 4096)
	for _, paging := range pagingModes() {
		tk.MustExec("set @@session.tidb_enable_paging = " + paging)
		tk.MustExec("set @@session.tidb_max_keys_read = 1024")
		tk.MustQuery("select count(*) from t1024").Check(testkit.Rows("1024"))
		requireMaxKeysReadExceeded(t, tk, "select count(*) from t4096")
		requireMaxKeysReadExceeded(t, tk, "select sum(id) from t4096")
		requireMaxKeysReadExceeded(t, tk, "select id from t4096 order by v desc limit 3")
		tk.MustExec("set @@session.tidb_max_keys_read = 0")
	}
}

// TestMaxKeysReadCappedThenUnlimitedSameTxn checks that a capped query does not
// leave a partial cached result for a later query in the same transaction.
func TestMaxKeysReadCappedThenUnlimitedSameTxn(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	createSeqTable(tk, "t", 4096)
	require.NoError(t, failpoint.Enable(mockCopCacheFP, `return(123)`))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(mockCopCacheFP)) })

	queries := []struct {
		sql      string
		expected [][]any
	}{
		{"select count(*) from t", testkit.Rows("4096")},
		{"select sum(id) from t", testkit.Rows("8390656")},
		{"select id from t order by v desc limit 3", testkit.Rows("4096", "4095", "4094")},
	}
	for _, paging := range pagingModes() {
		tk.MustExec("set @@session.tidb_enable_paging = " + paging)
		for _, q := range queries {
			tk.MustExec("begin")
			tk.MustExec("set @@session.tidb_max_keys_read = 1024")
			requireMaxKeysReadExceeded(t, tk, q.sql)
			// The same capped query again in the same snapshot must still fail.
			requireMaxKeysReadExceeded(t, tk, q.sql)
			// 4096 keys exactly fit the budget, so the query completes.
			tk.MustExec("set @@session.tidb_max_keys_read = 4096")
			tk.MustQuery(q.sql).Check(q.expected)
			tk.MustExec("set @@session.tidb_max_keys_read = 0")
			hitsBefore := promtestutils.ToFloat64(copr_metrics.CoprCacheCounterHit)
			tk.MustQuery(q.sql).Check(q.expected)
			tk.MustQuery(q.sql).Check(q.expected)
			// Complete responses stay cacheable and are reused.
			require.Greater(t, promtestutils.ToFloat64(copr_metrics.CoprCacheCounterHit), hitsBefore, "paging=%s sql=%s", paging, q.sql)
			tk.MustExec("rollback")
		}
	}
}

func keysExamined(t *testing.T, tk *testkit.TestKit) int {
	rows := tk.MustQuery("show session status like 'tidb_keys_examined'").Rows()
	require.Len(t, rows, 1)
	n, err := strconv.Atoi(fmt.Sprint(rows[0][1]))
	require.NoError(t, err)
	return n
}

// TestMaxKeysReadPagingBoundary covers budgets used up exactly at a paging
// boundary: the continuation reads no keys and must still be sent.
func TestMaxKeysReadPagingBoundary(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	tk.MustExec("set @@session.tidb_min_paging_size = 128")
	for _, rows := range []int{96, 128, 129, 224, 256, 257} {
		name := fmt.Sprintf("t_boundary_%d", rows)
		createSeqTable(tk, name, rows)
		// Every run gets its own always-true predicate, hence its own
		// coprocessor cache key, so no run depends on another run's cache
		// entries.
		seq := 0
		uniqueSQL := func() string {
			seq++
			return fmt.Sprintf("select id, v from %s where v > -%d", name, seq)
		}
		for _, paging := range pagingModes() {
			tk.MustExec("set @@session.tidb_enable_paging = " + paging)
			tk.MustExec(fmt.Sprintf("set @@session.tidb_max_keys_read = %d", rows-1))
			requireMaxKeysReadExceeded(t, tk, uniqueSQL())
			tk.MustExec(fmt.Sprintf("set @@session.tidb_max_keys_read = %d", rows))
			require.Len(t, tk.MustQuery(uniqueSQL()).Rows(), rows, "paging=%s rows=%d budget=%d", paging, rows, rows)
			tk.MustExec("set @@session.tidb_max_keys_read = 0")
		}
	}
}

// TestMaxKeysReadWithoutExecutionInfoMultiRegion: each region is under the
// budget, so only the statement-wide counter can catch the total, and it must
// not depend on execution info.
func TestMaxKeysReadWithoutExecutionInfoMultiRegion(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	createSeqTable(tk, "t", 4096)
	splitTableRegions(t, tk, "t", 4096, 8)
	instance := &config.GetGlobalConfig().Instance
	old := instance.EnableCollectExecutionInfo.Load()
	instance.EnableCollectExecutionInfo.Store(false)
	t.Cleanup(func() { config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(old) })
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 4")
	for _, paging := range pagingModes() {
		tk.MustExec("set @@session.tidb_enable_paging = " + paging)
		tk.MustExec("set @@session.tidb_max_keys_read = 2048")
		requireMaxKeysReadExceeded(t, tk, "select id, v from t")
		requireMaxKeysReadExceeded(t, tk, "select count(*) from t")
		tk.MustExec("set @@session.tidb_max_keys_read = 4096")
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("4096"))
		tk.MustExec("set @@session.tidb_max_keys_read = 0")
	}
}

const unistoreSendHookFP = "github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCClientSendHook"

// splitTableRegions splits a createSeqTable table into regions with the same
// number of rows each.
func splitTableRegions(t *testing.T, tk *testkit.TestKit, name string, rows, regions int) {
	points := make([]string, 0, regions-1)
	for i := 1; i < regions; i++ {
		points = append(points, fmt.Sprintf("(%d)", i*rows/regions+1))
	}
	res := tk.MustQuery(fmt.Sprintf("split table %s by %s", name, strings.Join(points, ", "))).Rows()
	require.Equal(t, fmt.Sprint(regions-1), fmt.Sprint(res[0][0]), "split %s", name)
}

// TestMaxKeysReadCacheHitCharged: each region is under the budget and gets
// cached, the statement is over it. Cache hits must be charged, otherwise a
// rerun passes once enough regions are cached.
func TestMaxKeysReadCacheHitCharged(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	createSeqTable(tk, "t", 4096)
	splitTableRegions(t, tk, "t", 4096, 8)
	require.NoError(t, failpoint.Enable(mockCopCacheFP, `return(123)`))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(mockCopCacheFP)) })
	// One region at a time: the first run stops after 5 regions, which are
	// cached; a rerun that charged hits as 0 would read only 3 regions and pass.
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 1")
	for _, paging := range pagingModes() {
		tk.MustExec("set @@session.tidb_enable_paging = " + paging)
		// The same SQL in every run, so later runs hit the cache; a different
		// one per paging mode, so the modes don't share cache entries.
		sql := fmt.Sprintf("select count(*) from t where v > -%d", len(paging))
		hits := promtestutils.ToFloat64(copr_metrics.CoprCacheCounterHit)
		tk.MustExec("set @@session.tidb_max_keys_read = 2500")
		for i := 0; i < 4; i++ {
			requireMaxKeysReadExceeded(t, tk, sql)
		}
		require.Greater(t, promtestutils.ToFloat64(copr_metrics.CoprCacheCounterHit), hits, "paging=%s: reruns never hit the cache", paging)
		// Cached responses are charged exactly once: the whole table fits a
		// budget equal to its size.
		tk.MustExec("set @@session.tidb_max_keys_read = 4096")
		tk.MustQuery(sql).Check(testkit.Rows("4096"))
		tk.MustExec("set @@session.tidb_max_keys_read = 0")
	}
}

// TestMaxKeysReadLimitWithConcurrency: responses fetched ahead but never
// consumed must not be charged. The match is in the first region, whose
// response is delayed until the other regions have been read.
func TestMaxKeysReadLimitWithConcurrency(t *testing.T) {
	tk, dom, _ := newMaxKeysReadTestKit(t)
	createSeqTable(tk, "t", 4096)
	splitTableRegions(t, tk, "t", 4096, 16)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	matchKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(10))
	delayFirstRegion := func(req *tikvrpc.Request) {
		copReq, ok := req.Req.(*coprocessor.Request)
		if !ok || req.Type != tikvrpc.CmdCop {
			return
		}
		for _, r := range copReq.Ranges {
			if bytes.Compare(r.Start, matchKey) <= 0 && (len(r.End) == 0 || bytes.Compare(matchKey, r.End) < 0) {
				time.Sleep(300 * time.Millisecond)
				return
			}
		}
	}
	unistore.UnistoreRPCClientSendHook.Store(&delayFirstRegion)
	t.Cleanup(func() { unistore.UnistoreRPCClientSendHook.Store(nil) })
	require.NoError(t, failpoint.Enable(unistoreSendHookFP, `return(true)`))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(unistoreSendHookFP)) })

	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15")
	for _, paging := range pagingModes() {
		tk.MustExec("set @@session.tidb_enable_paging = " + paging)
		tk.MustExec("set @@session.tidb_max_keys_read = 1000")
		for i := 0; i < 3; i++ {
			tk.MustQuery("select id from t where v = 10 order by id limit 1").Check(testkit.Rows("10"))
		}
		// The statement-wide budget still holds for a full scan.
		requireMaxKeysReadExceeded(t, tk, "select count(*) from t where v > 0")
		tk.MustExec("set @@session.tidb_max_keys_read = 0")
	}
}

// TestMaxKeysReadKeysExaminedMatchesBudget: tidb_keys_examined counts the same
// keys as the budget, with execution info ON or OFF and when responses come
// from the coprocessor cache.
func TestMaxKeysReadKeysExaminedMatchesBudget(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	createSeqTable(tk, "t", 4096)
	splitTableRegions(t, tk, "t", 4096, 8)
	require.NoError(t, failpoint.Enable(mockCopCacheFP, `return(123)`))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(mockCopCacheFP)) })
	instance := &config.GetGlobalConfig().Instance
	old := instance.EnableCollectExecutionInfo.Load()
	t.Cleanup(func() { config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(old) })
	seq := 0
	for _, info := range []bool{true, false} {
		instance.EnableCollectExecutionInfo.Store(info)
		for _, paging := range pagingModes() {
			tk.MustExec("set @@session.tidb_enable_paging = " + paging)
			seq++
			sql := fmt.Sprintf("select count(*) from t where v > -%d", seq)
			// Unlimited, then capped at exactly the table size; the second and
			// third runs can be served from the cache.
			for _, budget := range []int{0, 4096, 4096} {
				tk.MustExec(fmt.Sprintf("set @@session.tidb_max_keys_read = %d", budget))
				before := keysExamined(t, tk)
				tk.MustQuery(sql).Check(testkit.Rows("4096"))
				require.Equal(t, 4096, keysExamined(t, tk)-before, "info=%v paging=%s budget=%d", info, paging, budget)
			}
			tk.MustExec("set @@session.tidb_max_keys_read = 0")
		}
	}
}

// TestMaxKeysReadPlanCache: a plan cached without a budget must not be reused
// once the budget is set, since the budget disables index lookup push-down.
func TestMaxKeysReadPlanCache(t *testing.T) {
	tk, _, _ := newMaxKeysReadTestKit(t)
	tk.MustExec("create table t (id int primary key, val int, extra int, key idx_val(val))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2)")
	tk.MustExec("prepare st from 'select /*+ index_lookup_pushdown(t, idx_val) */ extra from t where val >= ?'")
	tk.MustExec("set @a = 2")
	tk.MustQuery("execute st using @a").Check(testkit.Rows("2"))
	tk.MustQuery("execute st using @a").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @@session.tidb_max_keys_read = 1")
	requireMaxKeysReadExceeded(t, tk, "execute st using @a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}
