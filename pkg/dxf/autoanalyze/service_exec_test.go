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

package autoanalyze_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/dxf/autoanalyze"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

// TestAnalyzeUserTableOnServiceNode runs ANALYZE of user keyspace tables with
// the SYSTEM keyspace domain, the way a DXF service node of the next-gen kernel does.
// The statement session sees only the table of the task meta.
func TestAnalyzeUserTableOnServiceNode(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("the DXF service only exists in the next-gen kernel")
	}
	const userKS = "ks1"
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 2})
	defer cluster.Terminate(t)
	keyspaceIDs := map[string]uint32{keyspace.System: 1, userKS: 2}
	etcdClis := make(map[string]*clientv3.Client, len(keyspaceIDs))
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cliP **clientv3.Client, ks string) {
			id, ok := keyspaceIDs[ks]
			require.True(t, ok)
			for i := range 2 {
				cli := cluster.Client(i)
				if cli == nil {
					continue
				}
				cluster.TakeClient(i)
				codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: ks})
				require.NoError(t, err)
				etcd.SetEtcdCliByNamespace(cli, keyspace.MakeKeyspaceEtcdNamespace(codec))
				etcdClis[ks] = cli
				*cliP = cli
				return
			}
			require.Fail(t, "no etcd client left for keyspace %s", ks)
		},
	)
	sysStore, sysDom := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)
	storeMap := map[string]kv.Storage{keyspace.System: sysStore}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
		func(fnP *func(string) (kv.Storage, error)) {
			*fnP = func(ks string) (kv.Storage, error) {
				return storeMap[ks], nil
			}
		},
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
		func(shouldCloseStore *bool) {
			*shouldCloseStore = false
		},
	)
	userStore, userDom := testkit.CreateMockStoreAndDomainForKS(t, userKS)
	storeMap[userKS] = userStore
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = keyspace.System
	})

	userTK := testkit.NewTestKit(t, userStore)
	sysTK := testkit.NewTestKit(t, sysStore)
	userTK.MustExec("use test")
	runtime, err := sysDom.AcquireKSRuntime(userKS, "test/analyze-on-service-node")
	require.NoError(t, err)
	t.Cleanup(runtime.Release)
	ctx := context.Background()
	tableRef := func(name string) (int64, autoanalyze.TaskMeta) {
		tbl, err := userDom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta().ID, autoanalyze.TaskMeta{DBID: tbl.Meta().DBID, TableID: tbl.Meta().ID}
	}
	run := func(meta autoanalyze.TaskMeta, sql string, params ...string) error {
		meta.SQL, meta.Params = sql, params
		return autoanalyze.RunOnServiceNode(ctx, runtime, sysDom, &meta)
	}

	// A plain table. The statistics go to the user keyspace, and nothing to the SYSTEM keyspace.
	userTK.MustExec("create table t (a int primary key, b int, c varchar(16), index idx_b(b))")
	userTK.MustExec("insert into t values (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 3, 'c')")
	tID, tRef := tableRef("t")
	require.NoError(t, run(tRef, "analyze table %n.%n", "test", "t"))
	userTK.MustQuery("select count, modify_count from mysql.stats_meta where table_id = ?", tID).Check(testkit.Rows("4 0"))
	userTK.MustQuery("select table_schema, table_name, state from mysql.analyze_jobs where job_info like 'auto analyze%'").Check(testkit.Rows("test t finished"))
	sysTK.MustQuery("select count(*) from mysql.stats_meta where table_id = ?", tID).Check(testkit.Rows("0"))
	sysTK.MustQuery("select count(*) from mysql.analyze_jobs").Check(testkit.Rows("0"))
	// The result equals what the user keyspace gets when it analyzes the table itself.
	histograms := "select hist_id, is_index, distinct_count, null_count, stats_ver from mysql.stats_histograms where table_id = ? order by is_index, hist_id"
	fromService := userTK.MustQuery(histograms, tID).Rows()
	userTK.MustExec("analyze table t")
	userTK.MustQuery(histograms, tID).Check(fromService)

	// The statement plans with the stats of the user keyspace: the sample rate
	// follows the row count that the keyspace has recorded for the table.
	userTK.MustExec("update mysql.stats_meta set count = 1100000 where table_id = ?", tID)
	require.NoError(t, run(tRef, "analyze table %n.%n", "test", "t"))
	userTK.MustQuery("select count(*) from mysql.analyze_jobs where table_name = 't' and job_info like '%, 0.1 samplerate'").Check(testkit.Rows("1"))

	// Partitions of a partitioned table, with the global statistics merged from them.
	userTK.MustExec("create table pt (a int primary key, b int, index idx_b(b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	userTK.MustExec("insert into pt values (1, 1), (2, 2), (11, 11), (12, 12), (13, 13)")
	ptID, ptRef := tableRef("pt")
	require.NoError(t, run(ptRef, "analyze table %n.%n partition %n, %n", "test", "pt", "p0", "p1"))
	userTK.MustQuery("select count from mysql.stats_meta where table_id = ?", ptID).Check(testkit.Rows("5"))
	userTK.MustQuery("select partition_name, left(job_info, 18), state from mysql.analyze_jobs where table_name = 'pt' order by partition_name, id").Check(
		testkit.Rows(" merge global stats finished", " merge global stats finished", "p0 auto analyze table finished", "p1 auto analyze table finished"))

	// A schema change of the user keyspace is seen by the next statement.
	userTK.MustExec("alter table t add column d int default 7")
	require.NoError(t, run(tRef, "analyze table %n.%n", "test", "t"))
	userTK.MustQuery("select count(*) from mysql.stats_histograms where table_id = ? and is_index = 0 and hist_id = 4 and stats_ver = 2", tID).Check(testkit.Rows("1"))

	// A cached table is analyzed like a normal one.
	userTK.MustExec("create table ct (a int primary key, b int)")
	userTK.MustExec("insert into ct values (1, 1), (2, 2), (3, 3)")
	userTK.MustExec("alter table ct cache")
	ctID, ctRef := tableRef("ct")
	require.NoError(t, run(ctRef, "analyze table %n.%n", "test", "ct"))
	userTK.MustQuery("select count from mysql.stats_meta where table_id = ?", ctID).Check(testkit.Rows("3"))

	// The analyze settings come from the user keyspace. The process-level values
	// hold those of the SYSTEM keyspace on a service node, so they are set apart here.
	userTK.MustExec("create table st (a int primary key, b int, c int, e int, index idx_b(b))")
	userTK.MustExec("insert into st values (1, 1, 1, 1), (2, 2, 2, 2)")
	userTK.MustQuery("select * from st where c > 0")
	require.NoError(t, userDom.StatsHandle().Usage().DumpColStatsUsageToKV())
	require.NoError(t, userDom.StatsHandle().Usage().DumpStatsDeltaToKV(true))
	ndvThreshold := userTK.MustQuery("select @@global.tidb_analyze_sampled_ndv_threshold").Rows()[0][0].(string)
	userTK.MustExec("set global tidb_analyze_default_num_buckets = 64")
	userTK.MustExec("set global tidb_analyze_column_options = 'PREDICATE'")
	userTK.MustExec("set global tidb_analyze_sampled_ndv_threshold = 0")
	// The runtime caches the global variables of the user keyspace. Like the
	// sysvar cache of a domain, it reloads them when etcd notifies a change.
	_, err = etcdClis[userKS].Put(ctx, ddlutil.SysVarCacheKey, "")
	require.NoError(t, err)
	userGlobalVars, err := sysDom.GetKSGlobalVarAccessor(userKS)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		val, err := userGlobalVars.GetGlobalSysVar(vardef.TiDBAnalyzeColumnOptions)
		return err == nil && val == "PREDICATE"
	}, 10*time.Second, 50*time.Millisecond)
	// The user keyspace keeps the default of tidb_persist_analyze_options, which is ON.
	vardef.PersistAnalyzeOptions.Store(false)
	vardef.AnalyzeDefaultNumBuckets.Store(256)
	vardef.AnalyzeColumnOptions.Store("ALL")
	// The table has more rows than this threshold, so this value would sample NDV.
	vardef.AnalyzeSampledNDVThreshold.Store(1)
	stID, stRef := tableRef("st")
	require.NoError(t, run(stRef, "analyze table %n.%n", "test", "st"))
	vardef.PersistAnalyzeOptions.Store(true)
	userTK.MustExec("set global tidb_analyze_default_num_buckets = 256")
	userTK.MustExec("set global tidb_analyze_column_options = 'ALL'")
	userTK.MustExec("set global tidb_analyze_sampled_ndv_threshold = " + ndvThreshold)
	userTK.MustQuery("select job_info like '%64 buckets%', job_info like '%all columns%' from mysql.analyze_jobs where table_name = 'st'").Check(testkit.Rows("1 0"))
	// The options are saved only if the planner and the executor both read ON from the user keyspace.
	userTK.MustQuery("select count(*) from mysql.analyze_options where table_id = ?", stID).Check(testkit.Rows("1"))
	userTK.MustQuery("select count(*) from mysql.analyze_jobs where table_name = 'st' and job_info like '%ndvrate%'").Check(testkit.Rows("0"))

	// The usage of a dropped column is removed before the statement reads the predicate columns.
	userTK.MustExec("create table dc (a int primary key, b int, c int)")
	userTK.MustExec("insert into dc values (1, 1, 1), (2, 2, 2)")
	userTK.MustQuery("select * from dc where c > 0")
	require.NoError(t, userDom.StatsHandle().Usage().DumpColStatsUsageToKV())
	dcID, dcRef := tableRef("dc")
	userTK.MustQuery("select count(*) from mysql.column_stats_usage where table_id = ? and column_id = 3", dcID).Check(testkit.Rows("1"))
	userTK.MustExec("alter table dc drop column c")
	require.NoError(t, run(dcRef, "analyze table %n.%n predicate columns", "test", "dc"))
	userTK.MustQuery("select count(*) from mysql.column_stats_usage where table_id = ? and column_id = 3", dcID).Check(testkit.Rows("0"))

	// Indexes, a global index, generated columns and a foreign key. The results equal those of the
	// user keyspace's own ANALYZE.
	require.NoError(t, run(tRef, "analyze table %n.%n index %n", "test", "t", "idx_b"))
	require.NoError(t, run(ptRef, "analyze table %n.%n partition %n index %n", "test", "pt", "p0", "idx_b"))
	userTK.MustExec("create table gpt (a int, b int, c int, primary key (a) nonclustered, unique key ub(b) global, index ic(c)) partition by hash(a) partitions 2")
	userTK.MustExec("insert into gpt values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)")
	gptID, gptRef := tableRef("gpt")
	require.NoError(t, run(gptRef, "analyze table %n.%n partition %n, %n", "test", "gpt", "p0", "p1"))
	require.NoError(t, run(gptRef, "analyze table %n.%n index %n", "test", "gpt", "ub"))
	fromService = userTK.MustQuery(histograms, gptID).Rows()
	userTK.MustExec("analyze table gpt")
	userTK.MustQuery(histograms, gptID).Check(fromService)
	userTK.MustExec("create table gc (a int, b int as (a + 1) virtual, c int, index ib(b), index ie((a + c)))")
	userTK.MustExec("insert into gc (a, c) values (1, 1), (2, 2), (3, 3)")
	gcID, gcRef := tableRef("gc")
	require.NoError(t, run(gcRef, "analyze table %n.%n", "test", "gc"))
	fromService = userTK.MustQuery(histograms, gcID).Rows()
	userTK.MustExec("analyze table gc")
	userTK.MustQuery(histograms, gcID).Check(fromService)
	userTK.MustExec("create table fkp (id int primary key)")
	userTK.MustExec("create table fkc (id int primary key, pid int, foreign key (pid) references fkp(id))")
	userTK.MustExec("insert into fkp values (1)")
	userTK.MustExec("insert into fkc values (1, 1)")
	_, fkcRef := tableRef("fkc")
	require.NoError(t, run(fkcRef, "analyze table %n.%n", "test", "fkc"))
	userTK.MustQuery("select count(*) from mysql.analyze_jobs where table_name in ('t', 'pt', 'gpt', 'gc', 'fkc') and state != 'finished'").Check(testkit.Rows("0"))

	// With tidb_enable_analyze_snapshot of the user keyspace, the rows written during the statement
	// are added to the count that the statement reads at its start.
	userTK.MustExec("set global tidb_enable_analyze_snapshot = ON")
	_, err = etcdClis[userKS].Put(ctx, ddlutil.SysVarCacheKey, "")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		val, err := userGlobalVars.GetGlobalSysVar(vardef.TiDBEnableAnalyzeSnapshot)
		return err == nil && val == "ON"
	}, 10*time.Second, 50*time.Millisecond)
	userTK.MustExec("create table sn (a int primary key, b int, index ib(b))")
	userTK.MustExec("insert into sn values (1, 1), (2, 2), (3, 3)")
	// The dispatcher flushes the stats deltas before it submits a task, and run does not.
	require.NoError(t, userDom.StatsHandle().Usage().DumpStatsDeltaToKV(true))
	snID, snRef := tableRef("sn")
	require.NoError(t, run(snRef, "analyze table %n.%n", "test", "sn"))
	userTK.MustQuery("select count, modify_count from mysql.stats_meta where table_id = ?", snID).Check(testkit.Rows("3 0"))
	var written atomic.Bool
	writeErr := make(chan error, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/analyzeBeforeSendToSaveResults", func() {
		if !written.CompareAndSwap(false, true) {
			return
		}
		_, err := userTK.Exec("insert into sn values (4, 4), (5, 5)")
		if err == nil {
			err = userDom.StatsHandle().Usage().DumpStatsDeltaToKV(true)
		}
		writeErr <- err
	})
	require.NoError(t, run(snRef, "analyze table %n.%n", "test", "sn"))
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/executor/analyzeBeforeSendToSaveResults")
	require.NoError(t, <-writeErr)
	// The statement read 3 rows. With the snapshot OFF, the count would be 3.
	userTK.MustQuery("select count, modify_count from mysql.stats_meta where table_id = ?", snID).Check(testkit.Rows("5 2"))
	userTK.MustExec("set global tidb_enable_analyze_snapshot = OFF")

	// A table dropped after the dispatch is not analyzed.
	userTK.MustExec("drop table dc")
	require.ErrorContains(t, run(dcRef, "analyze table %n.%n", "test", "dc"), "doesn't exist")

	// Only the table of the task meta is visible to the statement.
	require.ErrorContains(t, run(tRef, "analyze table %n.%n", "test", "pt"), "doesn't exist")
}
