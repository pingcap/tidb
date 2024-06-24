// Copyright 2017 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

// checkGoroutineExists
// nolint:unused
func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	err := profile.WriteTo(buf, 1)
	if err != nil {
		panic(err)
	}
	str := buf.String()
	return strings.Contains(str, keyword)
}

func TestCopClientSend(t *testing.T) {
	t.Skip("not stable")
	var cluster testutils.Cluster
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	if _, ok := store.GetClient().(*copr.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table copclient (id int primary key)")

	// Insert 1000 rows.
	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert copclient values " + strings.Join(values, ","))

	// Get table ID for split.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("copclient"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	ctx := context.Background()
	// Send coprocessor request when the table split.
	rs, err := tk.Exec("select sum(id) from copclient")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "499500", req.GetRow(0).GetMyDecimal(0).String())
	require.NoError(t, rs.Close())

	// Split one region.
	key := tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(500))
	region, _, _, _ := cluster.GetRegionByKey(key)
	peerID := cluster.AllocID()
	cluster.Split(region.GetId(), cluster.AllocID(), key, []uint64{peerID}, peerID)

	// Check again.
	rs, err = tk.Exec("select sum(id) from copclient")
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "499500", req.GetRow(0).GetMyDecimal(0).String())
	require.NoError(t, rs.Close())

	// Check there is no goroutine leak.
	rs, err = tk.Exec("select * from copclient order by id")
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	keyword := "(*copIterator).work"
	require.False(t, checkGoroutineExists(keyword))
}

func TestGetLackHandles(t *testing.T) {
	expectedHandles := []kv.Handle{kv.IntHandle(1), kv.IntHandle(2), kv.IntHandle(3), kv.IntHandle(4),
		kv.IntHandle(5), kv.IntHandle(6), kv.IntHandle(7), kv.IntHandle(8), kv.IntHandle(9), kv.IntHandle(10)}
	handlesMap := kv.NewHandleMap()
	for _, h := range expectedHandles {
		handlesMap.Set(h, true)
	}

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	diffHandles := executor.GetLackHandles(expectedHandles, handlesMap)
	require.Len(t, diffHandles, 0)
	require.Equal(t, 0, handlesMap.Len())

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 2, 3, 4, 6, 7, 8, 9
	retHandles := []kv.Handle{kv.IntHandle(2), kv.IntHandle(3), kv.IntHandle(4), kv.IntHandle(6),
		kv.IntHandle(7), kv.IntHandle(8), kv.IntHandle(9)}
	handlesMap = kv.NewHandleMap()
	handlesMap.Set(kv.IntHandle(1), true)
	handlesMap.Set(kv.IntHandle(5), true)
	handlesMap.Set(kv.IntHandle(10), true)
	diffHandles = executor.GetLackHandles(expectedHandles, handlesMap)
	require.Equal(t, diffHandles, retHandles) // deep equal
}

func TestInconsistentIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	idx := tbl.Meta().FindIndexByName("idx_a")
	idxOp := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), idx)
	ctx := mock.NewContext()
	ctx.Store = store

	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i+10, i))
		require.NoError(t, tk.QueryToErr("select * from t where a>=0"))
	}

	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("update t set a=%d where a=%d", i, i+10))
		require.NoError(t, tk.QueryToErr("select * from t where a>=0"))
	}

	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		_, err = idxOp.Create(ctx.GetTableCtx(), txn, types.MakeDatums(i+10), kv.IntHandle(100+i), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)

		err = tk.QueryToErr("select * from t use index(idx_a) where a >= 0")
		require.Equal(t, fmt.Sprintf("[executor:8133]data inconsistency in table: t, index: idx_a, index-count:%d != record-count:10", i+11), err.Error())
		// if has other conditions, the inconsistent index check doesn't work.
		err = tk.QueryToErr("select * from t where a>=0 and b<10")
		require.NoError(t, err)
	}

	// fix inconsistent problem to pass CI
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		err = idxOp.Delete(ctx.GetTableCtx(), txn, types.MakeDatums(i+10), kv.IntHandle(100+i))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
	}
}

func TestPartitionTableRandomlyIndexLookUpReader(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int, b int, key(a))
        partition by range (a) (
        partition p1 values less than (10),
        partition p2 values less than (20),
        partition p3 values less than (30),
        partition p4 values less than (40))`)
	tk.MustExec("create table tnormal (a int, b int, key(a))")
	values := make([]string, 0, 128)
	for i := 0; i < 128; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(40), rand.Intn(40)))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))

	randRange := func() (int, int) {
		a, b := rand.Intn(40), rand.Intn(40)
		if a > b {
			return b, a
		}
		return a, b
	}
	for i := 0; i < 256; i++ {
		la, ra := randRange()
		lb, rb := randRange()
		cond := fmt.Sprintf("(a between %v and %v) or (b between %v and %v)", la, ra, lb, rb)
		tk.MustQuery("select * from t use index(a) where " + cond).Sort().Check(
			tk.MustQuery("select * from tnormal where " + cond).Sort().Rows())
	}
}

func TestIndexLookUpStats(t *testing.T) {
	stats := &executor.IndexLookUpRunTimeStats{
		FetchHandleTotal:         int64(5 * time.Second),
		FetchHandle:              int64(2 * time.Second),
		TaskWait:                 int64(2 * time.Second),
		TableRowScan:             int64(2 * time.Second),
		TableTaskNum:             2,
		Concurrency:              1,
		NextWaitIndexScan:        time.Second,
		NextWaitTableLookUpBuild: 2 * time.Second,
		NextWaitTableLookUpResp:  3 * time.Second,
	}
	require.Equal(t, "index_task: {total_time: 5s, fetch_handle: 2s, build: 1s, wait: 2s}"+
		", table_task: {total_time: 2s, num: 2, concurrency: 1}"+
		", next: {wait_index: 1s, wait_table_lookup_build: 2s, wait_table_lookup_resp: 3s}", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "index_task: {total_time: 10s, fetch_handle: 4s, build: 2s, wait: 4s}"+
		", table_task: {total_time: 4s, num: 4, concurrency: 1}"+
		", next: {wait_index: 2s, wait_table_lookup_build: 4s, wait_table_lookup_resp: 6s}", stats.String())
}

func TestPartitionTableIndexJoinIndexLookUp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table t (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec("create table tnormal (a int, b int, key(a), key(b))")
	nRows := 512
	values := make([]string, 0, nRows)
	for i := 0; i < nRows; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(nRows), rand.Intn(nRows)))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))

	randRange := func() (int, int) {
		a, b := rand.Intn(nRows), rand.Intn(nRows)
		if a > b {
			return b, a
		}
		return a, b
	}
	for i := 0; i < nRows; i++ {
		lb, rb := randRange()
		cond := fmt.Sprintf("(t2.b between %v and %v)", lb, rb)
		result := tk.MustQuery("select t1.* from tnormal t1, tnormal t2 use index(a) where t1.a=t2.b and " + cond).Sort().Rows()
		tk.MustQuery("select /*+ TIDB_INLJ(t1, t2) */ t1.* from t t1, t t2 use index(a) where t1.a=t2.b and " + cond).Sort().Check(result)
	}
}

func TestCoprocessorPagingSize(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t_paging (a int, b int, key(a), key(b))")
	nRows := 512
	values := make([]string, 0, nRows)
	for i := 0; i < nRows; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(nRows), rand.Intn(nRows)))
	}
	tk.MustExec(fmt.Sprintf("insert into t_paging values %v", strings.Join(values, ", ")))
	tk.MustQuery("select @@tidb_min_paging_size").Check(testkit.Rows(strconv.FormatUint(paging.MinPagingSize, 10)))

	// Enable the coprocessor paging protocol.
	tk.MustExec("set @@tidb_enable_paging = on")

	// When the min paging size is small, we need more RPC roundtrip!
	// Check 'rpc_num' in the execution information
	//
	// mysql> explain analyze select * from t_paging;
	// +--------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	// | id                 |task      | execution info                                                                                                                                                                                                        |
	// +--------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	// | TableReader_5      |root      | time:7.27ms, loops:2, cop_task: {num: 10, max: 1.57ms, min: 313.3µs, avg: 675.9µs, p95: 1.57ms, tot_proc: 2ms, copr_cache_hit_ratio: 0.00, distsql_concurrency: 15}, rpc_info:{Cop:{num_rpc:10, total_time:6.69ms}}   |
	// | └─TableFullScan_4  |cop[tikv] | tikv_task:{proc max:1.48ms, min:294µs, avg: 629µs, p80:1.21ms, p95:1.48ms, iters:0, tasks:10}                                                                                                                         |
	// +--------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	// 2 rows in set (0.01 sec)

	getRPCNumFromExplain := func(rows [][]any) (res uint64) {
		re := regexp.MustCompile("num_rpc:([0-9]+)")
		for _, row := range rows {
			buf := bytes.NewBufferString("")
			_, _ = fmt.Fprintf(buf, "%s\n", row)
			if matched := re.FindStringSubmatch(buf.String()); matched != nil {
				require.Equal(t, len(matched), 2)
				c, err := strconv.ParseUint(matched[1], 10, 64)
				require.NoError(t, err)
				return c
			}
		}
		return res
	}

	// This is required here because only the chunk encoding collect the execution information and contains 'rpc_num'.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	tk.MustExec("set @@tidb_min_paging_size = 1")
	rows := tk.MustQuery("explain analyze select * from t_paging").Rows()
	rpcNum := getRPCNumFromExplain(rows)
	require.Greater(t, rpcNum, uint64(2))

	tk.MustExec("set @@tidb_min_paging_size = 1000")
	rows = tk.MustQuery("explain analyze select * from t_paging").Rows()
	rpcNum = getRPCNumFromExplain(rows)
	require.Equal(t, rpcNum, uint64(1))
}

func TestAdaptiveClosestRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect this UT
	tk.MustExec("drop table if exists t")
	// the avg row size is more accurate in check_rpc mode when unistre is used.
	// See: https://github.com/pingcap/tidb/issues/31744#issuecomment-1016309883
	tk.MustExec("set @@tidb_enable_chunk_rpc = '1'")

	readCounter := func(counter prometheus.Counter) float64 {
		var metric dto.Metric
		require.Nil(t, counter.Write(&metric))
		return metric.Counter.GetValue()
	}

	checkMetrics := func(q string, hit, miss int) {
		beforeHit := readCounter(metrics.DistSQLCoprClosestReadCounter.WithLabelValues("hit"))
		beforeMiss := readCounter(metrics.DistSQLCoprClosestReadCounter.WithLabelValues("miss"))
		tk.MustQuery(q)
		afterHit := readCounter(metrics.DistSQLCoprClosestReadCounter.WithLabelValues("hit"))
		afterMiss := readCounter(metrics.DistSQLCoprClosestReadCounter.WithLabelValues("miss"))
		require.Equal(t, hit, int(afterHit-beforeHit), "exec query '%s' check hit failed", q)
		require.Equal(t, miss, int(afterMiss-beforeMiss), "exec query '%s' check miss failed", q)
	}

	tk.MustExec("create table t(id int primary key, s varchar(8), p varchar(16));")
	tk.MustExec("insert into t values (1, '00000001', '0000000000000001'), (2, '00000003', '0000000000000002'), (3, '00000011', '0000000000000003');")
	tk.MustExec("analyze table t;")

	tk.MustExec("set @@tidb_partition_prune_mode  ='static';")
	tk.MustExec("set tidb_replica_read = 'closest-adaptive';")
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 25;")

	// table reader
	// estimate cost is 19
	checkMetrics("select s from t where id >= 1 and id < 2;", 0, 1)
	// estimate cost is 37
	checkMetrics("select * from t where id >= 1 and id < 2;", 1, 0)
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 50;")
	checkMetrics("select * from t where id >= 1 and id < 2;", 0, 1)
	// estimate cost is 74
	checkMetrics("select * from t where id >= 1 and id <= 2;", 1, 0)

	partitionDef := "PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (3), PARTITION p3 VALUES LESS THAN MAXVALUE);"

	// test TableReader with partition
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 30;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, s varchar(8), p varchar(16)) " + partitionDef)
	tk.MustExec("insert into t values (1, '00000001', '0000000000000001'), (2, '00000003', '0000000000000002'), (3, '00000011', '0000000000000003'), (4, '00000044', '0000000000000004');")
	tk.MustExec("analyze table t;")
	// estimate cost is 38
	checkMetrics("select s from t where id >= 1 and id < 3;", 1, 0)
	// estimate cost is 39 with 2 cop request
	checkMetrics("select s from t where id >= 2 and id < 4;", 0, 2)

	// index reader
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, s varchar(8), p varchar(8), key `idx_s_p`(`s`, `p`));")
	tk.MustExec("insert into t values (1, 'test1000', '11111111'), (2, 'test2000', '11111111');")
	tk.MustExec("analyze table t;")
	// avg row size = 27.91
	checkMetrics("select p from t where s >= 'test' and s < 'test11'", 0, 1)
	checkMetrics("select p from t where s >= 'test' and s < 'test22'", 1, 0)

	// index reader with partitions
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 30;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (v int, id int, p varchar(8), key `idx_id_p`(`id`, `p`)) " + partitionDef)
	tk.MustExec("insert into t values (1, 1, '11111111'), (2, 2, '22222222'), (3, 3, '33333333'), (4, 4, '44444444');")
	tk.MustExec("analyze table t;")
	// avg row size = 19
	checkMetrics("select p from t where id >= 1 and id < 3", 1, 0)
	checkMetrics("select p from t where id >= 2 and id < 4", 0, 2)
	checkMetrics("select p from t where id >= 1 and id < 4", 1, 1)

	// index lookup reader
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, s varchar(8), p varchar(50), key `idx_s`(`s`));")
	str := "this_is_a_string_with_length_of_50________________"
	tk.MustExec(fmt.Sprintf("insert into t values (1, 'test1000', '%s'), (2, 'test2000', '%s');", str, str))
	tk.MustExec("analyze table t;")
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 80;")
	// IndexReader cost is 22, TableReader cost (1 row) is 67
	checkMetrics("select/*+ FORCE_INDEX(t, idx_s) */ p from t where s >= 'test' and s < 'test11'", 0, 2)
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 100;")
	checkMetrics("select/*+ FORCE_INDEX(t, idx_s) */ p from t where s >= 'test' and s < 'test22'", 1, 1)

	// index merge reader
	tk.MustExec("drop table if exists t;")
	// use int field to avoid the planer estimation with big random fluctuation.
	tk.MustExec("create table t (id int, v bigint not null, s1 int not null, s2 int not null, key `idx_v_s1`(`s1`, `v`), key `idx_s2`(`s2`));")
	tk.MustExec("insert into t values (1, 1,  1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
	tk.MustExec("analyze table t;")
	tk.MustExec("set tidb_adaptive_closest_read_threshold = 30;")
	// 2 IndexScan with cost 19/56, 2 TableReader with cost 32.5/65.
	checkMetrics("select/* +USE_INDEX_MERGE(t) */ id from t use index(`idx_v_s1`) use index(idx_s2) where (s1 < 3 and v > 0) or s2 = 3;", 3, 1)
}

func TestCoprocessorPagingReqKeyRangeSorted(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/checkKeyRangeSortedForPaging", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/checkKeyRangeSortedForPaging"))
	}()

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `UK_COLLATION19523` (" +
		"`COL1` binary(1) DEFAULT NULL," +
		"`COL2` varchar(20) COLLATE utf8_general_ci DEFAULT NULL," +
		"`COL4` datetime DEFAULT NULL," +
		"`COL3` bigint(20) DEFAULT NULL," +
		"`COL5` float DEFAULT NULL," +
		"UNIQUE KEY `U_COL1` (`COL1`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")

	tk.MustExec("prepare stmt from 'SELECT/*+ HASH_JOIN(t1, t2) */ * FROM UK_COLLATION19523 t1 JOIN UK_COLLATION19523 t2 ON t1.col1 > t2.col1 WHERE t1.col1 IN (?, ?, ?) AND t2.col1 < ?;';")
	tk.MustExec("set @a=0x4F, @b=0xF8, @c=NULL, @d=0xBF;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")
	tk.MustExec("set @a=0x00, @b=0xD2, @c=9179987834981541375, @d=0xF8;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")

	tk.MustExec("CREATE TABLE `IDT_COLLATION26873` (" +
		"`COL1` varbinary(20) DEFAULT NULL," +
		"`COL2` varchar(20) COLLATE utf8_general_ci DEFAULT NULL," +
		"`COL4` datetime DEFAULT NULL," +
		"`COL3` bigint(20) DEFAULT NULL," +
		"`COL5` float DEFAULT NULL," +
		"KEY `U_COL1` (`COL1`))")
	tk.MustExec("prepare stmt from 'SELECT/*+ INL_JOIN(t1, t2) */ t2.* FROM IDT_COLLATION26873 t1 LEFT JOIN IDT_COLLATION26873 t2 ON t1.col1 = t2.col1 WHERE t1.col1 < ? AND t1.col1 IN (?, ?, ?);';")
	tk.MustExec("set @a=NULL, @b=NULL, @c=NULL, @d=NULL;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")
	tk.MustExec("set @a=0xE3253A6AC72A3A168EAF0E34A4779A947872CCCD, @b=0xD67BB26504EE152C2C356D7F6CAD897F03462963, @c=NULL, @d=0xDE735FEB375A4CF33479A39CA925470BFB229DB4;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")
	tk.MustExec("set @a=2606738829406840179, @b=1468233589368287363, @c=5174008984061521089, @d=7727946571160309462;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")
	tk.MustExec("set @a=0xFCABFE6198B6323EE8A46247EDD33830453B1BDE, @b=NULL, @c=6864108002939154648, @d=0xFCABFE6198B6323EE8A46247EDD33830453B1BDE;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")
	tk.MustExec("set @a=0xFCABFE6198B6323EE8A46247EDD33830453B1BDE, @b=0xFCABFE6198B6323EE8A46247EDD33830453B1BDE, @c=0xFCABFE6198B6323EE8A46247EDD33830453B1BDE, @d=0xFCABFE6198B6323EE8A46247EDD33830453B1BDE;")
	tk.MustExec("execute stmt using @a,@b,@c,@d;")

	tk.MustExec("CREATE TABLE `PK_SNPRE10114` (" +
		"`COL1` varbinary(10) NOT NULL DEFAULT 'S'," +
		"`COL2` varchar(20) DEFAULT NULL," +
		"`COL4` datetime DEFAULT NULL," +
		"`COL3` bigint(20) DEFAULT NULL," +
		"`COL5` float DEFAULT NULL," +
		"PRIMARY KEY (`COL1`) CLUSTERED)")
	tk.MustExec(`prepare stmt from 'SELECT * FROM PK_SNPRE10114 WHERE col1 IN (?, ?, ?) AND (col2 IS NULL OR col2 IN (?, ?)) AND (col3 IS NULL OR col4 IS NULL);';`)
	tk.MustExec(`set @a=0x0D5BDAEB79074756F203, @b=NULL, @c=0x6A911AAAC728F1ED3B4F, @d="鏖秿垙麜濇凗辯Ũ卮伄幖轒ƀ漭蝏雓轊恿磔徵", @e="訇廵纹髺釖寒近槩靏詗膦潳陒錃粓悧闒摔)乀";`)
	tk.MustExec(`execute stmt using @a,@b,@c,@d,@e;`)
	tk.MustExec(`set @a=7775448739068993371, @b=5641728652098016210, @c=6774432238941172824, @d="HqpP5rN", @e="8Fy";`)
	tk.MustExec(`execute stmt using @a,@b,@c,@d,@e;`)
	tk.MustExec(`set @a=0x61219F79C90D3541F70E, @b=5501707547099269248, @c=0xEC43EFD30131DEA2CB8B, @d="呣丼蒢咿卻鹻铴础湜僂頃ǆ縍套衞陀碵碼幓9", @e="鹹楞睕堚尛鉌翡佾搁紟精廬姆燵藝潐楻翇慸嵊";`)
	tk.MustExec(`execute stmt using @a,@b,@c,@d,@e;`)
}

func TestCoprocessorBatchByStore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(id int primary key, c1 int, c2 int, key i(c1))")
	tk.MustExec(`create table t1(id int primary key, c1 int, c2 int, key i(c1)) partition by range(id) (
    	partition p0 values less than(10000),
    	partition p1 values less than (50000),
    	partition p2 values less than (100000))`)
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values(?, ?, ?)", i*10000, i*10000, i%2)
		tk.MustExec("insert into t1 values(?, ?, ?)", i*10000, i*10000, i%2)
	}
	tk.MustQuery("split table t between (0) and (100000) regions 20").Check(testkit.Rows("20 1"))
	tk.MustQuery("split table t1 between (0) and (100000) regions 20").Check(testkit.Rows("60 1"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/setRangesPerTask", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/setRangesPerTask"))
	}()
	ranges := []string{
		"(c1 >= 0 and c1 < 5000)",
		"(c1 >= 10000 and c1 < 15000)",
		"(c1 >= 20000 and c1 < 25000)",
		"(c1 >= 30000 and c1 < 35000)",
		"(c1 >= 40000 and c1 < 45000)",
		"(c1 >= 50000 and c1 < 55000)",
		"(c1 >= 60000 and c1 < 65000)",
		"(c1 >= 70000 and c1 < 75000)",
		"(c1 >= 80000 and c1 < 85000)",
		"(c1 >= 90000 and c1 < 95000)",
	}
	evenRows := testkit.Rows("0 0 0", "20000 20000 0", "40000 40000 0", "60000 60000 0", "80000 80000 0")
	oddRows := testkit.Rows("10000 10000 1", "30000 30000 1", "50000 50000 1", "70000 70000 1", "90000 90000 1")
	reverseOddRows := testkit.Rows("90000 90000 1", "70000 70000 1", "50000 50000 1", "30000 30000 1", "10000 10000 1")
	for _, table := range []string{"t", "t1"} {
		baseSQL := fmt.Sprintf("select * from %s force index(i) where id < 100000 and (%s)", table, strings.Join(ranges, " or "))
		for _, paging := range []string{"on", "off"} {
			tk.MustExec("set session tidb_enable_paging=?", paging)
			for size := 0; size < 10; size++ {
				tk.MustExec("set session tidb_store_batch_size=?", size)
				tk.MustQuery(baseSQL + " and c2 = 0").Sort().Check(evenRows)
				tk.MustQuery(baseSQL + " and c2 = 1").Sort().Check(oddRows)
				tk.MustQuery(baseSQL + " and c2 = 0 order by c1 asc").Check(evenRows)
				tk.MustQuery(baseSQL + " and c2 = 1 order by c1 desc").Check(reverseOddRows)
				// every batched task will get region error and fallback.
				require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/batchCopRegionError", "return"))
				tk.MustQuery(baseSQL + " and c2 = 0").Sort().Check(evenRows)
				tk.MustQuery(baseSQL + " and c2 = 1").Sort().Check(oddRows)
				tk.MustQuery(baseSQL + " and c2 = 0 order by c1 asc").Check(evenRows)
				tk.MustQuery(baseSQL + " and c2 = 1 order by c1 desc").Check(reverseOddRows)
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/batchCopRegionError"))
			}
		}
	}
}

func TestCoprCacheWithoutExecutionInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values(1), (2), (3)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/cophandler/mockCopCacheInUnistore", `return(123)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/cophandler/mockCopCacheInUnistore"))
	}()

	defer tk.MustExec("set @@tidb_enable_collect_execution_info=1")
	ctx := context.WithValue(context.Background(), "CheckSelectRequestHook", func(_ *kv.Request) {
		tk1.MustExec("set @@tidb_enable_collect_execution_info=0")
	})
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2", "3"))
	tk.MustQueryWithContext(ctx, "select * from t").Check(testkit.Rows("1", "2", "3"))
}
