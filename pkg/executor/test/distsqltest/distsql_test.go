// Copyright 2023 PingCAP, Inc.
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

package distsql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDistsqlPartitionTableConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(id int primary key , val int)")
	partitions := make([]string, 0, 20)
	for i := range 20 {
		pid := i + 1
		partitions = append(partitions, fmt.Sprintf("PARTITION p%d VALUES LESS THAN (%d00)", pid, pid))
	}
	tk.MustExec("create table t2(id int primary key, val int)" +
		"partition by range(id)" +
		"(" + strings.Join(partitions[:10], ",") + ")")
	tk.MustExec("create table t3(id int primary key, val int)" +
		"partition by range(id)" +
		"(" + strings.Join(partitions, ",") + ")")
	for i := range 20 {
		for _, tbl := range []string{"t1", "t2", "t3"} {
			tk.MustExec(fmt.Sprintf("insert into %s values(%d, %d)", tbl, i*50, i*50))
		}
	}
	tk.MustExec("analyze table t1, t2, t3")
	// non-partitioned table checker
	ctx1 := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		require.Equal(t, req.KeyRanges.PartitionNum(), 1)
		require.Equal(t, req.Concurrency, 1)
	})
	// 10-ranges-partitioned table checker
	ctx2 := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		require.Equal(t, req.KeyRanges.PartitionNum(), 10)
		require.Equal(t, req.Concurrency, 10)
	})
	// 20-ranges-partitioned table checker
	ctx3 := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		require.Equal(t, req.KeyRanges.PartitionNum(), 20)
		require.Equal(t, req.Concurrency, vardef.DefDistSQLScanConcurrency)
	})
	ctxs := []context.Context{ctx1, ctx2, ctx3}
	for i, tbl := range []string{"t1", "t2", "t3"} {
		ctx := ctxs[i]
		// If order by is added here, the concurrency is always equal to 1.
		// Because we will use different kv.Request for each partition in TableReader.
		tk.MustQueryWithContext(ctx, fmt.Sprintf("select * from %s limit 1", tbl))
		tk.MustQueryWithContext(ctx, fmt.Sprintf("select * from %s limit 5", tbl))
		tk.MustQueryWithContext(ctx, fmt.Sprintf("select * from %s limit 1", tbl))
		tk.MustQueryWithContext(ctx, fmt.Sprintf("select * from %s limit 5", tbl))
	}
}

func TestDistSQLSharedKVRequestRace(t *testing.T) {
	// Test for issue https://github.com/pingcap/tidb/issues/60175
	store := testkit.CreateMockStore(t)
	originCfg := config.GetGlobalConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Labels = map[string]string{
			"zone": "us-east-1a",
		}
	})
	require.Equal(t, "us-east-1a", config.GetGlobalConfig().GetTiKVConfig().TxnScope)
	defer config.UpdateGlobal(func(conf *config.Config) {
		*conf = *originCfg
	})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set session tidb_enable_index_merge = ON")
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (
			a int,
			b int,
			c int,
			d int,
			primary key (a, d),
			index ib(b),
			index ic(c)
		)
		partition by range(d) (
			partition p1 values less than(1),
			partition p2 values less than(2),
			partition p3 values less than(3),
			partition p4 values less than (4)
		)`)
	tk.MustExec("begin")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, %d, %d);", i*1000, i*1000, i*1000, i%4))
	}
	tk.MustExec("commit")

	expects := make([]string, 0, 500)
	for i := 0; i < 1000; i++ {
		expect := fmt.Sprintf("%d %d %d %d", i*1000, i*1000, i*1000, i%4)
		expects = append(expects, expect)
		if len(expects) == 500 {
			break
		}
	}

	replicaReadModes := []string{
		"leader",
	}
	if !kerneltype.IsNextGen() {
		replicaReadModes = append(replicaReadModes,
			"follower",
			"leader-and-follower",
			"closest-adaptive",
			"closest-replicas",
		)
	}
	for _, mode := range replicaReadModes {
		tk.MustExec(fmt.Sprintf("set session tidb_replica_read = '%s'", mode))
		for i := 0; i < 20; i++ {
			// index lookup
			tk.MustQuery("select * from t force index(ic) order by c asc limit 500").Check(testkit.Rows(expects...))
			// index merge
			tk.MustQuery("select * from t where b >= 0 or c >= 0 order by c asc limit 500").Check(testkit.Rows(expects...))
		}
	}
}

func TestIndexReaderLimitConcurrencyWithUniqueKeyPoints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t_no_uk")
	// Like a secondary-index table: a non-unique index on (listing_id, object_id) for ordering, plus a
	// unique index on listing_id that bounds every listing_id point to at most one row.
	tk.MustExec(`create table t (
		object_id bigint primary key clustered,
		listing_id bigint default null,
		key idx_lo (listing_id, object_id),
		unique key uk_l (listing_id))`)
	tk.MustExec(`create table t_no_uk (
		object_id bigint primary key clustered,
		listing_id bigint default null,
		key idx_lo (listing_id, object_id))`)
	for i := range 200 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*10))
		tk.MustExec(fmt.Sprintf("insert into t_no_uk values (%d, %d)", i, i*10))
	}

	inList := func(n int) string {
		ids := make([]string, 0, n)
		for i := range n {
			ids = append(ids, fmt.Sprintf("%d", i*10))
		}
		return strings.Join(ids, ",")
	}
	expectConcurrency := func(expected int) context.Context {
		return context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
			require.Equal(t, expected, req.Concurrency)
		})
	}

	// 35 unique points can never reach LIMIT 100, so the Scan -> Limit request keeps the default concurrency.
	query := "select listing_id, object_id from %s force index(idx_lo) where listing_id in (%s) " +
		"order by listing_id, object_id limit 100"
	q := fmt.Sprintf(query, "t", inList(35))
	// The request must be a Scan -> Limit DAG under an IndexReader: columns are id, estRows, task, access object,
	// operator info.
	var hasReader, hasCopLimit bool
	for _, row := range tk.MustQuery("explain format='brief' " + q).Rows() {
		id, task, info := row[0].(string), row[2].(string), row[4].(string)
		hasReader = hasReader || strings.Contains(id, "IndexReader")
		hasCopLimit = hasCopLimit || (strings.Contains(id, "Limit") && task == "cop[tikv]" && info == "offset:0, count:100")
	}
	require.True(t, hasReader)
	require.True(t, hasCopLimit)
	require.Len(t, tk.MustQueryWithContext(expectConcurrency(vardef.DefDistSQLScanConcurrency), q).Rows(), 35)

	// More unique points than the limit: the limit may stop the scan early, keep the minimal concurrency.
	q = fmt.Sprintf(query, "t", inList(101))
	require.Len(t, tk.MustQueryWithContext(expectConcurrency(1), q).Rows(), 100)

	// Without a unique key on listing_id, a point may match many rows.
	q = fmt.Sprintf(query, "t_no_uk", inList(35))
	require.Len(t, tk.MustQueryWithContext(expectConcurrency(1), q).Rows(), 35)

	// A unique index allows duplicated NULLs.
	q = "select listing_id, object_id from t force index(idx_lo) where listing_id is null " +
		"order by listing_id, object_id limit 100"
	require.Len(t, tk.MustQueryWithContext(expectConcurrency(1), q).Rows(), 0)
}
