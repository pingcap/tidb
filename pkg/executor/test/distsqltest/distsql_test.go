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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
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
	for i := 0; i < 20; i++ {
		pid := i + 1
		partitions = append(partitions, fmt.Sprintf("PARTITION p%d VALUES LESS THAN (%d00)", pid, pid))
	}
	tk.MustExec("create table t2(id int primary key, val int)" +
		"partition by range(id)" +
		"(" + strings.Join(partitions[:10], ",") + ")")
	tk.MustExec("create table t3(id int primary key, val int)" +
		"partition by range(id)" +
		"(" + strings.Join(partitions, ",") + ")")
	for i := 0; i < 20; i++ {
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
		require.Equal(t, req.Concurrency, variable.DefDistSQLScanConcurrency)
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
		"follower",
		"leader-and-follower",
		"closest-adaptive",
		"closest-replicas",
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
