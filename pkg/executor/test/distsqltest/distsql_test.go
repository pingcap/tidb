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
