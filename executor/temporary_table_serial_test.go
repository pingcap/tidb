// Copyright 2021 PingCAP, Inc.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestTemporaryTableNoNetwork(t *testing.T) {
	t.Run("global", func(t *testing.T) {
		assertTemporaryTableNoNetwork(t, func(tk *testkit.TestKit) {
			tk.MustExec("create global temporary table tmp_t (id int primary key, a int, b int, index(a)) on commit delete rows")
		})
	})

	t.Run("local", func(t *testing.T) {
		assertTemporaryTableNoNetwork(t, func(tk *testkit.TestKit) {
			tk.MustExec("create temporary table tmp_t (id int primary key, a int, b int, index(a))")
		})
	})
}

func assertTemporaryTableNoNetwork(t *testing.T, createTable func(*testkit.TestKit)) {
	var done sync.WaitGroup
	defer done.Wait()

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	// Test that table reader/index reader/index lookup on the temporary table do not need to visit TiKV.
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk1.MustExec("use test")
	tk.MustExec("drop table if exists normal, tmp_t")
	tk.MustExec("create table normal (id int, a int, index(a))")
	createTable(tk)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy"))
	}()

	tk.MustExec("begin")
	tk.MustExec("insert into tmp_t values (1, 1, 1)")
	tk.MustExec("insert into tmp_t values (2, 2, 2)")

	// Make sure the fail point works.
	// With that failpoint, all requests to the TiKV is discard.
	rs, err := tk1.Exec("select * from normal")
	require.NoError(t, err)

	blocked := make(chan struct{}, 1)
	ctx, cancelFunc := context.WithCancel(context.Background())
	done.Add(1)
	go func() {
		defer done.Done()
		_, _ = session.ResultSetToStringSlice(ctx, tk1.Session(), rs)
		blocked <- struct{}{}
	}()

	select {
	case <-blocked:
		cancelFunc()
		require.FailNow(t, "The query should block when the failpoint is enabled.")
	case <-time.After(200 * time.Millisecond):
		cancelFunc()
	}

	// Check the temporary table do not send request to TiKV.
	// PointGet
	require.True(t, tk.HasPlan("select * from tmp_t where id=1", "Point_Get"))
	tk.MustQuery("select * from tmp_t where id=1").Check(testkit.Rows("1 1 1"))

	// BatchPointGet
	require.True(t, tk.HasPlan("select * from tmp_t where id in (1, 2)", "Batch_Point_Get"))
	tk.MustQuery("select * from tmp_t where id in (1, 2)").Check(testkit.Rows("1 1 1", "2 2 2"))

	// Table reader
	require.True(t, tk.HasPlan("select * from tmp_t", "TableReader"))
	tk.MustQuery("select * from tmp_t").Check(testkit.Rows("1 1 1", "2 2 2"))

	// Index reader
	require.True(t, tk.HasPlan("select /*+ USE_INDEX(tmp_t, a) */ a from tmp_t", "IndexReader"))
	tk.MustQuery("select /*+ USE_INDEX(tmp_t, a) */ a from tmp_t").Check(testkit.Rows("1", "2"))

	// Index lookup
	require.True(t, tk.HasPlan("select /*+ USE_INDEX(tmp_t, a) */ b from tmp_t where a = 1", "IndexLookUp"))
	tk.MustQuery("select /*+ USE_INDEX(tmp_t, a) */ b from tmp_t where a = 1").Check(testkit.Rows("1"))
	tk.MustExec("rollback")

	// prepare some data for local temporary table, when for global temporary table, the below operations have no effect.
	tk.MustExec("insert into tmp_t value(10, 10, 10)")
	tk.MustExec("insert into tmp_t value(11, 11, 11)")

	// Pessimistic lock
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tmp_t values (3, 3, 3)")
	tk.MustExec("insert ignore into tmp_t values (4, 4, 4)")
	tk.MustExec("insert into tmp_t values (5, 5, 5) on duplicate key update a=100")
	tk.MustExec("insert into tmp_t values (10, 10, 10) on duplicate key update a=100")
	tk.MustExec("insert ignore into tmp_t values (10, 10, 10) on duplicate key update id=11")
	tk.MustExec("replace into tmp_t values(6, 6, 6)")
	tk.MustExec("replace into tmp_t values(11, 100, 100)")
	tk.MustExec("update tmp_t set id = id + 1 where a = 1")
	tk.MustExec("delete from tmp_t where a > 1")
	tk.MustQuery("select count(*) from tmp_t where a >= 1 for update")
	tk.MustExec("rollback")

	// Check 'for update' will not write any lock too when table is unmodified
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tmp_t where id=1 for update")
	tk.MustExec("select * from tmp_t where id in (1, 2, 3) for update")
	tk.MustExec("select * from tmp_t where id > 1 for update")
	tk.MustExec("rollback")
}
