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

package executor_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestParallelApplyInflightQueryError(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := "unordered"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t1 (id int primary key, a int)")
			tk.MustExec("create table t2 (a int, b int)")
			tk.MustExec("insert into t1 values (0, 1), (1, 1), (2, 1), (3, 1)")
			tk.MustExec("insert into t2 values (1, 10), (1, 20)")
			tk.MustExec("analyze table t1, t2")
			tk.MustExec("set tidb_enable_parallel_apply = true")
			tk.MustExec("set tidb_executor_concurrency = 4")
			sql := "select t1.id, (select /*+ NO_DECORRELATE() */ t2.b from t2 where t2.a = t1.a) from t1"
			if ordered {
				sql += " order by t1.id"
			}

			waitersStarted := make(chan struct{})
			var waiters atomic.Int32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/parallelApplyWaitForInflight", func() {
				if waiters.Add(1) == 3 {
					close(waitersStarted)
				}
			})
			// The owner is paused after Open, before the MaxOneRow error.
			// Release it only after every other worker has joined its read.
			const pause = "github.com/pingcap/tidb/pkg/executor/parallelApplySlowInner"
			testfailpoint.Enable(t, pause, "pause")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done := make(chan struct{})
			var queryErr, closeErr error
			go func() {
				defer close(done)
				rs, err := tk.Exec(sql)
				if err != nil {
					queryErr = err
					return
				}
				chk := rs.NewChunk(nil)
				for {
					queryErr = rs.Next(ctx, chk)
					if queryErr != nil || chk.NumRows() == 0 {
						break
					}
					chk.Reset()
				}
				closeErr = rs.Close()
			}()
			defer func() {
				// Also unblock the query if a synchronization assertion fails.
				require.NoError(t, failpoint.Disable(pause))
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("query workers did not finish during cleanup")
				}
			}()
			select {
			case <-waitersStarted:
			case <-done:
				t.Fatalf("query finished before waiters joined: %v", queryErr)
			case <-ctx.Done():
				t.Fatal("waiters did not join the owner")
			}
			require.NoError(t, failpoint.Disable(pause))
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("query error or Close did not finish")
			}
			require.ErrorContains(t, queryErr, "Subquery returns more than 1 row")
			require.NoError(t, closeErr)
		})
	}
}
