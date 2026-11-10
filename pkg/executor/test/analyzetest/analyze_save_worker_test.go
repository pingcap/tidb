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

package analyzetest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

const saveWorkerStorageFailpoint = "github.com/pingcap/tidb/pkg/statistics/handle/storage/"

func newSaveWorkerTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_analyze_partition_concurrency=2")
	tk.MustExec("set @@tidb_build_stats_concurrency=2")
	tk.MustExec("create table save_worker (a int primary key, b int, key idx(b)) partition by hash(a) partitions 10")
	tk.MustExec("insert into save_worker values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)")
	return tk
}

// Ten partition results exceed two save workers and their two-slot buffer.
// Both workers must keep consuming after their first storage failure.
func TestAnalyzeSaveWorkerError(t *testing.T) {
	tk := newSaveWorkerTestKit(t)
	testfailpoint.Enable(t, saveWorkerStorageFailpoint+"slowStatsSaving", "return(true)")
	testfailpoint.Enable(t, saveWorkerStorageFailpoint+"failToSaveStats", "return(true)")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := tk.ExecWithContext(ctx, "analyze table save_worker")
	require.ErrorContains(t, err, "failed to update stats meta version during analyze result save")
	tk.MustExec("select 1")
	tk.MustQuery("select count(*) from mysql.analyze_jobs where table_name='save_worker' and state in ('pending','running')").Check(testkit.Rows("0"))
}

func TestAnalyzeSaveWorkerPanic(t *testing.T) {
	tk := newSaveWorkerTestKit(t)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/analyzeSaveStatsWorkerPanic", "panic(\"save-worker-panic\")")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := tk.ExecWithContext(ctx, "analyze table save_worker")
	require.Error(t, err)
	tk.MustExec("select 1")
	tk.MustQuery("select count(*) from mysql.analyze_jobs where table_name='save_worker' and state in ('pending','running')").Check(testkit.Rows("0"))
}

func TestAnalyzeSaveWorkerSuccess(t *testing.T) {
	tk := newSaveWorkerTestKit(t)
	tk.MustExec("analyze table save_worker")
	tk.MustQuery("select count(*) from mysql.analyze_jobs where table_name='save_worker' and state in ('pending','running','failed')").Check(testkit.Rows("0"))
}

func TestAnalyzeSaveWorkerCancelBacklog(t *testing.T) {
	for _, mode := range []string{"context", "kill"} {
		t.Run(mode, func(t *testing.T) {
			tk := newSaveWorkerTestKit(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			var released atomic.Bool
			unblock := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			defer unblock()
			testfailpoint.EnableCall(t, saveWorkerStorageFailpoint+"saveAnalyzeResultToStorage", func() {
				<-release
			})
			backlog := make(chan struct{})
			var sent atomic.Int32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/analyzeBeforeSendToSaveResults", func() {
				if sent.Add(1) == 5 {
					close(backlog)
				}
			})
			buffered := make(chan struct{})
			var built atomic.Int32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/analyzeAfterSendResult", func() {
				if built.Add(1) == 6 {
					close(buffered)
				}
			})
			workersDone := make(chan struct{})
			var finished atomic.Int32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/analyzeSaveStatsWorkerDone", func() {
				if finished.Add(1) == 2 {
					close(workersDone)
				}
			})
			done := make(chan error, 1)
			go func() {
				_, err := tk.ExecWithContext(ctx, "analyze table save_worker")
				done <- err
			}()
			for _, event := range []struct {
				name string
				done <-chan struct{}
			}{{"save backlog", backlog}, {"buffered build result", buffered}} {
				select {
				case <-event.done:
				case <-time.After(10 * time.Second):
					t.Fatalf("ANALYZE did not reach %s", event.name)
				}
			}
			if mode == "context" {
				cancel()
			} else {
				tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
			}
			unblock()
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("cancelled ANALYZE did not return")
			}
			// Saving uses an internal context. Synchronize with its owned
			// workers rather than assuming SQL return waits for in-flight saves.
			select {
			case <-workersDone:
			case <-time.After(10 * time.Second):
				t.Fatal("cancelled ANALYZE did not clean up its save workers")
			}
			tk.MustExec("select 1")
			tk.MustQuery("select count(*) from mysql.analyze_jobs where table_name='save_worker' and state in ('pending','running')").Check(testkit.Rows("0"))
		})
	}
}
