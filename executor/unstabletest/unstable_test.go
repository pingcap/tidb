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

package unstabletest

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
)

func TestCartesianJoinPanic(t *testing.T) {
	// original position at join_test.go
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set tidb_mem_quota_query = 1 << 30")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = off;")
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t select * from t")
	}
	err := tk.QueryToErr("desc analyze select * from t t1, t t2, t t3, t t4, t t5, t t6;")
	require.NotNil(t, err)
	require.True(t, strings.Contains(err.Error(), "Out Of Memory Quota!"))
}

func TestGlobalMemoryControl(t *testing.T) {
	// original position at executor_test.go
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_server_memory_limit = 512 << 20")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	tk1 := testkit.NewTestKit(t, store)
	tracker1 := tk1.Session().GetSessionVars().MemTracker
	tracker1.FallbackOldAndSetNewAction(&memory.PanicOnExceed{})

	tk2 := testkit.NewTestKit(t, store)
	tracker2 := tk2.Session().GetSessionVars().MemTracker
	tracker2.FallbackOldAndSetNewAction(&memory.PanicOnExceed{})

	tk3 := testkit.NewTestKit(t, store)
	tracker3 := tk3.Session().GetSessionVars().MemTracker
	tracker3.FallbackOldAndSetNewAction(&memory.PanicOnExceed{})

	sm := &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk1.Session().ShowProcess(), tk2.Session().ShowProcess(), tk3.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tracker1.Consume(100 << 20) // 100 MB
	tracker2.Consume(200 << 20) // 200 MB
	tracker3.Consume(300 << 20) // 300 MB

	test := make([]int, 128<<20)       // Keep 1GB HeapInUse
	time.Sleep(500 * time.Millisecond) // The check goroutine checks the memory usage every 100ms. The Sleep() make sure that Top1Tracker can be Canceled.

	// Kill Top1
	require.False(t, tracker1.NeedKill.Load())
	require.False(t, tracker2.NeedKill.Load())
	require.True(t, tracker3.NeedKill.Load())
	require.Equal(t, memory.MemUsageTop1Tracker.Load(), tracker3)
	util.WithRecovery( // Next Consume() will panic and cancel the SQL
		func() {
			tracker3.Consume(1)
		}, func(r interface{}) {
			require.True(t, strings.Contains(r.(string), "Out Of Memory Quota!"))
		})
	tracker2.Consume(300 << 20) // Sum 500MB, Not Panic, Waiting t3 cancel finish.
	time.Sleep(500 * time.Millisecond)
	require.False(t, tracker2.NeedKill.Load())
	// Kill Finished
	tracker3.Consume(-(300 << 20))
	// Simulated SQL is Canceled and the time is updated
	sm.PSMu.Lock()
	ps := *sm.PS[2]
	ps.Time = time.Now()
	sm.PS[2] = &ps
	sm.PSMu.Unlock()
	time.Sleep(500 * time.Millisecond)
	// Kill the Next SQL
	util.WithRecovery( // Next Consume() will panic and cancel the SQL
		func() {
			tracker2.Consume(1)
		}, func(r interface{}) {
			require.True(t, strings.Contains(r.(string), "Out Of Memory Quota!"))
		})
	require.Equal(t, test[0], 0) // Keep 1GB HeapInUse
}
