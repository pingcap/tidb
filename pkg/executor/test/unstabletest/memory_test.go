// Copyright 2019 PingCAP, Inc.
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
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/skip"
	"github.com/stretchr/testify/require"
)

func TestGlobalMemoryControl(t *testing.T) {
	// will timeout when data race enabled
	skip.UnderShort(t)
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
	require.NoError(t, tracker1.Killer.HandleSignal())
	require.NoError(t, tracker2.Killer.HandleSignal())
	require.True(t, exeerrors.ErrMemoryExceedForInstance.Equal(tracker3.Killer.HandleSignal()))
	require.Equal(t, memory.MemUsageTop1Tracker.Load(), tracker3)
	util.WithRecovery( // Next Consume() will panic and cancel the SQL
		func() {
			tracker3.Consume(1)
		}, func(r any) {
			require.True(t, exeerrors.ErrMemoryExceedForInstance.Equal(r.(error)))
		})
	tracker2.Consume(300 << 20) // Sum 500MB, Not Panic, Waiting t3 cancel finish.
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, tracker2.Killer.HandleSignal())
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
		}, func(r any) {
			require.True(t, exeerrors.ErrMemoryExceedForInstance.Equal(r.(error)))
		})
	require.Equal(t, test[0], 0) // Keep 1GB HeapInUse
}

func TestPBMemoryLeak(t *testing.T) {
	// will timeout when data race enabled
	skip.UnderShort(t)
	debug.SetGCPercent(1000)
	defer debug.SetGCPercent(100)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_mem")
	tk.MustExec("use test_mem")

	// prepare data
	totalSize := uint64(256 << 20) // 256MB
	blockSize := uint64(8 << 10)   // 8KB
	delta := totalSize / 5
	numRows := totalSize / blockSize
	tk.MustExec(fmt.Sprintf("create table t (c varchar(%v))", blockSize))
	sql := fmt.Sprintf("insert into t values (space(%v))", blockSize)
	for i := uint64(0); i < numRows; i++ {
		tk.MustExec(sql)
	}

	// read data
	runtime.GC()
	allocatedBegin, inUseBegin := readMem()
	records, err := tk.Session().Execute(context.Background(), "select * from t")
	require.NoError(t, err)
	record := records[0]
	rowCnt := 0
	chk := record.NewChunk(nil)
	for {
		require.NoError(t, record.Next(context.Background(), chk))
		rowCnt += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	require.Equal(t, int(numRows), rowCnt)

	// check memory before close
	runtime.GC()
	allocatedAfter, inUseAfter := readMem()
	require.GreaterOrEqual(t, allocatedAfter-allocatedBegin, totalSize)
	require.Less(t, memDiff(inUseAfter, inUseBegin), delta)

	runtime.GC()
	allocatedFinal, inUseFinal := readMem()
	require.Less(t, allocatedFinal-allocatedAfter, delta)
	require.Less(t, memDiff(inUseFinal, inUseAfter), delta)
}

// nolint:unused
func readMem() (allocated, heapInUse uint64) {
	var stat runtime.MemStats
	runtime.ReadMemStats(&stat)
	return stat.TotalAlloc, stat.HeapInuse
}

// nolint:unused
func memDiff(m1, m2 uint64) uint64 {
	if m1 > m2 {
		return m1 - m2
	}
	return m2 - m1
}
