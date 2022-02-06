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

package executor_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestPBMemoryLeak(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
		require.Nil(t, record.Next(context.Background(), chk))
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

func TestGlobalMemoryTrackerOnCleanUp(t *testing.T) {
	// TODO: assert the memory consume has happened in another way
	originConsume := executor.GlobalMemoryUsageTracker.BytesConsumed()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	// assert insert
	tk.MustExec("insert t (id) values (1)")
	tk.MustExec("insert t (id) values (2)")
	tk.MustExec("insert t (id) values (3)")
	afterConsume := executor.GlobalMemoryUsageTracker.BytesConsumed()
	require.Equal(t, afterConsume, originConsume)

	// assert update
	tk.MustExec("update t set id = 4 where id = 1")
	tk.MustExec("update t set id = 5 where id = 2")
	tk.MustExec("update t set id = 6 where id = 3")
	afterConsume = executor.GlobalMemoryUsageTracker.BytesConsumed()
	require.Equal(t, afterConsume, originConsume)
}
