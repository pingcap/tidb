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

package chunk

import (
	"crypto/rand"
	rand2 "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestNewRowContainer(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, 1024)
	require.NotNil(t, rc)
	require.False(t, rc.AlreadySpilledSafeForTest())
}

func TestSel(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	sz := 4
	rc := NewRowContainer(fields, sz)
	require.NotNil(t, rc)
	require.False(t, rc.AlreadySpilledSafeForTest())
	n := 64
	chk := NewChunkWithCapacity(fields, sz)
	numRows := 0
	for i := 0; i < n-sz; i++ {
		chk.AppendInt64(0, int64(i))
		if chk.NumRows() == sz {
			chk.SetSel([]int{0, 2})
			numRows += 2
			err := rc.Add(chk)
			require.NoError(t, err)
			chk = NewChunkWithCapacity(fields, sz)
		}
	}
	require.Equal(t, numRows/2, rc.NumChunks())
	require.Equal(t, numRows, rc.NumRow())
	for i := n - sz; i < n; i++ {
		chk.AppendInt64(0, int64(i))
	}
	chk.SetSel([]int{0, 1, 2})

	checkByIter := func(it Iterator) {
		i := 0
		for row := it.Begin(); row != it.End(); row = it.Next() {
			require.Equal(t, int64(i), row.GetInt64(0))
			if i < n-sz {
				i += 2
			} else {
				i++
			}
		}
		require.Equal(t, n-1, i)
	}
	checkByIter(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4Chunk(chk)))
	rc.SpillToDisk()
	err := rc.m.records.spillError
	require.NoError(t, err)
	require.True(t, rc.AlreadySpilledSafeForTest())
	checkByIter(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4Chunk(chk)))
	err = rc.Close()
	require.NoError(t, err)
	require.Equal(t, int64(0), rc.memTracker.BytesConsumed())
	require.Greater(t, rc.memTracker.MaxConsumed(), int64(0))
}

func TestSpillAction(t *testing.T) {
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewAction(rc.ActionSpillForTest())
	require.False(t, rc.AlreadySpilledSafeForTest())
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	require.NoError(t, err)
	require.False(t, rc.AlreadySpilledSafeForTest())
	require.Equal(t, chk.MemoryUsage(), rc.GetMemTracker().BytesConsumed())
	// The following line is erroneous, since chk is already handled by rc, Add it again causes duplicated memory usage account.
	// It is only for test of spill, do not double-add a chunk elsewhere.
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	require.NoError(t, err)
	require.True(t, rc.AlreadySpilledSafeForTest())

	// Read
	resChk, err := rc.GetChunk(0)
	require.NoError(t, err)
	require.Equal(t, chk.NumRows(), resChk.NumRows())
	for rowIdx := 0; rowIdx < resChk.NumRows(); rowIdx++ {
		require.Equal(t, chk.GetRow(rowIdx).GetDatumRow(fields), resChk.GetRow(rowIdx).GetDatumRow(fields))
	}
	// Write again
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	require.NoError(t, err)
	require.True(t, rc.AlreadySpilledSafeForTest())

	// Read
	resChk, err = rc.GetChunk(2)
	require.NoError(t, err)
	require.Equal(t, chk.NumRows(), resChk.NumRows())
	for rowIdx := 0; rowIdx < resChk.NumRows(); rowIdx++ {
		require.Equal(t, chk.GetRow(rowIdx).GetDatumRow(fields), resChk.GetRow(rowIdx).GetDatumRow(fields))
	}

	err = rc.Reset()
	require.NoError(t, err)
}

func TestNewSortedRowContainer(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewSortedRowContainer(fields, 1024, nil, nil, nil)
	require.NotNil(t, rc)
	require.False(t, rc.AlreadySpilledSafeForTest())
}

func TestSortedRowContainerSortSpillAction(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	byItemsDesc := []bool{false}
	keyColumns := []int{0}
	keyCmpFuncs := []CompareFunc{cmpInt64}
	sz := 20
	rc := NewSortedRowContainer(fields, sz, byItemsDesc, keyColumns, keyCmpFuncs)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + int64(8*chk.NumRows()) + 1)
	tracker.FallbackOldAndSetNewAction(rc.ActionSpillForTest())
	require.False(t, rc.AlreadySpilledSafeForTest())
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	require.NoError(t, err)
	require.False(t, rc.AlreadySpilledSafeForTest())
	require.Equal(t, chk.MemoryUsage()+int64(8*chk.NumRows()), rc.GetMemTracker().BytesConsumed())
	// The following line is erroneous, since chk is already handled by rc, Add it again causes duplicated memory usage account.
	// It is only for test of spill, do not double-add a chunk elsewhere.
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	require.NoError(t, err)
	require.True(t, rc.AlreadySpilledSafeForTest())
	// The result has been sorted.
	for i := 0; i < sz*2; i++ {
		row, err := rc.GetSortedRow(i)
		require.NoError(t, err)
		require.Equal(t, int64(i/2), row.GetInt64(0))
	}
	// Can't insert records again.
	err = rc.Add(chk)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCannotAddBecauseSorted)
	err = rc.Reset()
	require.NoError(t, err)
}

func TestRowContainerResetAndAction(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	sz := 20
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewAction(rc.ActionSpillForTest())
	require.False(t, rc.AlreadySpilledSafeForTest())
	err = rc.Add(chk)
	require.NoError(t, err)
	require.Equal(t, int64(0), rc.GetDiskTracker().BytesConsumed())
	err = rc.Add(chk)
	require.NoError(t, err)
	rc.actionSpill.WaitForTest()
	require.Greater(t, rc.GetDiskTracker().BytesConsumed(), int64(0))
	// Reset and Spill again.
	err = rc.Reset()
	require.NoError(t, err)
	require.Equal(t, int64(0), rc.GetDiskTracker().BytesConsumed())
	err = rc.Add(chk)
	require.NoError(t, err)
	require.Equal(t, int64(0), rc.GetDiskTracker().BytesConsumed())
	err = rc.Add(chk)
	require.NoError(t, err)
	rc.actionSpill.WaitForTest()
	require.Greater(t, rc.GetDiskTracker().BytesConsumed(), int64(0))
}

func TestSpillActionDeadLock(t *testing.T) {
	// Maybe get deadlock if we use two RLock in one goroutine, for oom-action call stack.
	// Now the implement avoids the situation.
	// Goroutine 1: rc.Add() (RLock) -> list.Add() -> tracker.Consume() -> SpillDiskAction -> rc.AlreadySpilledSafeForTest() (RLock)
	// Goroutine 2: ------------------> SpillDiskAction -> new Goroutine to spill -> ------------------
	// new Goroutine created by 2: ---> rc.SpillToDisk (Lock)
	// In golang, RLock will be blocked after try to get Lock. So it will cause deadlock.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/testRowContainerDeadLock", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/testRowContainerDeadLock"))
	}()
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(1)
	ac := rc.ActionSpillForTest()
	tracker.FallbackOldAndSetNewAction(ac)
	require.False(t, rc.AlreadySpilledSafeForTest())
	go func() {
		time.Sleep(200 * time.Millisecond)
		ac.Action(tracker)
	}()
	err = rc.Add(chk)
	require.NoError(t, err)
	rc.actionSpill.WaitForTest()
	require.True(t, rc.AlreadySpilledSafeForTest())
}

func TestActionBlocked(t *testing.T) {
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	// Case 1, test Broadcast in Action.
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(1450)
	ac := rc.ActionSpill()
	tracker.FallbackOldAndSetNewAction(ac)
	for i := 0; i < 10; i++ {
		err = rc.Add(chk)
		require.NoError(t, err)
	}

	ac.cond.L.Lock()
	for ac.cond.status == notSpilled ||
		ac.cond.status == spilling {
		ac.cond.Wait()
	}
	ac.cond.L.Unlock()
	ac.cond.L.Lock()
	require.Equal(t, spilledYet, ac.cond.status)
	ac.cond.L.Unlock()
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Greater(t, tracker.MaxConsumed(), int64(0))
	require.Greater(t, rc.GetDiskTracker().BytesConsumed(), int64(0))

	// Case 2, test Action will block when spilling.
	rc = NewRowContainer(fields, sz)
	tracker = rc.GetMemTracker()
	ac = rc.ActionSpill()
	starttime := time.Now()
	ac.setStatus(spilling)
	go func() {
		time.Sleep(200 * time.Millisecond)
		ac.setStatus(spilledYet)
		ac.cond.Broadcast()
	}()
	ac.Action(tracker)
	require.GreaterOrEqual(t, time.Since(starttime), 200*time.Millisecond)
}

func insertBytesRowsIntoRowContainer(t *testing.T, chkCount int, rowPerChk int) (*RowContainer, [][]byte) {
	longVarCharTyp := types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).SetFlen(4096).Build()
	fields := []*types.FieldType{&longVarCharTyp}

	rc := NewRowContainer(fields, chkCount)

	allRows := [][]byte{}
	// insert chunks
	for i := 0; i < chkCount; i++ {
		chk := NewChunkWithCapacity(fields, rowPerChk)
		// insert rows for each chunk
		for j := 0; j < rowPerChk; j++ {
			length := rand2.Uint32()
			randomBytes := make([]byte, length%4096)
			_, err := rand.Read(randomBytes)
			require.NoError(t, err)

			chk.AppendBytes(0, randomBytes)
			allRows = append(allRows, randomBytes)
		}
		require.NoError(t, rc.Add(chk))
	}

	return rc, allRows
}

func TestRowContainerReaderInDisk(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	rc, allRows := insertBytesRowsIntoRowContainer(t, 16, 16)
	rc.SpillToDisk()

	reader := NewRowContainerReader(rc)
	defer reader.Close()
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			row := reader.Current()
			require.Equal(t, allRows[i*16+j], row.GetBytes(0))
			reader.Next()
		}
	}
}

func TestCloseRowContainerReader(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	rc, allRows := insertBytesRowsIntoRowContainer(t, 16, 16)
	rc.SpillToDisk()

	// read 8.5 of these chunks
	reader := NewRowContainerReader(rc)
	defer reader.Close()
	for i := 0; i < 8; i++ {
		for j := 0; j < 16; j++ {
			row := reader.Current()
			require.Equal(t, allRows[i*16+j], row.GetBytes(0))
			reader.Next()
		}
	}
	for j := 0; j < 8; j++ {
		row := reader.Current()
		require.Equal(t, allRows[8*16+j], row.GetBytes(0))
		reader.Next()
	}
}

func TestConcurrentSpillWithRowContainerReader(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	rc, allRows := insertBytesRowsIntoRowContainer(t, 16, 1024)

	var wg sync.WaitGroup
	// concurrently read and spill to disk
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := NewRowContainerReader(rc)
		defer reader.Close()

		for i := 0; i < 16; i++ {
			for j := 0; j < 1024; j++ {
				row := reader.Current()
				require.Equal(t, allRows[i*1024+j], row.GetBytes(0))
				reader.Next()
			}
		}
	}()
	rc.SpillToDisk()
	wg.Wait()
}

func TestReadAfterSpillWithRowContainerReader(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	rc, allRows := insertBytesRowsIntoRowContainer(t, 16, 1024)

	reader := NewRowContainerReader(rc)
	defer reader.Close()
	for i := 0; i < 8; i++ {
		for j := 0; j < 1024; j++ {
			row := reader.Current()
			require.Equal(t, allRows[i*1024+j], row.GetBytes(0))
			reader.Next()
		}
	}
	rc.SpillToDisk()
	for i := 8; i < 16; i++ {
		for j := 0; j < 1024; j++ {
			row := reader.Current()
			require.Equal(t, allRows[i*1024+j], row.GetBytes(0))
			reader.Next()
		}
	}
}

func TestPanicWhenSpillToDisk(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	sz := 20
	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}

	rc := NewRowContainer(fields, sz)
	tracker := rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewAction(rc.ActionSpillForTest())
	require.False(t, rc.AlreadySpilledSafeForTest())

	require.NoError(t, rc.Add(chk))
	rc.actionSpill.WaitForTest()
	require.False(t, rc.AlreadySpilledSafeForTest())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/spillToDiskOutOfDiskQuota", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/spillToDiskOutOfDiskQuota"))
	}()
	require.NoError(t, rc.Add(chk))
	rc.actionSpill.WaitForTest()
	require.True(t, rc.AlreadySpilledSafeForTest())

	_, err := rc.GetRow(RowPtr{})
	require.EqualError(t, err, "out of disk quota when spilling")
	require.EqualError(t, rc.Add(chk), "out of disk quota when spilling")
}

func TestPanicDuringSortedRowContainerSpill(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	byItemsDesc := []bool{false}
	keyColumns := []int{0}
	keyCmpFuncs := []CompareFunc{cmpInt64}
	sz := 20
	rc := NewSortedRowContainer(fields, sz, byItemsDesc, keyColumns, keyCmpFuncs)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + int64(8*chk.NumRows()) + 1)
	tracker.FallbackOldAndSetNewAction(rc.ActionSpillForTest())
	require.False(t, rc.AlreadySpilledSafeForTest())
	err = rc.Add(chk)
	require.NoError(t, err)
	rc.actionSpill.WaitForTest()
	require.False(t, rc.AlreadySpilledSafeForTest())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/errorDuringSortRowContainer", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/errorDuringSortRowContainer"))
	}()
	err = rc.Add(chk)
	require.NoError(t, err)
	rc.actionSpill.WaitForTest()
	require.True(t, rc.AlreadySpilledSafeForTest())

	_, err = rc.GetRow(RowPtr{})
	require.EqualError(t, err, "sort meet error")
}

func TestInterruptedDuringSpilling(t *testing.T) {
	rootTracker := memory.NewTracker(-1, -1)
	rootTracker.IsRootTrackerOfSess = true
	rootTracker.Killer = &sqlkiller.SQLKiller{ConnID: 1}
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
	}
	sz := 1024
	rc := NewRowContainer(fields, sz)
	rc.GetMemTracker().AttachTo(rootTracker)
	defer rc.Close()
	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(i))
		chk.AppendInt64(2, int64(i))
		chk.AppendString(3, "testtesttest")
		chk.AppendInt64(4, int64(i))
	}
	for i := 0; i < 102400; i++ {
		rc.Add(chk)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	var cancelTime time.Time
	go func() {
		time.Sleep(200 * time.Millisecond)
		rootTracker.Killer.SendKillSignal(sqlkiller.QueryInterrupted)
		cancelTime = time.Now()
		wg.Done()
	}()
	rc.spillToDisk(nil)
	wg.Wait()
	cancelDuration := time.Since(cancelTime)
	require.Less(t, cancelDuration, 1*time.Second)
}

func BenchmarkRowContainerReaderInDiskWithRowSize512(b *testing.B) {
	benchmarkRowContainerReaderInDiskWithRowLength(b, 512)
}

func BenchmarkRowContainerReaderInDiskWithRowSize1024(b *testing.B) {
	benchmarkRowContainerReaderInDiskWithRowLength(b, 1024)
}

func BenchmarkRowContainerReaderInDiskWithRowSize4096(b *testing.B) {
	benchmarkRowContainerReaderInDiskWithRowLength(b, 4096)
}

func benchmarkRowContainerReaderInDiskWithRowLength(b *testing.B, rowLength int) {
	b.StopTimer()

	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = b.TempDir()
	})

	longVarCharTyp := types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).SetFlen(rowLength).Build()
	fields := []*types.FieldType{&longVarCharTyp}

	randomBytes := make([]byte, rowLength)
	_, err := rand.Read(randomBytes)
	require.NoError(b, err)

	// create a row container which stores the data in disk
	rc := NewRowContainer(fields, 1<<10)
	rc.SpillToDisk()

	// insert `b.N * 1<<10` rows (`b.N` chunks) into the rc
	for i := 0; i < b.N; i++ {
		chk := NewChunkWithCapacity(fields, 1<<10)
		for j := 0; j < 1<<10; j++ {
			chk.AppendBytes(0, randomBytes)
		}

		rc.Add(chk)
	}

	reader := NewRowContainerReader(rc)
	defer reader.Close()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1<<10; i++ {
			reader.Next()
		}
	}
	require.NoError(b, reader.Error())
}
