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
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
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
