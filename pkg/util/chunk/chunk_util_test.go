// Copyright 2018 PingCAP, Inc.
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
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// getChk generate a chunk of data, isLast3ColTheSame means the last three columns are the same.
func getChk(isLast3ColTheSame bool) (*Chunk, *Chunk, []bool) {
	numRows := 1024
	srcChk := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	selected := make([]bool, numRows)
	var row Row
	for j := range numRows {
		if isLast3ColTheSame {
			if j%7 == 0 {
				row = MutRowFromValues("abc", "abcdefg", nil, 123, types.ZeroDatetime, "abcdefg").ToRow()
			} else {
				row = MutRowFromValues("abc", "abcdefg", j, 123, types.ZeroDatetime, "abcdefg").ToRow()
			}
		} else {
			if j%7 == 0 {
				row = MutRowFromValues("abc", "abcdefg", nil, rand.Int(), types.ZeroDatetime, "abcdefg").ToRow()
			} else {
				row = MutRowFromValues("aabc", "ab234fg", j, 123, types.ZeroDatetime, "abcdefg").ToRow()
			}
		}
		if j%7 != 0 {
			selected[j] = true
		}
		srcChk.AppendPartialRow(0, row)
	}
	dstChk := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	return srcChk, dstChk, selected
}

func TestCopySelectedJoinRows(t *testing.T) {
	srcChk, dstChk, selected := getChk(true)
	numRows := srcChk.NumRows()
	for i := range numRows {
		if !selected[i] {
			continue
		}
		dstChk.AppendRow(srcChk.GetRow(i))
	}
	// batch copy
	dstChk2 := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	_, err := CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 3, 3, 3, selected, dstChk2)
	require.NoError(t, err)

	require.Equal(t, dstChk, dstChk2)
	numSelected := 0
	for i := range selected {
		if selected[i] {
			numSelected++
		}
	}
	require.Equal(t, numSelected, dstChk2.numVirtualRows)
	require.Equal(t, numSelected, dstChk2.NumRows())
}

func TestCopySelectedJoinRowsWithoutSameOuters(t *testing.T) {
	srcChk, dstChk, selected := getChk(false)
	numRows := srcChk.NumRows()
	for i := range numRows {
		if !selected[i] {
			continue
		}
		dstChk.AppendRow(srcChk.GetRow(i))
	}
	// batch copy
	dstChk2 := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	_, err := CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 6, 0, 0, selected, dstChk2)
	require.NoError(t, err)

	require.Equal(t, dstChk, dstChk2)
	numSelected := 0
	for i := range selected {
		if selected[i] {
			numSelected++
		}
	}
	require.Equal(t, numSelected, dstChk2.numVirtualRows)
	require.Equal(t, numSelected, dstChk2.NumRows())
}

func TestCopySelectedJoinRowsDirect(t *testing.T) {
	srcChk, dstChk, selected := getChk(false)
	numRows := srcChk.NumRows()
	for i := range numRows {
		if !selected[i] {
			continue
		}
		dstChk.AppendRow(srcChk.GetRow(i))
	}
	// batch copy
	dstChk2 := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	_, err := CopySelectedJoinRowsDirect(srcChk, selected, dstChk2)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(dstChk, dstChk2) {
		t.Fatal()
	}
	numSelected := 0
	for i := range selected {
		if selected[i] {
			numSelected++
		}
	}
	require.Equal(t, numSelected, dstChk2.numVirtualRows)
	require.Equal(t, numSelected, dstChk2.NumRows())
}

func TestCopySelectedVirtualNum(t *testing.T) {
	// srcChk does not contain columns
	srcChk := newChunk()
	srcChk.TruncateTo(3)
	dstChk := newChunk()
	selected := []bool{true, false, true}
	ok, err := CopySelectedJoinRowsDirect(srcChk, selected, dstChk)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, dstChk.numVirtualRows)

	dstChk = newChunk()
	ok, err = CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 0, 0, 0, selected, dstChk)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, dstChk.numVirtualRows)

	srcChk = newChunk(8)
	srcChk.TruncateTo(0)
	srcChk.AppendInt64(0, 0)
	srcChk.AppendInt64(0, 1)
	srcChk.AppendInt64(0, 2)
	dstChk = newChunkWithInitCap(0, 8)
	ok, err = CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 1, 1, 0, selected, dstChk)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, dstChk.numVirtualRows)
	require.Equal(t, 2, dstChk.NumRows())

	row0, row1 := dstChk.GetRow(0).GetInt64(0), dstChk.GetRow(1).GetInt64(0)
	require.Equal(t, int64(0), row0)
	require.Equal(t, int64(2), row1)

	srcChk = newChunk(8)
	srcChk.TruncateTo(0)
	srcChk.AppendInt64(0, 3)
	srcChk.AppendInt64(0, 3)
	srcChk.AppendInt64(0, 3)
	dstChk = newChunkWithInitCap(0, 8)
	ok, err = CopySelectedJoinRowsWithSameOuterRows(srcChk, 1, 0, 0, 1, selected, dstChk)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, dstChk.numVirtualRows)
	require.Equal(t, 2, dstChk.NumRows())
	row0, row1 = dstChk.GetRow(0).GetInt64(0), dstChk.GetRow(1).GetInt64(0)
	require.Equal(t, int64(3), row0)
	require.Equal(t, int64(3), row1)
}

func BenchmarkCopySelectedJoinRows(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(true)
	b.ResetTimer()
	for range b.N {
		dstChk.Reset()
		_, err := CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 3, 3, 3, selected, dstChk)
		if err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkCopySelectedJoinRowsDirect(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(false)
	b.ResetTimer()
	for range b.N {
		dstChk.Reset()
		_, err := CopySelectedJoinRowsDirect(srcChk, selected, dstChk)
		if err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkAppendSelectedRow(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(true)
	numRows := srcChk.NumRows()
	b.ResetTimer()
	for range b.N {
		dstChk.Reset()
		for j := range numRows {
			if !selected[j] {
				continue
			}
			dstChk.AppendRow(srcChk.GetRow(j))
		}
	}
}

func TestMergeInputIdxToOutputIdxes(t *testing.T) {
	inputIdxToOutputIdxes := make(map[int][]int)
	// input 0th should be column referred as 0th and 1st in output columns.
	inputIdxToOutputIdxes[0] = []int{0, 1}
	// input 1th should be column referred as 2nd and 3rd in output columns.
	inputIdxToOutputIdxes[1] = []int{2, 3}
	columnEval := ColumnSwapHelper{InputIdxToOutputIdxes: inputIdxToOutputIdxes}

	input := NewEmptyChunk([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)})
	input.AppendInt64(0, 99)
	// input chunk's 0th and 1st are column referred itself.
	input.MakeRef(0, 1)

	// chunk:     col1 <---(ref) col2
	// ____________/ \___________/  \___
	// proj:  col1   col2     col3   col4
	//
	// original case after inputIdxToOutputIdxes[0], the original col2 will be nil pointer
	// cause consecutive col3,col4 ref projection are invalid.
	//
	// after fix, the new inputIdxToOutputIdxes should be: inputIdxToOutputIdxes[0]: {0, 1, 2, 3}

	output := NewEmptyChunk([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)})

	err := columnEval.SwapColumns(input, output)
	require.NoError(t, err)
	// all four columns are column-referred, pointing to the first one.
	require.Equal(t, output.Column(0), output.Column(1))
	require.Equal(t, output.Column(1), output.Column(2))
	require.Equal(t, output.Column(2), output.Column(3))
	require.Equal(t, output.GetRow(0).GetInt64(0), int64(99))

	require.Equal(t, len(*columnEval.mergedInputIdxToOutputIdxes.Load()), 1)
	slices.Sort((*columnEval.mergedInputIdxToOutputIdxes.Load())[0])
	require.Equal(t, (*columnEval.mergedInputIdxToOutputIdxes.Load())[0], []int{0, 1, 2, 3})
}
