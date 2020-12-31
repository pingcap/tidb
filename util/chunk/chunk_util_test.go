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
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/types"
)

// getChk generate a chunk of data, isLast3ColTheSame means the last three columns are the same.
func getChk(isLast3ColTheSame bool) (*Chunk, *Chunk, []bool) {
	numRows := 1024
	srcChk := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	selected := make([]bool, numRows)
	var row Row
	for j := 0; j < numRows; j++ {
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
	for i := 0; i < numRows; i++ {
		if !selected[i] {
			continue
		}
		dstChk.AppendRow(srcChk.GetRow(i))
	}
	// batch copy
	dstChk2 := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 3, 3, 3, selected, dstChk2)

	if !reflect.DeepEqual(dstChk, dstChk2) {
		t.Fatal()
	}
	numSelected := 0
	for i := range selected {
		if selected[i] {
			numSelected++
		}
	}
	if dstChk2.numVirtualRows != numSelected || dstChk2.NumRows() != numSelected {
		t.Fatal(dstChk2.numVirtualRows, dstChk2.NumRows(), numSelected)
	}
}

func TestCopySelectedJoinRowsWithoutSameOuters(t *testing.T) {
	srcChk, dstChk, selected := getChk(false)
	numRows := srcChk.NumRows()
	for i := 0; i < numRows; i++ {
		if !selected[i] {
			continue
		}
		dstChk.AppendRow(srcChk.GetRow(i))
	}
	// batch copy
	dstChk2 := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 6, 0, 0, selected, dstChk2)

	if !reflect.DeepEqual(dstChk, dstChk2) {
		t.Fatal()
	}
	numSelected := 0
	for i := range selected {
		if selected[i] {
			numSelected++
		}
	}
	if dstChk2.numVirtualRows != numSelected || dstChk2.NumRows() != numSelected {
		t.Fatal(dstChk2.numVirtualRows, dstChk2.NumRows(), numSelected)
	}
}

func TestCopySelectedJoinRowsDirect(t *testing.T) {
	srcChk, dstChk, selected := getChk(false)
	numRows := srcChk.NumRows()
	for i := 0; i < numRows; i++ {
		if !selected[i] {
			continue
		}
		dstChk.AppendRow(srcChk.GetRow(i))
	}
	// batch copy
	dstChk2 := newChunkWithInitCap(numRows, 0, 0, 8, 8, sizeTime, 0)
	CopySelectedJoinRowsDirect(srcChk, selected, dstChk2)

	if !reflect.DeepEqual(dstChk, dstChk2) {
		t.Fatal()
	}
	numSelected := 0
	for i := range selected {
		if selected[i] {
			numSelected++
		}
	}
	if dstChk2.numVirtualRows != numSelected || dstChk2.NumRows() != numSelected {
		t.Fatal(dstChk2.numVirtualRows, dstChk2.NumRows(), numSelected)
	}
}

func TestCopySelectedVirtualNum(t *testing.T) {
	// srcChk does not contain columns
	srcChk := newChunk()
	srcChk.TruncateTo(3)
	dstChk := newChunk()
	selected := []bool{true, false, true}
	ok, err := CopySelectedJoinRowsDirect(srcChk, selected, dstChk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if dstChk.numVirtualRows != 2 {
		t.Fatal(dstChk.numVirtualRows)
	}

	dstChk = newChunk()
	ok, err = CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 0, 0, 0, selected, dstChk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if dstChk.numVirtualRows != 2 {
		t.Fatal(dstChk.numVirtualRows)
	}

	srcChk = newChunk(8)
	srcChk.TruncateTo(0)
	srcChk.AppendInt64(0, 0)
	srcChk.AppendInt64(0, 1)
	srcChk.AppendInt64(0, 2)
	dstChk = newChunkWithInitCap(0, 8)
	ok, err = CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 1, 1, 0, selected, dstChk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if dstChk.numVirtualRows != 2 || dstChk.NumRows() != 2 {
		t.Fatal(dstChk.numVirtualRows, dstChk.NumRows())
	}
	if row0, row1 := dstChk.GetRow(0).GetInt64(0), dstChk.GetRow(1).GetInt64(0); row0 != 0 || row1 != 2 {
		t.Fatal(row0, row1)
	}

	srcChk = newChunk(8)
	srcChk.TruncateTo(0)
	srcChk.AppendInt64(0, 3)
	srcChk.AppendInt64(0, 3)
	srcChk.AppendInt64(0, 3)
	dstChk = newChunkWithInitCap(0, 8)
	ok, err = CopySelectedJoinRowsWithSameOuterRows(srcChk, 1, 0, 0, 1, selected, dstChk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if dstChk.numVirtualRows != 2 || dstChk.NumRows() != 2 {
		t.Fatal(dstChk.numVirtualRows, dstChk.NumRows())
	}
	if row0, row1 := dstChk.GetRow(0).GetInt64(0), dstChk.GetRow(1).GetInt64(0); row0 != 3 || row1 != 3 {
		t.Fatal(row0, row1)
	}
}

func BenchmarkCopySelectedJoinRows(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstChk.Reset()
		CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 3, 3, 3, selected, dstChk)
	}
}
func BenchmarkCopySelectedJoinRowsDirect(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstChk.Reset()
		CopySelectedJoinRowsDirect(srcChk, selected, dstChk)
	}
}
func BenchmarkAppendSelectedRow(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(true)
	numRows := srcChk.NumRows()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstChk.Reset()
		for j := 0; j < numRows; j++ {
			if !selected[j] {
				continue
			}
			dstChk.AppendRow(srcChk.GetRow(j))
		}
	}
}
