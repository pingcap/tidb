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

// getChk generate a chunk of data, isFirst3ColTheSame means the first three columns are the same.
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
	CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 3, selected, dstChk2)

	if !reflect.DeepEqual(dstChk, dstChk2) {
		t.Fatal()
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
}

func BenchmarkCopySelectedJoinRows(b *testing.B) {
	b.ReportAllocs()
	srcChk, dstChk, selected := getChk(true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstChk.Reset()
		CopySelectedJoinRowsWithSameOuterRows(srcChk, 0, 3, selected, dstChk)
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
