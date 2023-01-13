// Copyright 2022 PingCAP, Inc.
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

package addindextest

import "testing"

func TestPiTRCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7},
		{2, 5, 8},
		{3, 6, 9},
	}
	ctx := initCompCtx(t)
	ctx.CompCtx.isPiTR = true
	testOneColFrame(ctx, colIDs, addIndexNonUnique)
}

func TestPiTRCreateUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 6},
		{11},
		{19},
	}
	ctx := initCompCtx(t)
	ctx.CompCtx.isPiTR = true
	testOneColFrame(ctx, colIDs, addIndexUnique)
}

func TestPiTRCreatePrimaryKey(t *testing.T) {
	ctx := initCompCtx(t)
	ctx.CompCtx.isPiTR = true
	testOneIndexFrame(ctx, 0, addIndexPK)
}

func TestPiTRCreateGenColIndex(t *testing.T) {
	ctx := initCompCtx(t)
	ctx.CompCtx.isPiTR = true
	testOneIndexFrame(ctx, 29, addIndexGenCol)
}

func TestPiTRCreateMultiColsIndex(t *testing.T) {
	var coliIDs = [][]int{
		{1},
		{8},
		{11},
	}
	var coljIDs = [][]int{
		{16},
		{23},
		{27},
	}
	ctx := initCompCtx(t)
	ctx.CompCtx.isPiTR = true
	testTwoColsFrame(ctx, coliIDs, coljIDs, addIndexMultiCols)
}
