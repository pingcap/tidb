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

import (
	"testing"

	"github.com/pingcap/tidb/tests/realtikvtest/util"
)

func TestPiTRCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7},
		{2, 5, 8},
		{3, 6, 9},
	}
	ctx := util.InitCompCtx(t)
	ctx.CompCtx.IsPiTR = true
	util.TestOneColFrame(ctx, colIDs, util.AddIndexNonUnique)
}

func TestPiTRCreateUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 6},
		{11},
		{19},
	}
	ctx := util.InitCompCtx(t)
	ctx.CompCtx.IsPiTR = true
	util.TestOneColFrame(ctx, colIDs, util.AddIndexUnique)
}

func TestPiTRCreatePrimaryKey(t *testing.T) {
	ctx := util.InitCompCtx(t)
	ctx.CompCtx.IsPiTR = true
	util.TestOneIndexFrame(ctx, 0, util.AddIndexPK)
}

func TestPiTRCreateGenColIndex(t *testing.T) {
	ctx := util.InitCompCtx(t)
	ctx.CompCtx.IsPiTR = true
	util.TestOneIndexFrame(ctx, 29, util.AddIndexGenCol)
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
	ctx := util.InitCompCtx(t)
	ctx.CompCtx.IsPiTR = true
	util.TestTwoColsFrame(ctx, coliIDs, coljIDs, util.AddIndexMultiCols)
}
