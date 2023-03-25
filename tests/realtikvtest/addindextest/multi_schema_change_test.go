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
)

func initCompCtx(t *testing.T) *suiteContext {
	ctx := initTest(t)
	initCompCtxParams(ctx)
	return ctx
}
func TestMultiSchemaChangeCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7},
		{2, 5, 8},
		{3, 6, 9},
	}
	ctx := initCompCtx(t)
	ctx.CompCtx.isMultiSchemaChange = true
	testOneColFrame(ctx, colIDs, addIndexNonUnique)
}

func TestMultiSchemaChangeCreateUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 6, 8},
		{2, 19},
		{11},
	}
	ctx := initCompCtx(t)
	ctx.CompCtx.isMultiSchemaChange = true
	testOneColFrame(ctx, colIDs, addIndexUnique)
}

func TestMultiSchemaChangeCreatePrimaryKey(t *testing.T) {
	ctx := initCompCtx(t)
	ctx.CompCtx.isMultiSchemaChange = true
	testOneIndexFrame(ctx, 0, addIndexPK)
}

func TestMultiSchemaChangeCreateGenColIndex(t *testing.T) {
	ctx := initCompCtx(t)
	ctx.CompCtx.isMultiSchemaChange = true
	testOneIndexFrame(ctx, 29, addIndexGenCol)
}

func TestMultiSchemaChangeMultiColsIndex(t *testing.T) {
	var coliIDs = [][]int{
		{1},
		{2},
		{3},
	}
	var coljIDs = [][]int{
		{16},
		{14},
		{18},
	}
	ctx := initCompCtx(t)
	ctx.CompCtx.isMultiSchemaChange = true
	testTwoColsFrame(ctx, coliIDs, coljIDs, addIndexMultiCols)
}
