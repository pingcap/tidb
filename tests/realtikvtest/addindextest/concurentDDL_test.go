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
	"github.com/stretchr/testify/require"
	"testing"
)

func initConcurrentDDLTest(t *testing.T, colIIDs [][]int, colJIDs [][]int, tType testType) *suiteContext {
	ctx := initCompCtx(t)
	ctx.CompCtx.isConcurrentDDL = true
	ctx.CompCtx.tType = tType
	ctx.CompCtx.colIIDs = colIIDs
	ctx.CompCtx.colJIDs = colJIDs
	return ctx
}
func TestConcurrentDDLCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7, 10, 13, 16, 19, 22, 25},
		{2, 5, 8, 11, 14, 17, 20, 23, 26},
		{3, 6, 9, 12, 15, 18, 21, 24, 27},
	}
	ctx := initConcurrentDDLTest(t, colIDs, nil, TestNonUnique)
	ctx.CompCtx.start(ctx)
	err := ctx.CompCtx.stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreateUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{2, 9, 11, 17},
		{3, 12, 25},
	}
	ctx := initConcurrentDDLTest(t, colIDs, nil, TestUnique)
	ctx.CompCtx.start(ctx)
	err := ctx.CompCtx.stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreatePrimaryKey(t *testing.T) {
	ctx := initConcurrentDDLTest(t, nil, nil, TestPK)
	ctx.CompCtx.start(ctx)
	err := ctx.CompCtx.stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreateGenColIndex(t *testing.T) {
	ctx := initConcurrentDDLTest(t, nil, nil, TestGenIndex)
	ctx.CompCtx.start(ctx)
	err := ctx.CompCtx.stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreateMultiColsIndex(t *testing.T) {
	var coliIDs = [][]int{
		{1, 4, 7, 10, 13},
		{2, 5, 8, 11},
		{3, 6, 9, 12, 15},
	}
	var coljIDs = [][]int{
		{16, 19, 22, 25},
		{14, 17, 20, 23, 26},
		{18, 21, 24, 27},
	}
	ctx := initConcurrentDDLTest(t, coliIDs, coljIDs, TestMultiCols)
	ctx.CompCtx.start(ctx)
	err := ctx.CompCtx.stop(ctx)
	require.NoError(t, err)
}
