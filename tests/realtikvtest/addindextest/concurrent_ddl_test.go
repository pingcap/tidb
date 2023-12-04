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

	"github.com/pingcap/tidb/tests/realtikvtest/addindextestutil"
	"github.com/stretchr/testify/require"
)

func TestConcurrentDDLCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7, 10, 13},
		{14, 17, 20, 23, 26},
		{3, 6, 9, 21, 24},
	}
	ctx := addindextestutil.InitConcurrentDDLTest(t, colIDs, nil, addindextestutil.TestNonUnique)
	ctx.CompCtx.Start(ctx)
	err := ctx.CompCtx.Stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreateUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 6, 11, 13},
		{2, 11, 17},
		{3, 19, 25},
	}
	ctx := addindextestutil.InitConcurrentDDLTest(t, colIDs, nil, addindextestutil.TestUnique)
	ctx.CompCtx.Start(ctx)
	err := ctx.CompCtx.Stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreatePrimaryKey(t *testing.T) {
	ctx := addindextestutil.InitConcurrentDDLTest(t, nil, nil, addindextestutil.TestPK)
	ctx.CompCtx.Start(ctx)
	err := ctx.CompCtx.Stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreateGenColIndex(t *testing.T) {
	ctx := addindextestutil.InitConcurrentDDLTest(t, nil, nil, addindextestutil.TestGenIndex)
	ctx.CompCtx.Start(ctx)
	err := ctx.CompCtx.Stop(ctx)
	require.NoError(t, err)
}

func TestConcurrentDDLCreateMultiColsIndex(t *testing.T) {
	var coliIDs = [][]int{
		{7},
		{11},
		{14},
	}
	var coljIDs = [][]int{
		{16},
		{23},
		{19},
	}
	ctx := addindextestutil.InitConcurrentDDLTest(t, coliIDs, coljIDs, addindextestutil.TestMultiCols)
	ctx.CompCtx.Start(ctx)
	err := ctx.CompCtx.Stop(ctx)
	require.NoError(t, err)
}
