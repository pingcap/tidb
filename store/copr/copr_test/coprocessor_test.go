// Copyright 2021 PingCAP, Inc.
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

package copr_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestBuildCopIteratorWithRowCountHint(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, []byte("g"), []byte("n"), []byte("t"))
		}),
	)
	require.NoError(t, err)
	defer require.NoError(t, store.Close())
	copClient := store.GetClient().(*copr.CopClient)
	ctx := context.Background()
	killed := uint32(0)
	vars := kv.NewVariables(&killed)
	opt := &kv.ClientSendOption{}

	req := &kv.Request{
		Tp:                kv.ReqTypeDAG,
		KeyRanges:         copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z"),
		FixedRowCountHint: []int{1, 1, 3, copr.CopSmallTaskRow},
		Concurrency:       15,
	}
	it, errRes := copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc := it.GetConcurrency()
	rateLimit := it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 1)
	require.Equal(t, rateLimit.GetCapacity(), 2)

	req = &kv.Request{
		Tp:                kv.ReqTypeDAG,
		KeyRanges:         copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z"),
		FixedRowCountHint: []int{1, 1, 3, 3},
		Concurrency:       15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 2)
	require.Equal(t, rateLimit.GetCapacity(), 3)

	// cross-region long range
	req = &kv.Request{
		Tp:                kv.ReqTypeDAG,
		KeyRanges:         copr.BuildKeyRanges("a", "z"),
		FixedRowCountHint: []int{10},
		Concurrency:       15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 2)
	require.Equal(t, rateLimit.GetCapacity(), 3)

	req = &kv.Request{
		Tp:                kv.ReqTypeDAG,
		KeyRanges:         copr.BuildKeyRanges("a", "z"),
		FixedRowCountHint: []int{copr.CopSmallTaskRow + 1},
		Concurrency:       15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 4)
	require.Equal(t, smallConc, 0)
	require.Equal(t, rateLimit.GetCapacity(), 4)
}
