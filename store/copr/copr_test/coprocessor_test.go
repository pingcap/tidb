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

	ranges := copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req := &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, copr.CopSmallTaskRow}),
		Concurrency: 15,
	}
	it, errRes := copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc := it.GetConcurrency()
	rateLimit := it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 1)
	require.Equal(t, rateLimit.GetCapacity(), 2)

	ranges = copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req = &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency: 15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 2)
	require.Equal(t, rateLimit.GetCapacity(), 3)

	// cross-region long range
	ranges = copr.BuildKeyRanges("a", "z")
	req = &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{10}),
		Concurrency: 15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 1)
	require.Equal(t, smallConc, 2)
	require.Equal(t, rateLimit.GetCapacity(), 3)

	ranges = copr.BuildKeyRanges("a", "z")
	req = &kv.Request{
		Tp:          kv.ReqTypeDAG,
		KeyRanges:   kv.NewNonParitionedKeyRangesWithHint(ranges, []int{copr.CopSmallTaskRow + 1}),
		Concurrency: 15,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	conc, smallConc = it.GetConcurrency()
	rateLimit = it.GetSendRate()
	require.Equal(t, conc, 4)
	require.Equal(t, smallConc, 0)
	require.Equal(t, rateLimit.GetCapacity(), 4)
}

func TestBuildCopIteratorWithBatchStoreCopr(t *testing.T) {
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

	ranges := copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req := &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:    15,
		StoreBatchSize: 1,
	}
	it, errRes := copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks := it.GetTasks()
	require.Equal(t, len(tasks), 2)
	require.Equal(t, len(tasks[0].ToPBBatchTasks()), 1)
	require.Equal(t, tasks[0].RowCountHint, 5)
	require.Equal(t, len(tasks[1].ToPBBatchTasks()), 1)
	require.Equal(t, tasks[1].RowCountHint, 9)

	ranges = copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req = &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:    15,
		StoreBatchSize: 3,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks = it.GetTasks()
	require.Equal(t, len(tasks), 1)
	require.Equal(t, len(tasks[0].ToPBBatchTasks()), 3)
	require.Equal(t, tasks[0].RowCountHint, 14)

	// paging will disable store batch.
	ranges = copr.BuildKeyRanges("a", "c", "d", "e", "h", "x", "y", "z")
	req = &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 1, 3, 3}),
		Concurrency:    15,
		StoreBatchSize: 3,
		Paging: struct {
			Enable        bool
			MinPagingSize uint64
			MaxPagingSize uint64
		}{
			Enable:        true,
			MinPagingSize: 1,
			MaxPagingSize: 1024,
		},
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks = it.GetTasks()
	require.Equal(t, len(tasks), 4)

	// only small tasks will be batched.
	ranges = copr.BuildKeyRanges("a", "b", "h", "i", "o", "p")
	req = &kv.Request{
		Tp:             kv.ReqTypeDAG,
		KeyRanges:      kv.NewNonParitionedKeyRangesWithHint(ranges, []int{1, 33, 32}),
		Concurrency:    15,
		StoreBatchSize: 3,
	}
	it, errRes = copClient.BuildCopIterator(ctx, req, vars, opt)
	require.Nil(t, errRes)
	tasks = it.GetTasks()
	require.Equal(t, len(tasks), 2)
	require.Equal(t, len(tasks[0].ToPBBatchTasks()), 1)
	require.Equal(t, len(tasks[1].ToPBBatchTasks()), 0)
}
