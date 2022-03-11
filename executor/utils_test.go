// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestBatchRetrieverHelper(t *testing.T) {
	rangeStarts := make([]int, 0)
	rangeEnds := make([]int, 0)
	collect := func(start, end int) error {
		rangeStarts = append(rangeStarts, start)
		rangeEnds = append(rangeEnds, end)
		return nil
	}

	r := &batchRetrieverHelper{}
	err := r.nextBatch(collect)
	require.NoError(t, err)
	require.Equal(t, rangeStarts, []int{})
	require.Equal(t, rangeEnds, []int{})

	r = &batchRetrieverHelper{
		retrieved: true,
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(collect)
	require.NoError(t, err)
	require.Equal(t, rangeStarts, []int{})
	require.Equal(t, rangeEnds, []int{})

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(func(start, end int) error {
		return errors.New("some error")
	})
	require.Error(t, err)
	require.True(t, r.retrieved)

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0, 3, 6, 9})
	require.Equal(t, rangeEnds, []int{3, 6, 9, 10})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 9,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0, 3, 6})
	require.Equal(t, rangeEnds, []int{3, 6, 9})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 100,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0})
	require.Equal(t, rangeEnds, []int{10})
}
