// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conflictedkv

import (
	"sync/atomic"
	"testing"

	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHandleFilter(t *testing.T) {
	var hf *HandleFilter
	// we allow nil HandleFilter
	require.False(t, hf.needSkip(tidbkv.Key("row-key-1")))

	sharedSize := atomic.Int64{}
	set := NewBoundedHandleSet(nil, &sharedSize, 1024)
	set.Add(tidbkv.Key("row-key-1"))
	hf = NewHandleFilter(set)
	require.True(t, hf.needSkip(tidbkv.Key("row-key-1")))
	require.False(t, hf.needSkip(tidbkv.Key("row-key-2")))
}

func TestBoundedHandleSet(t *testing.T) {
	t.Run("partition handle uses physical row key", func(t *testing.T) {
		const logicalTableID int64 = 100
		rowKey1 := encodeDataRowKey(logicalTableID, tidbkv.NewPartitionHandle(101, tidbkv.IntHandle(1)))
		rowKey2 := encodeDataRowKey(logicalTableID, tidbkv.NewPartitionHandle(102, tidbkv.IntHandle(1)))

		require.Equal(t, tablecodec.EncodeRowKeyWithHandle(101, tidbkv.IntHandle(1)), rowKey1)
		require.Equal(t, tablecodec.EncodeRowKeyWithHandle(102, tidbkv.IntHandle(1)), rowKey2)
		require.NotEqual(t, rowKey1, rowKey2)
	})

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	sharedSize := atomic.Int64{}
	limit := 3 * (1 + rowKeyMapEntryShallowSize)
	set := NewBoundedHandleSet(logger, &sharedSize, limit)
	require.False(t, set.Contains(tidbkv.Key{1}))

	// add row keys within limit
	for i := range 3 {
		rowKey := tidbkv.Key{byte(i + 1)}
		set.Add(rowKey)
		require.True(t, set.Contains(rowKey))
	}
	require.EqualValues(t, limit, sharedSize.Load())
	require.True(t, set.BoundExceeded())

	// adding another row key exceeds limit
	set.Add(tidbkv.Key{4})
	require.False(t, set.Contains(tidbkv.Key{4}))

	// create another set with the shared current size, it should exceed limit directly
	set2 := NewBoundedHandleSet(logger, &sharedSize, limit)
	require.True(t, set2.BoundExceeded())
	set2.Add(tidbkv.Key{5})
	require.False(t, set2.Contains(tidbkv.Key{5}))

	// merge sets
	set2.Merge(nil)
	require.Empty(t, set2.rowKeys)
	set2.Merge(set)
	require.EqualValues(t, set.rowKeys, set2.rowKeys)
}
