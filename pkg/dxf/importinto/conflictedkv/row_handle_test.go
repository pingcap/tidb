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

func TestKeyFilter(t *testing.T) {
	var keyFilter *KeyFilter
	// we allow nil KeyFilter
	require.False(t, keyFilter.isHandledGlobally(tidbkv.Key("row-key-1")))
	require.False(t, keyFilter.isHandledLocally("row-key-1"))
	keyFilter.addLocal("row-key-1")

	var sharedSize atomic.Int64
	globalSet := NewBoundedKeySet(nil, &sharedSize, 1024)
	localSet := NewBoundedKeySet(nil, &sharedSize, 1024)
	globalSet.Add(tidbkv.Key("row-key-1"))
	keyFilter = NewKeyFilter(globalSet, localSet)
	require.True(t, keyFilter.isHandledGlobally(tidbkv.Key("row-key-1")))
	require.False(t, keyFilter.isHandledGlobally(tidbkv.Key("row-key-2")))
	require.False(t, keyFilter.isHandledLocally("row-key-2"))
	keyFilter.addLocal("row-key-2")
	require.True(t, keyFilter.isHandledLocally("row-key-2"))
	require.False(t, keyFilter.isHandledGlobally(tidbkv.Key("row-key-2")))
}

func TestBoundedKeySet(t *testing.T) {
	t.Run("partition handle uses physical row key", func(t *testing.T) {
		const logicalTableID int64 = 100
		rowKey1 := encodeDataRowKey(logicalTableID, tidbkv.NewPartitionHandle(101, tidbkv.IntHandle(1)))
		rowKey2 := encodeDataRowKey(logicalTableID, tidbkv.NewPartitionHandle(102, tidbkv.IntHandle(1)))

		require.Equal(t, tablecodec.EncodeRowKeyWithHandle(101, tidbkv.IntHandle(1)), rowKey1)
		require.Equal(t, tablecodec.EncodeRowKeyWithHandle(102, tidbkv.IntHandle(1)), rowKey2)
		require.NotEqual(t, rowKey1, rowKey2)
	})

	t.Run("string keys honor size limit", func(t *testing.T) {
		var sharedSize atomic.Int64
		set := NewBoundedKeySet(zap.NewNop(), &sharedSize, 1024)
		require.False(t, set.containsStrKey("row-key"))
		set.addStr("row-key")
		require.True(t, set.containsStrKey("row-key"))
		require.EqualValues(t, int64(len("row-key"))+rowKeyMapEntryShallowSize, sharedSize.Load())

		var exceededSize atomic.Int64
		exceededSet := NewBoundedKeySet(zap.NewNop(), &exceededSize, 0)
		exceededSet.addStr("row-key")
		require.True(t, exceededSet.BoundExceeded())
		require.False(t, exceededSet.containsStrKey("row-key"))
		require.Zero(t, exceededSize.Load())
	})

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	sharedSize := atomic.Int64{}
	limit := 3 * (1 + rowKeyMapEntryShallowSize)
	set := NewBoundedKeySet(logger, &sharedSize, limit)
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
	set2 := NewBoundedKeySet(logger, &sharedSize, limit)
	require.True(t, set2.BoundExceeded())
	set2.Add(tidbkv.Key{5})
	require.False(t, set2.Contains(tidbkv.Key{5}))

	// merge sets
	set2.Merge(nil)
	require.Empty(t, set2.rowKeys)
	set2.Merge(set)
	require.EqualValues(t, set.rowKeys, set2.rowKeys)
}
