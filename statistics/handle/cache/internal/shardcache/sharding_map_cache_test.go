// Copyright 2023 PingCAP, Inc.
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

package shardcache

import (
	"testing"

	"github.com/pingcap/tidb/statistics/handle/cache/internal/testutil"
	"github.com/stretchr/testify/require"
)

var (
	mockCMSMemoryUsage  = int64(4)
	mockTopNMemoryUsage = int64(64)
	mockHistMemoryUsage = int64(289)
)

func TestLFUPutGetDel(t *testing.T) {
	cache := NewMapCache()
	mockTable := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	mockTableID := int64(1)
	cache.Put(mockTableID, mockTable, false)

	cache.Del(mockTableID)
	v, ok := cache.Get(mockTableID, false)
	require.False(t, ok)
	require.Nil(t, v)
}

func TestLFUFreshMemUsage(t *testing.T) {
	cache := NewMapCache()
	t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	t2 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	t3 := testutil.NewMockStatisticsTable(3, 3, true, false, false)
	cache.Put(int64(1), t1, false)
	cache.Put(int64(2), t2, false)
	cache.Put(int64(3), t3, false)
	require.Equal(t, cache.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
	t4 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	cache.Put(int64(1), t4, false)
	require.Equal(t, cache.Cost(), 6*mockCMSMemoryUsage+7*mockCMSMemoryUsage)
	t5 := testutil.NewMockStatisticsTable(2, 2, true, false, false)
	cache.Put(int64(1), t5, false)
	require.Equal(t, cache.Cost(), 7*mockCMSMemoryUsage+7*mockCMSMemoryUsage)

	t6 := testutil.NewMockStatisticsTable(1, 2, true, false, false)
	cache.Put(int64(1), t6, false)
	require.Equal(t, cache.Cost(), 7*mockCMSMemoryUsage+6*mockCMSMemoryUsage)

	t7 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	cache.Put(int64(1), t7, false)
	require.Equal(t, cache.Cost(), 6*mockCMSMemoryUsage+6*mockCMSMemoryUsage)
}

func TestCacheLen(t *testing.T) {
	cache := NewMapCache()
	t1 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	require.Equal(t, int64(12), t1.MemoryUsage().TotalTrackingMemUsage())
	cache.Put(int64(1), t1, false)
	t2 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
	cache.Put(int64(2), t2, false)
	require.Equal(t, cache.Len(), 2)
	t3 := testutil.NewMockStatisticsTable(2, 1, true, false, false)
	cache.Put(int64(3), t3, false)

	require.Equal(t, cache.Len(), 3)
}
