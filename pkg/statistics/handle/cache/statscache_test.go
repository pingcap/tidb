// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"testing"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/stretchr/testify/require"
)

func TestCacheOfBatchUpdate(t *testing.T) {
	markedAsUpdated := make([]int64, 0)
	markedAsDeleted := make([]int64, 0)
	testBatchSize := 3
	cached := newCacheOfBatchUpdate(testBatchSize, func(toUpdate []*statistics.Table, toDelete []int64) {
		for _, table := range toUpdate {
			markedAsUpdated = append(markedAsUpdated, table.PhysicalID)
		}
		markedAsDeleted = append(markedAsDeleted, toDelete...)
	})

	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 1}})
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 2}})
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 3}})
	require.Len(t, markedAsUpdated, 0)
	require.Len(t, cached.toUpdate, 3)
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 4}})
	require.Len(t, markedAsUpdated, 3)
	require.Equal(t, int64(1), markedAsUpdated[0])
	require.Equal(t, int64(2), markedAsUpdated[1])
	require.Equal(t, int64(3), markedAsUpdated[2])
	require.Len(t, cached.toUpdate, 1)
	require.Equal(t, int64(4), cached.toUpdate[0].PhysicalID)

	cached.addToDelete(5)
	cached.addToDelete(6)
	cached.addToDelete(7)
	require.Len(t, markedAsDeleted, 0)
	require.Len(t, cached.toDelete, 3)
	cached.addToUpdate(&statistics.Table{HistColl: statistics.HistColl{PhysicalID: 8}})
	require.Len(t, cached.toUpdate, 2)
	cached.addToDelete(9)
	require.Len(t, markedAsDeleted, 3)
	require.Equal(t, int64(5), markedAsDeleted[0])
	require.Equal(t, int64(6), markedAsDeleted[1])
	require.Equal(t, int64(7), markedAsDeleted[2])
	require.Len(t, cached.toDelete, 1)
	require.Equal(t, int64(9), cached.toDelete[0])
	require.Len(t, markedAsUpdated, 5)
	require.Equal(t, int64(4), markedAsUpdated[3])
	require.Equal(t, int64(8), markedAsUpdated[4])

	cached.flush()
	require.Len(t, cached.toUpdate, 0)
	require.Len(t, cached.toDelete, 0)
	require.Len(t, markedAsDeleted, 4)
	require.Equal(t, int64(9), markedAsDeleted[3])
}
