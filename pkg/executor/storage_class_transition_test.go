// Copyright 2026 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/stretchr/testify/require"
)

func TestStorageClassTransitionToDatums(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 2, 3, 456000000, time.UTC)
	transition := ddl.StorageClassTransition{
		TableSchema:   "test",
		TableName:     "orders",
		TableID:       100,
		Direction:     "TO_IA",
		StartTime:     start,
		Duration:      2500 * time.Millisecond,
		TotalReplicas: 3,
		Progress:      0.5,
	}

	row := storageClassTransitionToDatums(time.UTC, transition)
	require.Len(t, row, 12)
	require.True(t, row[3].IsNull())
	require.True(t, row[4].IsNull())
	require.True(t, row[6].IsNull())
	require.True(t, row[7].IsNull())
	require.True(t, row[8].IsNull())
	require.Equal(t, uint64(2), row[10].GetUint64())
	require.True(t, row[11].IsNull())

	transition.PartitionName = "p0"
	transition.PartitionID = 101
	transition.StatusValid = true
	transition.ProgressValid = true
	transition.CompletedReplicas = 1
	transition.LastUpdateTime = start.Add(time.Second)
	row = storageClassTransitionToDatums(time.UTC, transition)
	require.Equal(t, "p0", row[3].GetString())
	require.Equal(t, int64(101), row[4].GetInt64())
	require.Equal(t, uint64(3), row[6].GetUint64())
	require.Equal(t, uint64(1), row[7].GetUint64())
	require.Equal(t, 0.5, row[8].GetFloat64())
	require.False(t, row[11].IsNull())
}
