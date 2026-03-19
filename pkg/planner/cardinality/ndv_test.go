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

package cardinality

import (
	"testing"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/stretchr/testify/require"
)

func TestAggregateSelectedPartitionCounts(t *testing.T) {
	realtimeCount, modifyCount, ok := AggregateSelectedPartitionCounts([]*statistics.Table{
		{HistColl: *statistics.NewHistColl(1, true, 10, 2, 0, 0)},
		{HistColl: *statistics.NewHistColl(2, true, 30, 4, 0, 0)},
	})
	require.True(t, ok)
	require.Equal(t, int64(40), realtimeCount)
	require.Equal(t, int64(6), modifyCount)

	pseudoStats := &statistics.Table{HistColl: *statistics.NewHistColl(2, true, 30, 4, 0, 0)}
	pseudoStats.Pseudo = true
	_, _, ok = AggregateSelectedPartitionCounts([]*statistics.Table{
		{HistColl: *statistics.NewHistColl(1, true, 10, 2, 0, 0)},
		pseudoStats,
	})
	require.False(t, ok)
}
