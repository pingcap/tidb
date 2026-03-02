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

package stmtstats

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/util/topsql/state"
	"go.uber.org/atomic"
)

// makeRUBatchForBench creates an RUIncrementMap with numUsers users and numSQLsPerUser SQLs per user.
// userOffset is added to user indices so that multiple batches can have distinct keys (e.g. for 16 stats × 10k keys = 160k distinct keys).
// Same shape as reporter's makeRUBatch for comparable benchmark data.
func makeRUBatchForBench(numUsers, numSQLsPerUser, userOffset int) RUIncrementMap {
	batch := make(RUIncrementMap, numUsers*numSQLsPerUser)
	for u := 0; u < numUsers; u++ {
		for s := 0; s < numSQLsPerUser; s++ {
			key := RUKey{
				User:       fmt.Sprintf("u%04d", userOffset+u),
				SQLDigest:  BinaryDigest(fmt.Sprintf("sql%04d_%04d", userOffset+u, s)),
				PlanDigest: BinaryDigest("plan"),
			}
			batch[key] = &RUIncrement{
				TotalRU:      float64(numUsers*numSQLsPerUser - u*numSQLsPerUser - s),
				ExecCount:    1,
				ExecDuration: 1,
			}
		}
	}
	return batch
}

// refillStatsRU fills each stats' finishedRUBuffer with a fresh batch so that the next drainAndPushRU has data to merge.
// Each stats gets a batch with distinct keys (using userOffset so keys don't overlap across stats).
func refillStatsRU(statsList []*StatementStats, numUsers, numSQLsPerUser int) {
	for i, stats := range statsList {
		stats.finishedRUBuffer = makeRUBatchForBench(numUsers, numSQLsPerUser, i*numUsers)
	}
}

// BenchmarkDrainAndPushRU_10kKeys measures one drainAndPushRU tick with 10k keys total (1 stats × 10k keys).
// Simulates maxRUKeysPerAggregate=10000 upstream. Run with -benchmem for B/op and allocs/op.
func BenchmarkDrainAndPushRU_10kKeys(b *testing.B) {
	state.EnableTopRU()
	defer state.DisableTopRU()

	const numUsers, numSQLsPerUser = 100, 100 // 10k keys
	a := newAggregator()
	stats := &StatementStats{
		data:             StatementStatsMap{},
		finished:         atomic.NewBool(false),
		finishedRUBuffer: RUIncrementMap{},
	}
	a.register(stats)
	a.registerRUCollector(&mockRUCollector{f: func(RUIncrementMap) {}})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		refillStatsRU([]*StatementStats{stats}, numUsers, numSQLsPerUser)
		a.drainAndPushRU()
	}
}

// BenchmarkDrainAndPushRU_160kKeys measures one drainAndPushRU tick with 160k keys from 16 stats (16 × 10k).
// After merge, total is capped at maxRUKeysPerAggregate (10000). Run with -benchmem for B/op and allocs/op.
func BenchmarkDrainAndPushRU_160kKeys(b *testing.B) {
	state.EnableTopRU()
	defer state.DisableTopRU()

	const numUsers, numSQLsPerUser = 100, 100 // 10k keys per stats
	const numStats = 16                       // 160k keys total
	statsList := make([]*StatementStats, numStats)
	for i := range statsList {
		statsList[i] = &StatementStats{
			data:             StatementStatsMap{},
			finished:         atomic.NewBool(false),
			finishedRUBuffer: RUIncrementMap{},
		}
	}
	a := newAggregator()
	for _, s := range statsList {
		a.register(s)
	}
	a.registerRUCollector(&mockRUCollector{f: func(RUIncrementMap) {}})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		refillStatsRU(statsList, numUsers, numSQLsPerUser)
		a.drainAndPushRU()
	}
}

func BenchmarkDrainAndPushRU_160kKeys_Preloaded(b *testing.B) {
	state.EnableTopRU()
	defer state.DisableTopRU()

	const numUsers, numSQLsPerUser = 100, 100 // 10k keys per stats
	const numStats = 16                       // 160k keys total
	statsList := make([]*StatementStats, numStats)
	for i := range statsList {
		statsList[i] = &StatementStats{
			data:             StatementStatsMap{},
			finished:         atomic.NewBool(false),
			finishedRUBuffer: RUIncrementMap{},
		}
	}
	a := newAggregator()
	for _, s := range statsList {
		a.register(s)
	}
	a.registerRUCollector(&mockRUCollector{f: func(RUIncrementMap) {}})

	// Setup: one immutable batch per stats with non-overlapping keys.
	batches := make([]RUIncrementMap, numStats)
	for i := range batches {
		batches[i] = makeRUBatchForBench(numUsers, numSQLsPerUser, i*numUsers)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, stats := range statsList {
			stats.finishedRUBuffer = batches[j]
		}
		a.drainAndPushRU()
	}
}
