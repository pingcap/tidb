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

package join

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHashJoinRuntimeStats(t *testing.T) {
	stats := &hashJoinRuntimeStats{
		fetchAndBuildHashTable: 2 * time.Second,
		hashStat: hashStatistic{
			probeCollision:   1,
			buildTableElapse: time.Millisecond * 100,
		},
		fetchAndProbe:    int64(5 * time.Second),
		probe:            int64(4 * time.Second),
		concurrent:       4,
		maxFetchAndProbe: int64(2 * time.Second),
	}
	require.Equal(t, "build_hash_table:{total:2s, fetch:1.9s, build:100ms}, probe:{concurrency:4, total:5s, max:2s, probe:4s, fetch and wait:1s, probe_collision:1}", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "build_hash_table:{total:4s, fetch:3.8s, build:200ms}, probe:{concurrency:4, total:10s, max:2s, probe:8s, fetch and wait:2s, probe_collision:2}", stats.String())
}

func TestIndexJoinRuntimeStats(t *testing.T) {
	stats := indexLookUpJoinRuntimeStats{
		concurrency: 5,
		probe:       int64(time.Second),
		innerWorker: innerWorkerRuntimeStats{
			totalTime: int64(time.Second * 5),
			task:      16,
			construct: int64(100 * time.Millisecond),
			fetch:     int64(300 * time.Millisecond),
			build:     int64(250 * time.Millisecond),
			join:      int64(150 * time.Millisecond),
		},
	}
	require.Equal(t, "inner:{total:5s, concurrency:5, task:16, construct:100ms, fetch:300ms, build:250ms, join:150ms}, probe:1s", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "inner:{total:10s, concurrency:5, task:32, construct:200ms, fetch:600ms, build:500ms, join:300ms}, probe:2s", stats.String())
}
