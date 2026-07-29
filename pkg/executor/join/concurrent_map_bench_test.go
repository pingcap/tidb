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

package join

import (
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

var benchmarkConcurrentMapSink *concurrentMapHashTable

func BenchmarkNewConcurrentMapInitialization(b *testing.B) {
	var sink concurrentMap
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sink = newConcurrentMap()
	}
	b.StopTimer()
	if len(sink) != ShardCount {
		b.Fatalf("unexpected shard count: %d", len(sink))
	}
}

func BenchmarkConcurrentMapHashTableBuild(b *testing.B) {
	cases := []struct {
		name      string
		rows      int
		sameShard bool
	}{
		{name: "empty"},
		{name: "rows=10/single-shard", rows: 10, sameShard: true},
		{name: "rows=10/spread", rows: 10},
		{name: "rows=1000/all-shards", rows: 1000},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			keys := make([]uint64, bc.rows)
			rows := make([]chunk.RowPtr, bc.rows)
			for i := range bc.rows {
				if bc.sameShard {
					keys[i] = uint64(i * ShardCount)
				} else {
					keys[i] = uint64(i)
				}
				rows[i] = chunk.RowPtr{ChkIdx: uint32(i / 32), RowIdx: uint32(i % 32)}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				table := NewConcurrentMapHashTable()
				for i := range keys {
					table.Put(keys[i], rows[i])
				}
				benchmarkConcurrentMapSink = table
			}
			b.StopTimer()
			if got := benchmarkConcurrentMapSink.Len(); got != uint64(bc.rows) {
				b.Fatalf("case %s: got %d rows", strconv.Quote(bc.name), got)
			}
		})
	}
}
