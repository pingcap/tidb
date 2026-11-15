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

package ddl

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

// This benchmark can override the production client RPC batch size while
// retaining the production range reader and bounded consumer queues.
type spfreshSupplyBenchStore struct {
	kv.Storage
	batchSize int
}

func (s spfreshSupplyBenchStore) GetSnapshot(version kv.Version) kv.Snapshot {
	return spfreshSupplyBenchSnapshot{Snapshot: s.Storage.GetSnapshot(version), batchSize: s.batchSize}
}

type spfreshSupplyBenchSnapshot struct {
	kv.Snapshot
	batchSize int
}

func (s spfreshSupplyBenchSnapshot) Iter(start, end kv.Key) (kv.Iterator, error) {
	// Apply the experimental override after production snapshot options.
	s.Snapshot.SetOption(kv.ScanBatchSize, s.batchSize)
	return s.Snapshot.Iter(start, end)
}

// BenchmarkSPFreshSourceSupply reads an explicitly selected, existing dense
// integer-handle interval. It never writes data or creates an index. Decoding,
// fingerprinting and object uploads are measured separately by
// BenchmarkSPFreshFragmentBoundaries; this benchmark isolates source supply.
// Run with -benchtime=1x and pin the process and backing cluster separately.
func BenchmarkSPFreshSourceSupply(b *testing.B) {
	path := os.Getenv("SPFRESH_BENCH_TIKV_PATH")
	if path == "" {
		b.Skip("SPFRESH_BENCH_TIKV_PATH and explicit table/handle interval are required")
	}
	integer := func(name string) int64 {
		value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
		require.NoError(b, err, name)
		return value
	}
	tableID, first, rows := integer("SPFRESH_BENCH_TABLE"), integer("SPFRESH_BENCH_FIRST"), integer("SPFRESH_BENCH_ROWS")
	total, err := memory.MemTotal()
	require.NoError(b, err)
	if limit := memory.ServerMemoryLimit.Load(); limit != 0 {
		total = min(total, limit)
	}
	if value := os.Getenv("SPFRESH_REPLAY_MEMORY"); value != "" {
		limit, err := strconv.ParseUint(value, 10, 64)
		require.NoError(b, err)
		total = min(total, limit)
	}
	cpus, err := cgroup.EffectiveCPUCount()
	require.NoError(b, err)
	readers := spfreshBulkReadConcurrency(cpus, total)
	if os.Getenv("SPFRESH_BENCH_READERS") != "auto" {
		readers = int(integer("SPFRESH_BENCH_READERS"))
	}
	batchSize := 0
	if os.Getenv("SPFRESH_BENCH_BATCH") != "auto" {
		batchSize = int(integer("SPFRESH_BENCH_BATCH"))
		require.Greater(b, batchSize, 1)
	}
	require.Positive(b, tableID)
	require.Positive(b, rows)
	require.Positive(b, readers)
	require.LessOrEqual(b, int64(readers), rows)
	require.Greater(b, first+rows, first)
	store, err := openSPFreshBulkRealTiKVStore(path)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, store.Close()) })
	var source kv.Storage = store
	if batchSize != 0 {
		source = spfreshSupplyBenchStore{Storage: store, batchSize: batchSize}
	}
	b.Logf("GOMAXPROCS=%d available_cpus=%d readers=%d RPC_batch=%s", runtime.GOMAXPROCS(0), runtime.NumCPU(), readers, os.Getenv("SPFRESH_BENCH_BATCH"))
	version := currentSPFreshBulkTestVersion(b, store)
	run := newSPFreshBulkScanTestRun(1, version, "source-supply-bench")
	prefix := tablecodec.GenTableRecordPrefix(tableID)
	var totalBytes uint64
	var totalWait, totalRead, totalConsume time.Duration
	b.ReportAllocs()
	fmt.Printf("SPFRESH_BENCH_START stage=supply readers=%d cpus=%d memory=%d rows=%d\n", readers, cpus, total, rows)
	started := time.Now()
	b.ResetTimer()
	for range b.N {
		ordinal := 0
		nextRange := func(context.Context) (kv.KeyRange, error) {
			if ordinal == readers {
				return kv.KeyRange{}, io.EOF
			}
			lo, hi := first+rows/int64(readers)*int64(ordinal), first+rows/int64(readers)*int64(ordinal+1)
			ordinal++
			if ordinal == readers {
				hi = first + rows
			}
			return kv.KeyRange{StartKey: tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(lo)),
				EndKey: tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(hi))}, nil
		}
		var count, bytes atomic.Uint64
		consumer := func(context.Context) (spfreshBulkRangeCallbacks, error) {
			var seen bool
			var previous int64
			var localRows, localBytes uint64
			return spfreshBulkRangeCallbacks{
				row: func(ordinal int, handle kv.Handle, value []byte) (bool, error) {
					current := handle.IntValue()
					lo, hi := first+rows/int64(readers)*int64(ordinal), first+rows/int64(readers)*int64(ordinal+1)
					if ordinal == readers-1 {
						hi = first + rows
					}
					if current < lo || current >= hi || (seen && current != previous+1) || (!seen && current != lo) {
						return false, fmt.Errorf("unexpected source handle %d in range %d after %d", current, ordinal, previous)
					}
					seen, previous = true, current
					localRows++
					localBytes += uint64(len(value))
					return true, nil
				},
				finish: func(int) error { count.Add(localRows); bytes.Add(localBytes); return nil },
			}, nil
		}
		wait, consume, read, err := readSPFreshBulkRanges(context.Background(), NewReorgContext(),
			source, prefix, run, readers,
			int(min(uint64(spfreshBulkReadAheadBytes), total/16)), nextRange, consumer)
		require.NoError(b, err)
		require.Equal(b, uint64(rows), count.Load())
		totalBytes += bytes.Load()
		totalWait += wait
		totalRead += read
		totalConsume += consume
	}
	b.StopTimer()
	fmt.Printf("SPFRESH_BENCH_END stage=supply elapsed_s=%.6f\n", time.Since(started).Seconds())
	fmt.Printf("SPFRESH_BENCH_VALIDATED records=%d bytes=%d\n", rows*int64(b.N), totalBytes)
	b.SetBytes(int64(totalBytes / uint64(b.N)))
	b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
	b.ReportMetric(totalWait.Seconds()/float64(b.N), "source-wait-s/op")
	b.ReportMetric(totalRead.Seconds()/float64(b.N), "read-work-s/op")
	b.ReportMetric(totalConsume.Seconds()/float64(b.N), "consume-s/op")
}
