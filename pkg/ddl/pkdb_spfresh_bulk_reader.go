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
	"io"
	"time"

	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	spfreshBulkReadBatchBytes     = 1 << 20
	spfreshBulkReadAheadBytes     = 256 << 20
	spfreshBulkReadRPCVectorBytes = 4 << 20
	spfreshBulkReadRPCMaxRows     = 1024
)

// Tune only SPFresh source snapshots; ordinary DDL iterators retain their
// existing client batch size. This target covers vector bytes, not stored
// payloads or the complete RPC/process memory footprint.
type spfreshBulkReadStore struct {
	kv.Storage
	batchRows int
}

func (s spfreshBulkReadStore) GetSnapshot(version kv.Version) kv.Snapshot {
	snapshot := s.Storage.GetSnapshot(version)
	snapshot.SetOption(kv.ScanBatchSize, s.batchRows)
	return snapshot
}

func spfreshBulkReadRPCBatchRows(dimension uint32) int {
	return min(spfreshBulkReadRPCMaxRows, spfreshBulkReadRPCVectorBytes/(int(dimension)*4))
}

type spfreshBulkReadRow struct {
	handle kv.Handle
	value  []byte
}

type spfreshBulkReadBatch struct {
	rows  []spfreshBulkReadRow
	arena []byte
}

type spfreshBulkRangeCallbacks struct {
	// Each range has its own consumer. Its callbacks run serially and rows
	// are increasing; callbacks for different ranges execute concurrently.
	row    func(ordinal int, handle kv.Handle, value []byte) (bool, error)
	finish func(ordinal int) error
}

// Factories run on the scheduler, before the range starts. Mutable decoder and
// fragment state belongs to the returned consumer, never to another range.
type spfreshBulkRangeConsumerFactory func(context.Context) (spfreshBulkRangeCallbacks, error)

// Allow two reader/consumer pipelines per three usable CPUs: source RPCs and
// uploads overlap computation. Keep one quarter of the effective CPU allowance
// aside, and reserve 16 MiB per pair from 1/32 of the memory quota for snapshot
// RPC buffers and scratch, separately from the bounded raw queue. This is a
// sizing estimate, not an RSS cap (stored payloads and GC can retain more).
func spfreshBulkReadConcurrency(cpus int, total uint64) int {
	cpuReaders, memoryReaders := spfreshBulkReaderLimits(cpus, total)
	return int(min(uint64(cpuReaders), memoryReaders))
}

// Keep the sizing decision and its diagnostic candidate limits identical.
func spfreshBulkReaderLimits(cpus int, total uint64) (int, uint64) {
	usableCPU := max(1, cpus-cpus/4)
	return (usableCPU*2 + 2) / 3, total / 32 / (16 << 20)
}

// Existing Region boundaries partition reads, never stored data. Consume ready
// streams without retaining rows from completed ranges.
func iterateSPFreshBulkSnapshot(
	ctx context.Context, reorgCtx *ReorgContext, store kv.Storage,
	tbl table.PhysicalTable, run *spfreshBulkRunContext, concurrency, readAheadBytes int, newConsumer spfreshBulkRangeConsumerFactory,
) error {
	start, end := tbl.RecordPrefix(), tbl.RecordPrefix().PrefixNext()
	var discover time.Duration
	var regions, pending []kv.KeyRange
	var regionCount, rangeCount int
	var probeRows, probeBytes uint64
	nextRange := func(ctx context.Context) (kv.KeyRange, error) {
		started := time.Now()
		defer func() { discover += time.Since(started) }()
		for len(pending) == 0 {
			if err := ctx.Err(); err != nil {
				return kv.KeyRange{}, err
			}
			if len(regions) == 0 {
				if start.Cmp(end) >= 0 {
					return kv.KeyRange{}, io.EOF
				}
				ranges, err := loadTableRanges(ctx, tbl, store, start, end, nil, concurrency)
				if err != nil {
					return kv.KeyRange{}, err
				}
				if len(ranges) == 0 || ranges[len(ranges)-1].EndKey.Cmp(start) <= 0 {
					return kv.KeyRange{}, errors.New("SPFresh source ranges did not advance")
				}
				start = ranges[len(ranges)-1].EndKey
				regions = ranges
			}
			region := regions[0]
			regions = regions[1:]
			regionCount++
			var rows, bytes uint64
			var err error
			pending, rows, bytes, err = splitSPFreshBulkReadRange(ctx, reorgCtx, store, tbl, run, region, concurrency)
			probeRows += rows
			probeBytes += bytes
			if err != nil {
				return kv.KeyRange{}, err
			}
		}
		result := pending[0]
		pending = pending[1:]
		rangeCount++
		return result, nil
	}
	wait, consume, readWork, err := readSPFreshBulkRanges(ctx, reorgCtx, store,
		tbl.RecordPrefix(), run, concurrency, readAheadBytes, nextRange, newConsumer)
	if err != nil {
		return err
	}
	for _, phase := range []struct {
		name     string
		duration time.Duration
	}{{"range_discovery", discover}, {"source_wait", wait}, {"row_decode_fingerprint", consume}, {"source_read_work", readWork}} {
		logSPFreshBulkScanUploadStage(run, phase.name, phase.duration,
			zap.Int("read_concurrency", concurrency), zap.Int("range_count", rangeCount),
			zap.Int("region_range_count", regionCount), zap.Uint64("probe_rows", probeRows), zap.Uint64("probe_bytes", probeBytes),
			zap.Int("rpc_batch_rows", spfreshBulkReadRPCBatchRows(run.dimension)),
			zap.Int("read_batch_bytes", spfreshBulkReadBatchBytes), zap.Int("read_ahead_bytes", readAheadBytes))
	}
	return nil
}

// Split only the read work, never Regions or output shards. Probe both endpoints
// at B so table-prefix bounds and sparse signed handles cannot create enormous
// empty tails. Each physical range expands to at most the existing reader count.
func splitSPFreshBulkReadRange(
	ctx context.Context, reorgCtx *ReorgContext, store kv.Storage, tbl table.PhysicalTable,
	run *spfreshBulkRunContext, keyRange kv.KeyRange, parts int,
) ([]kv.KeyRange, uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, 0, err
	}
	if parts <= 1 {
		return []kv.KeyRange{keyRange}, 0, 0, nil
	}
	snapshot := store.GetSnapshot(kv.Version{Ver: run.sourceSnapshotTS})
	snapshot.SetOption(kv.Priority, kv.PriorityLow)
	// The client scanner treats sizes <= 1 as the default 256-row batch.
	snapshot.SetOption(kv.ScanBatchSize, 2)
	snapshot.SetOption(kv.RequestSourceInternal, true)
	snapshot.SetOption(kv.RequestSourceType, reorgCtx.ddlJobSourceType())
	snapshot.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDDL)
	snapshot.SetOption(kv.ResourceGroupName, reorgCtx.resourceGroupName)
	if tagger := reorgCtx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snapshot.SetOption(kv.ResourceGroupTagger, tagger)
	}
	var probeRows, probeBytes uint64
	endpoint := func(reverse bool) (kv.Handle, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var it kv.Iterator
		var err error
		if reverse {
			it, err = snapshot.IterReverse(keyRange.EndKey, keyRange.StartKey)
		} else {
			it, err = snapshot.Iter(keyRange.StartKey, keyRange.EndKey)
		}
		if err != nil {
			return nil, err
		}
		defer it.Close()
		if !it.Valid() {
			return nil, nil
		}
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return nil, err
		}
		if !handle.IsInt() {
			return nil, errors.New("SPFresh source range requires signed integer handles")
		}
		probeRows++
		probeBytes += uint64(len(it.Value()))
		// Account for the second value fetched by the minimum supported batch,
		// without advancing past it and issuing a third-row request.
		if err := it.Next(); err != nil {
			return nil, err
		}
		if it.Valid() {
			probeRows++
			probeBytes += uint64(len(it.Value()))
		}
		return handle, nil
	}
	first, err := endpoint(false)
	if err != nil || first == nil {
		return nil, probeRows, probeBytes, err
	}
	last, err := endpoint(true)
	if err != nil {
		return nil, probeRows, probeBytes, err
	}
	if last == nil || last.IntValue() < first.IntValue() {
		return nil, probeRows, probeBytes, errors.New("SPFresh source range endpoints disagree at snapshot")
	}
	type span struct{ first, last int64 }
	spans := []span{{first.IntValue(), last.IntValue()}}
	for len(spans) < parts {
		widest := -1
		var width uint64
		for i, span := range spans {
			distance := uint64(span.last) - uint64(span.first)
			if distance > width {
				widest, width = i, distance
			}
		}
		if widest < 0 {
			break
		}
		// Unsigned subtraction covers the full signed-handle domain without
		// overflowing at MinInt64/MaxInt64. The midpoint is strictly interior.
		middle := int64(uint64(spans[widest].first) + width/2 + 1)
		right := span{middle, spans[widest].last}
		spans[widest].last = middle - 1
		spans = append(spans, span{})
		copy(spans[widest+2:], spans[widest+1:])
		spans[widest+1] = right
	}
	ranges := make([]kv.KeyRange, len(spans))
	start := keyRange.StartKey
	for i := range spans {
		end := keyRange.EndKey
		if i+1 < len(spans) {
			end = tablecodec.EncodeRowKeyWithHandle(tbl.GetPhysicalID(), kv.IntHandle(spans[i+1].first))
		}
		ranges[i] = kv.KeyRange{StartKey: start, EndKey: end}
		start = end
	}
	return ranges, probeRows, probeBytes, nil
}

func readSPFreshBulkRanges(
	ctx context.Context, reorgCtx *ReorgContext, store kv.Storage, prefix kv.Key,
	run *spfreshBulkRunContext, concurrency, readAheadBytes int,
	nextRange func(context.Context) (kv.KeyRange, error), newConsumer spfreshBulkRangeConsumerFactory,
) (wait, consume, readWork time.Duration, err error) {
	if run.dimension == 0 || run.dimension > pkdbspfreshbulk.MaxDimension {
		return 0, 0, 0, errors.New("SPFresh source dimension is outside the supported range")
	}
	store = spfreshBulkReadStore{Storage: store, batchRows: spfreshBulkReadRPCBatchRows(run.dimension)}
	started := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(ctx)
	// Keep only the active window and one metadata page.
	initial := make([]kv.KeyRange, 0, concurrency)
	for range concurrency {
		keyRange, err := nextRange(groupCtx)
		if errors.ErrorEqual(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, 0, 0, err
		}
		initial = append(initial, keyRange)
	}
	queueCapacity := max(1, readAheadBytes/spfreshBulkReadBatchBytes/max(1, len(initial)))
	if len(initial) == 1 {
		queueCapacity = 1
	}
	type rangeStream struct {
		rows, bytes                       uint64
		err                               error
		ordinal                           int
		started, finished                 time.Time
		sendWait                          time.Duration
		wait, consume                     time.Duration
		recycled                          chan spfreshBulkReadBatch
		consumerStarted, consumerFinished time.Time
	}
	allocated := metrics.SPFreshBulkSourceBufferBytes.WithLabelValues("allocated")
	reused := metrics.SPFreshBulkSourceBufferBytes.WithLabelValues("reused")
	completed := make(chan *rangeStream, len(initial))
	nextOrdinal := 0
	startRange := func(keyRange kv.KeyRange, free chan spfreshBulkReadBatch) error {
		callbacks, err := newConsumer(groupCtx)
		if err != nil {
			return err
		}
		stream := make(chan spfreshBulkReadBatch, queueCapacity)
		if free == nil {
			free = make(chan spfreshBulkReadBatch, queueCapacity+2)
		}
		result := &rangeStream{recycled: free, ordinal: nextOrdinal}
		nextOrdinal++
		group.Go(func() (err error) {
			active := metrics.SPFreshBulkPipelineWorkers.WithLabelValues("reader", "active")
			active.Inc()
			defer active.Dec()
			defer close(stream)
			result.started = time.Now()
			defer func() {
				result.finished = time.Now()
				result.err = err
			}()
			var batch spfreshBulkReadBatch
			send := func() error {
				if len(batch.rows) == 0 {
					return nil
				}
				t := time.Now()
				defer func() { result.sendWait += time.Since(t) }()
				select {
				case stream <- batch:
					batch = spfreshBulkReadBatch{}
					return nil
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
			// The shared iterator has an inclusive end. Stop at our exclusive
			// boundary before forwarding a row, including a split inside a row key.
			err = iterateSnapshotKeys(reorgCtx, store, kv.PriorityLow, prefix, run.sourceSnapshotTS,
				keyRange.StartKey, keyRange.EndKey, func(handle kv.Handle, key kv.Key, value []byte) (bool, error) {
					if err := groupCtx.Err(); err != nil {
						return false, err
					}
					if key.Cmp(keyRange.EndKey) >= 0 {
						return false, nil
					}
					if len(batch.rows) > 0 && len(batch.arena)+len(value) > spfreshBulkReadBatchBytes {
						if err := send(); err != nil {
							return false, err
						}
					}
					if batch.arena == nil {
						if len(value) <= spfreshBulkReadBatchBytes {
							select {
							case batch = <-free:
								reused.Add(float64(cap(batch.arena)))
							default:
							}
						}
						if batch.arena == nil {
							batch.arena = make([]byte, 0, max(spfreshBulkReadBatchBytes, len(value)))
							allocated.Add(float64(cap(batch.arena)))
						}
					}
					result.rows++
					result.bytes += uint64(len(value))
					begin := len(batch.arena)
					batch.arena = append(batch.arena, value...)
					batch.rows = append(batch.rows, spfreshBulkReadRow{handle: handle, value: batch.arena[begin:len(batch.arena):len(batch.arena)]})
					return true, nil
				})
			if err != nil {
				return err
			}
			return send()
		})
		group.Go(func() error {
			active := metrics.SPFreshBulkPipelineWorkers.WithLabelValues("consumer", "active")
			active.Inc()
			defer active.Dec()
			result.consumerStarted = time.Now()
			for {
				started := time.Now()
				var batch spfreshBulkReadBatch
				var ok bool
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case batch, ok = <-stream:
				}
				result.wait += time.Since(started)
				if !ok {
					if result.err != nil {
						return result.err
					}
					started = time.Now()
					if err := callbacks.finish(result.ordinal); err != nil {
						return err
					}
					result.consume += time.Since(started)
					result.consumerFinished = time.Now()
					select {
					case completed <- result:
						return nil
					case <-groupCtx.Done():
						return groupCtx.Err()
					}
				}
				started = time.Now()
				for _, row := range batch.rows {
					if err := groupCtx.Err(); err != nil {
						return err
					}
					if row.handle == nil || !row.handle.IsInt() {
						return errors.New("SPFresh source requires signed integer handles")
					}
					more, err := callbacks.row(result.ordinal, row.handle, row.value)
					if err != nil {
						return err
					}
					if !more {
						return errors.New("SPFresh source consumer stopped before EOF")
					}
				}
				result.consume += time.Since(started)
				// Recycle only after this range's consumer has released all
				// raw views. Oversized rows never enter the reuse pool.
				if cap(batch.arena) == spfreshBulkReadBatchBytes {
					clear(batch.rows)
					batch.rows = batch.rows[:0]
					batch.arena = batch.arena[:0]
					select {
					case free <- batch:
					default:
					}
				}
			}
		})
		return nil
	}
	consumeRanges := func() error {
		for _, keyRange := range initial {
			if err := startRange(keyRange, nil); err != nil {
				return err
			}
		}
		for active := len(initial); active > 0; {
			var current *rangeStream
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case current = <-completed:
			}
			wait += current.wait
			consume += current.consume
			work := current.finished.Sub(current.started) - current.sendWait
			readWork += work
			logSPFreshBulkScanUploadStage(run, "source_range_read", work,
				zap.Int("range_ordinal", current.ordinal), zap.Uint64("record_count", current.rows),
				zap.Uint64("bytes_in", current.bytes), zap.Int64("producer_wait_ms", current.sendWait.Milliseconds()),
				zap.Int64("consumer_started_unix_ms", current.consumerStarted.UnixMilli()),
				zap.Int64("consumer_finished_unix_ms", current.consumerFinished.UnixMilli()),
				zap.Int64("consumer_wait_ms", current.wait.Milliseconds()), zap.Int64("consumer_work_ms", current.consume.Milliseconds()),
				zap.Int64("started_unix_ms", current.started.UnixMilli()), zap.Int64("finished_unix_ms", current.finished.UnixMilli()))
			keyRange, err := nextRange(groupCtx)
			if errors.ErrorEqual(err, io.EOF) {
				active--
			} else if err != nil {
				return err
			} else if err := startRange(keyRange, current.recycled); err != nil {
				return err
			}
		}
		return nil
	}

	err = consumeRanges()
	if err != nil {
		cancel()
	}
	workerErr := group.Wait()
	if err != nil && !errors.ErrorEqual(err, context.Canceled) {
		return wait, consume, readWork, err
	}
	if workerErr != nil {
		return wait, consume, readWork, workerErr
	}
	if err == nil {
		logSPFreshBulkScanUploadStage(run, "source_range_scheduling", time.Since(started),
			zap.Int("read_concurrency", len(initial)), zap.Int("consumer_concurrency", len(initial)))
	}
	return wait, consume, readWork, err
}
