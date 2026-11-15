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
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type spfreshBulkScanConfig struct {
	fragmentTargetBytes int
}

var productionSPFreshBulkScanConfig = spfreshBulkScanConfig{
	fragmentTargetBytes: 256 << 20,
}

type spfreshBulkSampleItem struct {
	priority   [sha256.Size]byte
	handle     int64
	vectorSlot int
	digest     [sha256.Size]byte
}

// spfreshBulkSampleHeap keeps the worst selected item at index zero.
type spfreshBulkSampleHeap []spfreshBulkSampleItem

func (h spfreshBulkSampleHeap) Len() int { return len(h) }
func (h spfreshBulkSampleHeap) Less(i, j int) bool {
	comparison := bytes.Compare(h[i].priority[:], h[j].priority[:])
	if comparison != 0 {
		return comparison > 0
	}
	return h[i].handle > h[j].handle
}
func (h spfreshBulkSampleHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *spfreshBulkSampleHeap) Push(value any) {
	*h = append(*h, value.(spfreshBulkSampleItem))
}
func (h *spfreshBulkSampleHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// Bound each lazy allocation and keep it stable as the sample grows, so
// adding sample rows never recopies vectors that are already stored.
const spfreshBulkSampleBlockRows = 1024

type spfreshBulkBottomKSample struct {
	mu sync.Mutex
	// A conservative prefix of the worst selected priority. It only decreases
	// once full, so rejected rows can avoid the shared heap lock safely.
	cutoff       atomic.Uint64
	limit        int
	dimension    int
	heap         spfreshBulkSampleHeap
	vectorBlocks [][]float32
}

func newSPFreshBulkBottomKSample(limit, dimension int) (*spfreshBulkBottomKSample, error) {
	if limit <= 0 {
		return nil, errors.New("SPFresh MVP sample target rows must be positive")
	}
	if dimension <= 0 || dimension > int(pkdbspfreshbulk.MaxDimension) {
		return nil, errors.New("SPFresh MVP sample dimension is outside the supported range")
	}
	if limit > int(^uint(0)>>1)/dimension {
		return nil, errors.New("SPFresh MVP sample vector capacity overflows")
	}
	sample := &spfreshBulkBottomKSample{
		limit:     limit,
		dimension: dimension,
	}
	sample.cutoff.Store(^uint64(0))
	return sample, nil
}

// AddWithDigest supports concurrent range consumers while keeping one bounded
// sample reservoir. RecordsWithDigests is called only after all consumers join.
func (s *spfreshBulkBottomKSample) AddWithDigest(record pkdbspfreshbulk.BaseFragmentRecord, digest [sha256.Size]byte) {
	var handleBytes [8]byte
	binary.LittleEndian.PutUint64(handleBytes[:], uint64(record.Handle))
	priority := sha256.Sum256(handleBytes[:])
	if binary.BigEndian.Uint64(priority[:8]) > s.cutoff.Load() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heap) == s.limit {
		worst := s.heap[0]
		comparison := bytes.Compare(priority[:], worst.priority[:])
		if comparison > 0 || comparison == 0 && record.Handle >= worst.handle {
			return
		}
	}
	s.addWithPriority(priority, record, digest)
	if len(s.heap) == s.limit {
		s.cutoff.Store(binary.BigEndian.Uint64(s.heap[0].priority[:8]))
	}
}

func (s *spfreshBulkBottomKSample) addWithPriority(
	priority [sha256.Size]byte,
	record pkdbspfreshbulk.BaseFragmentRecord,
	digest [sha256.Size]byte,
) {
	item := spfreshBulkSampleItem{
		priority: priority,
		handle:   record.Handle,
		digest:   digest,
	}
	if len(s.heap) < s.limit {
		item.vectorSlot = len(s.heap)
		s.copyVector(item.vectorSlot, record.Vector)
		heap.Push(&s.heap, item)
		return
	}
	worst := s.heap[0]
	item.vectorSlot = worst.vectorSlot
	s.copyVector(item.vectorSlot, record.Vector)
	s.heap[0] = item
	heap.Fix(&s.heap, 0)
}

func (s *spfreshBulkBottomKSample) copyVector(slot int, vector []float32) {
	if slot/spfreshBulkSampleBlockRows == len(s.vectorBlocks) {
		rows := min(spfreshBulkSampleBlockRows, s.limit-slot)
		s.vectorBlocks = append(s.vectorBlocks, make([]float32, rows*s.dimension))
	}
	copy(s.vector(slot), vector)
}

func (s *spfreshBulkBottomKSample) vector(slot int) []float32 {
	start := (slot % spfreshBulkSampleBlockRows) * s.dimension
	end := start + s.dimension
	return s.vectorBlocks[slot/spfreshBulkSampleBlockRows][start:end:end]
}

func (s *spfreshBulkBottomKSample) Records(limit int) []pkdbspfreshbulk.BaseFragmentRecord {
	records, _ := s.RecordsWithDigests(limit)
	return records
}

func (s *spfreshBulkBottomKSample) RecordsWithDigests(
	limit int,
) ([]pkdbspfreshbulk.BaseFragmentRecord, [][sha256.Size]byte) {
	items := append(spfreshBulkSampleHeap(nil), s.heap...)
	sort.Slice(items, func(i, j int) bool {
		comparison := bytes.Compare(items[i].priority[:], items[j].priority[:])
		if comparison != 0 {
			return comparison < 0
		}
		return items[i].handle < items[j].handle
	})
	limit = min(limit, len(items))
	selected := make([]struct {
		record pkdbspfreshbulk.BaseFragmentRecord
		digest [sha256.Size]byte
	}, limit)
	for i, item := range items[:limit] {
		selected[i] = struct {
			record pkdbspfreshbulk.BaseFragmentRecord
			digest [sha256.Size]byte
		}{
			record: pkdbspfreshbulk.BaseFragmentRecord{
				Handle: item.handle,
				Vector: s.vector(item.vectorSlot),
			},
			digest: item.digest,
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].record.Handle < selected[j].record.Handle })
	records := make([]pkdbspfreshbulk.BaseFragmentRecord, limit)
	digests := make([][sha256.Size]byte, limit)
	for i, item := range selected {
		records[i] = item.record
		digests[i] = item.digest
	}
	return records, digests
}

const spfreshBulkPendingFragments = 4

type spfreshBulkFragment struct {
	rangeOrdinal       int
	ordinal            int
	key                string
	records            []pkdbspfreshbulk.BaseFragmentRecord
	storedPayloadArena []byte
	fingerprint        pkdbspfreshbulk.MultisetFingerprint
}

type spfreshBulkFragmentResult struct {
	rangeOrdinal     int
	ordinal          int
	uri              string
	bytes            uint64
	sha256           string
	ref              pkdbspfreshbulk.ManifestRef
	fingerprint      pkdbspfreshbulk.MultisetFingerprint
	encodeStartedAt  time.Time
	encodeFinishedAt time.Time
	encodeDuration   time.Duration
	writeStartedAt   time.Time
	writeFinishedAt  time.Time
	writeDuration    time.Duration
}

// One active source range owns its fragment and ordering state. The total
// fragment target is divided across readers, rather than multiplied by them.
type spfreshBulkRangeFragment struct {
	records       []pkdbspfreshbulk.BaseFragmentRecord
	vectors       []float32
	payload       []byte
	fingerprint   pkdbspfreshbulk.FingerprintAccumulator
	nextOrdinal   int
	bufferedBytes int
	lastHandle    int64
	haveHandle    bool
}

type spfreshBulkTimedInterval struct {
	startedAt  time.Time
	finishedAt time.Time
}

type spfreshBulkScanUploadTimings struct {
	scanDuration    time.Duration
	queueWait       time.Duration
	encodeDuration  time.Duration
	writeDuration   time.Duration
	encodeIntervals []spfreshBulkTimedInterval
	writeIntervals  []spfreshBulkTimedInterval
	fragmentCount   int
}

func scanAndUploadSPFreshBaseFragments(
	ctx context.Context,
	w *worker,
	tbl table.Table,
	reorgCtx *ReorgContext,
	run *spfreshBulkRunContext,
	config spfreshBulkScanConfig,
) error {
	physicalTable, ok := tbl.(table.PhysicalTable)
	if !ok {
		return errors.New("SPFresh MVP bulk scan requires one non-partitioned physical table")
	}
	layout, err := prepareSPFreshBulkLayout(ctx, w, run)
	if err != nil {
		return err
	}
	backend, err := storage.ParseBackend(run.cloudStorageURI, nil)
	if err != nil {
		return errors.Annotate(err, "parse SPFresh MVP BaseFragment storage")
	}
	externalStorage, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return errors.Annotate(err, "open SPFresh MVP BaseFragment storage")
	}
	if err := initializeSPFreshBulkLifecycle(ctx, externalStorage); err != nil {
		externalStorage.Close()
		return err
	}
	err = scanAndUploadSPFreshBaseFragmentsToStore(
		ctx,
		w.sess.Session().GetExprCtx(),
		reorgCtx,
		w.store,
		physicalTable,
		run,
		externalStorage,
		layout,
		config,
	)
	closeStarted := time.Now()
	externalStorage.Close()
	logSPFreshBulkScanUploadStage(run, "externalStorage.Close", time.Since(closeStarted))
	return err
}

func scanAndUploadSPFreshBaseFragmentsToStore(
	ctx context.Context,
	exprCtx expression.BuildContext,
	reorgCtx *ReorgContext,
	store kv.Storage,
	tbl table.PhysicalTable,
	run *spfreshBulkRunContext,
	externalStorage storage.ExternalStorage,
	layout spfreshBulkLayout,
	config spfreshBulkScanConfig,
) error {
	if run == nil {
		return errors.New("SPFresh MVP bulk scan requires run context")
	}
	baseRecordBytes := (pkdbspfreshbulk.BaseFragmentRecord{}).EncodedBytes(run.dimension)
	if config.fragmentTargetBytes < baseRecordBytes {
		return errors.New("SPFresh MVP fragment target is smaller than one record")
	}
	indexInfo := tbl.Meta().FindIndexByID(run.indexID)
	if indexInfo == nil || len(indexInfo.Columns) != 1 {
		return errors.New("SPFresh MVP bulk scan cannot find the admitted vector index")
	}
	vectorOffset := indexInfo.Columns[0].Offset
	columns := tbl.Cols()
	if vectorOffset < 0 || vectorOffset >= len(columns) {
		return errors.New("SPFresh MVP bulk scan vector column offset is invalid")
	}
	vectorColumn := columns[vectorOffset]
	if vectorColumn == nil {
		return errors.New("SPFresh MVP bulk scan vector column is missing")
	}
	convertible, ok := exprCtx.(exprctx.StaticConvertibleExprContext)
	if !ok {
		return errors.New("SPFresh bulk scan requires a detachable expression context")
	}
	frozenExprCtx := exprstatic.MakeExprContextStatic(convertible)
	if layout.shardCount == 0 {
		return errors.New("SPFresh bulk scan requires a pre-scan layout")
	}
	var sample *spfreshBulkBottomKSample
	if layout.sampleTargetRows > 0 {
		var err error
		sample, err = newSPFreshBulkBottomKSample(int(layout.sampleTargetRows), int(run.dimension))
		if err != nil {
			return err
		}
	}
	quota, quotaErr := memory.MemTotal()
	if quotaErr != nil {
		return errors.Annotate(quotaErr, "determine SPFresh source memory quota")
	}
	visibleMemory := quota
	serverMemoryLimit := memory.ServerMemoryLimit.Load()
	if serverMemoryLimit != 0 {
		quota = min(quota, serverMemoryLimit)
	}
	cpuBudget, cpuErr := cgroup.DiscoverCPUBudget()
	if cpuErr != nil {
		return errors.Annotate(cpuErr, "determine SPFresh source CPU quota")
	}
	cpus := cpuBudget.Effective
	concurrency := spfreshBulkReadConcurrency(cpus, quota)
	cpuReaders, memoryReaders := spfreshBulkReaderLimits(cpus, quota)
	logutil.DDLLogger().Info("SPFresh source resource budget selected",
		zap.String("build_id", run.buildID), zap.String("run_id", run.runID),
		zap.Int("effective_cpus", cpus), zap.Int("gomaxprocs", cpuBudget.GOMAXPROCS),
		zap.Int("available_cpus", cpuBudget.Available), zap.Int("affinity_cpus", cpuBudget.Affinity),
		zap.Int("cgroup_cpu_limit", cpuBudget.Cgroup), zap.Uint64("visible_memory_bytes", visibleMemory),
		zap.Uint64("server_memory_limit_bytes", serverMemoryLimit), zap.Uint64("memory_quota_bytes", quota),
		zap.Int("cpu_reader_limit", cpuReaders), zap.Uint64("memory_reader_limit", memoryReaders),
		zap.String("reader_limit_reason", spfreshBulkLimitReason(uint64(cpuReaders), memoryReaders)),
		zap.Int("read_concurrency", concurrency))
	if concurrency == 0 {
		return errors.New("SPFresh source memory quota cannot fit one reader")
	}
	readAheadBytes := int(min(uint64(spfreshBulkReadAheadBytes), quota/16))
	fragmentBudget := int(min(uint64(config.fragmentTargetBytes), quota/16))
	fragmentTargetBytes := max(baseRecordBytes, fragmentBudget/concurrency)
	uploadWorkers := spfreshBulkUploadConcurrency(concurrency, cpus, quota, fragmentTargetBytes)
	recordCapacity := fragmentTargetBytes / baseRecordBytes
	vectorCapacity := recordCapacity * int(run.dimension)
	if recordCapacity <= 0 || vectorCapacity/recordCapacity != int(run.dimension) {
		return errors.New("SPFresh MVP fragment vector capacity overflows")
	}
	extraCPU, extraMemory := spfreshBulkUploadLimits(concurrency, cpus, quota, fragmentTargetBytes)
	logutil.DDLLogger().Info("SPFresh source pipeline configured",
		zap.String("build_id", run.buildID), zap.String("run_id", run.runID),
		zap.Int("read_concurrency", concurrency), zap.Int("consumer_concurrency", concurrency),
		zap.Int("upload_workers", uploadWorkers), zap.Int("extra_upload_cpu_limit", extraCPU),
		zap.Uint64("extra_upload_memory_limit", extraMemory), zap.Int("extra_upload_reader_limit", concurrency),
		zap.Int("rpc_batch_rows", spfreshBulkReadRPCBatchRows(run.dimension)),
		zap.Int("read_ahead_bytes", readAheadBytes), zap.Int("fragment_target_bytes", fragmentTargetBytes),
		zap.Int("pending_fragments", spfreshBulkPendingFragments))
	for role, count := range map[string]int{"reader": concurrency, "consumer": concurrency, "uploader": uploadWorkers} {
		configured := metrics.SPFreshBulkPipelineWorkers.WithLabelValues(role, "configured")
		configured.Add(float64(count))
		defer configured.Sub(float64(count))
	}
	var totalsMu sync.Mutex
	var sourceRows, sourceRawBytes uint64
	var sourceFingerprint pkdbspfreshbulk.FingerprintAccumulator
	queueWait := time.Duration(0)

	jobs := make(chan spfreshBulkFragment, spfreshBulkPendingFragments)
	stopQueueMetrics := observeSPFreshPendingFragments(func() int { return len(jobs) })
	defer stopQueueMetrics()
	results := make(chan spfreshBulkFragmentResult, uploadWorkers)
	uploaded := make([]spfreshBulkFragmentResult, 0)
	resultsDone := make(chan struct{})
	pipelineCtx, cancelPipeline := context.WithCancel(ctx)
	defer cancelPipeline()
	group, groupCtx := errgroup.WithContext(pipelineCtx)
	// Keep only a small number of raw fragments in flight so scan memory stays
	// bounded while encoding and object upload overlap the source iterator.
	go func() {
		defer close(resultsDone)
		for result := range results {
			uploaded = append(uploaded, result)
		}
	}()
	for workerID := 0; workerID < uploadWorkers; workerID++ {
		group.Go(func() error {
			for fragment := range jobs {
				active := metrics.SPFreshBulkPipelineWorkers.WithLabelValues("uploader", "active")
				active.Inc()
				result, err := encodeAndUploadSPFreshBulkFragment(
					groupCtx,
					externalStorage,
					run,
					fragment,
				)
				active.Dec()
				if err != nil {
					return err
				}
				select {
				case results <- result:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
			return nil
		})
	}

	newConsumer := func(consumerCtx context.Context) (spfreshBulkRangeCallbacks, error) {
		exprCtx := exprstatic.MakeExprContextStatic(frozenExprCtx)
		vectorDecoder := rowcodec.NewDatumMapDecoder(
			[]rowcodec.ColInfo{{ID: vectorColumn.ID, Ft: &vectorColumn.FieldType}},
			exprCtx.GetEvalCtx().Location(),
		)
		vectorRow := make(map[int64]types.Datum, 1)
		storedEncoder, err := newSPFreshBulkStoredEncoder(tbl, indexInfo, vectorColumn, exprCtx.GetEvalCtx().Location())
		if err != nil {
			return spfreshBulkRangeCallbacks{}, err
		}
		hasStoredColumns := len(storedEncoder.columns) > 0
		buffer := &spfreshBulkRangeFragment{}
		var rangeRows, rangeRawBytes uint64
		var rangeFingerprint pkdbspfreshbulk.FingerprintAccumulator
		var rangeQueueWait time.Duration
		flushFragment := func(rangeOrdinal int, buffer *spfreshBulkRangeFragment) error {
			if len(buffer.records) == 0 {
				return nil
			}
			ordinal := buffer.nextOrdinal
			artifactID := fmt.Sprintf("part-%020d-%020d", rangeOrdinal, ordinal)
			key := fmt.Sprintf("base/%s.spfmvp", artifactID)
			task := spfreshBulkFragment{
				rangeOrdinal:       rangeOrdinal,
				ordinal:            ordinal,
				key:                key,
				records:            buffer.records,
				storedPayloadArena: buffer.payload,
				fingerprint:        buffer.fingerprint.Fingerprint(),
			}
			queueStarted := time.Now()
			select {
			case jobs <- task:
			case <-consumerCtx.Done():
				return consumerCtx.Err()
			}
			rangeQueueWait += time.Since(queueStarted)
			buffer.nextOrdinal++
			buffer.records = nil
			buffer.bufferedBytes = 0
			buffer.vectors = nil
			buffer.payload = nil
			buffer.fingerprint = pkdbspfreshbulk.FingerprintAccumulator{}
			return nil
		}

		return spfreshBulkRangeCallbacks{row: func(rangeOrdinal int, handle kv.Handle, rawRecord []byte) (bool, error) {
			if err := consumerCtx.Err(); err != nil {
				return false, err
			}
			if handle == nil || !handle.IsInt() {
				return false, errors.New("SPFresh MVP bulk scan requires signed integer handles")
			}
			handleValue := handle.IntValue()
			if buffer.haveHandle && handleValue <= buffer.lastHandle {
				return false, errors.New("SPFresh MVP bulk scan handles must be strictly increasing")
			}
			var vector []float32
			var storedPayload []byte
			var err error
			if hasStoredColumns {
				vector, storedPayload, buffer.payload, err = storedEncoder.decodeAndEncode(
					exprCtx,
					handle,
					rawRecord,
					vectorColumn.ID,
					buffer.payload,
				)
				if err != nil {
					return false, err
				}
			} else {
				vectorDatum, err := decodeSPFreshBulkVector(vectorDecoder, vectorRow, vectorColumn.ID, rawRecord)
				if err != nil {
					return false, errors.Annotate(err, "decode SPFresh MVP source vector")
				}
				vector, err = extractSPFreshBulkVector(vectorDatum, run.dimension)
				if err != nil {
					return false, err
				}
			}
			record := pkdbspfreshbulk.BaseFragmentRecord{
				Handle:        handleValue,
				Vector:        vector,
				StoredPayload: storedPayload,
			}
			if len(buffer.records) > 0 && buffer.bufferedBytes+record.EncodedBytes(run.dimension) > fragmentTargetBytes {
				if err := flushFragment(rangeOrdinal, buffer); err != nil {
					return false, err
				}
				// The first encoding was only used to size the boundary row. Re-encode
				// it into the new fragment arena so every row slice stays owned by
				// exactly one in-flight fragment.
				if hasStoredColumns {
					_, storedPayload, buffer.payload, err = storedEncoder.decodeAndEncode(
						exprCtx,
						handle,
						rawRecord,
						vectorColumn.ID,
						buffer.payload,
					)
					if err != nil {
						return false, err
					}
					record.StoredPayload = storedPayload
				}
			}
			// Decide the boundary before copying the vector. Otherwise a full arena
			// grows just to hold the next fragment's first row, which also retains
			// the previous fragment's allocation while that row is in flight.
			if buffer.vectors == nil {
				buffer.vectors = make([]float32, 0, vectorCapacity)
			}
			vectorStart := len(buffer.vectors)
			buffer.vectors = append(buffer.vectors, vector...)
			record.Vector = buffer.vectors[vectorStart:]
			digest, err := pkdbspfreshbulk.RecordDigestWithPayload(record.Handle, record.Vector, record.StoredPayload)
			if err != nil {
				return false, errors.Annotate(err, "fingerprint SPFresh MVP source row")
			}
			buffer.bufferedBytes += record.EncodedBytes(run.dimension)
			record.SourceDigestSHA256 = digest
			if sample != nil {
				sample.AddWithDigest(record, digest)
			}
			if err := rangeFingerprint.AddDigest(digest); err != nil {
				return false, errors.Annotate(err, "fingerprint SPFresh MVP source row")
			}
			if err := buffer.fingerprint.AddDigest(digest); err != nil {
				return false, errors.Annotate(err, "fingerprint SPFresh MVP BaseFragment")
			}
			buffer.records = append(buffer.records, record)
			buffer.haveHandle = true
			buffer.lastHandle = handleValue
			rangeRows++
			rangeRawBytes += uint64(len(rawRecord))
			return true, nil
		}, finish: func(ordinal int) error {
			if err := flushFragment(ordinal, buffer); err != nil {
				return err
			}
			totalsMu.Lock()
			defer totalsMu.Unlock()
			sourceRows += rangeRows
			sourceRawBytes += rangeRawBytes
			queueWait += rangeQueueWait
			return sourceFingerprint.Merge(rangeFingerprint.Fingerprint())
		}}, nil
	}
	scanStarted := time.Now()
	err := iterateSPFreshBulkSnapshot(groupCtx, reorgCtx, store, tbl, run, concurrency, readAheadBytes, newConsumer)

	scanDuration := time.Since(scanStarted)
	close(jobs)
	if err != nil {
		cancelPipeline()
	}
	workerErr := group.Wait()
	close(results)
	<-resultsDone
	if err != nil && !errors.ErrorEqual(err, context.Canceled) {
		return err
	}
	if workerErr != nil {
		return workerErr
	}
	if err != nil {
		return err
	}
	if sourceRows == 0 {
		// Zero input is a complete scan result. Publication initializes an empty
		// index at the same B; there are no artifacts to build, prove or import.
		run.sourceRows = 0
		run.sourceFingerprint = sourceFingerprint.Fingerprint()
		return nil
	}
	sort.Slice(uploaded, func(i, j int) bool {
		if uploaded[i].rangeOrdinal != uploaded[j].rangeOrdinal {
			return uploaded[i].rangeOrdinal < uploaded[j].rangeOrdinal
		}
		return uploaded[i].ordinal < uploaded[j].ordinal
	})
	fragmentURIs := make([]string, len(uploaded))
	fragmentBytes := make([]uint64, len(uploaded))
	fragmentSHA256 := make([]string, len(uploaded))
	fragmentRefs := make([]pkdbspfreshbulk.ManifestRef, len(uploaded))
	var baseFingerprint pkdbspfreshbulk.FingerprintAccumulator
	var baseObjectBytes uint64
	var encodeDuration, writeDuration time.Duration
	var encodeIntervals, writeIntervals []spfreshBulkTimedInterval
	for i, result := range uploaded {
		expected := 0
		if i > 0 && uploaded[i-1].rangeOrdinal == result.rangeOrdinal {
			expected = uploaded[i-1].ordinal + 1
		}
		if result.ordinal != expected {
			return errors.New("SPFresh BaseFragment ordinals within a source range are not contiguous")
		}
		fragmentURIs[i] = result.uri
		fragmentBytes[i] = result.bytes
		fragmentSHA256[i] = result.sha256
		fragmentRefs[i] = result.ref
		baseObjectBytes += result.bytes
		encodeDuration += result.encodeDuration
		writeDuration += result.writeDuration
		encodeIntervals = append(encodeIntervals, spfreshBulkTimedInterval{
			startedAt:  result.encodeStartedAt,
			finishedAt: result.encodeFinishedAt,
		})
		writeIntervals = append(writeIntervals, spfreshBulkTimedInterval{
			startedAt:  result.writeStartedAt,
			finishedAt: result.writeFinishedAt,
		})
		if err := baseFingerprint.Merge(result.fingerprint); err != nil {
			return errors.Annotate(err, "merge SPFresh MVP BaseFragment fingerprint")
		}
	}
	if sourceFingerprint.Fingerprint() != baseFingerprint.Fingerprint() {
		return errors.New("SPFresh MVP source and BaseFragment fingerprints differ")
	}
	timings := spfreshBulkScanUploadTimings{
		scanDuration:    scanDuration,
		queueWait:       queueWait,
		encodeDuration:  encodeDuration,
		writeDuration:   writeDuration,
		encodeIntervals: encodeIntervals,
		writeIntervals:  writeIntervals,
		fragmentCount:   len(uploaded),
	}
	targetShards := layout.shardCount
	layout, err = layout.finish(sourceRows, run.dimension)
	if err != nil {
		return err
	}
	var sampleRefs []pkdbspfreshbulk.ManifestRef
	var sampleRows, sampleBytes uint64
	var sampleFingerprint pkdbspfreshbulk.MultisetFingerprint
	if layout.sampleTargetRows > 0 {
		sampleRecords, sampleDigests := sample.RecordsWithDigests(int(layout.sampleTargetRows))
		sampleRefs, sampleRows, sampleBytes, sampleFingerprint, err = writeSPFreshBulkSampleFragmentsWithDigests(
			ctx,
			externalStorage,
			run,
			config.fragmentTargetBytes,
			sampleRecords,
			sampleDigests,
			&timings,
		)
		if err != nil {
			return err
		}
	}
	logSPFreshBulkScanUploadStage(run, "scan_upload_wall", time.Since(scanStarted),
		zap.Uint64("record_count", sourceRows), zap.Uint64("bytes_in", sourceRawBytes),
		zap.Int64("started_unix_ms", scanStarted.UnixMilli()), zap.Int64("finished_unix_ms", time.Now().UnixMilli()))
	logSPFreshBulkScanUploadStage(
		run,
		"source_scan_wall",
		timings.scanDuration,
		zap.Uint64("record_count", sourceRows),
		zap.Uint64("bytes_in", sourceRawBytes),
	)
	logSPFreshBulkScanUploadStage(
		run,
		"fragment_queue_wait",
		timings.queueWait,
		zap.Int("fragment_count", timings.fragmentCount),
		zap.Int("pending_fragments", spfreshBulkPendingFragments),
		zap.Int("fragment_target_bytes", fragmentTargetBytes),
		zap.Int("upload_workers", uploadWorkers),
		zap.Int("consumer_concurrency", concurrency),
		zap.Int("effective_cpus", cpus),
		zap.Uint64("memory_quota_bytes", quota),
		zap.Int("read_ahead_bytes", readAheadBytes),
		zap.Int("read_concurrency", concurrency),
	)
	logSPFreshBulkScanUploadStage(
		run,
		"encode",
		unionSPFreshBulkTimedIntervals(timings.encodeIntervals),
		zap.Int("fragment_count", timings.fragmentCount),
		zap.Int64("work_duration_ms", timings.encodeDuration.Milliseconds()),
	)
	logSPFreshBulkScanUploadStage(
		run,
		"WriteImmutableObject",
		unionSPFreshBulkTimedIntervals(timings.writeIntervals),
		zap.Int("fragment_count", timings.fragmentCount),
		zap.Int64("work_duration_ms", timings.writeDuration.Milliseconds()),
	)

	run.baseFragmentURIs = fragmentURIs
	run.baseFragmentBytes = fragmentBytes
	run.baseFragmentSHA256 = fragmentSHA256
	run.baseFragmentRefs = fragmentRefs
	run.sourceRows = sourceRows
	run.sourceRawBytes = sourceRawBytes
	run.baseObjectBytes = baseObjectBytes
	run.sourceFingerprint = sourceFingerprint.Fingerprint()
	run.sampleFragmentRefs = sampleRefs
	run.sampleRows = sampleRows
	run.sampleObjectBytes = sampleBytes
	run.sampleFingerprint = sampleFingerprint
	run.shardCount = layout.shardCount
	logutil.DDLLogger().Info("SPFresh scan layout completed", zap.String("build_id", run.buildID),
		zap.Uint32("target_shards", targetShards), zap.Uint32("effective_shards", layout.shardCount),
		zap.Uint64("source_rows", sourceRows), zap.Uint64("sample_rows", sampleRows),
		zap.Uint64("post_scan_sampling_read_bytes", 0))
	return nil
}

// Preserve the existing scan window. Extra uploaders may consume at most 1/16
// of the effective memory quota, charging three fragment sizes for raw rows,
// encoded bytes and allocation slack. Combined extra CPU work leaves a quarter
// of GOMAXPROCS available; at most double the reader count can upload.
func spfreshBulkUploadConcurrency(readers, cpus int, quota uint64, fragmentBytes int) int {
	extraCPU, extraMemory := spfreshBulkUploadLimits(readers, cpus, quota, fragmentBytes)
	extra := min(readers, extraCPU, int(min(extraMemory, uint64(readers))))
	return readers + extra
}

func spfreshBulkUploadLimits(readers, cpus int, quota uint64, fragmentBytes int) (int, uint64) {
	return max(0, cpus-cpus/4-2*readers), quota / 16 / uint64(fragmentBytes) / 3
}

func spfreshBulkLimitReason(cpu, memory uint64) string {
	if cpu < memory {
		return "cpu"
	}
	if memory < cpu {
		return "memory"
	}
	return "cpu_and_memory"
}

// Sampling channel length avoids adding synchronization to fragment delivery.
// Each build contributes only its own delta, and removes it even on cancellation.
func observeSPFreshPendingFragments(pending func() int) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		previous := 0
		defer func() { metrics.SPFreshBulkPendingFragments.Sub(float64(previous)) }()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				current := pending()
				metrics.SPFreshBulkPendingFragments.Add(float64(current - previous))
				previous = current
			}
		}
	}()
	return func() { close(stop); <-done }
}

func encodeAndUploadSPFreshBulkFragment(
	ctx context.Context,
	externalStorage storage.ExternalStorage,
	run *spfreshBulkRunContext,
	fragment spfreshBulkFragment,
) (spfreshBulkFragmentResult, error) {
	records := fragment.records
	header := pkdbspfreshbulk.BaseFragmentHeader{
		CommonHeader: pkdbspfreshbulk.CommonHeader{
			BuildID:     run.buildID,
			ArtifactID:  fmt.Sprintf("part-%020d-%020d", fragment.rangeOrdinal, fragment.ordinal),
			TableID:     run.tableID,
			IndexID:     run.indexID,
			Dimension:   run.dimension,
			Distance:    run.distance,
			RecordCount: uint64(len(records)),
			Producer:    "tidb",
		},
		SourceSnapshotTS: run.sourceSnapshotTS,
		MinHandle:        records[0].Handle,
		MaxHandle:        records[len(records)-1].Handle,
	}
	encodeStarted := time.Now()
	encoded, err := pkdbspfreshbulk.EncodeBaseFragment(header, records)
	if err != nil {
		return spfreshBulkFragmentResult{}, errors.Annotate(err, "encode SPFresh MVP BaseFragment")
	}
	encodeFinished := time.Now()
	encodeDuration := encodeFinished.Sub(encodeStarted)
	writeStarted := time.Now()
	if err := pkdbspfreshbulk.WriteImmutableObject(ctx, externalStorage, fragment.key, encoded); err != nil {
		return spfreshBulkFragmentResult{}, err
	}
	writeFinished := time.Now()
	writeDuration := writeFinished.Sub(writeStarted)
	digest := sha256.Sum256(encoded)
	uri := run.artifactPrefix + "/" + fragment.key
	return spfreshBulkFragmentResult{
		rangeOrdinal:     fragment.rangeOrdinal,
		ordinal:          fragment.ordinal,
		uri:              uri,
		bytes:            uint64(len(encoded)),
		sha256:           hex.EncodeToString(digest[:]),
		ref:              pkdbspfreshbulk.NewManifestRefFromDigest(uri, digest),
		fingerprint:      fragment.fingerprint,
		encodeStartedAt:  encodeStarted,
		encodeFinishedAt: encodeFinished,
		encodeDuration:   encodeDuration,
		writeStartedAt:   writeStarted,
		writeFinishedAt:  writeFinished,
		writeDuration:    writeDuration,
	}, nil
}

func logSPFreshBulkScanUploadStage(
	run *spfreshBulkRunContext,
	stage string,
	duration time.Duration,
	extra ...zap.Field,
) {
	if run == nil {
		return
	}
	metrics.SPFreshBulkScanUploadDuration.WithLabelValues(stage).Observe(duration.Seconds())
	fields := []zap.Field{
		zap.String("path", "bulk"),
		zap.String("build_id", run.buildID),
		zap.String("run_id", run.runID),
		zap.String("scope", "scan_upload"),
		zap.String("stage", stage),
		zap.Uint64("source_snapshot_ts", run.sourceSnapshotTS),
		zap.Int64("duration_ms", duration.Milliseconds()),
	}
	fields = append(fields, extra...)
	logutil.DDLLogger().Info("SPFresh MVP bulk stage completed", fields...)
}

func unionSPFreshBulkTimedIntervals(intervals []spfreshBulkTimedInterval) time.Duration {
	if len(intervals) == 0 {
		return 0
	}
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].startedAt.Before(intervals[j].startedAt)
	})
	start, end := intervals[0].startedAt, intervals[0].finishedAt
	var duration time.Duration
	for _, interval := range intervals[1:] {
		if interval.startedAt.After(end) {
			duration += end.Sub(start)
			start, end = interval.startedAt, interval.finishedAt
			continue
		}
		if interval.finishedAt.After(end) {
			end = interval.finishedAt
		}
	}
	return duration + end.Sub(start)
}

func decodeSPFreshBulkVector(
	decoder *rowcodec.DatumMapDecoder,
	rowMap map[int64]types.Datum,
	vectorColumnID int64,
	rawRecord []byte,
) (types.Datum, error) {
	delete(rowMap, vectorColumnID)
	rowMap, err := decoder.DecodeToDatumMap(rawRecord, rowMap)
	if err != nil {
		return types.Datum{}, err
	}
	vectorDatum, ok := rowMap[vectorColumnID]
	if !ok {
		return types.Datum{}, errors.New("SPFresh MVP source row is missing the vector column")
	}
	return vectorDatum, nil
}

func extractSPFreshBulkVector(vectorDatum types.Datum, dimension uint32) ([]float32, error) {
	if vectorDatum.IsNull() {
		return nil, errors.New("SPFresh MVP source vector must not be NULL")
	}
	if vectorDatum.Kind() != types.KindVectorFloat32 {
		return nil, errors.New("SPFresh MVP source vector has unexpected SQL type")
	}
	vector := vectorDatum.GetVectorFloat32()
	if vector.Len() != int(dimension) {
		return nil, errors.Errorf(
			"SPFresh MVP source vector has dimension %d, expected %d",
			vector.Len(),
			dimension,
		)
	}
	return vector.Elements(), nil
}

func writeSPFreshBulkSampleFragmentsWithDigests(
	ctx context.Context,
	externalStorage storage.ExternalStorage,
	run *spfreshBulkRunContext,
	fragmentTargetBytes int,
	records []pkdbspfreshbulk.BaseFragmentRecord,
	digests [][sha256.Size]byte,
	timings *spfreshBulkScanUploadTimings,
) ([]pkdbspfreshbulk.ManifestRef, uint64, uint64, pkdbspfreshbulk.MultisetFingerprint, error) {
	if len(records) == 0 {
		return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, errors.New("SPFresh MVP sample is empty")
	}
	if len(digests) != len(records) {
		return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, errors.Errorf(
			"SPFresh MVP sample digest count %d does not match record count %d",
			len(digests),
			len(records),
		)
	}
	recordsPerFragment := fragmentTargetBytes / ((pkdbspfreshbulk.BaseFragmentRecord{}).EncodedBytes(run.dimension))
	if recordsPerFragment <= 0 {
		return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, errors.New("SPFresh MVP sample fragment target is too small")
	}
	refs := make([]pkdbspfreshbulk.ManifestRef, 0, (len(records)+recordsPerFragment-1)/recordsPerFragment)
	var objectBytes uint64
	var fingerprint pkdbspfreshbulk.FingerprintAccumulator
	for start, end, ordinal := 0, 0, 0; start < len(records); start, ordinal = end, ordinal+1 {
		end = spfreshBulkFragmentEnd(records, start, fragmentTargetBytes, run.dimension)
		fragmentRecords := append([]pkdbspfreshbulk.BaseFragmentRecord(nil), records[start:end]...)
		for i := range fragmentRecords {
			fragmentRecords[i].SourceDigestSHA256 = digests[start+i]
		}
		artifactID := fmt.Sprintf("sample-part-%05d", ordinal)
		key := fmt.Sprintf("sample/part-%05d.spfmvp", ordinal)
		header := pkdbspfreshbulk.BaseFragmentHeader{
			CommonHeader: pkdbspfreshbulk.CommonHeader{
				BuildID:     run.buildID,
				ArtifactID:  artifactID,
				TableID:     run.tableID,
				IndexID:     run.indexID,
				Dimension:   run.dimension,
				Distance:    run.distance,
				RecordCount: uint64(len(fragmentRecords)),
				Producer:    "tidb",
			},
			SourceSnapshotTS: run.sourceSnapshotTS,
			MinHandle:        fragmentRecords[0].Handle,
			MaxHandle:        fragmentRecords[len(fragmentRecords)-1].Handle,
		}
		encodeStarted := time.Now()
		encoded, err := pkdbspfreshbulk.EncodeBaseFragment(header, fragmentRecords)
		if err != nil {
			return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, errors.Annotate(err, "encode SPFresh MVP sample fragment")
		}
		encodeFinished := time.Now()
		writeStarted := time.Now()
		if err := pkdbspfreshbulk.WriteImmutableObject(ctx, externalStorage, key, encoded); err != nil {
			return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, err
		}
		writeFinished := time.Now()
		if timings != nil {
			timings.encodeDuration += encodeFinished.Sub(encodeStarted)
			timings.writeDuration += writeFinished.Sub(writeStarted)
			timings.encodeIntervals = append(timings.encodeIntervals, spfreshBulkTimedInterval{
				startedAt: encodeStarted, finishedAt: encodeFinished,
			})
			timings.writeIntervals = append(timings.writeIntervals, spfreshBulkTimedInterval{
				startedAt: writeStarted, finishedAt: writeFinished,
			})
			timings.fragmentCount++
		}
		uri := run.artifactPrefix + "/" + key
		digest := sha256.Sum256(encoded)
		refs = append(refs, pkdbspfreshbulk.NewManifestRefFromDigest(uri, digest))
		objectBytes += uint64(len(encoded))
		var fragmentFingerprint pkdbspfreshbulk.FingerprintAccumulator
		for i := range fragmentRecords {
			digest := digests[start+i]
			if err := fragmentFingerprint.AddDigest(digest); err != nil {
				return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, errors.Annotate(err, "fingerprint SPFresh MVP sample fragment")
			}
		}
		if err := fingerprint.Merge(fragmentFingerprint.Fingerprint()); err != nil {
			return nil, 0, 0, pkdbspfreshbulk.MultisetFingerprint{}, errors.Annotate(err, "merge SPFresh MVP sample fingerprint")
		}
	}
	return refs, uint64(len(records)), objectBytes, fingerprint.Fingerprint(), nil
}

func buildAndWriteSPFreshSortedRuns(
	ctx context.Context,
	httpClient *http.Client,
	run *spfreshBulkRunContext,
) error {
	// The offline build can exceed the shared internal HTTP client's five-minute
	// timeout. Keep transport settings, but let the DDL context own cancellation.
	buildClient := httpClient
	if httpClient != nil {
		client := *httpClient
		client.Timeout = 0
		buildClient = &client
	}
	result, err := remote.BuildSPFreshBulkSortedRuns(
		ctx,
		buildClient,
		run.workerURL,
		remote.SPFreshBulkBuildRequest{
			SourceSnapshotTS: run.sourceSnapshotTS,
			StoredColumns:    run.storedColumns,
			BuildID:          run.buildID,
			KeyspaceID:       run.keyspaceID,
			TableID:          run.tableID,
			IndexID:          run.indexID,
			BaseFragments:    append([]pkdbspfreshbulk.ManifestRef(nil), run.baseFragmentRefs...),
			Dimension:        run.dimension,
			Distance:         run.distance,
			TargetOccupancy:  run.targetOccupancy,
			AlgorithmSeed:    run.algorithmSeed,
			MinPartitionSize: run.minPartitionSize,
			MaxPartitionSize: run.maxPartitionSize,
			OutputPrefix:     run.artifactPrefix,
		},
		remote.SPFreshBulkBuildExpected{
			InputRows:     run.sourceRows,
			InputBytes:    run.baseObjectBytes,
			NonRootRunURI: run.nonRootRunURI,
			RootRunURI:    run.rootRunURI,
		},
	)
	if err != nil {
		return err
	}
	run.leafEntryCount = result.LeafEntryCount
	if result.NonRoot == nil {
		run.nonRootRunURI = ""
		run.nonRootRecordCount = 0
		run.nonRootObjectBytes = 0
		run.nonRootRef = pkdbspfreshbulk.ManifestRef{}
	} else {
		run.nonRootRecordCount = result.NonRoot.RecordCount
		run.nonRootObjectBytes = result.NonRoot.ObjectBytes
		run.nonRootRef = result.NonRoot.Reference
	}
	run.rootRecordCount = result.Root.RecordCount
	run.rootObjectBytes = result.Root.ObjectBytes
	run.rootRef = result.Root.Reference
	run.builtPartitionCount = result.PartitionCount
	run.builtLeafPartitions = result.LeafPartitionCount
	run.builtRootLevel = result.RootLevel
	run.builtMaxPartitionKey = result.MaxPartitionKey
	return nil
}
