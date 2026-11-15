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
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lightningconfig "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	spfreshBulkNonRootRole = "non_root"
	spfreshBulkRootRole    = "root"
)

type spfreshBulkSortedRun struct {
	header       pkdbspfreshbulk.SortedRunHeader
	records      []pkdbspfreshbulk.SortedRunRecord
	encodedBytes uint64
}

// A consumer finishes using the immutable decoded body before returning. The
// caller owns its read lease; consumers may retain only compact evidence.
type spfreshBulkRunConsumer func(context.Context, spfreshBulkSortedRun, pkdbspfreshbulk.ManifestRef) error

// The attempt imports non-root runs while other shards are building. Each
// memory batch borrows authenticated records until all import jobs have drained.
// Root bytes are consumed once after non-root completion and aggregate checks.
type spfreshBulkStaging struct {
	localBackend *local.Backend
	run          *spfreshBulkRunContext
	path         string
	batchBytes   int64
	concurrency  int
	slots        *semaphore.Weighted
	importEngine func(context.Context, *spfreshBulkImportEngine) error
	mu           sync.Mutex
	refs         map[string]pkdbspfreshbulk.ManifestRef
	err          error
	rootRef      pkdbspfreshbulk.ManifestRef
}

// Bound the work handed to each importer without copying the decoded records.
// Whole-object memory remains covered by the existing shared read lease.
const spfreshBulkBatchBytes = int64(256 << 20)

// Import buffers have their own process-wide allowance. An importer holds its
// object's read lease while waiting here; the two disjoint pools never borrow
// from one another. Reserve two batch sizes for Lightning/RPC working memory.
var spfreshBulkImports = spfreshBulkReadLimiter{changed: make(chan struct{})}

func spfreshBulkImportLimits(total uint64, cpus int) (int64, int) {
	batchBytes := int64(min(uint64(spfreshBulkBatchBytes), total/16))
	if batchBytes == 0 {
		return 0, 0
	}
	memorySlots := int(min(uint64(max(cpus, 1)), total/4/uint64(2*batchBytes)))
	return batchBytes, max(1, min(max(1, cpus-cpus/4), memorySlots))
}

func newSPFreshBulkStaging(ctx context.Context, store kv.Storage, job *model.Job, run *spfreshBulkRunContext) (*spfreshBulkStaging, error) {
	storeWithPD, ok := store.(kv.StorageWithPD)
	if !ok {
		return nil, errors.Errorf("SPFresh import requires StorageWithPD, got %T", store)
	}
	// Building(B) already protects B from GC and excludes online index writers.
	// Every batch uses this one T > B, including the root imported after proof.
	ts, err := allocNewTS(ctx, storeWithPD)
	if err != nil {
		return nil, errors.Annotate(err, "allocate SPFresh import timestamp")
	}
	if ts <= run.sourceSnapshotTS {
		return nil, errors.New("SPFresh import timestamp must follow source snapshot")
	}
	run.bulkImportTS = ts

	total := spfreshBulkMemoryLimit()
	cpus, err := cgroup.EffectiveCPUCount()
	if err != nil {
		return nil, errors.Annotate(err, "determine SPFresh import CPU quota")
	}
	batchBytes, concurrency := spfreshBulkImportLimits(total, cpus)
	if batchBytes == 0 {
		return nil, errors.New("SPFresh import requires a known positive memory quota")
	}
	cfg, localBackend, err := ingest.CreateLocalBackend(ctx, store, job, false, false, 0)
	if err != nil {
		return nil, errors.Annotate(err, "create SPFresh Lightning local backend")
	}
	staging := newSPFreshImportPipeline(localBackend, run, cfg.LocalStoreDir, batchBytes, concurrency)
	staging.importEngine = func(ctx context.Context, engine *spfreshBulkImportEngine) error {
		return localBackend.ImportIngestEngine(ctx, engine, int64(lightningconfig.SplitRegionSize), int64(lightningconfig.SplitRegionKeys))
	}
	logutil.DDLLogger().Info("SPFresh bounded import pipeline", zap.String("build_id", run.buildID), zap.String("source", "cpu_memory"), zap.Int("effective_cpus", cpus), zap.Uint64("memory_quota_bytes", total), zap.Int64("batch_budget_bytes", staging.batchBytes), zap.Int("batch_concurrency", concurrency), zap.Uint64("object_budget_bytes", total/2), zap.Uint64("import_budget_bytes", total/4), zap.Uint64("bulk_import_ts", ts))
	return staging, nil
}

func newSPFreshImportPipeline(localBackend *local.Backend, run *spfreshBulkRunContext, path string, batchBytes int64, concurrency int) *spfreshBulkStaging {
	return &spfreshBulkStaging{
		localBackend: localBackend, run: run, path: path, batchBytes: batchBytes, concurrency: concurrency,
		slots: semaphore.NewWeighted(int64(concurrency)), refs: make(map[string]pkdbspfreshbulk.ManifestRef),
	}
}

func (staging *spfreshBulkStaging) append(ctx context.Context, decoded spfreshBulkSortedRun, ref pkdbspfreshbulk.ManifestRef) error {
	role := decoded.header.RunRole
	if role != spfreshBulkRootRole && role != spfreshBulkNonRootRole {
		return errors.New("SPFresh import requires non-root or root run")
	}
	staging.mu.Lock()
	if staging.err != nil {
		err := staging.err
		staging.mu.Unlock()
		return err
	}
	if _, duplicate := staging.refs[ref.URI]; duplicate {
		staging.mu.Unlock()
		return errors.Errorf("SPFresh run %q was imported twice", ref.URI)
	}
	if role == spfreshBulkRootRole {
		if staging.rootRef.URI != "" {
			staging.mu.Unlock()
			return errors.New("SPFresh import requires exactly one root run")
		}
		staging.rootRef = ref
	}
	staging.refs[ref.URI] = ref
	staging.mu.Unlock()
	err := staging.importRun(ctx, decoded, ref)
	if err != nil {
		staging.mu.Lock()
		if staging.err == nil {
			staging.err = err
		}
		staging.mu.Unlock()
	}
	return err
}

func (staging *spfreshBulkStaging) importRun(ctx context.Context, decoded spfreshBulkSortedRun, ref pkdbspfreshbulk.ManifestRef) error {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(workCtx)
	// Also overlap batches within a single admitted object. Goroutines
	// borrow its records only until Wait, still inside the caller's read lease.
	group.SetLimit(staging.concurrency)
	var splitErr error
	for start := 0; start < len(decoded.records) && groupCtx.Err() == nil; {
		end, weight := start, int64(0)
		for end < len(decoded.records) {
			size := int64(len(decoded.records[end].Key)) + int64(len(decoded.records[end].Value))
			if size > staging.batchBytes {
				splitErr = errors.New("SPFresh record exceeds import batch budget")
				break
			}
			if size > staging.batchBytes-weight {
				break
			}
			weight += size
			end++
		}
		if splitErr != nil {
			cancel()
			break
		}
		batch := spfreshBulkSortedRun{header: decoded.header, records: decoded.records[start:end], encodedBytes: uint64(weight)}
		batch.header.RecordCount = uint64(end - start)
		waitStarted := time.Now()
		group.Go(func() error { return staging.importBatch(groupCtx, batch, ref) })
		metrics.SPFreshBulkImportDuration.WithLabelValues("run_slot_wait").Observe(time.Since(waitStarted).Seconds())
		start = end
	}
	// Even a producer error must join all remote imports before releasing input
	// memory or allowing DDL teardown/delete-range to start.
	err := group.Wait()
	if splitErr != nil {
		return splitErr
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (staging *spfreshBulkStaging) importBatch(ctx context.Context, batch spfreshBulkSortedRun, ref pkdbspfreshbulk.ManifestRef) (err error) {
	waitStarted := time.Now()
	err = staging.slots.Acquire(ctx, 1)
	metrics.SPFreshBulkImportDuration.WithLabelValues("slot_wait").Observe(time.Since(waitStarted).Seconds())
	if err != nil {
		return err
	}
	defer staging.slots.Release(1)
	waitStarted = time.Now()
	release, err := spfreshBulkImports.acquire(ctx, uint64(2*staging.batchBytes), func() uint64 { return spfreshBulkMemoryLimit() / 4 })
	metrics.SPFreshBulkImportDuration.WithLabelValues("memory_wait").Observe(time.Since(waitStarted).Seconds())
	if err != nil {
		return err
	}
	defer release()
	metrics.SPFreshBulkImportInFlight.Inc()
	defer metrics.SPFreshBulkImportInFlight.Dec()
	staging.mu.Lock()
	err = staging.err
	staging.mu.Unlock()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			staging.mu.Lock()
			if staging.err == nil {
				staging.err = err
			}
			staging.mu.Unlock()
		}
	}()
	engine := newSPFreshBulkImportEngine(batch, ref.URI, staging.run.bulkImportTS)
	defer engine.Close()
	started := time.Now()
	err = staging.importEngine(ctx, engine)
	metrics.SPFreshBulkImportDuration.WithLabelValues("execute").Observe(time.Since(started).Seconds())
	if err != nil {
		return errors.Annotatef(err, "import SPFresh %s SortedRun batch %q", engine.role, ref.URI)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	size, count := engine.ImportedStatistics()
	if size != engine.totalBytes || count != int64(len(batch.records)) {
		return errors.Errorf("SPFresh import incomplete: got %d KVs/%d bytes, expected %d KVs/%d bytes", count, size, len(batch.records), engine.totalBytes)
	}
	logSPFreshBulkImportPhase(staging.run, engine.role, "import_memory", started, ref.URI, 1, uint64(count), uint64(size))
	failpoint.Inject("spfreshBulkAfterBatchImport", func() { failpoint.Return(errors.New("injected failure after SPFresh batch ingest")) })
	return nil
}

// The orchestrator calls close only after every build/proof/import worker joins.
// Owner loss never resumes a partial attempt; the non-public index rolls back.
func (staging *spfreshBulkStaging) close() {
	staging.localBackend.Close()
	// The backend creates an empty attempt directory; no local engine/SST is opened.
	if staging.path != "" {
		if err := os.RemoveAll(staging.path); err != nil {
			logutil.DDLLogger().Warn("remove SPFresh attempt temporary directory", zap.String("path", staging.path), zap.Error(err))
		}
	}
}

// Every run, including the root, was authenticated and consumed once. Only
// successful completion of those imports may advance to Ready.
func importSPFreshFinalManifestRuns(_ context.Context, run *spfreshBulkRunContext) error {
	if run == nil || run.finalManifest == nil || run.finalManifestRef.URI == "" || run.staging == nil {
		return errors.New("SPFresh distributed import requires an accepted FinalManifest")
	}
	if err := run.staging.validateManifest(*run.finalManifest); err != nil {
		return err
	}
	return finishSPFreshBulkImport(run)
}

func finishSPFreshBulkImport(run *spfreshBulkRunContext) error {
	if run.staging.err != nil {
		return run.staging.err
	}
	if run.bulkImportTS <= run.sourceSnapshotTS || run.staging.rootRef.URI == "" {
		return errors.New("SPFresh import lacks timestamp or completed root")
	}
	run.importCompleted = true
	return nil
}

func (staging *spfreshBulkStaging) validateManifest(manifest pkdbspfreshbulk.FinalManifest) error {
	expectedCount := len(manifest.ShardRuns) + 1
	if manifest.MergeNonRoot != nil {
		expectedCount++
	}
	if len(manifest.ShardRuns) == 0 || expectedCount != len(staging.refs) {
		return errors.New("SPFresh staged runs do not match FinalManifest")
	}
	matches := func(ref pkdbspfreshbulk.ManifestRef) error {
		if staging.refs[ref.URI] != ref {
			return errors.Errorf("SPFresh staged run %q does not match FinalManifest", ref.URI)
		}
		return nil
	}
	for _, shard := range manifest.ShardRuns {
		if err := matches(shard.NonRoot); err != nil {
			return err
		}
	}
	if manifest.MergeNonRoot != nil {
		if err := matches(*manifest.MergeNonRoot); err != nil {
			return err
		}
	}
	return matches(manifest.Root)
}

func importSPFreshSortedRuns(ctx context.Context, store kv.Storage, job *model.Job, run *spfreshBulkRunContext) error {
	if run == nil || run.shardCount != 1 || run.staging != nil {
		return errors.New("SPFresh single-tree import requires an unstaged single-shard run")
	}
	objectBackend, err := storage.ParseBackend(run.cloudStorageURI, nil)
	if err != nil {
		return errors.Annotate(err, "parse SPFresh single-tree storage")
	}
	objectStore, err := storage.NewWithDefaultOpt(ctx, objectBackend)
	if err != nil {
		return err
	}
	defer objectStore.Close()
	run.staging, err = newSPFreshBulkStaging(ctx, store, job, run)
	if err != nil {
		return err
	}
	// The orchestrator owns cleanup on every exit, for both single and distributed builds.
	if err := proveAndStageSPFreshSingleRuns(ctx, objectStore, run, run.staging.append); err != nil {
		return err
	}
	return finishSPFreshBulkImport(run)
}

func logSPFreshBulkImportPhase(
	run *spfreshBulkRunContext,
	role string,
	phase string,
	started time.Time,
	artifactURI string,
	artifactCount int,
	recordCount uint64,
	encodedBytes uint64,
) {
	logutil.DDLLogger().Info("SPFresh MVP bulk import phase completed",
		zap.String("path", "bulk"),
		zap.String("build_id", run.buildID),
		zap.String("run_id", run.runID),
		zap.String("role", role),
		zap.String("phase", phase),
		zap.Int64("duration_ms", time.Since(started).Milliseconds()),
		zap.String("artifact_uri", artifactURI),
		zap.Int("artifact_count", artifactCount),
		zap.Uint64("record_count", recordCount),
		zap.Uint64("bytes", encodedBytes),
		zap.Uint64("bulk_import_ts", run.bulkImportTS),
	)
}

func spfreshBulkRunStats(run *spfreshBulkRunContext, role string) (recordCount uint64, objectBytes uint64, err error) {
	switch role {
	case spfreshBulkNonRootRole:
		return run.nonRootRecordCount, run.nonRootObjectBytes, nil
	case spfreshBulkRootRole:
		return run.rootRecordCount, run.rootObjectBytes, nil
	default:
		return 0, 0, errors.Errorf("unknown SPFresh MVP SortedRun role %q", role)
	}
}
