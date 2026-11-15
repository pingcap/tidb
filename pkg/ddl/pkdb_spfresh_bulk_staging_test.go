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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// Consume the production memory iterators without PD; the import callback owns
// no buffer and returns only after all simulated writes/ingests finish.
func withSPFreshStagingEngines(t testing.TB, run *spfreshBulkRunContext, consume func(*spfreshBulkStaging)) {
	t.Helper()
	run.bulkImportTS = run.sourceSnapshotTS + 1
	staging := newSPFreshImportPipeline(nil, run, "", spfreshBulkBatchBytes, 2)
	var mu sync.Mutex
	var engines []*spfreshBulkImportEngine
	staging.importEngine = func(ctx context.Context, engine *spfreshBulkImportEngine) error {
		require.Equal(t, run.sourceSnapshotTS+1, engine.GetTS())
		mu.Lock()
		engines = append(engines, engine)
		mu.Unlock()
		iter := engine.NewIter(ctx, nil, nil, nil)
		defer iter.Close()
		var size, count int64
		for iter.First(); iter.Valid(); iter.Next() {
			size += int64(len(iter.Key()) + len(iter.Value()))
			count++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		engine.Finish(size, count)
		return nil
	}
	consume(staging)
	for _, engine := range engines {
		require.Zero(t, engine.refs.Load(), "import references leaked")
		require.Nil(t, engine.records, "completed import retained input views")
		require.Nil(t, engine.splitKeys)
	}
	require.True(t, staging.slots.TryAcquire(int64(staging.concurrency)), "batch slots leaked")
	staging.slots.Release(int64(staging.concurrency))
}

type countingSPFreshRunStore struct {
	storage.ExternalStorage
	mu    sync.Mutex
	reads map[string]int
}

func (s *countingSPFreshRunStore) Open(ctx context.Context, key string, options *storage.ReaderOption) (storage.ExternalFileReader, error) {
	if strings.Contains(key, "/input/") || key == "final.json" {
		return nil, fmt.Errorf("unexpected full input or manifest reread: %s", key)
	}
	if strings.HasSuffix(key, "non-root.spfmvp") || key == "merge/root.spfmvp" {
		s.mu.Lock()
		s.reads[key]++
		s.mu.Unlock()
	}
	return s.ExternalStorage.Open(ctx, key, options)
}

func TestSPFreshBulkProofStagesEachSortedRunOnce(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	counted := &countingSPFreshRunStore{ExternalStorage: store, reads: make(map[string]int)}
	withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
		require.NoError(t, proveAndStageSPFreshTestBuild(context.Background(), counted, run, staging.append))
		require.NoError(t, staging.validateManifest(*run.finalManifest))
		require.Len(t, staging.refs, 6)
		require.Equal(t, run.finalManifest.Root, staging.rootRef)
		require.False(t, run.importCompleted) // Completion is recorded after the import barrier.
		for _, count := range counted.reads {
			require.Equal(t, 1, count)
		}
		require.Len(t, counted.reads, 6)
		ref := run.finalManifest.Root
		delete(staging.refs, ref.URI)
		require.ErrorContains(t, staging.validateManifest(*run.finalManifest), "do not match")
		ref.SHA256 = strings.Repeat("0", 64)
		staging.refs[ref.URI] = ref
		require.ErrorContains(t, staging.validateManifest(*run.finalManifest), "does not match")
	})
}

func TestSPFreshBulkPipelineImportsBeforeFinalProofAndRootLast(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	counted := &countingSPFreshRunStore{ExternalStorage: store, reads: make(map[string]int)}
	withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
		run.staging = staging
		staging.batchBytes = 16 << 10
		var imported, roots atomic.Int32
		baseImport := staging.importEngine
		staging.importEngine = func(ctx context.Context, engine *spfreshBulkImportEngine) error {
			require.Equal(t, run.sourceSnapshotTS+1, engine.GetTS())
			if engine.role == spfreshBulkRootRole {
				require.NotNil(t, run.finalManifest)
				roots.Add(1)
			} else {
				require.Nil(t, run.finalManifest, "non-root ingest must overlap global construction/proof")
				require.Zero(t, roots.Load())
			}
			imported.Add(1)
			return baseImport(ctx, engine)
		}
		require.NoError(t, proveAndStageSPFreshTestBuild(context.Background(), counted, run, staging.append))
		require.Greater(t, imported.Load(), int32(5), "large runs must split across byte-bounded engines")
		require.Positive(t, roots.Load())
		require.False(t, run.importCompleted)
		require.NoError(t, staging.validateManifest(*run.finalManifest))
		require.NoError(t, finishSPFreshBulkImport(run))
		require.True(t, run.importCompleted)
		require.Positive(t, roots.Load())
		for _, reads := range counted.reads {
			require.Equal(t, 1, reads)
		}
	})
}

func TestSPFreshBulkPipelinePartialImportFailureReleasesMemoryAndBlocksReady(t *testing.T) {
	store, run := newSPFreshBulkFinalManifestFixture(t)
	withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
		run.staging = staging
		staging.batchBytes = 16 << 10
		var imported atomic.Int32
		baseImport := staging.importEngine
		staging.importEngine = func(ctx context.Context, engine *spfreshBulkImportEngine) error {
			require.Equal(t, spfreshBulkNonRootRole, engine.role)
			if err := baseImport(ctx, engine); err != nil {
				return err
			}
			// The first batch succeeded. A later batch can also have partially reached
			// TiKV before reporting failure: neither outcome is resumable or publishable.
			if imported.Add(1) >= 2 {
				return fmt.Errorf("injected partial remote ingest")
			}
			return nil
		}
		err := proveAndStageSPFreshTestBuild(context.Background(), store, run, staging.append)
		require.ErrorContains(t, err, "injected partial remote ingest")
		require.GreaterOrEqual(t, imported.Load(), int32(2))
		require.Nil(t, run.finalManifest)
		require.False(t, run.importCompleted)
		require.Error(t, run.checkReadyForPublic())
		require.Error(t, finishSPFreshBulkImport(run))
	})
}

func TestSPFreshBulkPipelineCancellationReleasesBlockedAdmission(t *testing.T) {
	_, run := newSPFreshBulkFinalManifestFixture(t)
	withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
		require.NoError(t, staging.slots.Acquire(context.Background(), 2))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- staging.importBatch(ctx, spfreshBulkSortedRun{}, pkdbspfreshbulk.ManifestRef{}) }()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		staging.slots.Release(int64(staging.concurrency))
	})
}

func TestSPFreshBulkPipelineBoundsConcurrentImportsUntilIngestFinishes(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			_, run := newSPFreshBulkFinalManifestFixture(t)
			withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
				staging.concurrency = width
				staging.batchBytes = 1 << 20
				staging.slots = semaphore.NewWeighted(int64(width))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				entered := make(chan struct{}, width+1)
				baseImport := staging.importEngine
				staging.importEngine = func(ctx context.Context, engine *spfreshBulkImportEngine) error {
					if err := baseImport(ctx, engine); err != nil {
						return err
					}
					entered <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				}
				batch := spfreshBulkSortedRun{
					header:  pkdbspfreshbulk.SortedRunHeader{RunRole: spfreshBulkNonRootRole, CommonHeader: pkdbspfreshbulk.CommonHeader{RecordCount: 1}},
					records: []pkdbspfreshbulk.SortedRunRecord{{Key: []byte("key"), Value: []byte("value")}},
				}
				done := make(chan error, width+1)
				for range width {
					go func() { done <- staging.importBatch(ctx, batch, pkdbspfreshbulk.ManifestRef{}) }()
				}
				for range width {
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("imports did not overlap")
					}
				}
				require.False(t, staging.slots.TryAcquire(1), "ingest must retain its slot")
				go func() { done <- staging.importBatch(ctx, batch, pkdbspfreshbulk.ManifestRef{}) }()
				cancel()
				for range width + 1 {
					require.ErrorIs(t, <-done, context.Canceled)
				}
			})
		})
	}
}

func TestSPFreshBulkPipelineRootFailureBlocksCompletion(t *testing.T) {
	for _, failBeforeIngest := range []bool{false, true} {
		t.Run(fmt.Sprint(failBeforeIngest), func(t *testing.T) {
			store, run := newSPFreshBulkFinalManifestFixture(t)
			withSPFreshStagingEngines(t, run, func(staging *spfreshBulkStaging) {
				run.staging = staging
				baseImport := staging.importEngine
				staging.importEngine = func(ctx context.Context, engine *spfreshBulkImportEngine) error {
					if err := baseImport(ctx, engine); err != nil {
						return err
					}
					if engine.role == spfreshBulkRootRole {
						return fmt.Errorf("injected partial root ingest")
					}
					return nil
				}
				consume := staging.append
				if failBeforeIngest {
					consume = func(ctx context.Context, decoded spfreshBulkSortedRun, ref pkdbspfreshbulk.ManifestRef) error {
						if decoded.header.RunRole == spfreshBulkRootRole {
							canceled, cancel := context.WithCancel(ctx)
							cancel()
							ctx = canceled
						}
						return staging.append(ctx, decoded, ref)
					}
				}
				require.Error(t, proveAndStageSPFreshTestBuild(context.Background(), store, run, consume))
				require.Error(t, finishSPFreshBulkImport(run))
				require.False(t, run.importCompleted)
				require.Error(t, run.checkReadyForPublic())
			})
		})
	}
}
