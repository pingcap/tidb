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
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestSPFreshProofImportReplay is an explicitly enabled offline resource probe.
// It replays one pinned fixture as independent reads, decodes it and consumes
// the production import iterators with a checksum sink. It performs no KV RPC,
// logical tree proof, publication or index write; it cannot measure Lightning
// throughput. Preparation and fixture validation are outside the timed region.
func TestSPFreshProofImportReplay(t *testing.T) {
	path := os.Getenv("SPFRESH_PROOF_REPLAY")
	if path == "" {
		t.Skip("explicit offline fixture required")
	}
	var fixture struct {
		URI       string                      `json:"uri"`
		Prefix    string                      `json:"prefix"`
		Reference pkdbspfreshbulk.ManifestRef `json:"reference"`
		Records   uint64                      `json:"records"`
		Checksum  uint64                      `json:"checksum"`
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	total, err := strconv.ParseUint(os.Getenv("SPFRESH_REPLAY_MEMORY"), 10, 64)
	require.NoError(t, err)
	copies, err := strconv.Atoi(os.Getenv("SPFRESH_REPLAY_OBJECTS"))
	require.NoError(t, err)
	require.Positive(t, copies)
	previous := memory.ServerMemoryLimit.Swap(total)
	defer memory.ServerMemoryLimit.Store(previous)
	backend, err := storage.ParseBackend(fixture.URI, nil)
	require.NoError(t, err)
	store, err := storage.NewWithDefaultOpt(context.Background(), backend)
	require.NoError(t, err)
	defer store.Close()
	run := &spfreshBulkRunContext{buildID: "offline-replay", sourceSnapshotTS: 1, bulkImportTS: 2}
	cpus, err := cgroup.EffectiveCPUCount()
	require.NoError(t, err)
	batchBytes, concurrency := spfreshBulkImportLimits(total, cpus)
	staging := newSPFreshImportPipeline(nil, run, "", batchBytes, concurrency)
	var consumed, checksum atomic.Uint64
	staging.importEngine = func(ctx context.Context, e *spfreshBulkImportEngine) error {
		iter := e.NewIter(ctx, nil, nil, nil)
		defer iter.Close()
		var bytes, rows int64
		var sum uint64
		hash := sha256.New()
		var digest [sha256.Size]byte
		for iter.First(); iter.Valid(); iter.Next() {
			hash.Reset()
			_, _ = hash.Write(iter.Key())
			_, _ = hash.Write(iter.Value())
			hash.Sum(digest[:0])
			sum += binary.LittleEndian.Uint64(digest[:8])
			bytes += int64(len(iter.Key()) + len(iter.Value()))
			rows++
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		e.Finish(bytes, rows)
		consumed.Add(uint64(rows))
		checksum.Add(sum)
		return nil
	}
	runtime.GC()
	fmt.Printf("SPFRESH_BENCH_START stage=proof objects=%d memory=%d object_budget=%d slots=%d batch=%d cpus=%d\n", copies, total, spfreshBulkReadMemoryBudget(), concurrency, batchBytes, cpus)
	started := time.Now()
	group, ctx := errgroup.WithContext(context.Background())
	group.SetLimit(min(copies, cpus))
	for range copies {
		group.Go(func() error {
			encoded, release, err := readSPFreshBudgetedPinnedSortedRun(ctx, store, fixture.Prefix, fixture.Reference)
			if err != nil {
				return err
			}
			defer release()
			header, records, err := pkdbspfreshbulk.DecodeSortedRun(encoded)
			if err != nil {
				return err
			}
			return staging.importRun(ctx, spfreshBulkSortedRun{header: header, records: records, encodedBytes: uint64(len(encoded))}, fixture.Reference)
		})
	}
	require.NoError(t, group.Wait())
	fmt.Printf("SPFRESH_BENCH_END stage=proof elapsed_s=%.6f\n", time.Since(started).Seconds())
	require.Equal(t, fixture.Records*uint64(copies), consumed.Load())
	require.Equal(t, fixture.Checksum*uint64(copies), checksum.Load())
	require.Zero(t, spfreshBulkReads.used)
	require.Zero(t, spfreshBulkImports.used)
	fmt.Printf("SPFRESH_BENCH_VALIDATED records=%d checksum=%d\n", consumed.Load(), checksum.Load())
}
