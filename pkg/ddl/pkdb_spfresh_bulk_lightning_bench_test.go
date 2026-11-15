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
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
)

// TestSPFreshLightningReplay imports deterministic incompressible 1 KiB values
// through the production batch scheduler and real Lightning MultiIngest. The
// caller must allocate a fresh, empty scratch table and drop it afterwards.
// Its explicit ID scopes every write. This isolates import, not object reading,
// logical SPFresh proof or publication. All records and timestamp visibility are
// checked outside the timed interval. Run one test per process.
func TestSPFreshLightningReplay(t *testing.T) {
	path := os.Getenv("SPFRESH_IMPORT_TIKV_PATH")
	if path == "" {
		t.Skip("explicit scratch table and real TiKV path required")
	}
	integer := func(name string) int64 {
		v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
		require.NoError(t, err, name)
		require.Positive(t, v, name)
		return v
	}
	tableID := integer("SPFRESH_IMPORT_SCRATCH_TABLE")
	rows := integer("SPFRESH_IMPORT_ROWS")
	total := uint64(integer("SPFRESH_REPLAY_MEMORY"))
	previous := memory.ServerMemoryLimit.Swap(total)
	defer memory.ServerMemoryLimit.Store(previous)
	ctx := context.Background()
	store, err := openSPFreshBulkRealTiKVStore(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	originalConfig := config.GetGlobalConfig()
	cfg := *originalConfig
	cfg.Store, cfg.TempDir = "tikv", t.TempDir()
	_, _, cfg.KeyspaceName, err = tikvconfig.ParsePath(path)
	require.NoError(t, err)
	config.StoreGlobalConfig(&cfg)
	defer config.StoreGlobalConfig(originalConfig)
	memRoot, diskRoot, initialized := ingest.LitMemRoot, ingest.LitDiskRoot, ingest.LitInitialized
	defer func() { ingest.LitMemRoot, ingest.LitDiskRoot, ingest.LitInitialized = memRoot, diskRoot, initialized }()
	require.True(t, ingest.InitGlobalLightningEnv(t.TempDir()))
	version := currentSPFreshBulkTestVersion(t, store)
	run := newSPFreshBulkScanTestRun(1, version, "lightning-replay")
	job := newSPFreshBulkTestJob()
	job.ID, job.TableID, job.RealStartTS = tableID, tableID, version
	job.Type, job.ReorgMeta.UseCloudStorage = model.ActionAddIndex, true
	job.ReorgMeta.SetConcurrency(4)
	staging, err := newSPFreshBulkStaging(ctx, store, job, run)
	require.NoError(t, err)
	defer staging.close()
	prefix := tablecodec.EncodeTableIndexPrefix(tableID, 1)
	// Reserve the entire immutable fixture, including slice/key overhead, before
	// allocating it. Import working memory uses the separate production pool.
	release, err := spfreshBulkReads.acquire(ctx, uint64(rows)*1152, spfreshBulkReadMemoryBudget)
	require.NoError(t, err)
	defer release()
	values := make([]byte, int(rows)*1024)
	rng := rand.New(rand.NewPCG(1, 2))
	for offset := 0; offset < len(values); offset += 8 {
		binary.LittleEndian.PutUint64(values[offset:], rng.Uint64())
	}
	records := make([]pkdbspfreshbulk.SortedRunRecord, int(rows))
	logicalKey := func(i int) kv.Key {
		return binary.BigEndian.AppendUint64(bytes.Clone(prefix), uint64(i))
	}
	for i := range records {
		records[i] = pkdbspfreshbulk.SortedRunRecord{Key: store.GetCodec().EncodeKey(logicalKey(i)), Value: values[i*1024 : (i+1)*1024]}
	}
	assertAbsent := func(ts uint64) {
		iter, err := store.GetSnapshot(kv.NewVersion(ts)).Iter(prefix, prefix.PrefixNext())
		require.NoError(t, err)
		defer iter.Close()
		require.False(t, iter.Valid(), "scratch prefix must be empty before import timestamp")
	}
	assertAbsent(run.bulkImportTS - 1)
	runtime.GC()
	fmt.Printf("SPFRESH_BENCH_START stage=lightning rows=%d memory=%d slots=%d batch=%d cpus=%d table=%d\n", rows, total, staging.concurrency, staging.batchBytes, runtime.GOMAXPROCS(0), tableID)
	started := time.Now()
	err = staging.importRun(ctx, spfreshBulkSortedRun{
		header: pkdbspfreshbulk.SortedRunHeader{RunRole: spfreshBulkNonRootRole}, records: records,
	}, pkdbspfreshbulk.ManifestRef{URI: "offline-import"})
	fmt.Printf("SPFRESH_BENCH_END stage=lightning elapsed_s=%.6f\n", time.Since(started).Seconds())
	require.NoError(t, err)
	assertAbsent(run.bulkImportTS - 1)
	iter, err := store.GetSnapshot(kv.NewVersion(run.bulkImportTS)).Iter(prefix, prefix.PrefixNext())
	require.NoError(t, err)
	defer iter.Close()
	count := 0
	for ; iter.Valid(); require.NoError(t, iter.Next()) {
		require.Less(t, count, len(records))
		require.Equal(t, logicalKey(count), iter.Key())
		require.Equal(t, records[count].Value, iter.Value())
		count++
	}
	require.Equal(t, len(records), count)
	require.Zero(t, spfreshBulkImports.used)
	fmt.Printf("SPFRESH_BENCH_VALIDATED records=%d timestamp=%d prior_snapshot_empty=true\n", count, run.bulkImportTS)
}
