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
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

// TestSPFreshScanUploadReplay measures the production bounded scan/decode/hash/
// encode/PUT pipeline over a preloaded mock snapshot and an explicit object store.
// Fixture loading and output SHA/fingerprint verification are outside the timed
// interval. It does not measure remote TiKV source supply or issue index DDL.
func TestSPFreshScanUploadReplay(t *testing.T) {
	uri := os.Getenv("SPFRESH_SCAN_REPLAY_URI")
	if uri == "" {
		t.Skip("explicit offline fixture and output prefix required")
	}
	rows, err := strconv.Atoi(os.Getenv("SPFRESH_REPLAY_ROWS"))
	require.NoError(t, err)
	require.Positive(t, rows)
	total, err := strconv.ParseUint(os.Getenv("SPFRESH_REPLAY_MEMORY"), 10, 64)
	require.NoError(t, err)
	previous := memory.ServerMemoryLimit.Swap(total)
	defer memory.ServerMemoryLimit.Store(previous)
	workers := variable.GetDDLReorgWorkerCounter()
	variable.SetDDLReorgWorkerCounter(4)
	defer variable.SetDDLReorgWorkerCounter(workers)
	runID, prefix, err := parseSPFreshBulkStorageURI(uri)
	require.NoError(t, err)
	backend, err := storage.ParseBackend(uri, nil)
	require.NoError(t, err)
	objects, err := storage.NewWithDefaultOpt(context.Background(), backend)
	require.NoError(t, err)
	defer objects.Close()
	input, err := os.Open(os.Getenv("SPFRESH_SCAN_REPLAY_VECTORS"))
	require.NoError(t, err)
	defer input.Close()
	procs := runtime.GOMAXPROCS(0)
	store := newSPFreshBulkMockStore(t)
	// Mock-store setup caps tests at 16; preserve this probe's explicit budget.
	runtime.GOMAXPROCS(procs)
	tbl, index := newSPFreshBulkScanTestTable(t)
	var expected pkdbspfreshbulk.FingerprintAccumulator
	raw := make([]byte, 768*4)
	for start := 0; start < rows; start += 1024 {
		batch := make(map[int64][]float32, min(1024, rows-start))
		for row := start; row < min(rows, start+1024); row++ {
			_, err := io.ReadFull(input, raw)
			require.NoError(t, err)
			vector := make([]float32, 768)
			for d := range vector {
				vector[d] = math.Float32frombits(binary.LittleEndian.Uint32(raw[d*4:]))
			}
			batch[int64(row)] = vector
			require.NoError(t, expected.AddRecord(int64(row), vector))
		}
		writeSPFreshBulkTestRows(t, store, tbl.Meta(), batch)
	}
	run := newSPFreshBulkScanTestRun(index.ID, currentSPFreshBulkTestVersion(t, store), runID)
	run.artifactPrefix = prefix
	runtime.GC()
	fmt.Printf("SPFRESH_BENCH_START stage=scan rows=%d cpus=%d memory=%d\n", rows, runtime.GOMAXPROCS(0), total)
	started := time.Now()
	require.NoError(t, scanAndUploadSPFreshBaseFragmentsToStore(context.Background(), exprstatic.NewExprContext(), NewReorgContext(), store, tbl, run, objects, testSPFreshBulkScanLayout(t, 4), productionSPFreshBulkScanConfig))
	fmt.Printf("SPFRESH_BENCH_END stage=scan elapsed_s=%.6f\n", time.Since(started).Seconds())
	require.Equal(t, uint64(rows), run.sourceRows)
	require.Equal(t, expected.Fingerprint(), run.sourceFingerprint)
	var output pkdbspfreshbulk.FingerprintAccumulator
	for _, ref := range run.baseFragmentRefs {
		key, err := exactSPFreshBulkObjectKey(prefix, ref.URI)
		require.NoError(t, err)
		bytes, err := pkdbspfreshbulk.ReadObject(context.Background(), objects, key)
		require.NoError(t, err)
		digest := sha256.Sum256(bytes)
		require.Equal(t, ref.SHA256, hex.EncodeToString(digest[:]))
		_, records, err := pkdbspfreshbulk.DecodeBaseFragment(bytes)
		require.NoError(t, err)
		for _, record := range records {
			require.NoError(t, output.AddBaseRecord(record))
		}
	}
	require.Equal(t, expected.Fingerprint(), output.Fingerprint())
	fmt.Printf("SPFRESH_BENCH_VALIDATED rows=%d objects=%d fingerprint=%+v\n", rows, len(run.baseFragmentRefs), output.Fingerprint())
}
