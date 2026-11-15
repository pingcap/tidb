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
	"fmt"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

// BenchmarkSPFreshFragmentBoundaries measures the real scan/encode pipeline on
// a fixed snapshot crossing four full vector arenas. The mock source and object
// store remove remote IO; B/op includes the complete pipeline, not just vectors.
func BenchmarkSPFreshFragmentBoundaries(b *testing.B) {
	store := newSPFreshBulkMockStore(b)
	tbl, index := newSPFreshBulkScanTestTable(b)
	const rows = 4100
	values := make(map[int64][]float32, rows)
	for i := int64(0); i < rows; i++ {
		values[i] = testSPFreshBulkVector(i, uint32(i)+0x3f800000)
	}
	writeSPFreshBulkTestRows(b, store, tbl.Meta(), values)
	snapshot := currentSPFreshBulkTestVersion(b, store)
	ctx := context.Background()
	exprCtx := exprstatic.NewExprContext()
	config := spfreshBulkScanConfig{fragmentTargetBytes: 1024 * spfreshBulkBaseRecordEncodedBytes}
	previousWorkers := variable.GetDDLReorgWorkerCounter()
	previousProcs := runtime.GOMAXPROCS(4)
	b.Cleanup(func() { variable.SetDDLReorgWorkerCounter(previousWorkers); runtime.GOMAXPROCS(previousProcs) })
	for _, consumers := range []int32{1, 4} {
		b.Run(fmt.Sprintf("consumers-%d", consumers), func(b *testing.B) {
			variable.SetDDLReorgWorkerCounter(consumers)
			b.ReportAllocs()
			b.SetBytes(rows * 768 * 4)
			b.ResetTimer()
			for range b.N {
				run := newSPFreshBulkScanTestRun(index.ID, snapshot, "fragment-boundaries")
				objectStore := storage.NewMemStorage()
				require.NoError(b, scanAndUploadSPFreshBaseFragmentsToStore(ctx, exprCtx, NewReorgContext(),
					store, tbl, run, objectStore, testSPFreshBulkScanLayout(b, 4), config))
				require.Equal(b, uint64(rows), run.sourceRows)
				require.NotEmpty(b, run.baseFragmentURIs)
				objectStore.Close()
			}
		})
	}
}

// BenchmarkSPFreshSampleStorage isolates sample selection and vector storage.
// Reusing an input vector also exercises ownership during slot replacement.
func BenchmarkSPFreshSampleStorage(b *testing.B) {
	for _, dimension := range []int{768, 960, 1024} {
		for _, limit := range []int{256, 4096, 16384} {
			b.Run(fmt.Sprintf("d%d/k%d", dimension, limit), func(b *testing.B) {
				vector := make([]float32, dimension)
				var digest [sha256.Size]byte
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					sample, err := newSPFreshBulkBottomKSample(limit, dimension)
					require.NoError(b, err)
					for handle := 0; handle < 2*limit; handle++ {
						vector[0] = float32(handle)
						sample.AddWithDigest(pkdbspfreshbulk.BaseFragmentRecord{Handle: int64(handle), Vector: vector}, digest)
					}
					require.Len(b, sample.heap, limit)
				}
			})
		}
	}
}
