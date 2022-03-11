// Copyright 2021 PingCAP, Inc.
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

package distsql

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/util/benchdaily"
	"github.com/pingcap/tidb/util/chunk"
)

func BenchmarkSelectResponseChunk_BigResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sctx := newMockSessionContext()
		sctx.GetSessionVars().InitChunkSize = 32
		sctx.GetSessionVars().MaxChunkSize = 1024
		selectResult, colTypes := createSelectNormalByBenchmarkTest(4000, 20000, sctx)
		chk := chunk.NewChunkWithCapacity(colTypes, 1024)
		b.StartTimer()
		for {
			err := selectResult.Next(context.TODO(), chk)
			if err != nil {
				panic(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
	}
}

func BenchmarkSelectResponseChunk_SmallResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sctx := newMockSessionContext()
		sctx.GetSessionVars().InitChunkSize = 32
		sctx.GetSessionVars().MaxChunkSize = 1024
		selectResult, colTypes := createSelectNormalByBenchmarkTest(32, 3200, sctx)
		chk := chunk.NewChunkWithCapacity(colTypes, 1024)
		b.StartTimer()
		for {
			err := selectResult.Next(context.TODO(), chk)
			if err != nil {
				panic(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
	}
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkSelectResponseChunk_BigResponse,
		BenchmarkSelectResponseChunk_SmallResponse,
	)
}
