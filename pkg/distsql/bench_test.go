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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type fixedFetchResponse struct {
	subsets []kv.ResultSubset
	index   int
}

func (r *fixedFetchResponse) Next(context.Context) (kv.ResultSubset, error) {
	if r.index == len(r.subsets) {
		return nil, nil
	}
	subset := r.subsets[r.index]
	r.index++
	return subset, nil
}

func (*fixedFetchResponse) Close() error { return nil }

func BenchmarkSelectResultFetchRespReuse(b *testing.B) {
	data, err := (&tipb.SelectResponse{
		Chunks: []tipb.Chunk{{RowsData: []byte{1, 2, 3, 4}}},
	}).Marshal()
	if err != nil {
		b.Fatal(err)
	}
	response := &fixedFetchResponse{
		subsets: []kv.ResultSubset{&mockResultSubset{data}, &mockResultSubset{data}},
	}
	result := selectResult{
		label:   "dag",
		sqlType: "general",
		resp:    response,
		ctx:     newMockSessionContext().GetDistSQLCtx(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		response.index = 0
		result.selectResp = nil
		result.selectRespSize = 0
		for range 2 {
			if err := result.fetchResp(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	}
}

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
