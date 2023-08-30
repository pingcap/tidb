// Copyright 2023 PingCAP, Inc.
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

package aggregate

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/memory"
)

// AggPartialResultMapper contains aggregate function results
type AggPartialResultMapper map[string][]aggfuncs.PartialResult

// baseHashAggWorker stores the common attributes of HashAggFinalWorker and HashAggPartialWorker.
// nolint:structcheck
type baseHashAggWorker struct {
	ctx          sessionctx.Context
	finishCh     <-chan struct{}
	aggFuncs     []aggfuncs.AggFunc
	maxChunkSize int
	stats        *AggWorkerStat

	memTracker *memory.Tracker
	BInMap     int // indicate there are 2^BInMap buckets in Golang Map.
}

func newBaseHashAggWorker(ctx sessionctx.Context, finishCh <-chan struct{}, aggFuncs []aggfuncs.AggFunc,
	maxChunkSize int, memTrack *memory.Tracker) baseHashAggWorker {
	baseWorker := baseHashAggWorker{
		ctx:          ctx,
		finishCh:     finishCh,
		aggFuncs:     aggFuncs,
		maxChunkSize: maxChunkSize,
		memTracker:   memTrack,
		BInMap:       0,
	}
	return baseWorker
}

func (w *baseHashAggWorker) getPartialResult(_ *stmtctx.StatementContext, groupKey [][]byte, mapper AggPartialResultMapper) [][]aggfuncs.PartialResult {
	n := len(groupKey)
	partialResults := make([][]aggfuncs.PartialResult, n)
	allMemDelta := int64(0)
	partialResultSize := w.getPartialResultSliceLenConsiderByteAlign()
	for i := 0; i < n; i++ {
		var ok bool
		if partialResults[i], ok = mapper[string(groupKey[i])]; ok {
			continue
		}
		partialResults[i] = make([]aggfuncs.PartialResult, partialResultSize)
		for j, af := range w.aggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResults[i][j] = partialResult
			allMemDelta += memDelta // the memory usage of PartialResult
		}
		allMemDelta += int64(partialResultSize * 8)
		// Map will expand when count > bucketNum * loadFactor. The memory usage will double.
		if len(mapper)+1 > (1<<w.BInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
			w.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMap))
			w.BInMap++
		}
		mapper[string(groupKey[i])] = partialResults[i]
		allMemDelta += int64(len(groupKey[i]))
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(allMemDelta)
	return partialResults
}

func (w *baseHashAggWorker) getPartialResultSliceLenConsiderByteAlign() int {
	length := len(w.aggFuncs)
	if len(w.aggFuncs) == 1 {
		return 1
	}
	return length + length&1
}
