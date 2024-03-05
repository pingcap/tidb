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
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// baseHashAggWorker stores the common attributes of HashAggFinalWorker and HashAggPartialWorker.
// nolint:structcheck
type baseHashAggWorker struct {
	finishCh     <-chan struct{}
	aggFuncs     []aggfuncs.AggFunc
	maxChunkSize int
	stats        *AggWorkerStat

	memTracker *memory.Tracker
	BInMap     int // indicate there are 2^BInMap buckets in Golang Map.
}

func newBaseHashAggWorker(finishCh <-chan struct{}, aggFuncs []aggfuncs.AggFunc, maxChunkSize int, memTrack *memory.Tracker) baseHashAggWorker {
	baseWorker := baseHashAggWorker{
		finishCh:     finishCh,
		aggFuncs:     aggFuncs,
		maxChunkSize: maxChunkSize,
		memTracker:   memTrack,
		BInMap:       0,
	}
	return baseWorker
}

func (w *baseHashAggWorker) getPartialResultSliceLenConsiderByteAlign() int {
	length := len(w.aggFuncs)
	if length == 1 {
		return 1
	}
	return length + length&1
}

func (w *baseHashAggWorker) checkFinishChClosed() bool {
	select {
	case <-w.finishCh:
		return true
	default:
	}
	return false
}
