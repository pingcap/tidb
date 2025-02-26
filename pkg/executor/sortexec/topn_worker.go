// Copyright 2024 PingCAP, Inc.
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

package sortexec

import (
	"container/heap"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// topNWorker is used only when topn spill is triggered
type topNWorker struct {
	workerIDForTest int

	chunkChannel           <-chan *chunk.Chunk
	fetcherAndWorkerSyncer *sync.WaitGroup
	errOutputChan          chan<- rowWithError
	finishChan             <-chan struct{}

	topn       *TopNExec
	chkHeap    *topNChunkHeap
	memTracker *memory.Tracker
}

func newTopNWorker(
	idForTest int,
	chunkChannel <-chan *chunk.Chunk,
	fetcherAndWorkerSyncer *sync.WaitGroup,
	errOutputChan chan<- rowWithError,
	finishChan <-chan struct{},
	topn *TopNExec,
	chkHeap *topNChunkHeap,
	memTracker *memory.Tracker) *topNWorker {
	return &topNWorker{
		workerIDForTest:        idForTest,
		chunkChannel:           chunkChannel,
		fetcherAndWorkerSyncer: fetcherAndWorkerSyncer,
		errOutputChan:          errOutputChan,
		finishChan:             finishChan,
		chkHeap:                chkHeap,
		topn:                   topn,
		memTracker:             memTracker,
	}
}

func (t *topNWorker) initWorker() {
	// Offset of heap in worker should be 0, as we need to spill all data
	t.chkHeap.init(t.topn, t.memTracker, t.topn.Limit.Offset+t.topn.Limit.Count, 0, t.topn.greaterRow, t.topn.RetFieldTypes())
}

func (t *topNWorker) fetchChunksAndProcess() {
	for t.fetchChunksAndProcessImpl() {
	}
}

func (t *topNWorker) fetchChunksAndProcessImpl() bool {
	select {
	case <-t.finishChan:
		return false
	case chk, ok := <-t.chunkChannel:
		if !ok {
			return false
		}
		defer func() {
			t.fetcherAndWorkerSyncer.Done()
		}()

		t.injectFailPointForTopNWorker(3)

		if uint64(t.chkHeap.rowChunks.Len()) < t.chkHeap.totalLimit {
			if !t.chkHeap.isInitialized {
				t.chkHeap.init(t.topn, t.memTracker, t.topn.Limit.Offset+t.topn.Limit.Count, 0, t.topn.greaterRow, t.topn.RetFieldTypes())
			}
			t.chkHeap.rowChunks.Add(chk)
		} else {
			if !t.chkHeap.isRowPtrsInit {
				t.chkHeap.initPtrs()
				heap.Init(t.chkHeap)
			}
			t.chkHeap.processChk(chk)
		}
	}
	return true
}

func (t *topNWorker) run() {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(t.errOutputChan, r)
		}

		// Consume all chunks to avoid hang of fetcher
		for range t.chunkChannel {
			t.fetcherAndWorkerSyncer.Done()
		}
	}()

	t.fetchChunksAndProcess()
}

func (t *topNWorker) injectFailPointForTopNWorker(triggerFactor int32) {
	injectTopNRandomFail(triggerFactor)
	failpoint.Inject("SlowSomeWorkers", func(val failpoint.Value) {
		if val.(bool) {
			if t.workerIDForTest%2 == 0 {
				randNum := rand.Int31n(10000)
				if randNum < 10 {
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	})
}
