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
	"sync"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

// topNWorker is used only when topn spill is triggered
type topNWorker struct {
	chunkChannel           <-chan *chunk.Chunk
	fetcherAndWorkerSyncer *sync.WaitGroup
	errOutputChan          chan<- rowWithError
	finishChan             <-chan struct{}

	chkHeap *topNChunkHeap
}

func newTopNWorker(
	chunkChannel <-chan *chunk.Chunk,
	fetcherAndWorkerSyncer *sync.WaitGroup,
	errOutputChan chan<- rowWithError,
	finishChan <-chan struct{},
	chkHeap *topNChunkHeap) *topNWorker {
	return &topNWorker{
		chunkChannel:           chunkChannel,
		fetcherAndWorkerSyncer: fetcherAndWorkerSyncer,
		errOutputChan:          errOutputChan,
		finishChan:             finishChan,
		chkHeap:                chkHeap,
	}
}

func (t *topNWorker) fetchChunksAndProcess() {
	t.fetchChunksAndBuildHeap()
	t.fetchChunksAndUpdateHeap()
}

// Insert rows into heap until reaching to limit
func (t *topNWorker) fetchChunksAndBuildHeap() {
	for t.fetchChunksAndBuildHeapImpl() {
	}
}

func (t *topNWorker) fetchChunksAndBuildHeapImpl() bool {
	if uint64(t.chkHeap.rowChunks.Len()) >= t.chkHeap.totalLimit {
		return false
	}

	select {
	case <-t.finishChan:
		return false
	case chk, ok := <-t.chunkChannel:
		defer t.fetcherAndWorkerSyncer.Done() // TODO fix it
		if !ok {
			t.chkHeap.initPtrs()
			return false
		}
		t.chkHeap.rowChunks.Add(chk)
	}
	return true
}

func (t *topNWorker) fetchChunksAndUpdateHeap() {
	for t.fetchChunksAndUpdateHeapImpl() {
	}
}

func (t *topNWorker) fetchChunksAndUpdateHeapImpl() bool {
	var (
		chk *chunk.Chunk
		ok  bool
	)
	select {
	case <-t.finishChan:
		return false
	case chk, ok = <-t.chunkChannel:
		if !ok {
			return false
		}
		defer t.fetcherAndWorkerSyncer.Done()
		t.chkHeap.processChkWithSpill(chk)
	}
	return true
}

func (t *topNWorker) run() {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(t.errOutputChan, r)
		}
	}()

	t.fetchChunksAndProcess()
}
