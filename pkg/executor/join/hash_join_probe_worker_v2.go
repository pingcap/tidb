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

package join

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ProbeWorkerV2 is the probe worker used in hash join v2
type ProbeWorkerV2 struct {
	probeWorkerBase
	HashJoinCtx *HashJoinCtxV2
	// We build individual joinProbe for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	JoinProbe ProbeV2
}

func (w *ProbeWorkerV2) handleProbeWorkerPanic(r any) {
	if r != nil {
		w.HashJoinCtx.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (w *ProbeWorkerV2) scanRowTableAfterProbeDone(inSpillMode bool) error {
	w.JoinProbe.InitForScanRowTable(inSpillMode)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return nil
	}
	for !w.JoinProbe.IsScanRowTableDone() {
		joinResult = w.JoinProbe.ScanRowTable(joinResult, &w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller)
		if joinResult.err != nil {
			w.HashJoinCtx.joinResultCh <- joinResult
			return joinResult.err
		}
		if joinResult.chk.IsFull() {
			w.HashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			if !ok {
				return nil
			}
		}
	}
	if joinResult == nil {
		return nil
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	}
	return nil
}

func (w *ProbeWorkerV2) probeAndSendResult(joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var ok bool
	waitTime := int64(0)
	for !w.JoinProbe.IsCurrentChunkProbeDone() {
		ok, joinResult = w.JoinProbe.Probe(joinResult, &w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller)
		if !ok || joinResult.err != nil {
			return ok, waitTime, joinResult
		}

		failpoint.Inject("processOneProbeChunkPanic", nil)
		if joinResult.chk.IsFull() {
			waitStart := time.Now()
			w.HashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			waitTime += int64(time.Since(waitStart))
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

func (w *ProbeWorkerV2) processOneRestoredProbeChunk(probeChunk *chunk.Chunk, hashTable *hashTableV2, joinResult *hashjoinWorkerResult) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	waitTime = 0
	joinResult.err = w.JoinProbe.SetRestoredChunkForProbe(probeChunk, hashTable)
	if joinResult.err != nil {
		return false, 0, joinResult
	}
	return w.probeAndSendResult(joinResult)
}

func (w *ProbeWorkerV2) processOneProbeChunk(probeChunk *chunk.Chunk, joinResult *hashjoinWorkerResult, fetcherAndWorkerSyncer *sync.WaitGroup) (bool, int64, *hashjoinWorkerResult) {
	defer fetcherAndWorkerSyncer.Done()
	joinResult.err = w.JoinProbe.SetChunkForProbe(probeChunk)
	if joinResult.err != nil {
		return false, 0, joinResult
	}
	return w.probeAndSendResult(joinResult)
}

// TODO check if probe data are restored data and handle them in another way
func (w *ProbeWorkerV2) runJoinWorkerImpl(fetcherAndWorkerSyncer *sync.WaitGroup) {
	defer fetcherAndWorkerSyncer.Done()

	probeTime := int64(0)
	if w.HashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&w.HashJoinCtx.stats.probe, probeTime)
			atomic.AddInt64(&w.HashJoinCtx.stats.fetchAndProbe, int64(t))
			setMaxValue(&w.HashJoinCtx.stats.maxFetchAndProbe, int64(t))
		}()
	}

	// TODO add random failpoint to slow some workers

	var (
		probeSideResult *chunk.Chunk
	)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: w.probeResultCh,
	}
	for ok := true; ok; {
		if w.HashJoinCtx.finished.Load() {
			break
		}
		select {
		case <-w.HashJoinCtx.closeCh:
			return
		case probeSideResult, ok = <-w.probeResultCh:
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		if !ok {
			break
		}

		start := time.Now()
		waitTime := int64(0)
		ok, waitTime, joinResult = w.processOneProbeChunk(probeSideResult, joinResult, fetcherAndWorkerSyncer)
		probeTime += int64(time.Since(start)) - waitTime
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		w.probeChkResourceCh <- emptyProbeSideResult
	}

	err := w.JoinProbe.SpillRemainingProbeChunks()
	if err != nil {
		w.HashJoinCtx.joinResultCh <- &hashjoinWorkerResult{err: err}
		return
	}

	// TODO restore should be skipped when there is an error

	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
	}
}

func (w *ProbeWorkerV2) runJoinWorker(fetcherAndWorkerSyncer *sync.WaitGroup) {
	for {
		// TODO wait for the wake-up from fetcher
		// TODO check finish flag
		w.runJoinWorkerImpl(fetcherAndWorkerSyncer)
	}
}

func (w *ProbeWorkerV2) getNewJoinResult() (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		workerID: int(w.WorkerID),
		src:      w.joinChkResourceCh,
	}
	ok := true
	select {
	case <-w.HashJoinCtx.closeCh:
		ok = false
	case joinResult.chk, ok = <-w.joinChkResourceCh:
	}
	return ok, joinResult
}
