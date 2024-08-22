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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ProbeWorkerV2 is the probe worker used in hash join v2
type ProbeWorkerV2 struct {
	probeWorkerBase
	HashJoinCtx *HashJoinCtxV2
	// We build individual joinProbe for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	JoinProbe ProbeV2

	// TODO delete them
	totalOutputRowNum int
	spilledRowNum     int
}

func (w *ProbeWorkerV2) scanRowTableAfterProbeDone(inSpillMode bool) error {
	w.JoinProbe.InitForScanRowTable(inSpillMode)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return nil
	}

	totalOutputRowNum := 0
	defer func() {
		log.Info(fmt.Sprintf("xzxdebug final worker totalOutputRowNum: %d", totalOutputRowNum))
	}()
	for !w.JoinProbe.IsScanRowTableDone() {
		joinResult = w.JoinProbe.ScanRowTable(joinResult, &w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller)
		if joinResult.err != nil {
			w.HashJoinCtx.joinResultCh <- joinResult
			return joinResult.err
		}
		if joinResult.chk.IsFull() {
			err := triggerIntest(3)
			if err != nil {
				return err
			}

			totalOutputRowNum += joinResult.chk.NumRows()
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
		if joinResult.chk != nil {
			totalOutputRowNum += joinResult.chk.NumRows()
		}
		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
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
			w.totalOutputRowNum += joinResult.chk.NumRows()
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

func (w *ProbeWorkerV2) processOneRestoredProbeChunk(probeChunk *chunk.Chunk, joinResult *hashjoinWorkerResult) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	waitTime = 0
	joinResult.err = w.JoinProbe.SetRestoredChunkForProbe(probeChunk)
	if joinResult.err != nil {
		return false, 0, joinResult
	}
	return w.probeAndSendResult(joinResult)
}

func (w *ProbeWorkerV2) processOneProbeChunk(probeChunk *chunk.Chunk, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	joinResult.err = w.JoinProbe.SetChunkForProbe(probeChunk)
	if joinResult.err != nil {
		return false, 0, joinResult
	}
	return w.probeAndSendResult(joinResult)
}

func (w *ProbeWorkerV2) updateProbeStatistic(start time.Time, probeTime int64) {
	t := time.Since(start)
	atomic.AddInt64(&w.HashJoinCtx.stats.probe, probeTime)
	atomic.AddInt64(&w.HashJoinCtx.stats.fetchAndProbe, int64(t))
	setMaxValue(&w.HashJoinCtx.stats.maxFetchAndProbe, int64(t))
}

func (w *ProbeWorkerV2) runJoinWorker() {
	probeTime := int64(0)
	if w.HashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			w.updateProbeStatistic(start, probeTime)
		}()
	}

	var (
		probeSideResult *chunk.Chunk
	)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}

	defer func() {
		spilledInfo := w.JoinProbe.GetSpilledRowNum()
		info := ""
		for partID, rowNum := range spilledInfo {
			info = fmt.Sprintf("%s [%d %d]", info, partID, rowNum)
		}

		// log.Info(fmt.Sprintf("xzxdebug worker %d, totalOutputRowNum: %d", w.WorkerID, w.totalOutputRowNum))
		// w.totalOutputRowNum = 0
	}()

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: w.probeResultCh,
	}
	for ok := true; ok; {
		if w.HashJoinCtx.finished.Load() {
			return
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

		err := triggerIntest(3)
		if err != nil {
			handleError(w.HashJoinCtx.joinResultCh, &w.HashJoinCtx.finished, err)
			return
		}

		start := time.Now()
		waitTime := int64(0)
		ok, waitTime, joinResult = w.processOneProbeChunk(probeSideResult, joinResult)
		probeTime += int64(time.Since(start)) - waitTime
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult

		// Give back to probe fetcher
		w.probeChkResourceCh <- emptyProbeSideResult
	}

	err := w.JoinProbe.SpillRemainingProbeChunks()
	if err != nil {
		handleError(w.HashJoinCtx.joinResultCh, &w.HashJoinCtx.finished, err)
		return
	}

	spilledRowNum := w.JoinProbe.GetSpilledRowNum()

	defer func() {
		totalSpilledRowNum := 0
		detail := ""
		for partID, rowNum := range spilledRowNum {
			detail = fmt.Sprintf("%s[%d %d], ", detail, partID, rowNum)
			totalSpilledRowNum += rowNum
		}
	}()

	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil {
		handleError(w.HashJoinCtx.joinResultCh, &w.HashJoinCtx.finished, joinResult.err)
	} else if joinResult.chk != nil && joinResult.chk.NumRows() > 0 {
		if joinResult.chk != nil {
			w.totalOutputRowNum += joinResult.chk.NumRows()
		}
		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
	}
}

func (w *ProbeWorkerV2) restoreAndProbe(inDisk *chunk.DataInDiskByChunks) error {
	probeTime := int64(0)
	if w.HashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			w.updateProbeStatistic(start, probeTime)
		}()
	}

	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return nil
	}

	// defer func() {
	// 	log.Info(fmt.Sprintf("xzxdebug worker %d, output: %d", w.WorkerID, w.totalOutputRowNum))
	// 	w.totalOutputRowNum = 0
	// }()

	chunkNum := inDisk.NumChunks()

	for i := 0; i < chunkNum; i++ {
		if w.HashJoinCtx.finished.Load() {
			return nil
		}
		select {
		case <-w.HashJoinCtx.closeCh:
			return nil
		default:
		}
		failpoint.Inject("ConsumeRandomPanic", nil)

		// TODO reuse chunk
		chk, err := inDisk.GetChunk(i)
		if err != nil {
			return err
		}

		err = triggerIntest(3)
		if err != nil {
			return err
		}

		start := time.Now()
		waitTime := int64(0)
		ok, waitTime, joinResult = w.processOneRestoredProbeChunk(chk, joinResult)
		probeTime += int64(time.Since(start)) - waitTime
		if !ok {
			break
		}
	}

	err := w.JoinProbe.SpillRemainingProbeChunks()
	if err != nil {
		return err
	}

	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return nil
	} else if joinResult.err != nil {
		handleError(w.HashJoinCtx.joinResultCh, &w.HashJoinCtx.finished, joinResult.err)
	} else if joinResult.chk != nil && joinResult.chk.NumRows() > 0 {
		if joinResult.chk != nil {
			w.totalOutputRowNum += joinResult.chk.NumRows()
		}

		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
	}
	return nil
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
