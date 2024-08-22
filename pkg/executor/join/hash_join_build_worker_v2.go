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
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// BuildWorkerV2 is the build worker used in hash join v2
type BuildWorkerV2 struct {
	buildWorkerBase
	HashJoinCtx    *HashJoinCtxV2
	BuildTypes     []*types.FieldType
	HasNullableKey bool
	WorkerID       uint
	builder        *rowTableBuilder
}

// NewJoinBuildWorkerV2 create a BuildWorkerV2
func NewJoinBuildWorkerV2(ctx *HashJoinCtxV2, workID uint, buildSideExec exec.Executor, buildKeyColIdx []int, buildTypes []*types.FieldType) *BuildWorkerV2 {
	hasNullableKey := false
	for _, idx := range buildKeyColIdx {
		if !mysql.HasNotNullFlag(buildTypes[idx].GetFlag()) {
			hasNullableKey = true
			break
		}
	}
	worker := &BuildWorkerV2{
		HashJoinCtx:    ctx,
		BuildTypes:     buildTypes,
		WorkerID:       workID,
		HasNullableKey: hasNullableKey,
	}
	worker.BuildSideExec = buildSideExec
	worker.BuildKeyColIdx = buildKeyColIdx
	return worker
}

func (b *BuildWorkerV2) getSegmentsInRowTable(partID int) []*rowTableSegment {
	return b.HashJoinCtx.hashTableContext.getSegmentsInRowTable(int(b.WorkerID), partID)
}

func (b *BuildWorkerV2) clearSegmentsInRowTable(partID int) {
	b.HashJoinCtx.hashTableContext.clearSegmentsInRowTable(int(b.WorkerID), partID)
}

// buildHashTableForList builds hash table from `list`.
func (w *BuildWorkerV2) buildHashTable(taskCh chan *buildTask) {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			atomic.AddInt64(&w.HashJoinCtx.stats.buildHashTable, cost)
			setMaxValue(&w.HashJoinCtx.stats.maxBuildHashTable, cost)
		}
	}()

	for task := range taskCh {
		triggerIntest(4)
		if w.HashJoinCtx.finished.Load() {
			return
		}

		start := time.Now()
		partIdx, segStartIdx, segEndIdx := task.partitionIdx, task.segStartIdx, task.segEndIdx
		w.HashJoinCtx.hashTableContext.hashTable.tables[partIdx].build(segStartIdx, segEndIdx)
		failpoint.Inject("buildHashTablePanic", nil)
		cost += int64(time.Since(start))
	}
}

func (w *BuildWorkerV2) processOneChunk(typeCtx types.Context, chk *chunk.Chunk, cost *int64) error {
	start := time.Now()
	err := w.builder.processOneChunk(chk, typeCtx, w.HashJoinCtx, int(w.WorkerID))
	failpoint.Inject("splitPartitionPanic", nil)
	*cost += int64(time.Since(start))
	return err
}

func (w *BuildWorkerV2) splitPartitionAndAppendToRowTableImpl(typeCtx types.Context, chk *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup, cost *int64) error {
	defer func() {
		fetcherAndWorkerSyncer.Done()
	}()

	if w.HashJoinCtx.finished.Load() {
		return nil
	}

	err := triggerIntest(5)
	if err != nil {
		return err
	}

	err = w.processOneChunk(typeCtx, chk, cost)
	if err != nil {
		return err
	}
	return nil
}

func (w *BuildWorkerV2) updatePartitionData(cost int64) {
	atomic.AddInt64(&w.HashJoinCtx.stats.partitionData, cost)
	setMaxValue(&w.HashJoinCtx.stats.maxPartitionData, cost)
}

func (w *BuildWorkerV2) splitPartitionAndAppendToRowTable(typeCtx types.Context, srcChkCh chan *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup) {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			w.updatePartitionData(cost)
		}
	}()
	partitionNumber := w.HashJoinCtx.partitionNumber
	hashJoinCtx := w.HashJoinCtx

	w.builder = createRowTableBuilder(w.BuildKeyColIdx, hashJoinCtx.BuildKeyTypes, partitionNumber, w.HasNullableKey, hashJoinCtx.BuildFilter != nil, hashJoinCtx.needScanRowTableAfterProbeDone)

	for chk := range srcChkCh {
		err := w.splitPartitionAndAppendToRowTableImpl(typeCtx, chk, fetcherAndWorkerSyncer, &cost)
		if err != nil {
			// Do no directly exit the function as there may still be some chunks in `srcChkCh`
			handleError(hashJoinCtx.joinResultCh, &hashJoinCtx.finished, err)
		}
	}

	if w.HashJoinCtx.finished.Load() {
		return
	}

	start := time.Now()
	w.builder.appendRemainingRowLocations(int(w.WorkerID), w.HashJoinCtx.hashTableContext)
	cost += int64(time.Since(start))
}

func (w *BuildWorkerV2) processOneRestoredChunk(chk *chunk.Chunk, cost *int64) error {
	start := time.Now()
	err := w.builder.processOneRestoredChunk(chk, w.HashJoinCtx, int(w.WorkerID), int(w.HashJoinCtx.partitionNumber))
	if err != nil {
		return err
	}
	*cost += int64(time.Since(start))
	return nil
}

func (w *BuildWorkerV2) restoreAndPrebuildImpl(i int, inDisk *chunk.DataInDiskByChunks, fetcherAndWorkerSyncer *sync.WaitGroup, cost *int64) error {
	defer func() {
		fetcherAndWorkerSyncer.Done()
	}()

	if w.HashJoinCtx.finished.Load() {
		return nil
	}

	// TODO reuse chunk
	chk, err := inDisk.GetChunk(i)
	if err != nil {
		return err
	}

	err = triggerIntest(3)
	if err != nil {
		return err
	}

	err = w.processOneRestoredChunk(chk, cost)
	if err != nil {
		return err
	}
	return nil
}

func (w *BuildWorkerV2) restoreAndPrebuild(inDisk *chunk.DataInDiskByChunks, syncCh chan struct{}, waitForController chan struct{}, fetcherAndWorkerSyncer *sync.WaitGroup) {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			w.updatePartitionData(cost)
		}
	}()

	partitionNumber := w.HashJoinCtx.partitionNumber
	hashJoinCtx := w.HashJoinCtx

	w.builder = createRowTableBuilder(w.BuildKeyColIdx, hashJoinCtx.BuildKeyTypes, partitionNumber, w.HasNullableKey, hashJoinCtx.BuildFilter != nil, hashJoinCtx.needScanRowTableAfterProbeDone)

	chunkNum := inDisk.NumChunks()
	for i := 0; i < chunkNum; i++ {
		_, ok := <-syncCh
		if !ok {
			break
		}

		err := w.restoreAndPrebuildImpl(i, inDisk, fetcherAndWorkerSyncer, &cost)
		if err != nil {
			handleError(hashJoinCtx.joinResultCh, &hashJoinCtx.finished, err)
		}
	}

	// Wait for command from the controller so that we can avoid data race with the spill executed in controller
	<-waitForController

	start := time.Now()
	w.builder.appendRemainingRowLocations(int(w.WorkerID), w.HashJoinCtx.hashTableContext)
	cost += int64(time.Since(start))
}
