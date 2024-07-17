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

func (b *BuildWorkerV2) getSegments(partID int) []*rowTableSegment {
	return b.HashJoinCtx.hashTableContext.getSegments(int(b.WorkerID), partID)
}

func (b *BuildWorkerV2) clearSegments(partID int) {
	b.HashJoinCtx.hashTableContext.clearSegments(int(b.WorkerID), partID)
}

// buildHashTableForList builds hash table from `list`.
func (w *BuildWorkerV2) buildHashTable(taskCh chan *buildTask) error {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			atomic.AddInt64(&w.HashJoinCtx.stats.buildHashTable, cost)
			setMaxValue(&w.HashJoinCtx.stats.maxBuildHashTable, cost)
		}
	}()
	for task := range taskCh {
		start := time.Now()
		partIdx, segStartIdx, segEndIdx := task.partitionIdx, task.segStartIdx, task.segEndIdx
		w.HashJoinCtx.hashTableContext.hashTable.tables[partIdx].build(segStartIdx, segEndIdx)
		failpoint.Inject("buildHashTablePanic", nil)
		cost += int64(time.Since(start))
	}
	return nil
}

func (w *BuildWorkerV2) processOneChunk(typeCtx types.Context, chk *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup, cost *int64) error {
	defer func() {
		fetcherAndWorkerSyncer.Done()
	}()

	start := time.Now()
	err := w.builder.processOneChunk(chk, typeCtx, w.HashJoinCtx, int(w.WorkerID))
	failpoint.Inject("splitPartitionPanic", nil)
	*cost += int64(time.Since(start))
	return err
}

func (w *BuildWorkerV2) splitPartitionAndAppendToRowTable(typeCtx types.Context, srcChkCh chan *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup, syncer chan struct{}) (err error) {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			atomic.AddInt64(&w.HashJoinCtx.stats.partitionData, cost)
			setMaxValue(&w.HashJoinCtx.stats.maxPartitionData, cost)
		}
	}()
	partitionNumber := w.HashJoinCtx.PartitionNumber
	hashJoinCtx := w.HashJoinCtx

	// TODO add random failpoint to slow worker here, 20-40ms, enable it at any case.

	w.builder = createRowTableBuilder(w.BuildKeyColIdx, hashJoinCtx.BuildKeyTypes, partitionNumber, w.HasNullableKey, hashJoinCtx.BuildFilter != nil, hashJoinCtx.needScanRowTableAfterProbeDone)

	for chk := range srcChkCh {
		err = w.processOneChunk(typeCtx, chk, fetcherAndWorkerSyncer, &cost)
		if err != nil {
			return err
		}
	}

	start := time.Now()
	w.builder.appendRemainingRowLocations(int(w.WorkerID), w.HashJoinCtx.hashTableContext)
	cost += int64(time.Since(start))
	return nil
}
