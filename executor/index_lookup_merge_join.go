// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"golang.org/x/net/context"
)

type IndexLookUpMergeJoin struct {
	baseExecutor

	resultTask <-chan *loopUpMergeJoinTask
	cancelFunc context.cancelFunc
	workerWg   *sync.WaitGroup

	outerMergeCtx outerMergeCtx
	innerMergeCtx innerMergeCtx

	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	innerPtrBytes [][]byte
	
	memTracker *memory.Tracker // track memory usage
}

type outerMergeCtx struct {
	rowTypes []*types.FieldType
	keyCols  []int
	filter   expression.CNFExprs
}

type innerMergeCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
}

type lookUpMergeJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	innerResult *chunk.List
	encodedLookUpKeys *chunk.Chunk
	matchedInners []chunk.Row

	doneCh chan error
	cursor int
	hasMatch bool

	
}

type outerMergeWorker struct {
	outerMergeCtx

	ctx      sessionctx.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int
	
	resultCh chan<- *chunk.Chunk
	innerCh  chan<- *chunk.Chunk

	parentMemTracker *memory.Tracker
}

type innerWorker struct {

}

func (e *IndexLookUpMergeJoin) Open(ctx context.Context) error {
	err := e.children[0].Open(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.startWorkers(ctx)
	return nil
}

func (e IndexLookUpMergeJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.,GetSessionVars().IndexLookupJoinConcurrency
	resultCh := make(chan *chunk.Chunk, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc

}