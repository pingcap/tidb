// Copyright 2020 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
)

var (
	_ Executor = &GraceHashJoinExec{}
)

// GraceHashJoinExec implements the grace hash join algorithm.
// It first partitioning both build side and probe side chunks via a hash fucntion,
// and writing these partitions out to disk. Then we load each pairs of partition
// from disk and implements hash join algorithm using HashJoinExec.
//
// The execution flow is as the following graph shows:
//
//             +-----------------------------+
//             |      GraceHashJoinExec      |
//             +-----------------------------+
//                 ^          |          ^
//                 |          |          |
//                 |          |          |
//                 |          |          v
//    +-----------------+     |     +--------------+
//    | hashPartitioner |     |     | HashJoinExec |
//    +-----------------+     |     +--------------+
//            ^               |              ^
//            |               |              |
//            |               |              |
//    +-----------------+     |    +-----------------+
//    |   dataSource    |     |    | PartitionerExec |
//    +-----------------+     |    +-----------------+
//                            |              ^
//                            |              |
//                            v              |
//             +------------------------------------------------+
//             |                  PartitionInDisk               |
//             +------------------------------------------------+
//               ^            ^                ^              ^
//               |            |                |              |
//               |            |                |              |
//               v            v                v              v
//    +------------+    +------------+    +------------+    +------------+
//    | ListInDisk |    | ListInDisk |    | ListInDisk |    | ListInDisk |
//    +------------+    +------------+    +------------+    +------------+
//
////////////////////////////////////////////////////////////////////////////////////////
type GraceHashJoinExec struct {
	baseExecutor

	probeSideExec        Executor
	buildSideExec        Executor
	hashJoinExec         Executor
	buildPartitionerExec *PartitionerExec
	probePartitionerExec *PartitionerExec
	buildHashPartitioner *hashPartitioner
	probeHashPartitioner *hashPartitioner

	probeKeys  []*expression.Column
	buildKeys  []*expression.Column
	probeTypes []*types.FieldType
	buildTypes []*types.FieldType

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker   // track disk usage.

	prepared     bool
	partitionCnt int
	partitionIdx int
}

// Open implements the Executor Open interface.
func (e *GraceHashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	if err := e.hashJoinExec.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.id, -1)
	e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)

	if e.probeTypes == nil {
		e.probeTypes = retTypes(e.probeSideExec)
	}
	if e.buildTypes == nil {
		e.buildTypes = retTypes(e.buildSideExec)
	}

	e.partitionIdx = 0

	buildKeyColIdx := make([]int, len(e.buildKeys))
	for i := range e.buildKeys {
		buildKeyColIdx[i] = e.buildKeys[i].Index
	}
	buildHashCtx := &hashContext{
		allTypes:  e.buildTypes,
		keyColIdx: buildKeyColIdx,
	}
	e.buildHashPartitioner = newHashPartitioner(e.ctx, e.partitionCnt, buildHashCtx)

	probeKeyColIdx := make([]int, len(e.probeKeys))
	for i := range e.probeKeys {
		probeKeyColIdx[i] = e.probeKeys[i].Index
	}
	probeHashCtx := &hashContext{
		allTypes:  e.probeTypes,
		keyColIdx: probeKeyColIdx,
	}
	e.probeHashPartitioner = newHashPartitioner(e.ctx, e.partitionCnt, probeHashCtx)

	return nil
}

// Close implements the Executor Close interface.
func (e *GraceHashJoinExec) Close() error {
	err := e.buildPartitionerExec.partitionInDisk.Close()
	if err != nil {
		return err
	}
	err = e.probePartitionerExec.partitionInDisk.Close()
	if err != nil {
		return err
	}

	e.buildHashPartitioner = nil
	e.probeHashPartitioner = nil

	err = e.hashJoinExec.Close()
	if err != nil {
		return err
	}

	err = e.baseExecutor.Close()
	return err
}

// partitioning the chunk into partitions and write them to disk.
func (e *GraceHashJoinExec) partition(chk *chunk.Chunk, hashPartitioner *hashPartitioner, partitionInDisk *chunk.PartitionInDisk) (err error) {
	if chk.NumRows() == 0 {
		err := partitionInDisk.Flush()
		if err != nil {
			return err
		}
		return nil
	}

	partitions, err := hashPartitioner.split(chk)
	if err != nil {
		return err
	}
	err = partitionInDisk.Add(chk, partitions)
	if err != nil {
		return err
	}

	return nil
}

func (e *GraceHashJoinExec) buildPartitionInDisk(ctx context.Context) (err error) {

	for {
		chk := chunk.NewChunkWithCapacity(e.buildSideExec.base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
		err = Next(ctx, e.buildSideExec, chk)
		if err != nil {
			return err
		}
		err := e.partition(chk, e.buildHashPartitioner, e.buildPartitionerExec.partitionInDisk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
	}

	for {
		chk := chunk.NewChunkWithCapacity(e.probeSideExec.base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
		err = Next(ctx, e.probeSideExec, chk)
		if err != nil {
			return err
		}
		err := e.partition(chk, e.probeHashPartitioner, e.probePartitionerExec.partitionInDisk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	return nil
}

// Next implements the Executor Next interface.
// grace hash join is executed in two phases:
// phases 1 is partitioning. fetch data and partitioning them, then write to disk.
// phases 2 is joining. fetch data from disk and execting join by partition order.
func (e *GraceHashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		err := e.buildPartitionInDisk(ctx)
		if err != nil {
			return err
		}
		e.prepared = true
	}

	req.Reset()
	if e.partitionIdx == e.partitionCnt {
		return nil
	}

	for {
		err = Next(ctx, e.hashJoinExec, req)
		if err != nil {
			return err
		}
		if req.NumRows() > 0 {
			break
		}
		e.partitionIdx++
		if e.partitionIdx == e.partitionCnt {
			break
		}
		// this partition has fininshed
		// reset partitionIdx and chkIdx to begin next partition
		e.buildPartitionerExec.partitionIdx = e.partitionIdx
		e.buildPartitionerExec.chkIdx = 0
		e.probePartitionerExec.partitionIdx = e.partitionIdx
		e.probePartitionerExec.chkIdx = 0
		err = e.hashJoinExec.Close()
		if err != nil {
			return err
		}
		err = e.hashJoinExec.Open(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
