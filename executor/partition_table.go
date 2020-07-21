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
	"fmt"
	// "runtime"
	// "sync"
	// "sync/atomic"

	// "github.com/opentracing/opentracing-go"
	// "github.com/pingcap/errors"
	// "github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	// "github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/kv"
	// "github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	// "github.com/pingcap/tidb/util/timeutil"
	// "github.com/pingcap/tidb/util/logutil"
	// "go.uber.org/zap"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// PartitionTableExecutor handles partition pruning and works like TableReader from the caller's point of view.
type PartitionTableExecutor struct {
	baseExecutor

	dagReq    *tipb.DAGRequest
	startTS   uint64
	streaming bool
	desc      bool
	keepOrder bool
	storeType kv.StoreType
	ranges    []*ranger.Range
	// corColInFilter tells whether there's correlated column in filter.
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int
	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType
	// batchCop indicates whether use super batch coprocessor request, only works for TiFlash engine.
	// batchCop indicates whether use super batch coprocessor request, only works for TiFlash engine.
	batchCop bool

	selectResultHook
	plans    []plannercore.PhysicalPlan
	feedback *statistics.QueryFeedback

	// The partition table it acts on.
	table table.PartitionedTable
	// The query filter conditions.
	conds []expression.Expression

	// Jobs to be done.
	partitions []table.PhysicalTable
	cursor     int
	curr       distsql.SelectResult
}

// Open implements the Executor Open interface.
func (e *PartitionTableExecutor) Open(ctx context.Context) error {
	fmt.Println("before pruning === count partition ==")

	partitions, err := plannercore.PartitionPruning(e.ctx, e.table, e.conds)
	if err != nil {
		fmt.Println("partition pruning error === ", err)
		return err
	}
	fmt.Println("after pruning === count partition ==", len(partitions))

	e.partitions = partitions
	return nil
}

func (e *PartitionTableExecutor) handlePartition(ctx context.Context, partition table.PhysicalTable) (distsql.SelectResult, error) {
	sessVars := e.ctx.GetSessionVars()
	fmt.Println("handle partition", partition.GetPhysicalID(), e.ranges)

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(e.dagReq).
		SetStartTS(e.startTS).
		SetTableRanges(partition.GetPhysicalID(), e.ranges, nil).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(sessVars).
		// SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetAllowBatchCop(e.batchCop).
		Build()

	result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	// result, err := distsql.Select(ctx, e.ctx, kvReq, retTypes(e), nil)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, err
}

// Next implements the Executor Next interface.
func (e *PartitionTableExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	var err error
	for e.cursor < len(e.partitions) {
		if e.curr == nil {
			e.curr, err = e.handlePartition(ctx, e.partitions[e.cursor])
			if err != nil {
				fmt.Println("!!!!!!!!!!! erro ==", err)
				return err
			}
		}

		err = e.curr.Next(ctx, chk)
		if err != nil {
			return err
		}

		if chk.NumRows() > 0 {
			// The normal code execute path.
			fmt.Println("run here?666 666", chk.NumRows())
			break
		}

		fmt.Println(" ======== chunk size is nul???", e.cursor)
		e.curr = nil
		e.cursor++
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *PartitionTableExecutor) Close() error {
	return nil
}

type partitionDataSource struct {
	baseExecutor

	nextPartition
	partitions []table.PhysicalTable
	cursor     int
	curr       Executor
}

type nextPartition interface {
	nextPartition(context.Context, table.PhysicalTable) (Executor, error)
}

type nextPartitionForTableReader struct {
	exec *TableReaderExecutor
}

func (n nextPartitionForTableReader) nextPartition(ctx context.Context, tbl table.PhysicalTable) (Executor, error) {
	n.exec.table = tbl
	return n.exec, nil
}

type nextPartitionForIndexLookUp struct {
	exec *IndexLookUpExecutor
}

func (n nextPartitionForIndexLookUp) nextPartition(ctx context.Context, tbl table.PhysicalTable) (Executor, error) {
	n.exec.table = tbl
	n.exec.open(ctx)
	return n.exec, nil
}

func (e *partitionDataSource) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	var err error
	for e.cursor < len(e.partitions) {
		if e.curr == nil {
			n := e.nextPartition
			e.curr, err = n.nextPartition(ctx, e.partitions[e.cursor])
			if err != nil {
				return err
			}
			e.curr.Open(ctx)
		}

		err = e.curr.Next(ctx, chk)
		if err != nil {
			return err
		}

		if chk.NumRows() > 0 {
			break
		}

		e.curr.Close()
		e.curr = nil
		e.cursor++
	}
	return nil
}

func (e *partitionDataSource) Close() error {
	var err error
	if e.curr != nil {
		err = e.curr.Close()
		e.curr = nil
	}
	return err
}
