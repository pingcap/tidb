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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// PartitionTableExecutor is a Executor for partitioned table.
// It works by wrap the underlying TableReader/IndexReader/IndexLookUpReader.
type PartitionTableExecutor struct {
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
	n.exec.kvRanges = n.exec.kvRanges[:0]
	if err := updateDAGRequestTableID(ctx, n.exec.dagPB, tbl.Meta().ID, tbl.GetPhysicalID()); err != nil {
		return nil, err
	}
	return n.exec, nil
}

type nextPartitionForIndexLookUp struct {
	exec *IndexLookUpExecutor
}

func (n nextPartitionForIndexLookUp) nextPartition(ctx context.Context, tbl table.PhysicalTable) (Executor, error) {
	n.exec.table = tbl
	return n.exec, nil
}

type nextPartitionForIndexReader struct {
	exec *IndexReaderExecutor
}

func (n nextPartitionForIndexReader) nextPartition(ctx context.Context, tbl table.PhysicalTable) (Executor, error) {
	exec := n.exec
	exec.table = tbl
	exec.physicalTableID = tbl.GetPhysicalID()
	return exec, nil
}

type nextPartitionForIndexMerge struct {
	exec *IndexMergeReaderExecutor
}

func (n nextPartitionForIndexMerge) nextPartition(ctx context.Context, tbl table.PhysicalTable) (Executor, error) {
	exec := n.exec
	exec.table = tbl
	return exec, nil
}

type nextPartitionForUnionScan struct {
	b     *executorBuilder
	us    *plannercore.PhysicalUnionScan
	child nextPartition
}

// nextPartition implements the nextPartition interface.
// For union scan on partitioned table, the executor should be PartitionTable->UnionScan->TableReader rather than
// UnionScan->PartitionTable->TableReader
func (n nextPartitionForUnionScan) nextPartition(ctx context.Context, tbl table.PhysicalTable) (Executor, error) {
	childExec, err := n.child.nextPartition(ctx, tbl)
	if err != nil {
		return nil, err
	}

	n.b.err = nil
	ret := n.b.buildUnionScanFromReader(childExec, n.us)
	return ret, n.b.err
}

func nextPartitionWithTrace(ctx context.Context, n nextPartition, tbl table.PhysicalTable) (Executor, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("nextPartition %d", tbl.GetPhysicalID()), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return n.nextPartition(ctx, tbl)
}

// updateDAGRequestTableID update the table ID in the DAG request to partition ID.
// TiKV only use that table ID for log, but TiFlash use it.
func updateDAGRequestTableID(ctx context.Context, dag *tipb.DAGRequest, tableID, partitionID int64) error {
	// TiFlash set RootExecutor field and ignore Executors field.
	if dag.RootExecutor != nil {
		return updateExecutorTableID(ctx, dag.RootExecutor, tableID, partitionID, true)
	}
	for i := 0; i < len(dag.Executors); i++ {
		exec := dag.Executors[i]
		err := updateExecutorTableID(ctx, exec, tableID, partitionID, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateExecutorTableID(ctx context.Context, exec *tipb.Executor, tableID, partitionID int64, recursive bool) error {
	var child *tipb.Executor
	switch exec.Tp {
	case tipb.ExecType_TypeTableScan:
		exec.TblScan.TableId = partitionID
		// For test coverage.
		if tmp := ctx.Value("nextPartitionUpdateDAGReq"); tmp != nil {
			m := tmp.(map[int64]struct{})
			m[partitionID] = struct{}{}
		}
	case tipb.ExecType_TypeIndexScan:
		exec.IdxScan.TableId = partitionID
	case tipb.ExecType_TypeSelection:
		child = exec.Selection.Child
	case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
		child = exec.Aggregation.Child
	case tipb.ExecType_TypeTopN:
		child = exec.TopN.Child
	case tipb.ExecType_TypeLimit:
		child = exec.Limit.Child
	case tipb.ExecType_TypeJoin:
		// TiFlash currently does not support Join on partition table.
		// The planner should not generate this kind of plan.
		// So the code should never run here.
		return errors.New("wrong plan, join on partition table is not supported on TiFlash")
	default:
		return errors.Trace(fmt.Errorf("unknown new tipb protocol %d", exec.Tp))
	}
	if child != nil && recursive {
		return updateExecutorTableID(ctx, child, tableID, partitionID, recursive)
	}
	return nil
}

// Open implements the Executor interface.
func (e *PartitionTableExecutor) Open(ctx context.Context) error {
	// Open is actually done in the calling of Next()
	return nil
}

// Next implements the Executor interface.
func (e *PartitionTableExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	var err error
	for e.cursor < len(e.partitions) {
		if e.curr == nil {
			n := e.nextPartition
			e.curr, err = nextPartitionWithTrace(ctx, n, e.partitions[e.cursor])
			if err != nil {
				return err
			}
			if err := e.curr.Open(ctx); err != nil {
				return err
			}
		}

		err = Next(ctx, e.curr, chk)
		if err != nil {
			return err
		}

		if chk.NumRows() > 0 {
			break
		}

		err = e.curr.Close()
		if err != nil {
			return err
		}
		e.curr = nil
		e.cursor++
	}
	return nil
}

// Close implements the Executor interface.
func (e *PartitionTableExecutor) Close() error {
	var err error
	if e.curr != nil {
		err = e.curr.Close()
		e.curr = nil
	}
	return err
}
