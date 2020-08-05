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
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
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

func nextPartitionWithTrace(ctx context.Context, n nextPartition, tbl table.PhysicalTable) (Executor, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("nextPartition %d", tbl.GetPhysicalID()), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return n.nextPartition(ctx, tbl)
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
