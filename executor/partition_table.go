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
	// "runtime"
	// "sync"
	// "sync/atomic"

	// "github.com/opentracing/opentracing-go"
	// "github.com/pingcap/errors"
	// "github.com/pingcap/tidb/sessionctx"
	// "github.com/pingcap/tidb/statistics"
	// "github.com/pingcap/tidb/sessionctx/stmtctx"
	// "github.com/pingcap/tidb/kv"
	// "github.com/pingcap/tidb/sessionctx/variable"
	// "github.com/pingcap/tidb/distsql"
	// "github.com/pingcap/tidb/expression"
	// plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	// "github.com/pingcap/tidb/util/ranger"
	// "github.com/pingcap/tidb/util/timeutil"
	// "github.com/pingcap/tidb/util/logutil"
	// "go.uber.org/zap"
	// "github.com/pingcap/tidb/types"
	// "github.com/pingcap/tipb/go-tipb"
)

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
	n.exec.open(ctx)
	return n.exec, nil
}

func (e *PartitionTableExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
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

func (e *PartitionTableExecutor) Close() error {
	var err error
	if e.curr != nil {
		err = e.curr.Close()
		e.curr = nil
	}
	return err
}
