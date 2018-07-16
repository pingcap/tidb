// Copyright 2016 PingCAP, Inc.
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
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	rows   [][]string
	cursor int
}

// buildExplain builds a explain executor. `e.rows` collects final result and
// displays it in sql-shell.
func (b *executorBuilder) buildExplain(v *plan.Explain) Executor {
	e := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}
	e.rows = make([][]string, 0, len(v.Rows))
	for _, row := range v.Rows {
		e.rows = append(e.rows, row)
	}
	return e
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *ExplainExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := mathutil.Min(e.maxChunkSize, len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		for j := range e.rows[i] {
			chk.AppendString(j, e.rows[i][j])
		}
	}
	e.cursor += numCurRows
	return nil
}
