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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	goctx "golang.org/x/net/context"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	rows   [][]string
	cursor int
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next(goCtx goctx.Context) (Row, error) {
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	resultRow := make([]types.Datum, 0, len(e.rows[0]))
	for i := range e.rows[e.cursor] {
		resultRow = append(resultRow, types.NewStringDatum(e.rows[e.cursor][i]))
	}
	e.cursor++
	return resultRow, nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	return nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ExplainExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
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
