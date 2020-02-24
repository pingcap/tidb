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
	"context"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	explain     *core.Explain
	analyzeExec Executor
	rows        [][]string
	cursor      int
}

// Open implements the Executor Open interface.
func (e *ExplainExec) Open(ctx context.Context) error {
	if e.analyzeExec != nil {
		return e.analyzeExec.Open(ctx)
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	e.rows = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *ExplainExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		var err error
		e.rows, err = e.generateExplainInfo(ctx)
		if err != nil {
			return err
		}
	}

	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		for j := range e.rows[i] {
			req.AppendString(j, e.rows[i][j])
		}
	}
	e.cursor += numCurRows
	return nil
}

func (e *ExplainExec) generateExplainInfo(ctx context.Context) (rows [][]string, err error) {
	closed := false
	defer func() {
		if !closed && e.analyzeExec != nil {
			err = e.analyzeExec.Close()
			closed = true
		}
	}()
	if e.analyzeExec != nil {
		chk := newFirstChunk(e.analyzeExec)
		var nextErr, closeErr error
		for {
			nextErr = Next(ctx, e.analyzeExec, chk)
			if nextErr != nil || chk.NumRows() == 0 {
				break
			}
		}
		closeErr = e.analyzeExec.Close()
		closed = true
		if nextErr != nil {
			if closeErr != nil {
				err = errors.New(nextErr.Error() + ", " + closeErr.Error())
			} else {
				err = nextErr
			}
		} else if closeErr != nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	if err = e.explain.RenderResult(); err != nil {
		return nil, err
	}
	if e.analyzeExec != nil {
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	}
	return e.explain.Rows, nil
}
