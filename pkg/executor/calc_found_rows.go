// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// CalcFoundRowsExec streams the data child, then evaluates the count before EOF.
// Both branches use the statement's transaction context. Deferring count until
// EOF avoids delaying the first result row and does not publish a total for an
// error or an early close.
type CalcFoundRowsExec struct {
	exec.BaseExecutor
	// Serialize Next and Close; cancellation comes from the caller context.
	mu          sync.Mutex
	closed      bool
	countOpened bool
	dataOpened  bool
	done        bool
	foundRows   uint64
}

// Open implements exec.Executor.
func (e *CalcFoundRowsExec) Open(ctx context.Context) error {
	e.closed = false
	e.countOpened = false
	e.dataOpened = true
	e.done = false
	e.foundRows = 0
	return exec.Open(ctx, e.Children(0))
}

// Next implements exec.Executor.
func (e *CalcFoundRowsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	req.Reset()
	if e.closed {
		return context.Canceled
	}
	if e.done {
		return nil
	}
	if err := exec.Next(ctx, e.Children(0), req); err != nil {
		return err
	}
	if req.NumRows() != 0 {
		return nil
	}
	e.dataOpened = false
	if err := exec.Close(e.Children(0)); err != nil {
		return err
	}
	if err := e.releaseDataCTEStorage(); err != nil {
		return err
	}
	failpoint.InjectCall("afterCalcFoundRowsData", e.Ctx())
	if err := ctx.Err(); err != nil {
		return err
	}
	count := e.Children(1)
	e.countOpened = true
	if err := exec.Open(ctx, count); err != nil {
		return err
	}
	chk := exec.TryNewCacheChunk(count)
	if err := exec.Next(ctx, count, chk); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if chk.NumRows() != 1 || chk.GetRow(0).IsNull(0) {
		return errors.New("SQL_CALC_FOUND_ROWS count plan did not produce a count")
	}
	total := uint64(chk.GetRow(0).GetInt64(0))
	e.countOpened = false
	if err := exec.Close(count); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.foundRows = total
	e.done = true
	return nil
}

// FoundRows reports the total after successful count execution.
func (e *CalcFoundRowsExec) FoundRows() (uint64, bool) {
	return e.foundRows, e.done
}

// Close implements exec.Executor.
func (e *CalcFoundRowsExec) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	if e.countOpened {
		e.countOpened = false
		return exec.Close(e.Children(1))
	}
	if e.dataOpened {
		e.dataOpened = false
		return exec.Close(e.Children(0))
	}
	return nil
}

// releaseDataCTEStorage drops completed data-branch results before count starts.
// This executor must be at the root of the query (possibly under EXPLAIN), so
// no other CTE consumer remains active when the data child has been closed.
// CTE Close retains shared storage until statement cleanup. Resetting completed
// producers here preserves storage references and leaves the unopened count
// branch intact, while avoiding overlapping both branches' materialized data.
func (e *CalcFoundRowsExec) releaseDataCTEStorage() error {
	storageMap := e.Ctx().GetSessionVars().StmtCtx.CTEStorageMap
	if storageMap == nil {
		return nil
	}
	storages, ok := storageMap.(map[int]*CTEStorages)
	if !ok {
		return errors.New("type assertion for CTEStorageMap failed")
	}
	for _, storage := range storages {
		if storage == nil || storage.Producer == nil || !storage.Producer.hasCTEResult() {
			continue
		}
		storage.ResTbl.Lock()
		err := storage.Producer.reset()
		storage.ResTbl.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
