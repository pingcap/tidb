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
	// Open is serialized with other lifecycle calls. Close cancels before
	// taking mu so it can interrupt a child Open running inside Next.
	closeCtx context.Context
	cancel   context.CancelFunc
	// mu protects branch Open/Close and the completed total, but not child Next.
	mu          sync.Mutex
	countOpened bool
	dataOpened  bool
	done        bool
	foundRows   uint64
}

// Open implements exec.Executor.
func (e *CalcFoundRowsExec) Open(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeCtx, e.cancel = context.WithCancel(ctx)
	e.countOpened = false
	e.dataOpened = true
	e.done = false
	e.foundRows = 0
	return exec.Open(e.closeCtx, e.Children(0))
}

// Next implements exec.Executor.
func (e *CalcFoundRowsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	e.mu.Lock()
	closedErr, done := e.closeCtx.Err(), e.done
	e.mu.Unlock()
	if closedErr != nil {
		return closedErr
	}
	if done {
		return nil
	}
	if err := exec.Next(ctx, e.Children(0), req); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.closeCtx.Err(); err != nil {
		return err
	}
	if req.NumRows() != 0 {
		return nil
	}
	if err := e.finishData(); err != nil {
		return err
	}
	failpoint.InjectCall("afterCalcFoundRowsData", e.Ctx())
	// Count is consumed and closed within this call. Data reads keep their
	// caller's context because data workers may retain it between chunks.
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(e.closeCtx, cancel)
	defer func() { stop(); cancel() }()
	if err := e.openCount(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	count := e.Children(1)
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
	return e.finishCount(uint64(chk.GetRow(0).GetInt64(0)))
}

func (e *CalcFoundRowsExec) finishData() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.closeCtx.Err(); err != nil {
		return err
	}
	e.dataOpened = false
	if err := exec.Close(e.Children(0)); err != nil {
		return err
	}
	return e.releaseDataCTEStorage()
}

func (e *CalcFoundRowsExec) openCount(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.closeCtx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.countOpened = true
	return exec.Open(ctx, e.Children(1))
}

func (e *CalcFoundRowsExec) finishCount(total uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.closeCtx.Err(); err != nil {
		return err
	}
	e.countOpened = false
	if err := exec.Close(e.Children(1)); err != nil {
		return err
	}
	if err := e.closeCtx.Err(); err != nil {
		return err
	}
	e.foundRows = total
	e.done = true
	return nil
}

// FoundRows reports the total after successful count execution.
func (e *CalcFoundRowsExec) FoundRows() (uint64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.foundRows, e.done
}

// Close implements exec.Executor.
func (e *CalcFoundRowsExec) Close() error {
	if e.cancel != nil {
		e.cancel()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
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
