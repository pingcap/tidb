// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockstats

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/internal/exec"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
)

var _ exec.Executor = &LockExec{}
var _ exec.Executor = &UnlockExec{}

// LockExec represents a lock statistic executor.
type LockExec struct {
	exec.BaseExecutor
	Tables []*ast.TableName
}

// Next implements the Executor Next interface.
func (e *LockExec) Next(_ context.Context, _ *chunk.Chunk) error {
	do := domain.GetDomain(e.Ctx())
	is := do.InfoSchema()
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Lock Stats: handle is nil")
	}

	tableNum := len(e.Tables)
	if tableNum == 0 {
		return errors.New("Lock Stats: table should not empty ")
	}

	tids := make([]int64, 0, len(e.Tables))
	pids := make([]int64, 0)
	for _, table := range e.Tables {
		tbl, err := is.TableByName(table.Schema, table.Name)
		if err != nil {
			return err
		}
		tids = append(tids, tbl.Meta().ID)

		pi := tbl.Meta().GetPartitionInfo()
		if pi == nil {
			continue
		}
		for _, p := range pi.Definitions {
			pids = append(pids, p.ID)
		}
	}
	msg, err := h.AddLockedTables(tids, pids, e.Tables)
	if msg != "" {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
	}
	return err
}

// Close implements the Executor Close interface.
func (*LockExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*LockExec) Open(context.Context) error {
	return nil
}

// UnlockExec represents a unlock statistic executor.
type UnlockExec struct {
	exec.BaseExecutor
	Tables []*ast.TableName
}

// Next implements the Executor Next interface.
func (e *UnlockExec) Next(context.Context, *chunk.Chunk) error {
	do := domain.GetDomain(e.Ctx())
	is := do.InfoSchema()
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Unlock Stats: handle is nil")
	}

	tableNum := len(e.Tables)
	if tableNum == 0 {
		return errors.New("Unlock Stats: table should not empty ")
	}

	tids := make([]int64, 0, len(e.Tables))
	pids := make([]int64, 0)
	for _, table := range e.Tables {
		tbl, err := is.TableByName(table.Schema, table.Name)
		if err != nil {
			return err
		}
		tids = append(tids, tbl.Meta().ID)

		pi := tbl.Meta().GetPartitionInfo()
		if pi == nil {
			continue
		}
		for _, p := range pi.Definitions {
			pids = append(pids, p.ID)
		}
	}
	msg, err := h.RemoveLockedTables(tids, pids, e.Tables)
	if msg != "" {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
	}
	return err
}

// Close implements the Executor Close interface.
func (*UnlockExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*UnlockExec) Open(context.Context) error {
	return nil
}
