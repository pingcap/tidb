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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
)

var _ Executor = &LockStatsExec{}
var _ Executor = &UnlockStatsExec{}

// LockStatsExec represents a lock statistic executor.
type LockStatsExec struct {
	baseExecutor
	Tables []*ast.TableName
}

// lockStatsVarKeyType is a dummy type to avoid naming collision in context.
type lockStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k lockStatsVarKeyType) String() string {
	return "lock_stats_var"
}

// LockStatsVarKey is a variable key for lock statistic.
const LockStatsVarKey lockStatsVarKeyType = 0

// Next implements the Executor Next interface.
func (e *LockStatsExec) Next(_ context.Context, _ *chunk.Chunk) error {
	do := domain.GetDomain(e.ctx)
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
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
	}
	return err
}

// Close implements the Executor Close interface.
func (e *LockStatsExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LockStatsExec) Open(_ context.Context) error {
	return nil
}

// UnlockStatsExec represents a unlock statistic executor.
type UnlockStatsExec struct {
	baseExecutor
	Tables []*ast.TableName
}

// unlockStatsVarKeyType is a dummy type to avoid naming collision in context.
type unlockStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k unlockStatsVarKeyType) String() string {
	return "unlock_stats_var"
}

// UnlockStatsVarKey is a variable key for unlock statistic.
const UnlockStatsVarKey unlockStatsVarKeyType = 0

// Next implements the Executor Next interface.
func (e *UnlockStatsExec) Next(_ context.Context, _ *chunk.Chunk) error {
	do := domain.GetDomain(e.ctx)
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
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
	}
	return err
}

// Close implements the Executor Close interface.
func (e *UnlockStatsExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *UnlockStatsExec) Open(_ context.Context) error {
	return nil
}
