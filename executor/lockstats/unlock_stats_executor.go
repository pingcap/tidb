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

var _ exec.Executor = &UnlockExec{}

// UnlockExec represents a unlock statistic executor.
type UnlockExec struct {
	exec.BaseExecutor
	Tables []*ast.TableName
}

// Next implements the Executor Next interface.
func (e *UnlockExec) Next(context.Context, *chunk.Chunk) error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Unlock Stats: handle is nil")
	}
	if len(e.Tables) == 0 {
		return errors.New("Unlock Stats: table should not empty ")
	}
	is := do.InfoSchema()

	tids, pids, err := populateTableAndPartitionIDs(e.Tables, is)
	if err != nil {
		return err
	}

	msg, err := h.RemoveLockedTables(tids, pids, e.Tables)
	if err != nil {
		return err
	}
	if msg != "" {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.New(msg))
	}

	return nil
}

// Close implements the Executor Close interface.
func (*UnlockExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*UnlockExec) Open(context.Context) error {
	return nil
}
