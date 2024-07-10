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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var _ exec.Executor = &UnlockExec{}

// UnlockExec represents a unlock statistic executor.
type UnlockExec struct {
	exec.BaseExecutor
	// Tables is the list of tables to be unlocked.
	// It might contain partition names if we are unlocking partitions.
	// When unlocking partitions, Tables will only contain one table name.
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

	if e.onlyUnlockPartitions() {
		table := e.Tables[0]
		tid, pidNames, err := populatePartitionIDAndNames(table, table.PartitionNames, is)
		if err != nil {
			return err
		}
		tableName := fmt.Sprintf("%s.%s", table.Schema.O, table.Name.O)
		msg, err := h.RemoveLockedPartitions(tid, tableName, pidNames)
		if err != nil {
			return err
		}
		if msg != "" {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError(msg))
		}
	} else {
		tableWithPartitions, err := populateTableAndPartitionIDs(e.Tables, is)
		if err != nil {
			return err
		}
		msg, err := h.RemoveLockedTables(tableWithPartitions)
		if err != nil {
			return err
		}
		if msg != "" {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError(msg))
		}
	}

	return nil
}

func (e *UnlockExec) onlyUnlockPartitions() bool {
	return len(e.Tables) == 1 && len(e.Tables[0].PartitionNames) > 0
}

// Close implements the Executor Close interface.
func (*UnlockExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*UnlockExec) Open(context.Context) error {
	return nil
}
