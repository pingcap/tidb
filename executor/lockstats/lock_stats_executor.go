// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
)

var _ exec.Executor = &LockExec{}

// LockExec represents a lock statistic executor.
type LockExec struct {
	exec.BaseExecutor
	Tables []*ast.TableName
}

// Next implements the Executor Next interface.
func (e *LockExec) Next(_ context.Context, _ *chunk.Chunk) error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Lock Stats: handle is nil")
	}
	if len(e.Tables) == 0 {
		return errors.New("Lock Stats: table should not empty")
	}
	is := do.InfoSchema()

	tids, pids, err := populateTableAndPartitionIDs(e.Tables, is)
	if err != nil {
		return err
	}

	sv := e.Ctx().GetSessionVars()

	msg, err := h.AddLockedTables(tids, pids, e.Tables, sv.MaxChunkSize)
	if err != nil {
		return err
	}
	if msg != "" {
		sv.StmtCtx.AppendWarning(errors.New(msg))
	}

	return nil
}

// Close implements the Executor Close interface.
func (*LockExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*LockExec) Open(context.Context) error {
	return nil
}

// populateTableAndPartitionIDs returns table IDs and partition IDs for the given table names.
func populateTableAndPartitionIDs(tables []*ast.TableName, is infoschema.InfoSchema) ([]int64, []int64, error) {
	tids := make([]int64, 0, len(tables))
	pids := make([]int64, 0)

	for _, table := range tables {
		tbl, err := is.TableByName(table.Schema, table.Name)
		if err != nil {
			return nil, nil, err
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

	return tids, pids, nil
}
