// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ShowNextRowIDExec represents a show the next row ID executor.
type ShowNextRowIDExec struct {
	exec.BaseExecutor
	tblName *ast.TableName
	done    bool
}

var _ exec.Executor = &ShowNextRowIDExec{}

// Next implements the Executor Next interface.
func (e *ShowNextRowIDExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	tbl, err := is.TableByName(ctx, e.tblName.Schema, e.tblName.Name)
	if err != nil {
		return err
	}
	tblMeta := tbl.Meta()

	allocators := tbl.Allocators(e.Ctx().GetTableCtx())
	for _, alloc := range allocators.Allocs {
		nextGlobalID, err := alloc.NextGlobalAutoID()
		if err != nil {
			return err
		}

		var colName, idType string
		switch alloc.GetType() {
		case autoid.RowIDAllocType:
			idType = "_TIDB_ROWID"
			if tblMeta.PKIsHandle {
				if col := tblMeta.GetAutoIncrementColInfo(); col != nil {
					colName = col.Name.O
				}
			} else {
				colName = model.ExtraHandleName.O
			}
		case autoid.AutoIncrementType:
			idType = "AUTO_INCREMENT"
			if tblMeta.PKIsHandle {
				if col := tblMeta.GetAutoIncrementColInfo(); col != nil {
					colName = col.Name.O
				}
			} else {
				colName = model.ExtraHandleName.O
			}
		case autoid.AutoRandomType:
			idType = "AUTO_RANDOM"
			colName = tblMeta.GetPkName().O
		case autoid.SequenceType:
			idType = "SEQUENCE"
			colName = ""
		default:
			return autoid.ErrInvalidAllocatorType.GenWithStackByArgs()
		}

		req.AppendString(0, e.tblName.Schema.O)
		req.AppendString(1, e.tblName.Name.O)
		req.AppendString(2, colName)
		req.AppendInt64(3, nextGlobalID)
		req.AppendString(4, idType)
	}

	e.done = true
	return nil
}
