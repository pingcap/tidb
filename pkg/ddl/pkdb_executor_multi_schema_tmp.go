// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

func pkdbUpdateMultiSchemaTmpTableInfo(sctx sessionctx.Context, tmpTbl *model.TableInfo, prevSubJobCnt int) {
	if tmpTbl == nil {
		return
	}

	mci := sctx.GetSessionVars().StmtCtx.MultiSchemaInfo
	if mci == nil || len(mci.SubJobs) <= prevSubJobCnt {
		return
	}

	for _, sub := range mci.SubJobs[prevSubJobCnt:] {
		switch sub.Type {
		case model.ActionAddColumn:
			args := sub.JobArgs.(*model.TableColumnArgs)
			colInfo := args.Col.Clone()
			// Allocate a temporary offset so later specs (index/PK build) can resolve it.
			colInfo.Offset = len(tmpTbl.Columns)
			colInfo.State = model.StatePublic
			tmpTbl.Columns = append(tmpTbl.Columns, colInfo)
		case model.ActionModifyColumn:
			args := sub.JobArgs.(*model.ModifyColumnArgs)
			// Update the in-memory definition so later specs can validate against it (e.g. NOT NULL).
			for i, c := range tmpTbl.Columns {
				if c.Name.L != args.OldColumnName.L {
					continue
				}
				colInfo := args.Column.Clone()
				colInfo.Offset = c.Offset
				colInfo.State = model.StatePublic
				tmpTbl.Columns[i] = colInfo
				break
			}
		}
	}
}
