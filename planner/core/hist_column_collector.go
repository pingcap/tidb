// Copyright 2021 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
)

// CollectHistColumns collects hist-needed columns from plan
func CollectHistColumns(plan LogicalPlan) []model.TableColumnID {
	colMap := map[model.TableColumnID]struct{}{}
	collectColumnsFromPlan(plan, colMap)
	histColumns := make([]model.TableColumnID, 0, len(colMap))
	for col := range colMap {
		histColumns = append(histColumns, col)
	}
	return histColumns
}

func collectColumnsFromPlan(plan LogicalPlan, neededColumns map[model.TableColumnID]struct{}) {
	for _, child := range plan.Children() {
		collectColumnsFromPlan(child, neededColumns)
	}
	switch x := plan.(type) {
	case *DataSource:
		tblID := x.TableInfo().ID
		columns := expression.ExtractColumnsFromExpressions(nil, x.pushedDownConds, nil)
		for _, col := range columns {
			tblColID := model.TableColumnID{TableID: tblID, ColumnID: col.ID}
			neededColumns[tblColID] = struct{}{}
		}
		// TODO collect idx columns?
	}
}
