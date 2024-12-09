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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// CheckMaxOneRowCond check if a condition is the form of (uniqueKey = constant) or (uniqueKey =
// Correlated column), it returns at most one row.
func CheckMaxOneRowCond(eqColIDs map[int64]struct{}, childSchema *expression.Schema) bool {
	if len(eqColIDs) == 0 {
		return false
	}
	// We check `UniqueKeys` as well since the condition is `col = con | corr`, not `col <=> con | corr`.
	keys := make([]expression.KeyInfo, 0, len(childSchema.PKOrUK)+len(childSchema.NullableUK))
	keys = append(keys, childSchema.PKOrUK...)
	keys = append(keys, childSchema.NullableUK...)
	var maxOneRow bool
	for _, cols := range keys {
		maxOneRow = true
		for _, c := range cols {
			if _, ok := eqColIDs[c.UniqueID]; !ok {
				maxOneRow = false
				break
			}
		}
		if maxOneRow {
			return true
		}
	}
	return false
}

// CheckIndexCanBeKey checks whether an Index can be a Key in schema.
func CheckIndexCanBeKey(idx *model.IndexInfo, columns []*model.ColumnInfo, schema *expression.Schema) (uniqueKey, newKey expression.KeyInfo) {
	if !idx.Unique {
		return nil, nil
	}
	newKeyOK := true
	uniqueKeyOK := true
	for _, idxCol := range idx.Columns {
		// The columns of this index should all occur in column schema.
		// Since null value could be duplicate in unique key. So we check NotNull flag of every column.
		findUniqueKey := false
		for i, col := range columns {
			if idxCol.Name.L == col.Name.L {
				uniqueKey = append(uniqueKey, schema.Columns[i])
				findUniqueKey = true
				if newKeyOK {
					if !mysql.HasNotNullFlag(col.GetFlag()) {
						newKeyOK = false
						break
					}
					newKey = append(newKey, schema.Columns[i])
					break
				}
			}
		}
		if !findUniqueKey {
			newKeyOK = false
			uniqueKeyOK = false
			break
		}
	}
	if newKeyOK {
		return nil, newKey
	} else if uniqueKeyOK {
		return uniqueKey, nil
	}

	return nil, nil
}

// BuildKeyInfoPortal is a hook for other packages to build key info for logical plan.
var BuildKeyInfoPortal func(lp base.LogicalPlan)
