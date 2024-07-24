// Copyright 2017 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

type buildKeySolver struct{}

func (*buildKeySolver) optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	buildKeyInfo(p)
	return p, planChanged, nil
}

// buildKeyInfo recursively calls base.LogicalPlan's BuildKeyInfo method.
func buildKeyInfo(lp base.LogicalPlan) {
	for _, child := range lp.Children() {
		buildKeyInfo(child)
	}
	childSchema := make([]*expression.Schema, len(lp.Children()))
	for i, child := range lp.Children() {
		childSchema[i] = child.Schema()
	}
	lp.BuildKeyInfo(lp.Schema(), childSchema)
}

// If a condition is the form of (uniqueKey = constant) or (uniqueKey = Correlated column), it returns at most one row.
// This function will check it.
func checkMaxOneRowCond(eqColIDs map[int64]struct{}, childSchema *expression.Schema) bool {
	if len(eqColIDs) == 0 {
		return false
	}
	// We check `UniqueKeys` as well since the condition is `col = con | corr`, not `col <=> con | corr`.
	keys := make([]expression.KeyInfo, 0, len(childSchema.Keys)+len(childSchema.UniqueKeys))
	keys = append(keys, childSchema.Keys...)
	keys = append(keys, childSchema.UniqueKeys...)
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

// checkIndexCanBeKey checks whether an Index can be a Key in schema.
func checkIndexCanBeKey(idx *model.IndexInfo, columns []*model.ColumnInfo, schema *expression.Schema) (uniqueKey, newKey expression.KeyInfo) {
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

// BuildKeyInfo implements base.LogicalPlan BuildKeyInfo interface.
func (ds *DataSource) BuildKeyInfo(selfSchema *expression.Schema, _ []*expression.Schema) {
	selfSchema.Keys = nil
	var latestIndexes map[int64]*model.IndexInfo
	var changed bool
	var err error
	check := ds.SCtx().GetSessionVars().IsIsolation(ast.ReadCommitted) || ds.IsForUpdateRead
	check = check && ds.SCtx().GetSessionVars().ConnectionID > 0
	// we should check index valid while forUpdateRead, see detail in https://github.com/pingcap/tidb/pull/22152
	if check {
		latestIndexes, changed, err = getLatestIndexInfo(ds.SCtx(), ds.table.Meta().ID, 0)
		if err != nil {
			return
		}
	}
	for _, index := range ds.table.Meta().Indices {
		if ds.IsForUpdateRead && changed {
			latestIndex, ok := latestIndexes[index.ID]
			if !ok || latestIndex.State != model.StatePublic {
				continue
			}
		} else if index.State != model.StatePublic {
			continue
		}
		if uniqueKey, newKey := checkIndexCanBeKey(index, ds.Columns, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		} else if uniqueKey != nil {
			selfSchema.UniqueKeys = append(selfSchema.UniqueKeys, uniqueKey)
		}
	}
	if ds.TableInfo.PKIsHandle {
		for i, col := range ds.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				selfSchema.Keys = append(selfSchema.Keys, []*expression.Column{selfSchema.Columns[i]})
				break
			}
		}
	}
}

func (*buildKeySolver) name() string {
	return "build_keys"
}
