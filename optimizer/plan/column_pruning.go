// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
)

func retrieveIndex(cols []*expression.Column, schema expression.Schema) {
	for _, col := range cols {
		idx := schema.GetIndex(col)
		if col.Index == -1 {
			col.Index = idx
		}
	}
}

// ColumnPruning prune unused column and find index for columns.
func ColumnPruning(p Plan) error {
	//TODO: Currently we only implement index retrieving, column pruning will be implemented later.
	var cols []*expression.Column
	switch v := p.(type) {
	case *Projection:
		for _, expr := range v.exprs {
			cols = extractColumn(expr, cols)
		}
	case *Selection:
		for _, cond := range v.Conditions {
			cols = extractColumn(cond, cols)
		}
	case *Aggregation:
		for _, aggrFunc := range v.AggFuncs {
			for _, arg := range aggrFunc.GetArgs() {
				cols = extractColumn(arg, cols)
			}
		}
		for _, expr := range v.GroupByItems {
			cols = extractColumn(expr, cols)
		}
	case *Union, *NewTableScan:
	case *Join:
		for _, eqCond := range v.EqualConditions {
			cols = extractColumn(eqCond, cols)
		}
		for _, eqCond := range v.LeftConditions {
			cols = extractColumn(eqCond, cols)
		}
		for _, eqCond := range v.RightConditions {
			cols = extractColumn(eqCond, cols)
		}
		for _, eqCond := range v.OtherConditions {
			cols = extractColumn(eqCond, cols)
		}
	default:
		return fmt.Errorf("Unknown Type %T", v)
	}
	for _, child := range p.GetChildren() {
		retrieveIndex(cols, child.GetSchema())
	}
	for _, col := range cols {
		if col.Index == -1 {
			return fmt.Errorf("Can't find column %s", col.ColName.L)
		}
	}
	for _, child := range p.GetChildren() {
		err := ColumnPruning(child)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
