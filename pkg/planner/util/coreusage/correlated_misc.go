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

package coreusage

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

// ExtractCorrelatedCols4LogicalPlan recursively extracts all of the correlated columns
// from a plan tree by calling base.LogicalPlan.ExtractCorrelatedCols.
func ExtractCorrelatedCols4LogicalPlan(p base.LogicalPlan) []*expression.CorrelatedColumn {
	corCols := p.ExtractCorrelatedCols()
	for _, child := range p.Children() {
		corCols = append(corCols, ExtractCorrelatedCols4LogicalPlan(child)...)
	}
	return corCols
}

// ExtractCorrelatedCols4PhysicalPlan recursively extracts all of the correlated columns
// from a plan tree by calling PhysicalPlan.ExtractCorrelatedCols.
func ExtractCorrelatedCols4PhysicalPlan(p base.PhysicalPlan) []*expression.CorrelatedColumn {
	corCols := p.ExtractCorrelatedCols()
	for _, child := range p.Children() {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// ExtractCorColumnsBySchema4LogicalPlan only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorColumnsBySchema4LogicalPlan(p base.LogicalPlan,
	schema *expression.Schema) []*expression.CorrelatedColumn {
	corCols := ExtractCorrelatedCols4LogicalPlan(p)
	return ExtractCorColumnsBySchema(corCols, schema, false)
}

// ExtractCorColumnsBySchema4PhysicalPlan only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorColumnsBySchema4PhysicalPlan(p base.PhysicalPlan,
	schema *expression.Schema) []*expression.CorrelatedColumn {
	corCols := ExtractCorrelatedCols4PhysicalPlan(p)
	return ExtractCorColumnsBySchema(corCols, schema, true)
}

// ExtractCorColumnsBySchema only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorColumnsBySchema(corCols []*expression.CorrelatedColumn,
	schema *expression.Schema, resolveIndex bool) []*expression.CorrelatedColumn {
	resultCorCols := make([]*expression.CorrelatedColumn, schema.Len())
	for _, corCol := range corCols {
		idx := schema.ColumnIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema.Columns[idx],
					Data:   new(types.Datum),
				}
			}
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	length := 0
	for _, col := range resultCorCols {
		if col != nil {
			resultCorCols[length] = col
			length++
		}
	}
	resultCorCols = resultCorCols[:length]

	if resolveIndex {
		for _, corCol := range resultCorCols {
			corCol.Index = schema.ColumnIndex(&corCol.Column)
		}
	}
	return resultCorCols
}
