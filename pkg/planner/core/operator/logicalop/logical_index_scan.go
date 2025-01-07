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

package logicalop

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// LogicalIndexScan is the logical index scan operator for TiKV.
type LogicalIndexScan struct {
	LogicalSchemaProducer
	// DataSource should be read-only here.
	Source       *DataSource
	IsDoubleRead bool

	EqCondCount int
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range

	Index          *model.IndexInfo
	Columns        []*model.ColumnInfo
	FullIdxCols    []*expression.Column
	FullIdxColLens []int
	IdxCols        []*expression.Column
	IdxColLens     []int
}

// Init initializes LogicalIndexScan.
func (is LogicalIndexScan) Init(ctx base.PlanContext, offset int) *LogicalIndexScan {
	is.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeIdxScan, &is, offset)
	return &is
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (is *LogicalIndexScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(is.Source.ExplainInfo())
	index := is.Index
	if len(index.Columns) > 0 {
		buffer.WriteString(", index:")
		for i, idxCol := range index.Columns {
			if tblCol := is.Source.TableInfo.Columns[idxCol.Offset]; tblCol.Hidden {
				buffer.WriteString(tblCol.GeneratedExprString)
			} else {
				buffer.WriteString(idxCol.Name.O)
			}
			if i+1 < len(index.Columns) {
				buffer.WriteString(", ")
			}
		}
	}
	if len(is.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", is.AccessConds)
	}
	return buffer.String()
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.<0th> interface.

// PredicatePushDown inherits BaseLogicalPlan.<1st> interface.

// PruneColumns inherits BaseLogicalPlan.<2nd> interface.

// FindBestTask inherits BaseLogicalPlan.<3rd> interface.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (is *LogicalIndexScan) BuildKeyInfo(selfSchema *expression.Schema, _ []*expression.Schema) {
	selfSchema.PKOrUK = nil
	for _, path := range is.Source.PossibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		if uniqueKey, newKey := ruleutil.CheckIndexCanBeKey(path.Index, is.Columns, selfSchema); newKey != nil {
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, newKey)
		} else if uniqueKey != nil {
			selfSchema.NullableUK = append(selfSchema.NullableUK, uniqueKey)
		}
	}
	handle := is.GetPKIsHandleCol(selfSchema)
	if handle != nil {
		selfSchema.PKOrUK = append(selfSchema.PKOrUK, []*expression.Column{handle})
	}
}

// PushDownTopN inherits BaseLogicalPlan.<5th> interface.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (is *LogicalIndexScan) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema) (*property.StatsInfo, error) {
	return utilfuncp.DeriveStats4LogicalIndexScan(is, selfSchema)
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (is *LogicalIndexScan) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	if len(is.IdxCols) == 0 {
		return nil
	}
	result := make([][]*expression.Column, 0, is.EqCondCount+1)
	for i := 0; i <= is.EqCondCount; i++ {
		result = append(result, make([]*expression.Column, len(is.IdxCols)-i))
		copy(result[i], is.IdxCols[i:])
	}
	return result
}

// ExhaustPhysicalPlans inherits BaseLogicalPlan.LogicalPlan.<14th> implementation.

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// MatchIndexProp checks if the indexScan can match the required property.
func (is *LogicalIndexScan) MatchIndexProp(prop *property.PhysicalProperty) (match bool) {
	if prop.IsSortItemEmpty() {
		return true
	}
	if all, _ := prop.AllSameOrder(); !all {
		return false
	}
	sctx := is.SCtx()
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	for i, col := range is.IdxCols {
		if col.Equal(evalCtx, prop.SortItems[0].Col) {
			return matchIndicesProp(sctx, is.IdxCols[i:], is.IdxColLens[i:], prop.SortItems)
		} else if i >= is.EqCondCount {
			break
		}
	}
	return false
}

// GetPKIsHandleCol gets the handle column if PKIsHandle.
func (is *LogicalIndexScan) GetPKIsHandleCol(schema *expression.Schema) *expression.Column {
	// We cannot use p.Source.GetPKIsHandleCol() here,
	// Because we may re-prune p.Columns and p.schema during the transformation.
	// That will make p.Columns different from p.Source.Columns.
	return getPKIsHandleColFromSchema(is.Columns, schema, is.Source.TableInfo.PKIsHandle)
}

func matchIndicesProp(sctx base.PlanContext, idxCols []*expression.Column, colLens []int, propItems []property.SortItem) bool {
	if len(idxCols) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if colLens[i] != types.UnspecifiedLength || !item.Col.EqualByExprAndID(sctx.GetExprCtx().GetEvalCtx(), idxCols[i]) {
			return false
		}
	}
	return true
}
