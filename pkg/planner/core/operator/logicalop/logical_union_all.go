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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	LogicalSchemaProducer `hash64-equals:"true"`
}

// Init initializes LogicalUnionAll.
func (p LogicalUnionAll) Init(ctx base.PlanContext, offset int) *LogicalUnionAll {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeUnion, &p, offset)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan base.LogicalPlan, err error) {
	for i, proj := range p.Children() {
		newExprs := make([]expression.Expression, 0, len(predicates))
		newExprs = append(newExprs, predicates...)
		retCond, newChild, err := proj.PredicatePushDown(newExprs)
		if err != nil {
			return nil, nil, err
		}
		AddSelection(p, newChild, retCond, i)
	}
	return nil, p, nil
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	eCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	used := expression.GetUsedList(eCtx, parentUsedCols, p.Schema())
	hasBeenUsed := false
	for i := range used {
		hasBeenUsed = hasBeenUsed || used[i]
		if hasBeenUsed {
			break
		}
	}
	if !hasBeenUsed {
		parentUsedCols = make([]*expression.Column, len(p.Schema().Columns))
		copy(parentUsedCols, p.Schema().Columns)
		for i := range used {
			used[i] = true
		}
	}
	var err error
	for i, child := range p.Children() {
		p.Children()[i], err = child.PruneColumns(parentUsedCols)
		if err != nil {
			return nil, err
		}
	}

	prunedColumns := make([]*expression.Column, 0)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, p.Schema().Columns[i])
			p.Schema().Columns = slices.Delete(p.Schema().Columns, i, i+1)
		}
	}
	if hasBeenUsed {
		// It's possible that the child operator adds extra columns to the schema.
		// Currently, (*LogicalAggregation).PruneColumns() might do this.
		// But we don't need such columns, so we add an extra Projection to prune this column when this happened.
		for i, child := range p.Children() {
			if p.Schema().Len() < child.Schema().Len() {
				schema := p.Schema().Clone()
				exprs := make([]expression.Expression, len(p.Schema().Columns))
				for j, col := range schema.Columns {
					exprs[j] = col
				}
				proj := LogicalProjection{Exprs: exprs}.Init(p.SCtx(), p.QueryBlockOffset())
				proj.SetSchema(schema)

				proj.SetChildren(child)
				p.Children()[i] = proj
			}
		}
	}
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalUnionAll) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	for i, child := range p.Children() {
		var newTopN *LogicalTopN
		if topN != nil {
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset, PreferLimitToCop: topN.PreferLimitToCop}.Init(p.SCtx(), topN.QueryBlockOffset())
			for _, by := range topN.ByItems {
				newTopN.ByItems = append(newTopN.ByItems, &util.ByItems{Expr: by.Expr, Desc: by.Desc})
			}
			// newTopN to push down Union's child
		}
		p.Children()[i] = child.PushDownTopN(newTopN)
	}
	if topN != nil {
		return topN.AttachChild(p)
	}
	return p
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	reload := false
	for _, one := range reloads {
		reload = reload || one
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}
	p.SetStats(&property.StatsInfo{
		ColNDVs: make(map[int64]float64, selfSchema.Len()),
	})
	for _, childProfile := range childStats {
		p.StatsInfo().RowCount += childProfile.RowCount
		for _, col := range selfSchema.Columns {
			p.StatsInfo().ColNDVs[col.UniqueID] += childProfile.ColNDVs[col.UniqueID]
		}
	}
	return p.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD implement base.LogicalPlan.<22nd> interface.
func (p *LogicalUnionAll) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	childFDs := make([]*fd.FDSet, 0, len(p.children))
	for _, child := range p.children {
		childFD := child.ExtractFD()
		childFDs = append(childFDs, childFD)
	}
	// check the output columns' not-null property.
	res := &fd.FDSet{}
	notNullCols := intset.NewFastIntSet()
	for _, col := range p.Schema().Columns {
		notNullCols.Insert(int(col.UniqueID))
	}
	for _, childFD := range childFDs {
		notNullCols.IntersectionWith(childFD.NotNullCols)
	}
	res.MakeNotNull(notNullCols)
	// check the equivalency between children.
	equivs := fd.FindCommonEquivClasses(childFDs)
	for _, equiv := range equivs {
		res.AddEquivalenceUnion(equiv)
	}
	p.fdSet = res
	return res
}
