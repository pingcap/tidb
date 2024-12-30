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
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

// LogicalWindow represents a logical window function plan.
type LogicalWindow struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	WindowFuncDescs []*aggregation.WindowFuncDesc `hash64-equals:"true"`
	PartitionBy     []property.SortItem           `hash64-equals:"true"`
	OrderBy         []property.SortItem           `hash64-equals:"true"`
	Frame           *WindowFrame                  `hash64-equals:"true"`
}

// WindowFrame represents a window function frame.
type WindowFrame struct {
	Type  ast.FrameType
	Start *FrameBound
	End   *FrameBound
}

// Hash64 implements HashEquals interface.
func (wf *WindowFrame) Hash64(h base2.Hasher) {
	h.HashInt(int(wf.Type))
	if wf.Start != nil {
		h.HashByte(base2.NotNilFlag)
		wf.Start.Hash64(h)
	} else {
		h.HashByte(base2.NilFlag)
		wf.End.Hash64(h)
	}
}

// Equals implements HashEquals interface.
func (wf *WindowFrame) Equals(other any) bool {
	wf2, ok := other.(*WindowFrame)
	if !ok {
		return false
	}
	if wf == nil {
		return wf2 == nil
	}
	if wf2 == nil {
		return false
	}
	if wf.Type != wf2.Type || !wf.Start.Equals(wf2.Start) || !wf.End.Equals(wf2.End) {
		return false
	}
	return true
}

// Clone copies a window frame totally.
func (wf *WindowFrame) Clone() *WindowFrame {
	cloned := new(WindowFrame)
	*cloned = *wf

	cloned.Start = wf.Start.Clone()
	cloned.End = wf.End.Clone()

	return cloned
}

// FrameBound is the boundary of a frame.
type FrameBound struct {
	Type      ast.BoundType
	UnBounded bool
	Num       uint64
	// CalcFuncs is used for range framed windows.
	// We will build the date_add or date_sub functions for frames like `INTERVAL '2:30' MINUTE_SECOND FOLLOWING`,
	// and plus or minus for frames like `1 preceding`.
	CalcFuncs []expression.Expression
	// Sometimes we need to cast order by column to a specific type when frame type is range
	CompareCols []expression.Expression
	// CmpFuncs is used to decide whether one row is included in the current frame.
	CmpFuncs []expression.CompareFunc
	// This field is used for passing information to tiflash
	CmpDataType tipb.RangeCmpDataType
	// IsExplicitRange marks if this range explicitly appears in the sql
	IsExplicitRange bool
}

// Hash64 implement HashEquals interface.
func (fb *FrameBound) Hash64(h base2.Hasher) {
	h.HashInt(int(fb.Type))
	h.HashBool(fb.UnBounded)
	h.HashUint64(fb.Num)
	if fb.CalcFuncs == nil {
		h.HashByte(base2.NilFlag)
	} else {
		h.HashByte(base2.NotNilFlag)
		h.HashInt(len(fb.CalcFuncs))
		for _, one := range fb.CalcFuncs {
			one.Hash64(h)
		}
	}
	if fb.CompareCols == nil {
		h.HashByte(base2.NilFlag)
	} else {
		h.HashByte(base2.NotNilFlag)
		h.HashInt(len(fb.CompareCols))
		for _, one := range fb.CompareCols {
			one.Hash64(h)
		}
	}
	if fb.CmpFuncs == nil {
		h.HashByte(base2.NilFlag)
	} else {
		h.HashByte(base2.NotNilFlag)
		h.HashInt(len(fb.CmpFuncs))
		for _, f := range fb.CmpFuncs {
			h.HashString(fmt.Sprintf("%p", f))
		}
	}
	h.HashInt64(int64(fb.CmpDataType))
	h.HashBool(fb.IsExplicitRange)
}

// Equals implement HashEquals interface.
func (fb *FrameBound) Equals(other any) bool {
	fb2, ok := other.(*FrameBound)
	if !ok {
		return false
	}
	if fb == nil {
		return fb2 == nil
	}
	if fb2 == nil {
		return false
	}
	if fb.Type != fb2.Type || fb.UnBounded != fb2.UnBounded || fb.Num != fb2.Num {
		return false
	}
	if fb.CalcFuncs == nil && fb2.CalcFuncs != nil || fb.CalcFuncs != nil && fb2.CalcFuncs == nil || len(fb.CalcFuncs) != len(fb2.CmpFuncs) {
		return false
	}
	for i, one := range fb.CalcFuncs {
		if !one.Equals(fb2.CalcFuncs[i]) {
			return false
		}
	}
	if fb.CompareCols == nil && fb2.CompareCols != nil || fb.CompareCols != nil && fb2.CompareCols == nil || len(fb.CompareCols) != len(fb2.CompareCols) {
		return false
	}
	for i, one := range fb.CompareCols {
		if !one.Equals(fb2.CompareCols[i]) {
			return false
		}
	}
	if fb.CmpFuncs == nil && fb2.CmpFuncs != nil || fb.CmpFuncs != nil && fb2.CmpFuncs == nil || len(fb.CmpFuncs) != len(fb2.CmpFuncs) {
		return false
	}
	for i, one := range fb.CmpFuncs {
		// com function addr
		if fmt.Sprintf("%p", one) != fmt.Sprintf("%p", fb2.CmpFuncs[i]) {
			return false
		}
	}
	return fb.CmpDataType == fb2.CmpDataType && fb.IsExplicitRange == fb2.IsExplicitRange
}

// Clone copies a frame bound totally.
func (fb *FrameBound) Clone() *FrameBound {
	cloned := new(FrameBound)
	*cloned = *fb

	cloned.CalcFuncs = make([]expression.Expression, 0, len(fb.CalcFuncs))
	for _, it := range fb.CalcFuncs {
		cloned.CalcFuncs = append(cloned.CalcFuncs, it.Clone())
	}
	cloned.CmpFuncs = fb.CmpFuncs

	return cloned
}

// UpdateCmpFuncsAndCmpDataType updates CmpFuncs and CmpDataType.
func (fb *FrameBound) UpdateCmpFuncsAndCmpDataType(cmpDataType types.EvalType) {
	// When cmpDataType can't match to any condition, we can ignore it.
	//
	// For example:
	//   `create table test.range_test(p int not null,o text not null,v int not null);`
	//   `select *, first_value(v) over (partition by p order by o) as a from range_test;`
	//   The sql's frame type is range, but the cmpDataType is ETString and when the user explicitly use range frame
	//   the sql will raise error before generating logical plan, so it's ok to ignore it.
	switch cmpDataType {
	case types.ETInt:
		fb.CmpFuncs[0] = expression.CompareInt
		fb.CmpDataType = tipb.RangeCmpDataType_Int
	case types.ETDatetime, types.ETTimestamp:
		fb.CmpFuncs[0] = expression.CompareTime
		fb.CmpDataType = tipb.RangeCmpDataType_DateTime
	case types.ETDuration:
		fb.CmpFuncs[0] = expression.CompareDuration
		fb.CmpDataType = tipb.RangeCmpDataType_Duration
	case types.ETReal:
		fb.CmpFuncs[0] = expression.CompareReal
		fb.CmpDataType = tipb.RangeCmpDataType_Float
	case types.ETDecimal:
		fb.CmpFuncs[0] = expression.CompareDecimal
		fb.CmpDataType = tipb.RangeCmpDataType_Decimal
	}
}

// ToPB converts FrameBound to tipb structure.
func (fb *FrameBound) ToPB(ctx *base.BuildPBContext) (*tipb.WindowFrameBound, error) {
	pbBound := &tipb.WindowFrameBound{
		Type:      tipb.WindowBoundType(fb.Type),
		Unbounded: fb.UnBounded,
	}
	offset := fb.Num
	pbBound.Offset = &offset

	if fb.IsExplicitRange {
		rangeFrame, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), fb.CalcFuncs, ctx.GetClient())
		if err != nil {
			return nil, err
		}

		pbBound.FrameRange = rangeFrame[0]
		pbBound.CmpDataType = &fb.CmpDataType
	}

	return pbBound, nil
}

// UpdateCompareCols will update CompareCols.
func (fb *FrameBound) UpdateCompareCols(ctx sessionctx.Context, orderByCols []*expression.Column) error {
	ectx := ctx.GetExprCtx().GetEvalCtx()

	if len(fb.CalcFuncs) > 0 {
		fb.CompareCols = make([]expression.Expression, len(orderByCols))
		if fb.CalcFuncs[0].GetType(ectx).EvalType() != orderByCols[0].GetType(ectx).EvalType() {
			var err error
			fb.CompareCols[0], err = expression.NewFunctionBase(ctx.GetExprCtx(), ast.Cast, fb.CalcFuncs[0].GetType(ectx), orderByCols[0])
			if err != nil {
				return err
			}
		} else {
			for i, col := range orderByCols {
				fb.CompareCols[i] = col
			}
		}

		cmpDataType := expression.GetAccurateCmpType(ctx.GetExprCtx().GetEvalCtx(), fb.CompareCols[0], fb.CalcFuncs[0])
		fb.UpdateCmpFuncsAndCmpDataType(cmpDataType)
	}
	return nil
}

// Init initializes LogicalWindow.
func (p LogicalWindow) Init(ctx base.PlanContext, offset int) *LogicalWindow {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeWindow, &p, offset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// ReplaceExprColumns implements base.LogicalPlan interface.
func (p *LogicalWindow) ReplaceExprColumns(replace map[string]*expression.Column) {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			ruleutil.ResolveExprAndReplace(arg, replace)
		}
	}
	for _, item := range p.PartitionBy {
		ruleutil.ResolveColumnAndReplace(item.Col, replace)
	}
	for _, item := range p.OrderBy {
		ruleutil.ResolveColumnAndReplace(item.Col, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalWindow) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	partitionCols := expression.NewSchema(p.GetPartitionByCols()...)
	for _, cond := range predicates {
		// We can push predicate beneath Window, only if all of the
		// extractedCols are part of partitionBy columns.
		if expression.ExprFromSchema(cond, partitionCols) {
			canBePushed = append(canBePushed, cond)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	p.BaseLogicalPlan.PredicatePushDown(canBePushed, opt)
	return canNotBePushed, p
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	windowColumns := p.GetWindowResultColumns()
	cnt := 0
	for _, col := range parentUsedCols {
		used := false
		for _, windowColumn := range windowColumns {
			if windowColumn.EqualColumn(col) {
				used = true
				break
			}
		}
		if !used {
			parentUsedCols[cnt] = col
			cnt++
		}
	}
	parentUsedCols = parentUsedCols[:cnt]
	parentUsedCols = p.extractUsedCols(parentUsedCols)
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}

	p.SetSchema(p.Children()[0].Schema().Clone())
	p.Schema().Append(windowColumns...)
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements base.LogicalPlan.<11th> interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.StatsInfo().GroupNDVs = p.GetGroupNDVs(childStats)
		return p.StatsInfo(), nil
	}
	childProfile := childStats[0]
	p.SetStats(&property.StatsInfo{
		RowCount: childProfile.RowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	childLen := selfSchema.Len() - len(p.WindowFuncDescs)
	for i := 0; i < childLen; i++ {
		id := selfSchema.Columns[i].UniqueID
		p.StatsInfo().ColNDVs[id] = childProfile.ColNDVs[id]
	}
	for i := childLen; i < selfSchema.Len(); i++ {
		p.StatsInfo().ColNDVs[selfSchema.Columns[i].UniqueID] = childProfile.RowCount
	}
	p.StatsInfo().GroupNDVs = p.GetGroupNDVs(childStats)
	return p.StatsInfo(), nil
}

// ExtractColGroups implements base.LogicalPlan.<12th> interface.
func (p *LogicalWindow) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	if len(colGroups) == 0 {
		return nil
	}
	childSchema := p.Children()[0].Schema()
	_, offsets := childSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
}

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (p *LogicalWindow) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	result := make([]*expression.Column, 0, len(p.PartitionBy)+len(p.OrderBy))
	for i := range p.PartitionBy {
		result = append(result, p.PartitionBy[i].Col)
	}
	for i := range p.OrderBy {
		result = append(result, p.OrderBy[i].Col)
	}
	return [][]*expression.Column{result}
}

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalWindow) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalWindow(p, prop)
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (p *LogicalWindow) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
	}
	return corCols
}

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

// GetPartitionBy returns partition by fields.
func (p *LogicalWindow) GetPartitionBy() []property.SortItem {
	return p.PartitionBy
}

// EqualPartitionBy checks whether two LogicalWindow.Partitions are equal.
func (p *LogicalWindow) EqualPartitionBy(newWindow *LogicalWindow) bool {
	if len(p.PartitionBy) != len(newWindow.PartitionBy) {
		return false
	}
	partitionByColsMap := make(map[int64]struct{})
	for _, item := range p.PartitionBy {
		partitionByColsMap[item.Col.UniqueID] = struct{}{}
	}
	for _, item := range newWindow.PartitionBy {
		if _, ok := partitionByColsMap[item.Col.UniqueID]; !ok {
			return false
		}
	}
	return true
}

// EqualOrderBy checks whether two LogicalWindow.OrderBys are equal.
func (p *LogicalWindow) EqualOrderBy(ctx expression.EvalContext, newWindow *LogicalWindow) bool {
	if len(p.OrderBy) != len(newWindow.OrderBy) {
		return false
	}
	for i, item := range p.OrderBy {
		if !item.Col.Equal(ctx, newWindow.OrderBy[i].Col) ||
			item.Desc != newWindow.OrderBy[i].Desc {
			return false
		}
	}
	return true
}

// EqualFrame checks whether two LogicalWindow.Frames are equal.
func (p *LogicalWindow) EqualFrame(ctx expression.EvalContext, newWindow *LogicalWindow) bool {
	if (p.Frame == nil && newWindow.Frame != nil) ||
		(p.Frame != nil && newWindow.Frame == nil) {
		return false
	}
	if p.Frame == nil && newWindow.Frame == nil {
		return true
	}
	if p.Frame.Type != newWindow.Frame.Type ||
		p.Frame.Start.Type != newWindow.Frame.Start.Type ||
		p.Frame.Start.UnBounded != newWindow.Frame.Start.UnBounded ||
		p.Frame.Start.Num != newWindow.Frame.Start.Num ||
		p.Frame.End.Type != newWindow.Frame.End.Type ||
		p.Frame.End.UnBounded != newWindow.Frame.End.UnBounded ||
		p.Frame.End.Num != newWindow.Frame.End.Num {
		return false
	}
	for i, expr := range p.Frame.Start.CalcFuncs {
		if !expr.Equal(ctx, newWindow.Frame.Start.CalcFuncs[i]) {
			return false
		}
	}
	for i, expr := range p.Frame.End.CalcFuncs {
		if !expr.Equal(ctx, newWindow.Frame.End.CalcFuncs[i]) {
			return false
		}
	}
	return true
}

// GetWindowResultColumns returns the columns storing the result of the window function.
func (p *LogicalWindow) GetWindowResultColumns() []*expression.Column {
	return p.Schema().Columns[p.Schema().Len()-len(p.WindowFuncDescs):]
}

// GetPartitionKeys gets partition keys for a logical window, it will assign column id for expressions.
func (p *LogicalWindow) GetPartitionKeys() []*property.MPPPartitionColumn {
	partitionByCols := make([]*property.MPPPartitionColumn, 0, len(p.GetPartitionByCols()))
	for _, item := range p.PartitionBy {
		partitionByCols = append(partitionByCols, &property.MPPPartitionColumn{
			Col:       item.Col,
			CollateID: property.GetCollateIDByNameForPartition(item.Col.GetStaticType().GetCollate()),
		})
	}

	return partitionByCols
}

// CheckComparisonForTiFlash check Duration vs Datetime is invalid comparison as TiFlash can't handle it so far.
func (p *LogicalWindow) CheckComparisonForTiFlash(frameBound *FrameBound) bool {
	if len(frameBound.CompareCols) > 0 {
		orderByEvalType := p.OrderBy[0].Col.GetStaticType().EvalType()
		calFuncEvalType := frameBound.CalcFuncs[0].GetType(p.SCtx().GetExprCtx().GetEvalCtx()).EvalType()

		if orderByEvalType == types.ETDuration && (calFuncEvalType == types.ETDatetime || calFuncEvalType == types.ETTimestamp) {
			return false
		} else if calFuncEvalType == types.ETDuration && (orderByEvalType == types.ETDatetime || orderByEvalType == types.ETTimestamp) {
			return false
		}
	}
	return true
}

func (p *LogicalWindow) extractUsedCols(parentUsedCols []*expression.Column) []*expression.Column {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			parentUsedCols = append(parentUsedCols, expression.ExtractColumns(arg)...)
		}
	}
	for _, by := range p.PartitionBy {
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	for _, by := range p.OrderBy {
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	return parentUsedCols
}

// GetPartitionByCols extracts 'partition by' columns from the Window.
func (p *LogicalWindow) GetPartitionByCols() []*expression.Column {
	partitionCols := make([]*expression.Column, 0, len(p.PartitionBy))
	for _, partitionItem := range p.PartitionBy {
		partitionCols = append(partitionCols, partitionItem.Col)
	}
	return partitionCols
}

// GetGroupNDVs gets the GroupNDVs of the LogicalWindow.
func (*LogicalWindow) GetGroupNDVs(childStats []*property.StatsInfo) []property.GroupNDV {
	return childStats[0].GroupNDVs
}
