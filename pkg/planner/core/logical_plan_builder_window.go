// Copyright 2015 PingCAP, Inc.
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
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func getWindowName(name string) string {
	if name == "" {
		return "<unnamed window>"
	}
	return name
}

// buildProjectionForWindow builds the projection for expressions in the window specification that is not an column,
// so after the projection, window functions only needs to deal with columns.
func (b *PlanBuilder) buildProjectionForWindow(ctx context.Context, p base.LogicalPlan, spec *ast.WindowSpec, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) (
	_ base.LogicalPlan, _, _ []property.SortItem, newArgList []expression.Expression, err error) {
	b.optFlag |= rule.FlagEliminateProjection

	var partitionItems, orderItems []*ast.ByItem
	if spec.PartitionBy != nil {
		partitionItems = spec.PartitionBy.Items
	}
	if spec.OrderBy != nil {
		orderItems = spec.OrderBy.Items
	}

	projLen := len(p.Schema().Columns) + len(partitionItems) + len(orderItems) + len(args)
	proj := logicalop.LogicalProjection{Exprs: make([]expression.Expression, 0, projLen)}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, 0, projLen)...))
	proj.SetOutputNames(make([]*types.FieldName, p.Schema().Len(), projLen))
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
		proj.Schema().Append(col)
	}
	copy(proj.OutputNames(), p.OutputNames())

	propertyItems := make([]property.SortItem, 0, len(partitionItems)+len(orderItems))
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, partitionItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	lenPartition := len(propertyItems)
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, orderItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	newArgList = make([]expression.Expression, 0, len(args))
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg.Clone())
			continue
		}
		proj.Exprs = append(proj.Exprs, newArg)
		proj.SetOutputNames(append(proj.OutputNames(), types.EmptyName))
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		}
		proj.Schema().Append(col)
		newArgList = append(newArgList, col)
	}

	proj.SetChildren(p)
	return proj, propertyItems[:lenPartition], propertyItems[lenPartition:], newArgList, nil
}

func (b *PlanBuilder) buildArgs4WindowFunc(ctx context.Context, p base.LogicalPlan, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) ([]expression.Expression, error) {
	b.optFlag |= rule.FlagEliminateProjection

	newArgList := make([]expression.Expression, 0, len(args))
	// use below index for created a new col definition
	// it's okay here because we only want to return the args used in window function
	newColIndex := 0
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg.Clone())
			continue
		}
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		}
		newColIndex++
		newArgList = append(newArgList, col)
	}
	return newArgList, nil
}

func (b *PlanBuilder) buildByItemsForWindow(
	ctx context.Context,
	p base.LogicalPlan,
	proj *logicalop.LogicalProjection,
	items []*ast.ByItem,
	retItems []property.SortItem,
	aggMap map[*ast.AggregateFuncExpr]int,
) (base.LogicalPlan, []property.SortItem, error) {
	transformer := &itemTransformer{}
	for _, item := range items {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewrite(ctx, item.Expr, p, aggMap, true)
		if err != nil {
			return nil, nil, err
		}
		p = np
		if it.GetType(b.ctx.GetExprCtx().GetEvalCtx()).GetType() == mysql.TypeNull {
			continue
		}
		if col, ok := it.(*expression.Column); ok {
			retItems = append(retItems, property.SortItem{Col: col, Desc: item.Desc})
			// We need to attempt to add this column because a subquery may be created during the expression rewrite process.
			// Therefore, we need to ensure that the column from the newly created query plan is added.
			// If the column is already in the schema, we don't need to add it again.
			if !proj.Schema().Contains(col) {
				proj.Exprs = append(proj.Exprs, col)
				proj.SetOutputNames(append(proj.OutputNames(), types.EmptyName))
				proj.Schema().Append(col)
			}
			continue
		}
		proj.Exprs = append(proj.Exprs, it)
		proj.SetOutputNames(append(proj.OutputNames(), types.EmptyName))
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  it.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		}
		proj.Schema().Append(col)
		retItems = append(retItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return p, retItems, nil
}

// buildWindowFunctionFrameBound builds the bounds of window function frames.
// For type `Rows`, the bound expr must be an unsigned integer.
// For type `Range`, the bound expr must be temporal or numeric types.
func (b *PlanBuilder) buildWindowFunctionFrameBound(_ context.Context, spec *ast.WindowSpec, orderByItems []property.SortItem, boundClause *ast.FrameBound) (*logicalop.FrameBound, error) {
	frameType := spec.Frame.Type
	bound := &logicalop.FrameBound{Type: boundClause.Type, UnBounded: boundClause.UnBounded, IsExplicitRange: false}
	if bound.UnBounded {
		return bound, nil
	}

	if frameType == ast.Rows {
		if bound.Type == ast.CurrentRow {
			return bound, nil
		}
		numRows, _, _ := getUintFromNode(b.ctx.GetExprCtx(), boundClause.Expr, false)
		bound.Num = numRows
		return bound, nil
	}

	bound.CalcFuncs = make([]expression.Expression, len(orderByItems))
	bound.CmpFuncs = make([]expression.CompareFunc, len(orderByItems))
	if bound.Type == ast.CurrentRow {
		for i, item := range orderByItems {
			col := item.Col
			bound.CalcFuncs[i] = col
			bound.CmpFuncs[i] = expression.GetCmpFunction(b.ctx.GetExprCtx(), col, col)
		}
		return bound, nil
	}

	col := orderByItems[0].Col
	// TODO: We also need to raise error for non-deterministic expressions, like rand().
	val, err := evalAstExprWithPlanCtx(b.ctx, boundClause.Expr)
	if err != nil {
		return nil, plannererrors.ErrWindowRangeBoundNotConstant.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	expr := expression.Constant{Value: val, RetType: boundClause.Expr.GetType()}

	checker := &expression.ParamMarkerInPrepareChecker{}
	boundClause.Expr.Accept(checker)

	// If it has paramMarker and is in prepare stmt. We don't need to eval it since its value is not decided yet.
	if !checker.InPrepareStmt {
		// Do not raise warnings for truncate.
		exprCtx := exprctx.CtxWithHandleTruncateErrLevel(b.ctx.GetExprCtx(), errctx.LevelIgnore)
		uVal, isNull, err := expr.EvalInt(exprCtx.GetEvalCtx(), chunk.Row{})
		if uVal < 0 || isNull || err != nil {
			return nil, plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
	}

	bound.IsExplicitRange = true
	desc := orderByItems[0].Desc
	var funcName string
	if boundClause.Unit != ast.TimeUnitInvalid {
		// TODO: Perhaps we don't need to transcode this back to generic string
		unitVal := boundClause.Unit.String()
		unit := expression.Constant{
			Value:   types.NewStringDatum(unitVal),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}

		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName = ast.DateAdd
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			funcName = ast.DateSub
		}

		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx.GetExprCtx(), funcName, col.RetType, col, &expr, &unit)
		if err != nil {
			return nil, err
		}
	} else {
		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName = ast.Plus
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			funcName = ast.Minus
		}

		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx.GetExprCtx(), funcName, col.RetType, col, &expr)
		if err != nil {
			return nil, err
		}
	}

	cmpDataType := expression.GetAccurateCmpType(b.ctx.GetExprCtx().GetEvalCtx(), col, bound.CalcFuncs[0])
	bound.UpdateCmpFuncsAndCmpDataType(cmpDataType)
	return bound, nil
}

// buildWindowFunctionFrame builds the window function frames.
// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
func (b *PlanBuilder) buildWindowFunctionFrame(ctx context.Context, spec *ast.WindowSpec, orderByItems []property.SortItem) (*logicalop.WindowFrame, error) {
	frameClause := spec.Frame
	if frameClause == nil {
		return nil, nil
	}
	frame := &logicalop.WindowFrame{Type: frameClause.Type}
	var err error
	frame.Start, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.Start)
	if err != nil {
		return nil, err
	}
	frame.End, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.End)
	return frame, err
}

func (b *PlanBuilder) checkWindowFuncArgs(ctx context.Context, p base.LogicalPlan, windowFuncExprs []*ast.WindowFuncExpr, windowAggMap map[*ast.AggregateFuncExpr]int) error {
	checker := &expression.ParamMarkerInPrepareChecker{}
	for _, windowFuncExpr := range windowFuncExprs {
		if strings.ToLower(windowFuncExpr.Name) == ast.AggFuncGroupConcat {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("group_concat as window function")
		}
		args, err := b.buildArgs4WindowFunc(ctx, p, windowFuncExpr.Args, windowAggMap)
		if err != nil {
			return err
		}
		checker.InPrepareStmt = false
		for _, expr := range windowFuncExpr.Args {
			expr.Accept(checker)
		}
		desc, err := aggregation.NewWindowFuncDesc(b.ctx.GetExprCtx(), windowFuncExpr.Name, args, checker.InPrepareStmt)
		if err != nil {
			return err
		}
		if desc == nil {
			return plannererrors.ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFuncExpr.Name))
		}
	}
	return nil
}

func getAllByItems(itemsBuf []*ast.ByItem, spec *ast.WindowSpec) []*ast.ByItem {
	itemsBuf = itemsBuf[:0]
	if spec.PartitionBy != nil {
		itemsBuf = append(itemsBuf, spec.PartitionBy.Items...)
	}
	if spec.OrderBy != nil {
		itemsBuf = append(itemsBuf, spec.OrderBy.Items...)
	}
	return itemsBuf
}

func restoreByItemText(item *ast.ByItem) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	err := item.Expr.Restore(ctx)
	if err != nil {
		return ""
	}
	return sb.String()
}

func compareItems(lItems []*ast.ByItem, rItems []*ast.ByItem) bool {
	minLen := min(len(lItems), len(rItems))
	for i := range minLen {
		res := strings.Compare(restoreByItemText(lItems[i]), restoreByItemText(rItems[i]))
		if res != 0 {
			return res < 0
		}
		res = compareBool(lItems[i].Desc, rItems[i].Desc)
		if res != 0 {
			return res < 0
		}
	}
	return len(lItems) < len(rItems)
}

type windowFuncs struct {
	spec  *ast.WindowSpec
	funcs []*ast.WindowFuncExpr
}

// sortWindowSpecs sorts the window specifications by reversed alphabetical order, then we could add less `Sort` operator
// in physical plan because the window functions with the same partition by and order by clause will be at near places.
func sortWindowSpecs(groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, orderedSpec []*ast.WindowSpec) []windowFuncs {
	windows := make([]windowFuncs, 0, len(groupedFuncs))
	for _, spec := range orderedSpec {
		windows = append(windows, windowFuncs{spec, groupedFuncs[spec]})
	}
	lItemsBuf := make([]*ast.ByItem, 0, 4)
	rItemsBuf := make([]*ast.ByItem, 0, 4)
	sort.SliceStable(windows, func(i, j int) bool {
		lItemsBuf = getAllByItems(lItemsBuf, windows[i].spec)
		rItemsBuf = getAllByItems(rItemsBuf, windows[j].spec)
		return !compareItems(lItemsBuf, rItemsBuf)
	})
	return windows
}

func (b *PlanBuilder) buildWindowFunctions(ctx context.Context, p base.LogicalPlan, groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, orderedSpec []*ast.WindowSpec, aggMap map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, map[*ast.WindowFuncExpr]int, error) {
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containRecursiveForbiddenOperator = true
	}
	args := make([]ast.ExprNode, 0, 4)
	windowMap := make(map[*ast.WindowFuncExpr]int)
	for _, window := range sortWindowSpecs(groupedFuncs, orderedSpec) {
		args = args[:0]
		spec, funcs := window.spec, window.funcs
		for _, windowFunc := range funcs {
			args = append(args, windowFunc.Args...)
		}
		np, partitionBy, orderBy, args, err := b.buildProjectionForWindow(ctx, p, spec, args, aggMap)
		if err != nil {
			return nil, nil, err
		}
		if len(funcs) == 0 {
			// len(funcs) == 0 indicates this an unused named window spec,
			// so we just check for its validity and don't have to build plan for it.
			err := b.checkOriginWindowSpec(spec, orderBy)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		err = b.checkOriginWindowFuncs(funcs, orderBy)
		if err != nil {
			return nil, nil, err
		}
		frame, err := b.buildWindowFunctionFrame(ctx, spec, orderBy)
		if err != nil {
			return nil, nil, err
		}

		window := logicalop.LogicalWindow{
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Frame:       frame,
		}.Init(b.ctx, b.getSelectOffset())
		window.SetOutputNames(make([]*types.FieldName, np.Schema().Len()))
		copy(window.OutputNames(), np.OutputNames())
		schema := np.Schema().Clone()
		descs := make([]*aggregation.WindowFuncDesc, 0, len(funcs))
		preArgs := 0
		checker := &expression.ParamMarkerInPrepareChecker{}
		for _, windowFunc := range funcs {
			checker.InPrepareStmt = false
			for _, expr := range windowFunc.Args {
				expr.Accept(checker)
			}
			desc, err := aggregation.NewWindowFuncDesc(b.ctx.GetExprCtx(), windowFunc.Name, args[preArgs:preArgs+len(windowFunc.Args)], checker.InPrepareStmt)
			if err != nil {
				return nil, nil, err
			}
			if desc == nil {
				return nil, nil, plannererrors.ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFunc.Name))
			}
			preArgs += len(windowFunc.Args)
			desc.WrapCastForAggArgs(b.ctx.GetExprCtx())
			descs = append(descs, desc)
			windowMap[windowFunc] = schema.Len()
			schema.Append(&expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  desc.RetTp,
			})
			window.SetOutputNames(append(window.OutputNames(), types.EmptyName))
		}
		window.WindowFuncDescs = descs
		window.SetChildren(np)
		window.SetSchema(schema)
		p = window
	}
	return p, windowMap, nil
}

// checkOriginWindowFuncs checks the validity for original window specifications for a group of functions.
// Because the grouped specification is different from them, we should especially check them before build window frame.
func (b *PlanBuilder) checkOriginWindowFuncs(funcs []*ast.WindowFuncExpr, orderByItems []property.SortItem) error {
	for _, f := range funcs {
		if f.IgnoreNull {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("IGNORE NULLS")
		}
		if f.Distinct {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("<window function>(DISTINCT ..)")
		}
		if f.FromLast {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("FROM LAST")
		}
		spec := &f.Spec
		if f.Spec.Name.L != "" {
			spec = b.windowSpecs[f.Spec.Name.L]
		}
		if err := b.checkOriginWindowSpec(spec, orderByItems); err != nil {
			return err
		}
	}
	return nil
}

// checkOriginWindowSpec checks the validity for given window specification.
func (b *PlanBuilder) checkOriginWindowSpec(spec *ast.WindowSpec, orderByItems []property.SortItem) error {
	if spec.Frame == nil {
		return nil
	}
	if spec.Frame.Type == ast.Groups {
		return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("GROUPS")
	}
	start, end := spec.Frame.Extent.Start, spec.Frame.Extent.End
	if start.Type == ast.Following && start.UnBounded {
		return plannererrors.ErrWindowFrameStartIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if end.Type == ast.Preceding && end.UnBounded {
		return plannererrors.ErrWindowFrameEndIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if start.Type == ast.Following && (end.Type == ast.Preceding || end.Type == ast.CurrentRow) {
		return plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if (start.Type == ast.Following || start.Type == ast.CurrentRow) && end.Type == ast.Preceding {
		return plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}

	err := b.checkOriginWindowFrameBound(&start, spec, orderByItems)
	if err != nil {
		return err
	}
	err = b.checkOriginWindowFrameBound(&end, spec, orderByItems)
	if err != nil {
		return err
	}
	return nil
}

func (b *PlanBuilder) checkOriginWindowFrameBound(bound *ast.FrameBound, spec *ast.WindowSpec, orderByItems []property.SortItem) error {
	if bound.Type == ast.CurrentRow || bound.UnBounded {
		return nil
	}

	frameType := spec.Frame.Type
	if frameType == ast.Rows {
		if bound.Unit != ast.TimeUnitInvalid {
			return plannererrors.ErrWindowRowsIntervalUse.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		_, isNull, isExpectedType := getUintFromNode(b.ctx.GetExprCtx(), bound.Expr, false)
		if isNull || !isExpectedType {
			return plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		return nil
	}

	if len(orderByItems) != 1 {
		return plannererrors.ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	orderItemType := orderByItems[0].Col.RetType.GetType()
	isNumeric, isTemporal := types.IsTypeNumeric(orderItemType), types.IsTypeTemporal(orderItemType)
	if !isNumeric && !isTemporal {
		return plannererrors.ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit != ast.TimeUnitInvalid && !isTemporal {
		return plannererrors.ErrWindowRangeFrameNumericType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit == ast.TimeUnitInvalid && !isNumeric {
		return plannererrors.ErrWindowRangeFrameTemporalType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	return nil
}

func extractWindowFuncs(fields []*ast.SelectField) []*ast.WindowFuncExpr {
	extractor := &WindowFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.windowFuncs
}

func (b *PlanBuilder) handleDefaultFrame(spec *ast.WindowSpec, windowFuncName string) (*ast.WindowSpec, bool) {
	needFrame := aggregation.NeedFrame(windowFuncName)
	// According to MySQL, In the absence of a frame clause, the default frame depends on whether an ORDER BY clause is present:
	//   (1) With order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
	//   (2) Without order by, the default frame is includes all partition rows, equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
	//       or "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", which is the same as an empty frame.
	// https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
	if needFrame && spec.Frame == nil && spec.OrderBy != nil {
		newSpec := *spec
		newSpec.Frame = &ast.FrameClause{
			Type: ast.Ranges,
			Extent: ast.FrameExtent{
				Start: ast.FrameBound{Type: ast.Preceding, UnBounded: true},
				End:   ast.FrameBound{Type: ast.CurrentRow},
			},
		}
		return &newSpec, true
	}
	// "RANGE/ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" is equivalent to empty frame.
	if needFrame && spec.Frame != nil &&
		spec.Frame.Extent.Start.UnBounded && spec.Frame.Extent.End.UnBounded {
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	if !needFrame {
		var updated bool
		newSpec := *spec

		// For functions that operate on the entire partition, the frame clause will be ignored.
		if spec.Frame != nil {
			specName := spec.Name.O
			b.ctx.GetSessionVars().StmtCtx.AppendNote(plannererrors.ErrWindowFunctionIgnoresFrame.FastGenByArgs(windowFuncName, getWindowName(specName)))
			newSpec.Frame = nil
			updated = true
		}
		if b.ctx.GetSessionVars().EnablePipelinedWindowExec {
			useDefaultFrame, defaultFrame := aggregation.UseDefaultFrame(windowFuncName)
			if useDefaultFrame {
				newSpec.Frame = &defaultFrame
				updated = true
			}
		}
		if updated {
			return &newSpec, true
		}
	}
	return spec, false
}

// append ast.WindowSpec to []*ast.WindowSpec if absent
func appendIfAbsentWindowSpec(specs []*ast.WindowSpec, ns *ast.WindowSpec) []*ast.WindowSpec {
	if slices.Contains(specs, ns) {
		return specs
	}
	return append(specs, ns)
}

func specEqual(s1, s2 *ast.WindowSpec) (equal bool, err error) {
	if (s1 == nil && s2 != nil) || (s1 != nil && s2 == nil) {
		return false, nil
	}
	var sb1, sb2 strings.Builder
	ctx1 := format.NewRestoreCtx(0, &sb1)
	ctx2 := format.NewRestoreCtx(0, &sb2)
	if err = s1.Restore(ctx1); err != nil {
		return
	}
	if err = s2.Restore(ctx2); err != nil {
		return
	}
	return sb1.String() == sb2.String(), nil
}

// groupWindowFuncs groups the window functions according to the window specification name.
// TODO: We can group the window function by the definition of window specification.
func (b *PlanBuilder) groupWindowFuncs(windowFuncs []*ast.WindowFuncExpr) (map[*ast.WindowSpec][]*ast.WindowFuncExpr, []*ast.WindowSpec, error) {
	// updatedSpecMap is used to handle the specifications that have frame clause changed.
	updatedSpecMap := make(map[string][]*ast.WindowSpec)
	groupedWindow := make(map[*ast.WindowSpec][]*ast.WindowFuncExpr)
	orderedSpec := make([]*ast.WindowSpec, 0, len(windowFuncs))
	for _, windowFunc := range windowFuncs {
		if windowFunc.Spec.Name.L == "" {
			spec := &windowFunc.Spec
			if spec.Ref.L != "" {
				ref, ok := b.windowSpecs[spec.Ref.L]
				if !ok {
					return nil, nil, plannererrors.ErrWindowNoSuchWindow.GenWithStackByArgs(getWindowName(spec.Ref.O))
				}
				err := mergeWindowSpec(spec, ref)
				if err != nil {
					return nil, nil, err
				}
			}
			spec, _ = b.handleDefaultFrame(spec, windowFunc.Name)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
			continue
		}

		name := windowFunc.Spec.Name.L
		spec, ok := b.windowSpecs[name]
		if !ok {
			return nil, nil, plannererrors.ErrWindowNoSuchWindow.GenWithStackByArgs(windowFunc.Spec.Name.O)
		}
		newSpec, updated := b.handleDefaultFrame(spec, windowFunc.Name)
		if !updated {
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
		} else {
			var updatedSpec *ast.WindowSpec
			if _, ok := updatedSpecMap[name]; !ok {
				updatedSpecMap[name] = []*ast.WindowSpec{newSpec}
				updatedSpec = newSpec
			} else {
				for _, spec := range updatedSpecMap[name] {
					eq, err := specEqual(spec, newSpec)
					if err != nil {
						return nil, nil, err
					}
					if eq {
						updatedSpec = spec
						break
					}
				}
				if updatedSpec == nil {
					updatedSpec = newSpec
					updatedSpecMap[name] = append(updatedSpecMap[name], newSpec)
				}
			}
			groupedWindow[updatedSpec] = append(groupedWindow[updatedSpec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, updatedSpec)
		}
	}
	// Unused window specs should also be checked in b.buildWindowFunctions,
	// so we add them to `groupedWindow` with empty window functions.
	for _, spec := range b.windowSpecs {
		if _, ok := groupedWindow[spec]; !ok {
			if _, ok = updatedSpecMap[spec.Name.L]; !ok {
				groupedWindow[spec] = nil
				orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
			}
		}
	}
	return groupedWindow, orderedSpec, nil
}

// resolveWindowSpec resolve window specifications for sql like `select ... from t window w1 as (w2), w2 as (partition by a)`.
// We need to resolve the referenced window to get the definition of current window spec.
func resolveWindowSpec(spec *ast.WindowSpec, specs map[string]*ast.WindowSpec, inStack map[string]bool) error {
	if inStack[spec.Name.L] {
		return errors.Trace(plannererrors.ErrWindowCircularityInWindowGraph)
	}
	if spec.Ref.L == "" {
		return nil
	}
	ref, ok := specs[spec.Ref.L]
	if !ok {
		return plannererrors.ErrWindowNoSuchWindow.GenWithStackByArgs(spec.Ref.O)
	}
	inStack[spec.Name.L] = true
	err := resolveWindowSpec(ref, specs, inStack)
	if err != nil {
		return err
	}
	inStack[spec.Name.L] = false
	return mergeWindowSpec(spec, ref)
}

func mergeWindowSpec(spec, ref *ast.WindowSpec) error {
	if ref.Frame != nil {
		return plannererrors.ErrWindowNoInherentFrame.GenWithStackByArgs(ref.Name.O)
	}
	if spec.PartitionBy != nil {
		return errors.Trace(plannererrors.ErrWindowNoChildPartitioning)
	}
	if ref.OrderBy != nil {
		if spec.OrderBy != nil {
			return plannererrors.ErrWindowNoRedefineOrderBy.GenWithStackByArgs(getWindowName(spec.Name.O), ref.Name.O)
		}
		spec.OrderBy = ref.OrderBy
	}
	spec.PartitionBy = ref.PartitionBy
	spec.Ref = ast.NewCIStr("")
	return nil
}

func buildWindowSpecs(specs []ast.WindowSpec) (map[string]*ast.WindowSpec, error) {
	specsMap := make(map[string]*ast.WindowSpec, len(specs))
	for _, spec := range specs {
		if _, ok := specsMap[spec.Name.L]; ok {
			return nil, plannererrors.ErrWindowDuplicateName.GenWithStackByArgs(spec.Name.O)
		}
		newSpec := spec
		specsMap[spec.Name.L] = &newSpec
	}
	inStack := make(map[string]bool, len(specs))
	for _, spec := range specsMap {
		err := resolveWindowSpec(spec, specsMap, inStack)
		if err != nil {
			return nil, err
		}
	}
	return specsMap, nil
}
