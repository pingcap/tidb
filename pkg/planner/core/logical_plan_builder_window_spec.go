// Copyright 2019 PingCAP, Inc.
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
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

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
