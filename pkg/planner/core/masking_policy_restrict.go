// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (b *PlanBuilder) checkMaskingPolicyRestrictOnSelectPlan(ctx context.Context, p base.LogicalPlan, op ast.MaskingPolicyRestrictOps) error {
	if b.is == nil || p == nil || op == ast.MaskingPolicyRestrictOpNone {
		return nil
	}
	hasPolicies, err := b.maskingEnabled(ctx, p)
	if err != nil || !hasPolicies {
		return err
	}
	cols := p.Schema().Columns
	if len(cols) == 0 {
		return nil
	}

	sv := b.ctx.GetSessionVars()
	schemaVersion := b.is.SchemaMetaVersion()
	checkedPolicyIDs := make(map[int64]struct{})
	for i := range cols {
		for _, source := range b.extractMaskingPolicySourceColumnsFromOutputColumn(p, i) {
			tblInfo, colInfo := source.tblInfo, source.colInfo
			policy, ok, err := b.is.MaskingPolicyByTableColumn(ctx, tblInfo.ID, colInfo.ID)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if policy == nil || policy.Status != model.MaskingPolicyStatusEnable {
				continue
			}
			if policy.RestrictOps&op == 0 {
				continue
			}
			if _, ok := checkedPolicyIDs[policy.ID]; ok {
				continue
			}
			allowed, err := canCurrentSessionReadUnmaskedColumn(b.ctx.GetExprCtx(), sv, schemaVersion, policy, tblInfo, colInfo)
			if err != nil {
				return err
			}
			if !allowed {
				return plannererrors.ErrAccessDeniedToMaskedColumn.GenWithStackByArgs(colInfo.Name.O)
			}
			sv.StmtCtx.MaskingPolicyChecks = append(sv.StmtCtx.MaskingPolicyChecks,
				stmtctx.MaskingPolicyCheck{PolicyID: policy.ID, TableID: tblInfo.ID, ColumnID: colInfo.ID, Operation: op})
			checkedPolicyIDs[policy.ID] = struct{}{}
		}
	}
	return nil
}

// maskingPolicySourceColumn is a physical table column that a plan output is
// computed from.
type maskingPolicySourceColumn struct {
	tblInfo *model.TableInfo
	colInfo *model.ColumnInfo
}

func maskingScalarJoin(p base.LogicalPlan) *logicalop.LogicalJoin {
	var join *logicalop.LogicalJoin
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		join = x
	case *logicalop.LogicalApply:
		join = &x.LogicalJoin
	default:
		return nil
	}
	if join.JoinType != logicalop.LeftOuterSemiJoin && join.JoinType != logicalop.AntiLeftOuterSemiJoin {
		return nil
	}
	return join
}

// maskingScalarJoinInputs identifies the synthetic boolean produced by a
// scalar IN/ANY subquery. It has no corresponding column in either child;
// its source columns live in the join's comparison conditions instead.
func maskingScalarJoinInputs(p base.LogicalPlan, idx int) ([]expression.Expression, bool) {
	join := maskingScalarJoin(p)
	if join == nil || idx != p.Schema().Len()-1 {
		return nil, false
	}
	inputs := expression.ScalarFuncs2Exprs(join.EqualConditions)
	inputs = append(inputs, expression.ScalarFuncs2Exprs(join.NAEQConditions)...)
	inputs = append(inputs, join.LeftConditions...)
	inputs = append(inputs, join.RightConditions...)
	inputs = append(inputs, join.OtherConditions...)
	return inputs, true
}

// extractMaskingPolicySourceColumnsFromOutputColumn returns the table columns
// that the output column reads. The columns are those bound to the scans, so
// an alias, a CTE name or another table that shares a name with a protected
// table is never taken for it.
func (b *PlanBuilder) extractMaskingPolicySourceColumnsFromOutputColumn(
	p base.LogicalPlan,
	outputColIdx int,
) []maskingPolicySourceColumn {
	return b.extractMaskingPolicySourceColumns(p, outputColIdx, nil)
}

// extractMaskingPolicySourceColumns returns the table columns read by an
// output column of p or, when stored is not nil, by that expression over p.
// For stored, a column that a masking projection has already masked is not a
// source: the expression receives its masked value.
func (b *PlanBuilder) extractMaskingPolicySourceColumns(
	p base.LogicalPlan,
	outputColIdx int,
	stored expression.Expression,
) []maskingPolicySourceColumn {
	sources := make([]maskingPolicySourceColumn, 0, 2)
	seen := make(map[[2]int64]struct{}, 2)
	appendSource := func(ds *logicalop.DataSource, col *expression.Column) {
		if ds.TableInfo == nil || ds.TableInfo.TempTableType != model.TempTableNone {
			return
		}
		colInfo := model.FindColumnInfoByID(ds.TableInfo.Columns, col.ID)
		if colInfo == nil || colInfo.State != model.StatePublic {
			return
		}
		key := [2]int64{ds.TableInfo.ID, colInfo.ID}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		sources = append(sources, maskingPolicySourceColumn{tblInfo: ds.TableInfo, colInfo: colInfo})
	}

	visited := make(map[[2]int]struct{}, 4)
	var walkByIndex func(plan base.LogicalPlan, colIdx int)
	var walkByColumn func(plan base.LogicalPlan, col *expression.Column)
	walkExpression := func(plan base.LogicalPlan, expr expression.Expression) {
		for _, col := range expression.ExtractColumns(expr) {
			walkByColumn(plan, col)
		}
		for _, col := range expression.ExtractCorColumns(expr) {
			for _, outer := range b.maskingOuterPlans {
				walkByColumn(outer, &col.Column)
			}
		}
	}

	walkByIndex = func(plan base.LogicalPlan, colIdx int) {
		if plan == nil || colIdx < 0 || colIdx >= plan.Schema().Len() {
			return
		}
		visitKey := [2]int{plan.ID(), colIdx}
		if _, ok := visited[visitKey]; ok {
			return
		}
		visited[visitKey] = struct{}{}
		if source, ok := b.maskingSourcePlans[plan.ID()]; ok {
			if stored != nil {
				if proj, ok := plan.(*logicalop.LogicalProjection); ok && colIdx < len(proj.Exprs) {
					if _, raw := proj.Exprs[colIdx].(*expression.Column); !raw {
						return
					}
				}
			}
			walkByIndex(source, colIdx)
			return
		}
		if inputs, generated := maskingScalarJoinInputs(plan, colIdx); generated {
			for _, input := range inputs {
				walkExpression(plan, input)
			}
			return
		}

		if ds, ok := plan.(*logicalop.DataSource); ok {
			appendSource(ds, plan.Schema().Columns[colIdx])
			return
		}

		if proj, ok := plan.(*logicalop.LogicalProjection); ok && colIdx < len(proj.Exprs) && len(proj.Children()) > 0 {
			walkExpression(proj.Children()[0], proj.Exprs[colIdx])
			return
		}
		switch x := plan.(type) {
		case *logicalop.LogicalAggregation:
			for _, arg := range x.AggFuncs[colIdx].Args {
				walkExpression(x.Children()[0], arg)
			}
			return
		case *logicalop.LogicalWindow:
			child := x.Children()[0]
			if colIdx >= child.Schema().Len() {
				for _, arg := range x.WindowFuncDescs[colIdx-child.Schema().Len()].Args {
					walkExpression(child, arg)
				}
				return
			}
		case *logicalop.LogicalUnionAll:
			for _, child := range x.Children() {
				walkByIndex(child, colIdx)
			}
			return
		case *logicalop.LogicalCTE:
			walkByIndex(x.Cte.SeedPartLogicalPlan, colIdx)
			walkByIndex(x.Cte.RecursivePartLogicalPlan, colIdx)
			return
		}

		outputCol := plan.Schema().Columns[colIdx]
		for _, child := range plan.Children() {
			if child == nil {
				continue
			}
			childColIdx := child.Schema().ColumnIndex(outputCol)
			if childColIdx >= 0 {
				walkByIndex(child, childColIdx)
			}
		}
	}

	walkByColumn = func(plan base.LogicalPlan, col *expression.Column) {
		if plan == nil || col == nil {
			return
		}
		colIdx := plan.Schema().ColumnIndex(col)
		if colIdx >= 0 {
			walkByIndex(plan, colIdx)
			return
		}
		for _, child := range plan.Children() {
			if child != nil {
				walkByColumn(child, col)
			}
		}
	}

	if stored != nil {
		walkExpression(p, stored)
		return sources
	}
	walkByIndex(p, outputColIdx)
	return sources
}

// checkMaskingPolicyStateWrite rejects an expression that keeps the value of
// stored in session state, such as a user variable assignment, when stored
// reads a column that is masked for the current session. Masking applies to
// the statement result only, so a later statement could read the original
// value back from that state. In ON DUPLICATE KEY UPDATE, the columns of
// insertPlan's table are read from the existing row rather than from p.
func (b *PlanBuilder) checkMaskingPolicyStateWrite(ctx context.Context, p base.LogicalPlan, insertPlan *Insert, stored expression.Expression, shape string) error {
	cols := expression.ExtractColumns(stored)
	sv := b.ctx.GetSessionVars()
	if b.is == nil || p == nil || sv.InRestrictedSQL || len(cols) == 0 && len(expression.ExtractCorColumns(stored)) == 0 {
		return nil
	}
	var sources []maskingPolicySourceColumn
	if enabled, err := b.maskingEnabled(ctx, p); err != nil {
		return err
	} else if enabled {
		sources = b.extractMaskingPolicySourceColumns(p, 0, stored)
	}
	if insertPlan != nil && insertPlan.tableSchema != nil {
		tblInfo := insertPlan.Table.Meta()
		for _, col := range cols {
			colInfo := model.FindColumnInfoByID(tblInfo.Columns, col.ID)
			if colInfo != nil && tblInfo.TempTableType == model.TempTableNone && insertPlan.tableSchema.Contains(col) {
				sources = append(sources, maskingPolicySourceColumn{tblInfo: tblInfo, colInfo: colInfo})
			}
		}
	}
	for _, source := range sources {
		policy, ok, err := b.is.MaskingPolicyByTableColumn(ctx, source.tblInfo.ID, source.colInfo.ID)
		if err != nil {
			return err
		}
		if !ok || policy == nil || policy.Status != model.MaskingPolicyStatusEnable {
			continue
		}
		allowed, err := canCurrentSessionReadUnmaskedColumn(b.ctx.GetExprCtx(), sv, b.is.SchemaMetaVersion(), policy, source.tblInfo, source.colInfo)
		if err != nil {
			return err
		}
		if !allowed {
			return &maskingNotPreservedError{shape: shape}
		}
	}
	return nil
}

func canCurrentSessionReadUnmaskedColumn(
	ctx expression.BuildContext,
	sv *variable.SessionVars,
	schemaVersion int64,
	policy *model.MaskingPolicyInfo,
	tblInfo *model.TableInfo,
	colInfo *model.ColumnInfo,
) (bool, error) {
	expr, placeholder, err := getMaskingPolicyExpr(ctx, sv, schemaVersion, policy, tblInfo, colInfo)
	if err != nil {
		return false, err
	}
	if placeholder == nil {
		return false, nil
	}

	// Evaluate only session-stable deferred constants on this private clone.
	// The cached expression and physical plan retain their runtime identity checks.
	expr, err = foldMaskingPolicySessionConstants(ctx, expr)
	if err != nil {
		return false, err
	}
	column, ok := expr.(*expression.Column)
	return ok && column.Equal(ctx.GetEvalCtx(), placeholder), nil
}

func foldMaskingPolicySessionConstants(ctx expression.BuildContext, expr expression.Expression) (expression.Expression, error) {
	switch x := expr.(type) {
	case *expression.Column:
		x.RetType = x.RetType.Clone()
	case *expression.Constant:
		if x.DeferredExpr != nil && isMaskingPolicyExprCacheable(x.DeferredExpr) {
			value, err := x.DeferredExpr.Eval(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return nil, err
			}
			return &expression.Constant{Value: value, RetType: x.RetType}, nil
		}
	case *expression.ScalarFunction:
		x.RetType = x.RetType.Clone()
		for i, arg := range x.GetArgs() {
			folded, err := foldMaskingPolicySessionConstants(ctx, arg)
			if err != nil {
				return nil, err
			}
			x.GetArgs()[i] = folded
		}
		x.CleanHashCode()
		return expression.FoldConstant(ctx, x), nil
	}
	return expr, nil
}

// CanReadUnmaskedColumn reports whether the current session reads the stored
// values of the column: it has no enabled masking policy, or its policy leaves
// the column unmasked for this session.
func CanReadUnmaskedColumn(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, tblInfo *model.TableInfo, colInfo *model.ColumnInfo) (bool, error) {
	policy, ok, err := is.MaskingPolicyByTableColumn(ctx, tblInfo.ID, colInfo.ID)
	if err != nil {
		return false, err
	}
	if !ok || policy == nil || policy.Status != model.MaskingPolicyStatusEnable {
		return true, nil
	}
	return canCurrentSessionReadUnmaskedColumn(sctx.GetExprCtx(), sctx.GetSessionVars(), is.SchemaMetaVersion(), policy, tblInfo, colInfo)
}

func checkCachedMaskingPolicyAccess(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, checks []stmtctx.MaskingPolicyCheck) error {
	for _, check := range checks {
		policy, ok, err := is.MaskingPolicyByTableColumn(ctx, check.TableID, check.ColumnID)
		if err != nil {
			return err
		}
		if !ok || policy.ID != check.PolicyID {
			return plannererrors.ErrSchemaChanged.GenWithStack("masking policy binding changed")
		}
		if policy.Status != model.MaskingPolicyStatusEnable || policy.RestrictOps&check.Operation == 0 {
			continue
		}
		tbl, ok := is.TableByID(ctx, check.TableID)
		if !ok {
			return plannererrors.ErrSchemaChanged.GenWithStack("masking policy table changed")
		}
		col := model.FindColumnInfoByID(tbl.Meta().Columns, check.ColumnID)
		if col == nil {
			return plannererrors.ErrSchemaChanged.GenWithStack("masking policy column changed")
		}
		allowed, err := canCurrentSessionReadUnmaskedColumn(sctx.GetExprCtx(), sctx.GetSessionVars(), is.SchemaMetaVersion(), policy, tbl.Meta(), col)
		if err != nil {
			return err
		}
		if !allowed {
			return plannererrors.ErrAccessDeniedToMaskedColumn.GenWithStackByArgs(col.Name.O)
		}
	}
	return nil
}
