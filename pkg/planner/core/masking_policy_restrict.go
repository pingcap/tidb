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
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
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
		for _, candidate := range b.extractMaskingPolicyCandidateNamesFromOutputColumn(p, i) {
			policy, tblInfo, colInfo, err := b.findMaskingPolicyByFieldName(ctx, candidate)
			if err != nil {
				return err
			}
			if policy == nil || tblInfo == nil || colInfo == nil || policy.Status != model.MaskingPolicyStatusEnable {
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

func (b *PlanBuilder) extractMaskingPolicyCandidateNamesFromOutputColumn(
	p base.LogicalPlan,
	outputColIdx int,
) []*types.FieldName {
	candidates := make([]*types.FieldName, 0, 2)
	seen := make(map[string]struct{}, 2)
	appendCandidate := func(name *types.FieldName) {
		if name == nil {
			return
		}
		key := name.DBName.L + "/" + name.TblName.L + "/" + name.OrigTblName.L + "/" + name.ColName.L + "/" + name.OrigColName.L
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		candidates = append(candidates, name)
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

		names := plan.OutputNames()
		if colIdx < len(names) {
			appendCandidate(names[colIdx])
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

	walkByIndex(p, outputColIdx)
	return candidates
}

func (b *PlanBuilder) findMaskingPolicyByFieldName(ctx context.Context, name *types.FieldName) (*model.MaskingPolicyInfo, *model.TableInfo, *model.ColumnInfo, error) {
	if name == nil || name.Hidden {
		return nil, nil, nil, nil
	}
	tblName := name.OrigTblName
	if tblName.L == "" {
		tblName = name.TblName
	}
	if tblName.L == "" {
		return nil, nil, nil, nil
	}
	colName := name.OrigColName
	if colName.L == "" {
		colName = name.ColName
	}
	if colName.L == "" {
		return nil, nil, nil, nil
	}
	dbName := name.DBName
	if dbName.L == "" {
		dbName = pmodel.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if dbName.L == "" {
		return nil, nil, nil, nil
	}
	tbl, err := b.is.TableByName(ctx, dbName, tblName)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}
	tblInfo := tbl.Meta()
	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		return nil, nil, nil, nil
	}
	policy, ok, err := b.is.MaskingPolicyByTableColumn(ctx, tblInfo.ID, colInfo.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	if !ok {
		return nil, nil, nil, nil
	}
	return policy, tblInfo, colInfo, nil
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
