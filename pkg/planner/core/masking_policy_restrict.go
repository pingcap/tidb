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
	"slices"

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
	checkedPolicyIDs := make(map[int64]struct{})
	for i := range p.Schema().Columns {
		sources := b.extractMaskingPolicySourceColumnsFromOutputColumn(p, i)
		if err := b.checkMaskingPolicyRestrictOnSources(ctx, sources, op, checkedPolicyIDs); err != nil {
			return err
		}
	}
	return nil
}

// checkMaskingPolicyRestrictOnAssignment applies RESTRICT ON (UPDATE_SELECT)
// to an UPDATE assignment that reads a protected column directly, without a
// subquery: the value would be written to another column or another row.
// Assigning a column to itself writes nothing new and is not restricted.
func (b *PlanBuilder) checkMaskingPolicyRestrictOnAssignment(ctx context.Context, p base.LogicalPlan, target *expression.Column, expr expression.Expression) error {
	if b.is == nil || p == nil {
		return nil
	}
	if col, ok := expr.(*expression.Column); ok && col.UniqueID == target.UniqueID {
		return nil
	}
	hasPolicies, err := b.maskingEnabled(ctx, p)
	if err != nil || !hasPolicies {
		return err
	}
	sources := b.extractMaskingPolicySourceColumns(p, -1, expr)
	return b.checkMaskingPolicyRestrictOnSources(ctx, sources, ast.MaskingPolicyRestrictOpUpdateSelect, make(map[int64]struct{}))
}

// checkMaskingPolicyRestrictOnDuplicate applies RESTRICT ON (UPDATE_SELECT) to
// the assignments of INSERT ... ON DUPLICATE KEY UPDATE that read a protected
// column of the existing row: they update that row like an UPDATE assignment.
// The values of the new row, constants and assigning a column to itself are
// not restricted.
func (b *PlanBuilder) checkMaskingPolicyRestrictOnDuplicate(ctx context.Context, insertPlan *Insert, tblInfo *model.TableInfo) error {
	if b.is == nil || len(insertPlan.OnDuplicate) == 0 || b.ctx.GetSessionVars().InRestrictedSQL ||
		tblInfo.TempTableType != model.TempTableNone {
		return nil
	}
	hasPolicies, err := b.is.HasMaskingPolicies(ctx)
	if err != nil || !hasPolicies {
		return err
	}
	sources := make([]maskingPolicySourceColumn, 0, 2)
	appendSource := func(col *expression.Column) {
		if !insertPlan.tableSchema.Contains(col) {
			return
		}
		colInfo := model.FindColumnInfoByID(tblInfo.Columns, col.ID)
		if colInfo == nil || colInfo.State != model.StatePublic {
			return
		}
		sources = append(sources, maskingPolicySourceColumn{tblInfo: tblInfo, colInfo: colInfo})
	}
	for _, assign := range insertPlan.OnDuplicate {
		if assign.Expr == nil {
			continue
		}
		if col, ok := assign.Expr.(*expression.Column); ok && col.UniqueID == assign.Col.UniqueID {
			continue
		}
		for _, col := range expression.ExtractColumns(assign.Expr) {
			appendSource(col)
		}
		for _, col := range expression.ExtractCorColumns(assign.Expr) {
			appendSource(&col.Column)
		}
	}
	return b.checkMaskingPolicyRestrictOnSources(ctx, sources, ast.MaskingPolicyRestrictOpUpdateSelect, make(map[int64]struct{}))
}

// checkMaskingPolicyRestrictOnSources rejects the operation if the session
// reads any of the source columns masked under a policy that restricts it.
// The policies that allow it are recorded, to be checked again when the plan
// is reused from a plan cache.
func (b *PlanBuilder) checkMaskingPolicyRestrictOnSources(
	ctx context.Context,
	sources []maskingPolicySourceColumn,
	op ast.MaskingPolicyRestrictOps,
	checkedPolicyIDs map[int64]struct{},
) error {
	sv := b.ctx.GetSessionVars()
	schemaVersion := b.is.SchemaMetaVersion()
	for _, source := range sources {
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
	// applyOuters holds the outer children of the Apply operators, such as
	// those of LATERAL joins, whose inner side is being walked: they are the
	// sources of its correlated columns.
	var applyOuters []base.LogicalPlan
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
			for i := len(applyOuters) - 1; i >= 0; i-- {
				walkByColumn(applyOuters[i], &col.Column)
			}
		}
	}
	walkChild := func(plan base.LogicalPlan, childIdx int, walk func(base.LogicalPlan)) {
		if ap, ok := plan.(*logicalop.LogicalApply); ok && childIdx == 1 {
			applyOuters = append(applyOuters, ap.Children()[0])
			defer func() { applyOuters = applyOuters[:len(applyOuters)-1] }()
		}
		walk(plan.Children()[childIdx])
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
		for i, child := range plan.Children() {
			if child == nil {
				continue
			}
			childColIdx := child.Schema().ColumnIndex(outputCol)
			if childColIdx >= 0 {
				walkChild(plan, i, func(child base.LogicalPlan) { walkByIndex(child, childColIdx) })
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
		for i, child := range plan.Children() {
			if child != nil {
				walkChild(plan, i, func(child base.LogicalPlan) { walkByColumn(child, col) })
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

// recordMaskingPolicyRead records that the statement reads a column with an
// enabled masking policy. If the policy masks the column for the current
// session, the diagnostics of the statement are redacted: the statement
// evaluates its predicates and other internal expressions on the original
// values, which a warning or an error message would otherwise disclose.
func recordMaskingPolicyRead(ctx context.Context, sctx base.PlanContext, is infoschema.InfoSchema, tblInfo *model.TableInfo, colInfo *model.ColumnInfo) error {
	policy, ok, err := is.MaskingPolicyByTableColumn(ctx, tblInfo.ID, colInfo.ID)
	if err != nil || !ok || policy == nil || policy.Status != model.MaskingPolicyStatusEnable {
		return err
	}
	sv := sctx.GetSessionVars()
	check := stmtctx.MaskingPolicyCheck{PolicyID: policy.ID, TableID: tblInfo.ID, ColumnID: colInfo.ID}
	if !slices.Contains(sv.StmtCtx.MaskingPolicyChecks, check) {
		sv.StmtCtx.MaskingPolicyChecks = append(sv.StmtCtx.MaskingPolicyChecks, check)
	}
	if sv.StmtCtx.ReadsMaskedColumn {
		return nil
	}
	allowed, err := canCurrentSessionReadUnmaskedColumn(sctx.GetExprCtx(), sv, is.SchemaMetaVersion(), policy, tblInfo, colInfo)
	if err == nil && !allowed {
		sv.StmtCtx.SetRedactDiagnostics()
	}
	return err
}

// recordMaskingPolicyReads records the columns that the scans of an optimized
// logical plan read. Column pruning has removed the columns that no operator
// of the statement uses. A CTE is optimized, and so recorded, on its own.
func recordMaskingPolicyReads(ctx context.Context, sctx base.PlanContext, p base.LogicalPlan) error {
	if sctx.GetSessionVars().InRestrictedSQL {
		return nil
	}
	var sources []*logicalop.DataSource
	var collect func(base.LogicalPlan)
	collect = func(p base.LogicalPlan) {
		if ds, ok := p.(*logicalop.DataSource); ok && ds.TableInfo != nil && ds.TableInfo.TempTableType == model.TempTableNone {
			sources = append(sources, ds)
		}
		for _, child := range p.Children() {
			collect(child)
		}
	}
	collect(p)
	if len(sources) == 0 {
		return nil
	}
	is, ok := sctx.GetInfoSchema().(infoschema.InfoSchema)
	if !ok {
		return nil
	}
	if hasPolicies, err := is.HasMaskingPolicies(ctx); err != nil || !hasPolicies {
		return err
	}
	for _, ds := range sources {
		if err := recordMaskingPolicyColumnReads(ctx, sctx, is, ds.TableInfo, ds.Columns); err != nil {
			return err
		}
	}
	return nil
}

// recordMaskingPolicyColumnReads records the given columns of a table. A
// virtual generated column is evaluated from other columns of its row, so
// reading one reads the whole row.
func recordMaskingPolicyColumnReads(ctx context.Context, sctx base.PlanContext, is infoschema.InfoSchema, tblInfo *model.TableInfo, cols []*model.ColumnInfo) error {
	if slices.ContainsFunc(cols, func(col *model.ColumnInfo) bool { return col.IsGenerated() && !col.GeneratedStored }) {
		cols = tblInfo.Columns
	}
	for _, colInfo := range cols {
		if err := recordMaskingPolicyRead(ctx, sctx, is, tblInfo, colInfo); err != nil {
			return err
		}
	}
	return nil
}

// recordMaskingPolicyRowRead records every column of a table whose rows the
// statement reads as a whole.
func recordMaskingPolicyRowRead(ctx context.Context, sctx base.PlanContext, is infoschema.InfoSchema, tblInfo *model.TableInfo) error {
	if sctx.GetSessionVars().InRestrictedSQL || tblInfo.TempTableType != model.TempTableNone {
		return nil
	}
	if hasPolicies, err := is.HasMaskingPolicies(ctx); err != nil || !hasPolicies {
		return err
	}
	return recordMaskingPolicyColumnReads(ctx, sctx, is, tblInfo, tblInfo.Columns)
}

// RecordFastPlanMaskingPolicyReads records the columns that a plan built by
// TryFastPlan reads, as the optimizer does for the other plans. It returns
// false if the plan cannot be used and normal planning is required.
func RecordFastPlanMaskingPolicyReads(ctx context.Context, sctx base.PlanContext, p base.Plan) bool {
	if sctx.GetSessionVars().InRestrictedSQL {
		return true
	}
	var assignments []*expression.Assignment
	isSelect := true
	switch x := p.(type) {
	case *Update:
		isSelect = false
		p, assignments = x.SelectPlan, x.OrderedList
	case *Delete:
		isSelect = false
		p = x.SelectPlan
	}
	var tblInfo *model.TableInfo
	switch x := p.(type) {
	case *PointGetPlan:
		tblInfo = x.TblInfo
	case *BatchPointGetPlan:
		tblInfo = x.TblInfo
	}
	if tblInfo == nil || tblInfo.TempTableType != model.TempTableNone {
		return true
	}
	is, ok := sctx.GetInfoSchema().(infoschema.InfoSchema)
	if !ok {
		return false
	}
	if !infoschema.MaskingPoliciesLoaded(is) {
		// A checksum-only SELECT has no output column to record as masked.
		return isSelect && isChecksumOnlyPointGet(p.Schema(), p.OutputNames(), tblInfo)
	}
	if hasPolicies, err := is.HasMaskingPolicies(ctx); err != nil || !hasPolicies {
		return err == nil
	}
	// A fast UPDATE skips the check of RESTRICT ON (UPDATE_SELECT) on its
	// assignments. Leave an assignment that reads a column with such a policy
	// to the normal planner, which checks it for the current identity.
	for _, assign := range assignments {
		for _, col := range expression.ExtractColumns(assign.Expr) {
			policy, ok, err := is.MaskingPolicyByTableColumn(ctx, tblInfo.ID, col.ID)
			if err != nil {
				return false
			}
			if ok && policy != nil && policy.RestrictOps&ast.MaskingPolicyRestrictOpUpdateSelect != 0 {
				return false
			}
		}
	}
	cols := make([]*model.ColumnInfo, 0, p.Schema().Len())
	for _, col := range p.Schema().Columns {
		if colInfo := model.FindColumnInfoByID(tblInfo.Columns, col.ID); colInfo != nil {
			cols = append(cols, colInfo)
		}
	}
	return recordMaskingPolicyColumnReads(ctx, sctx, is, tblInfo, cols) == nil
}

func checkCachedMaskingPolicyAccess(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, checks []stmtctx.MaskingPolicyCheck) error {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	for _, check := range checks {
		policy, ok, err := is.MaskingPolicyByTableColumn(ctx, check.TableID, check.ColumnID)
		if err != nil {
			return err
		}
		if !ok || policy.ID != check.PolicyID {
			return plannererrors.ErrSchemaChanged.GenWithStack("masking policy binding changed")
		}
		if policy.Status != model.MaskingPolicyStatusEnable {
			continue
		}
		restricted := policy.RestrictOps&check.Operation != 0
		// A column that is only read decides whether diagnostics are redacted.
		if !restricted && (check.Operation != ast.MaskingPolicyRestrictOpNone || stmtCtx.ReadsMaskedColumn) {
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
		if allowed {
			continue
		}
		if restricted {
			return plannererrors.ErrAccessDeniedToMaskedColumn.GenWithStackByArgs(col.Name.O)
		}
		stmtCtx.SetRedactDiagnostics()
	}
	return nil
}
