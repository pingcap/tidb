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
	"bytes"
	"context"
	"math"
	"slices"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

type pkdbPlanBuilderState struct {
	// rowLocatorScopes tracks the syntactic table bindings visible to
	// TIDB_ROW_ID in each active query block.
	rowLocatorScopes []*rowLocatorScope
	// rowLocatorQueryBlockDepth lets query blocks without TIDB_ROW_ID avoid
	// allocating a placeholder scope while preserving nested-scope isolation.
	rowLocatorQueryBlockDepth int
	// updateAssignmentScope identifies the UPDATE query block whose SET
	// expressions are being built. TIDB_ROW_ID is not valid in that query block,
	// but remains valid in an independent SELECT subquery inside an assignment.
	updateAssignmentScope *rowLocatorScope
}

func resolveTiDBRowIDCandidates(
	evalCtx expression.EvalContext,
	tableInfo *model.TableInfo,
	allowedPartitionNames []pmodel.CIStr,
	candidates []expression.Expression,
) ([]kv.Handle, []int, error) {
	partitionInfo := tableInfo.GetPartitionInfo()
	allowedPartitions := make(map[int64]int)
	if partitionInfo != nil {
		allowedNames := make(map[string]struct{}, len(allowedPartitionNames))
		for _, name := range allowedPartitionNames {
			allowedNames[name.L] = struct{}{}
		}
		for idx, definition := range partitionInfo.Definitions {
			if len(allowedNames) > 0 {
				if _, ok := allowedNames[definition.Name.L]; !ok {
					continue
				}
			}
			allowedPartitions[definition.ID] = idx
		}
	}

	handles := make([]kv.Handle, 0, len(candidates))
	partitionIdxs := make([]int, 0, len(candidates))
	seenKeys := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		token, isNull, err := candidate.EvalString(evalCtx, chunk.Row{})
		if err != nil {
			return nil, nil, err
		}
		if isNull {
			continue
		}
		physicalID, handle, rawKey, _, err := tablecodec.DecodeTiDBRowID(token)
		if err != nil {
			return nil, nil, err
		}
		if tableInfo.IsCommonHandle {
			if handle.IsInt() {
				continue
			}
		} else if !handle.IsInt() {
			continue
		}

		partitionIdx := -1
		if partitionInfo == nil {
			if physicalID != tableInfo.ID {
				continue
			}
		} else {
			var ok bool
			partitionIdx, ok = allowedPartitions[physicalID]
			if !ok {
				continue
			}
		}
		if _, ok := seenKeys[string(rawKey)]; ok {
			continue
		}
		seenKeys[string(rawKey)] = struct{}{}
		handles = append(handles, handle)
		if partitionInfo != nil {
			partitionIdxs = append(partitionIdxs, partitionIdx)
		}
	}
	return handles, partitionIdxs, nil
}

func resolveTiDBRowIDRangeBounds(
	evalCtx expression.EvalContext,
	tableInfo *model.TableInfo,
	allowedPartitionNames []pmodel.CIStr,
	ops []expression.TiDBRowIDRangeOp,
	bounds []expression.Expression,
) ([]int64, []kv.KeyRange, error) {
	expectedBoundCount := 0
	for _, op := range ops {
		switch op {
		case expression.TiDBRowIDRangeLT,
			expression.TiDBRowIDRangeLE,
			expression.TiDBRowIDRangeGT,
			expression.TiDBRowIDRangeGE:
			expectedBoundCount++
		case expression.TiDBRowIDRangeBetween:
			expectedBoundCount += 2
		default:
			return nil, nil, tablecodec.ErrInvalidTiDBRowID
		}
	}
	if expectedBoundCount != len(bounds) {
		return nil, nil, tablecodec.ErrInvalidTiDBRowID
	}

	keys := make([]kv.Key, len(bounds))
	hasNull := false
	for i, bound := range bounds {
		token, isNull, err := bound.EvalString(evalCtx, chunk.Row{})
		if err != nil {
			return nil, nil, err
		}
		if isNull {
			hasNull = true
			continue
		}
		keys[i], err = validateTiDBRowIDRangeBound(tableInfo, token)
		if err != nil {
			return nil, nil, err
		}
	}
	if hasNull {
		return nil, nil, nil
	}

	var lower kv.Key
	var upper kv.Key
	boundOffset := 0
	for _, op := range ops {
		boundCount := 1
		if op == expression.TiDBRowIDRangeBetween {
			boundCount = 2
		}

		switch op {
		case expression.TiDBRowIDRangeLT:
			upper = minTiDBRowIDRangeKey(upper, keys[boundOffset])
		case expression.TiDBRowIDRangeLE:
			upper = minTiDBRowIDRangeKey(upper, keys[boundOffset].Next())
		case expression.TiDBRowIDRangeGT:
			lower = maxTiDBRowIDRangeKey(lower, keys[boundOffset].Next())
		case expression.TiDBRowIDRangeGE:
			lower = maxTiDBRowIDRangeKey(lower, keys[boundOffset])
		case expression.TiDBRowIDRangeBetween:
			lower = maxTiDBRowIDRangeKey(lower, keys[boundOffset])
			upper = minTiDBRowIDRangeKey(upper, keys[boundOffset+1].Next())
		}
		boundOffset += boundCount
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) >= 0 {
		return nil, nil, nil
	}

	physicalIDs := tiDBRowIDRangePhysicalIDs(tableInfo, allowedPartitionNames)
	resolvedIDs := make([]int64, 0, len(physicalIDs))
	ranges := make([]kv.KeyRange, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		physicalStart, physicalEnd, err := tiDBRowIDPhysicalTableRange(tableInfo, physicalID)
		if err != nil {
			return nil, nil, err
		}
		start := maxTiDBRowIDRangeKey(physicalStart, lower)
		end := minTiDBRowIDRangeKey(physicalEnd, upper)
		if bytes.Compare(start, end) >= 0 {
			continue
		}
		resolvedIDs = append(resolvedIDs, physicalID)
		ranges = append(ranges, kv.KeyRange{
			StartKey: slices.Clone(start),
			EndKey:   slices.Clone(end),
		})
	}
	return resolvedIDs, ranges, nil
}

func tiDBRowIDPhysicalTableRange(tableInfo *model.TableInfo, physicalID int64) (kv.Key, kv.Key, error) {
	if tableInfo.IsCommonHandle {
		startHandle, err := codec.EncodeKey(time.UTC, nil, types.MinNotNullDatum())
		if err != nil {
			return nil, nil, err
		}
		endHandle, err := codec.EncodeKey(time.UTC, nil, types.MaxValueDatum())
		if err != nil {
			return nil, nil, err
		}
		start := tablecodec.EncodeRowKey(physicalID, startHandle)
		end := tablecodec.EncodeRowKey(physicalID, kv.Key(endHandle).PrefixNext())
		return start, end, nil
	}
	start := tablecodec.EncodeRowKeyWithHandle(physicalID, kv.IntHandle(math.MinInt64))
	end := tablecodec.EncodeRowKeyWithHandle(physicalID, kv.IntHandle(math.MaxInt64)).Next()
	return start, end, nil
}

func validateTiDBRowIDRangeBound(tableInfo *model.TableInfo, token string) (kv.Key, error) {
	physicalID, handle, rawKey, _, err := tablecodec.DecodeTiDBRowID(token)
	if err != nil {
		return nil, err
	}
	if tableInfo.IsCommonHandle == handle.IsInt() {
		return nil, tablecodec.ErrInvalidTiDBRowID
	}
	partitionInfo := tableInfo.GetPartitionInfo()
	if partitionInfo == nil {
		if physicalID != tableInfo.ID {
			return nil, tablecodec.ErrInvalidTiDBRowID
		}
		return rawKey, nil
	}
	for _, definition := range partitionInfo.Definitions {
		if definition.ID == physicalID {
			return rawKey, nil
		}
	}
	return nil, tablecodec.ErrInvalidTiDBRowID
}

func tiDBRowIDRangePhysicalIDs(tableInfo *model.TableInfo, allowedPartitionNames []pmodel.CIStr) []int64 {
	partitionInfo := tableInfo.GetPartitionInfo()
	if partitionInfo == nil {
		return []int64{tableInfo.ID}
	}
	allowedNames := make(map[string]struct{}, len(allowedPartitionNames))
	for _, name := range allowedPartitionNames {
		allowedNames[name.L] = struct{}{}
	}
	physicalIDs := make([]int64, 0, len(partitionInfo.Definitions))
	for _, definition := range partitionInfo.Definitions {
		if len(allowedNames) > 0 {
			if _, ok := allowedNames[definition.Name.L]; !ok {
				continue
			}
		}
		physicalIDs = append(physicalIDs, definition.ID)
	}
	slices.Sort(physicalIDs)
	return physicalIDs
}

func minTiDBRowIDRangeKey(current, candidate kv.Key) kv.Key {
	if current == nil || candidate != nil && bytes.Compare(candidate, current) < 0 {
		return candidate
	}
	return current
}

func maxTiDBRowIDRangeKey(current, candidate kv.Key) kv.Key {
	if current == nil || candidate != nil && bytes.Compare(candidate, current) > 0 {
		return candidate
	}
	return current
}

func tiDBRowIDPredicateBelongsToDataSource(rowID expression.Expression, ds *logicalop.DataSource) bool {
	columns := expression.ExtractColumns(rowID)
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if !ds.Schema().Contains(column) {
			return false
		}
	}
	return true
}

func tiDBRowIDAllowedPartitionNames(ds *logicalop.DataSource) []pmodel.CIStr {
	if ds.PartitionDefIdx != nil {
		partitionInfo := ds.TableInfo.GetPartitionInfo()
		if partitionInfo != nil && *ds.PartitionDefIdx >= 0 && *ds.PartitionDefIdx < len(partitionInfo.Definitions) {
			return []pmodel.CIStr{partitionInfo.Definitions[*ds.PartitionDefIdx].Name}
		}
	}
	return ds.PartitionNames
}

// rowLocatorScope is a query-block-local inventory. Bindings start as
// syntactic sources and become eligible only when that exact source builds a
// persisted TiDB DataSource.
type rowLocatorScope struct {
	queryBlockDepth int
	byName          map[string][]*rowLocatorBinding
	byTableName     map[*ast.TableName]*rowLocatorBinding
}

type rowLocatorBinding struct {
	dataSource               *logicalop.DataSource
	physicalID               expression.Expression
	eligible                 bool
	selectPrivilegeCollected bool
}

func (b *PlanBuilder) pushRowLocatorScope(root ast.Node, from ast.ResultSetNode) {
	b.rowLocatorQueryBlockDepth++
	hasLocator := false
	scanDone := false
	if b.resolveCtx != nil {
		scanDone, hasLocator = b.resolveCtx.TiDBRowIDQueryBlockStatus(root)
	}
	if !scanDone {
		detector := &rowLocatorDetector{root: root}
		if root != nil {
			root.Accept(detector)
		}
		hasLocator = detector.found
	}
	if !hasLocator {
		return
	}

	scope := &rowLocatorScope{
		queryBlockDepth: b.rowLocatorQueryBlockDepth,
		byName:          make(map[string][]*rowLocatorBinding),
		byTableName:     make(map[*ast.TableName]*rowLocatorBinding),
	}
	scope.collectBindings(from)
	b.rowLocatorScopes = append(b.rowLocatorScopes, scope)
}

func (b *PlanBuilder) popRowLocatorScope() {
	if b.currentRowLocatorScope() != nil {
		b.rowLocatorScopes = b.rowLocatorScopes[:len(b.rowLocatorScopes)-1]
	}
	b.rowLocatorQueryBlockDepth--
}

func (b *PlanBuilder) currentRowLocatorScope() *rowLocatorScope {
	if len(b.rowLocatorScopes) == 0 {
		return nil
	}
	scope := b.rowLocatorScopes[len(b.rowLocatorScopes)-1]
	if scope.queryBlockDepth != b.rowLocatorQueryBlockDepth {
		return nil
	}
	return scope
}

func (b *PlanBuilder) collectTiDBRowIDSelectPrivilege(binding *rowLocatorBinding) {
	if binding.selectPrivilegeCollected {
		return
	}
	ds := binding.dataSource
	b.collectTableSelectPrivilege(ds.DBName.L, ds.TableInfo.Name.L)
	binding.selectPrivilegeCollected = true
}

func (s *rowLocatorScope) collectBindings(node ast.ResultSetNode) {
	switch x := node.(type) {
	case *ast.Join:
		if x.Left != nil {
			s.collectBindings(x.Left)
		}
		if x.Right != nil {
			s.collectBindings(x.Right)
		}
	case *ast.TableSource:
		switch source := x.Source.(type) {
		case *ast.TableName:
			name := source.Name.L
			if x.AsName.L != "" {
				name = x.AsName.L
			}
			binding := &rowLocatorBinding{}
			s.byName[name] = append(s.byName[name], binding)
			s.byTableName[source] = binding
		case *ast.Join:
			s.collectBindings(source)
		default:
			// Keep derived and set-operation aliases in the namespace so an
			// explicit reference is rejected as ineligible, without looking
			// through the query-block boundary.
			if x.AsName.L != "" {
				s.byName[x.AsName.L] = append(s.byName[x.AsName.L], &rowLocatorBinding{})
			}
		}
	}
}

func (b *PlanBuilder) attachRowLocatorDataSource(tn *ast.TableName, ds *logicalop.DataSource) {
	scope := b.currentRowLocatorScope()
	if scope == nil {
		return
	}
	binding := scope.byTableName[tn]
	if binding == nil || !isEligibleRowLocatorTable(ds.Table) || b.isCreateView {
		return
	}

	binding.dataSource = ds
	binding.eligible = true
	if ds.TableInfo.GetPartitionInfo() == nil {
		binding.physicalID = expression.NewInt64Const(ds.PhysicalTableID)
	} else {
		binding.physicalID = addExtraPhysTblIDColumn4DS(ds)
	}
}

func isEligibleRowLocatorTable(tbl table.Table) bool {
	if tbl == nil || !tbl.Type().IsNormalTable() {
		return false
	}
	meta := tbl.Meta()
	return meta.IsBaseTable() && meta.TempTableType == model.TempTableNone
}

func (b *PlanBuilder) buildTiDBRowIDExpression(
	call *ast.FuncCallExpr,
	planSchema *expression.Schema,
	plan base.LogicalPlan,
) (expression.Expression, error) {
	if err := expression.VerifyArgsWrapper(ast.TiDBRowID, len(call.Args)); err != nil {
		return nil, err
	}
	scope := b.currentRowLocatorScope()
	if scope == nil || !isTiDBRowIDClauseSupported(b.curClause) || b.isCreateView {
		return nil, plannererrors.ErrTiDBRowIDTableBinding
	}

	if b.updateAssignmentScope == scope {
		return nil, plannererrors.ErrTiDBRowIDTableBinding
	}
	binding, err := scope.resolve(call)
	if err != nil {
		return nil, err
	}
	b.collectTiDBRowIDSelectPrivilege(binding)

	ds := binding.dataSource
	handleArgs := make([]expression.Expression, 0, ds.HandleCols.NumCols())
	for i := 0; i < ds.HandleCols.NumCols(); i++ {
		handleArgs = append(handleArgs, ds.HandleCols.GetCol(i))
	}
	handleKind := expression.TiDBRowIDIntHandle
	var primaryIndex *model.IndexInfo
	if ds.TableInfo.IsCommonHandle {
		handleKind = expression.TiDBRowIDCommonHandle
		primaryIndex = tables.FindPrimaryIndex(ds.TableInfo)
	} else if ds.HandleCols.GetCol(0).ID == model.ExtraHandleID {
		handleKind = expression.TiDBRowIDExtraHandle
	}
	rowID, err := expression.NewTiDBRowID(
		b.ctx.GetExprCtx(),
		binding.physicalID,
		handleKind,
		ds.TableInfo,
		primaryIndex,
		handleArgs...,
	)
	if err != nil {
		return nil, err
	}

	if projection, ok := plan.(*logicalop.LogicalProjection); ok {
		evalCtx := b.ctx.GetExprCtx().GetEvalCtx()
		for i, projectedExpr := range projection.Exprs {
			if rowID.Equal(evalCtx, projectedExpr) {
				return projection.Schema().Columns[i], nil
			}
		}
		if len(projection.Children()) == 1 {
			resolved, err := rowID.ResolveIndices(projection.Children()[0].Schema())
			if err != nil {
				return nil, err
			}
			column := &expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  rowID.GetType(b.ctx.GetExprCtx().GetEvalCtx()).Clone(),
			}
			projection.Exprs = append(projection.Exprs, resolved)
			projection.Schema().Append(column)
			projection.SetOutputNames(append(projection.OutputNames(), types.EmptyName))
			return column, nil
		}
	}
	return rowID.ResolveIndices(planSchema)
}

func (b *PlanBuilder) rewriteAggregateArgWithRowLocatorScope(
	ctx context.Context,
	arg ast.ExprNode,
	plan base.LogicalPlan,
) (expression.Expression, base.LogicalPlan, error) {
	originalCheckColPriv := b.checkColPriv
	b.checkColPriv = reportColumnErrOption
	defer func() { b.checkColPriv = originalCheckColPriv }()
	if b.currentRowLocatorScope() == nil {
		return b.rewrite(ctx, arg, plan, nil, true)
	}
	detector := &rowLocatorDetector{root: arg}
	arg.Accept(detector)
	if !detector.found {
		return b.rewrite(ctx, arg, plan, nil, true)
	}

	// Aggregate arguments are input-row expressions evaluated before aggregation.
	// Rewrite a locator argument in field-list scope regardless of the surrounding
	// output clause so it can resolve its input table.
	originalClause := b.curClause
	b.curClause = fieldList
	defer func() { b.curClause = originalClause }()
	return b.rewrite(ctx, arg, plan, nil, true)
}

func (s *rowLocatorScope) resolve(call *ast.FuncCallExpr) (*rowLocatorBinding, error) {
	if err := expression.VerifyArgsWrapper(ast.TiDBRowID, len(call.Args)); err != nil {
		return nil, err
	}
	var binding *rowLocatorBinding
	if len(call.Args) == 0 {
		for _, candidate := range s.byTableName {
			if !candidate.eligible {
				continue
			}
			if binding != nil {
				return nil, plannererrors.ErrTiDBRowIDTableBinding
			}
			binding = candidate
		}
	} else {
		name, ok := call.Args[0].(*ast.ColumnNameExpr)
		if !ok || name.Name.Schema.L != "" || name.Name.Table.L != "" {
			return nil, plannererrors.ErrWrongArguments.FastGenByArgs(ast.TiDBRowID)
		}
		candidates := s.byName[name.Name.Name.L]
		if len(candidates) == 1 && candidates[0].eligible {
			binding = candidates[0]
		}
	}
	if binding == nil || binding.physicalID == nil || binding.dataSource.HandleCols == nil {
		return nil, plannererrors.ErrTiDBRowIDTableBinding
	}
	return binding, nil
}

func (b *PlanBuilder) nonAggregateRowLocatorDependencies(plan base.LogicalPlan, expr ast.ExprNode) []*types.FieldName {
	scope := b.currentRowLocatorScope()
	if scope == nil {
		return nil
	}
	visitor := &nonAggregateRowLocatorVisitor{}
	expr.Accept(visitor)
	dependencies := make([]*types.FieldName, 0, len(visitor.calls))
	for _, call := range visitor.calls {
		binding, err := scope.resolve(call)
		if err != nil {
			continue
		}
		for i := 0; i < binding.dataSource.HandleCols.NumCols(); i++ {
			dependencies = appendRowLocatorDependency(dependencies, plan, binding.dataSource.HandleCols.GetCol(i))
		}
		if physicalID, ok := binding.physicalID.(*expression.Column); ok {
			dependencies = appendRowLocatorDependency(dependencies, plan, physicalID)
		}
	}
	return dependencies
}

func appendRowLocatorDependency(
	dependencies []*types.FieldName,
	plan base.LogicalPlan,
	column *expression.Column,
) []*types.FieldName {
	index := plan.Schema().ColumnIndex(column)
	if index < 0 {
		return dependencies
	}
	name := plan.OutputNames()[index]
	for _, dependency := range dependencies {
		if dependency == name {
			return dependencies
		}
	}
	return append(dependencies, name)
}

func isTiDBRowIDClauseSupported(clause clauseCode) bool {
	switch clause {
	case fieldList, onClause, orderByClause, whereClause, groupByClause:
		return true
	default:
		return false
	}
}

// rowLocatorDetector scans one query block while treating nested SELECT and
// set-operation nodes as independent scopes.
type rowLocatorDetector struct {
	root  ast.Node
	found bool
}

func (d *rowLocatorDetector) Enter(node ast.Node) (ast.Node, bool) {
	switch x := node.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		if node != d.root {
			return node, true
		}
	case *ast.FuncCallExpr:
		if x.FnName.L == ast.TiDBRowID {
			d.found = true
			return node, true
		}
	}
	return node, false
}

func (*rowLocatorDetector) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

type nonAggregateRowLocatorVisitor struct {
	calls []*ast.FuncCallExpr
}

func (v *nonAggregateRowLocatorVisitor) Enter(node ast.Node) (ast.Node, bool) {
	switch x := node.(type) {
	case *ast.AggregateFuncExpr, *ast.SubqueryExpr:
		return node, true
	case *ast.FuncCallExpr:
		if x.FnName.L == ast.AnyValue {
			return node, true
		}
		if x.FnName.L == ast.TiDBRowID {
			v.calls = append(v.calls, x)
			return node, true
		}
	}
	return node, false
}

func (*nonAggregateRowLocatorVisitor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}
