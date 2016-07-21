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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// Error instances.
var (
	ErrUnsupportedType      = terror.ClassOptimizerPlan.New(CodeUnsupportedType, "Unsupported type")
	SystemInternalErrorType = terror.ClassOptimizerPlan.New(SystemInternalError, "System internal error")
)

// Error codes.
const (
	CodeUnsupportedType terror.ErrCode = 1
	SystemInternalError terror.ErrCode = 2
)

// BuildPlan builds a plan from a node.
// It returns ErrUnsupportedType if ast.Node type is not supported yet.
func BuildPlan(node ast.Node, sb SubQueryBuilder) (Plan, error) {
	builder := planBuilder{sb: sb, allocator: new(idAllocator)}
	p := builder.build(node)
	return p, builder.err
}

// planBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type planBuilder struct {
	err          error
	hasAgg       bool
	sb           SubQueryBuilder
	obj          interface{}
	allocator    *idAllocator
	ctx          context.Context
	is           infoschema.InfoSchema
	outerSchemas []expression.Schema
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
}

func (b *planBuilder) build(node ast.Node) Plan {
	switch x := node.(type) {
	case *ast.AdminStmt:
		return b.buildAdmin(x)
	case *ast.AlterTableStmt:
		return b.buildDDL(x)
	case *ast.AnalyzeTableStmt:
		return b.buildSimple(x)
	case *ast.BinlogStmt:
		return b.buildSimple(x)
	case *ast.CreateDatabaseStmt:
		return b.buildDDL(x)
	case *ast.CreateIndexStmt:
		return b.buildDDL(x)
	case *ast.CreateTableStmt:
		return b.buildDDL(x)
	case *ast.DeallocateStmt:
		return &Deallocate{Name: x.Name}
	case *ast.DeleteStmt:
		return b.buildDelete(x)
	case *ast.DropDatabaseStmt:
		return b.buildDDL(x)
	case *ast.DropIndexStmt:
		return b.buildDDL(x)
	case *ast.DropTableStmt:
		return b.buildDDL(x)
	case *ast.ExecuteStmt:
		return &Execute{Name: x.Name, UsingVars: x.UsingVars}
	case *ast.ExplainStmt:
		return b.buildExplain(x)
	case *ast.InsertStmt:
		return b.buildInsert(x)
	case *ast.PrepareStmt:
		return b.buildPrepare(x)
	case *ast.SelectStmt:
		if UseNewPlanner {
			return b.buildNewSelect(x)
		}
		return b.buildSelect(x)
	case *ast.UnionStmt:
		if UseNewPlanner {
			return b.buildNewUnion(x)
		}
		return b.buildUnion(x)
	case *ast.UpdateStmt:
		return b.buildUpdate(x)
	case *ast.UseStmt:
		return b.buildSimple(x)
	case *ast.SetStmt:
		return b.buildSimple(x)
	case *ast.ShowStmt:
		return b.buildShow(x)
	case *ast.DoStmt:
		return b.buildSimple(x)
	case *ast.BeginStmt:
		return b.buildSimple(x)
	case *ast.CommitStmt:
		return b.buildSimple(x)
	case *ast.RollbackStmt:
		return b.buildSimple(x)
	case *ast.CreateUserStmt:
		return b.buildSimple(x)
	case *ast.SetPwdStmt:
		return b.buildSimple(x)
	case *ast.GrantStmt:
		return b.buildSimple(x)
	case *ast.TruncateTableStmt:
		return b.buildDDL(x)
	}
	b.err = ErrUnsupportedType.Gen("Unsupported type %T", node)
	return nil
}

// Detect aggregate function or groupby clause.
func (b *planBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.GetResultFields() {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

// extractSelectAgg extracts aggregate functions and converts ColumnNameExpr to aggregate function.
func (b *planBuilder) extractSelectAgg(sel *ast.SelectStmt) []*ast.AggregateFuncExpr {
	extractor := &ast.AggregateFuncExtractor{AggFuncs: make([]*ast.AggregateFuncExpr, 0)}
	for _, f := range sel.GetResultFields() {
		n, ok := f.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr!")
			return nil
		}
		ve, ok := f.Expr.(*ast.ValueExpr)
		if ok && len(f.Column.Name.O) > 0 {
			agg := &ast.AggregateFuncExpr{
				F:    ast.AggFuncFirstRow,
				Args: []ast.ExprNode{ve},
			}
			agg.SetType(ve.GetType())
			extractor.AggFuncs = append(extractor.AggFuncs, agg)
			n = agg
		}
		f.Expr = n.(ast.ExprNode)
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			b.err = errors.New("Failed to extract agg expr from having clause")
			return nil
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	// Extract agg funcs from orderby clause.
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				b.err = errors.New("Failed to extract agg expr from orderby clause")
				return nil
			}
			item.Expr = n.(ast.ExprNode)
			// If item is PositionExpr, we need to rebind it.
			// For PositionExpr will refer to a ResultField in fieldlist.
			// After extract AggExpr from fieldlist, it may be changed (See the code above).
			if pe, ok := item.Expr.(*ast.PositionExpr); ok {
				pe.Refer = sel.GetResultFields()[pe.N-1]
			}
		}
	}
	return extractor.AggFuncs
}

func (b *planBuilder) buildSubquery(n ast.Node) {
	sv := &subqueryVisitor{
		builder: b,
	}
	_, ok := n.Accept(sv)
	if !ok {
		log.Errorf("Extract subquery error")
	}
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) Plan {
	var aggFuncs []*ast.AggregateFuncExpr
	hasAgg := b.detectSelectAgg(sel)
	canPushLimit := !hasAgg
	if hasAgg {
		aggFuncs = b.extractSelectAgg(sel)
	}
	// Build subquery
	// Convert subquery to expr with plan
	b.buildSubquery(sel)
	var p Plan
	if sel.From != nil {
		p = b.buildFrom(sel)
		if b.err != nil {
			return nil
		}
		if sel.LockTp != ast.SelectLockNone {
			p = b.buildSelectLock(p, sel.LockTp)
			if b.err != nil {
				return nil
			}
		}
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, sel.GroupBy)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	} else {
		canPushLimit = false
		if sel.Where != nil {
			p = b.buildTableDual(sel)
		}
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, nil)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	}
	if sel.Having != nil {
		p = b.buildHaving(p, sel.Having)
		if b.err != nil {
			return nil
		}
	}
	if sel.Distinct {
		canPushLimit = false
		p = b.buildDistinct(p)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil && !pushOrder(p, sel.OrderBy.Items) {
		canPushLimit = false
		p = b.buildSort(p, sel.OrderBy.Items)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		if canPushLimit {
			pushLimit(p, sel.Limit)
		}
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	return p
}

func (b *planBuilder) buildFrom(sel *ast.SelectStmt) Plan {
	from := sel.From.TableRefs
	if from.Right == nil {
		return b.buildTableSource(sel)
	}
	return b.buildJoin(sel)
}

func (b *planBuilder) buildTableSource(sel *ast.SelectStmt) Plan {
	from := sel.From.TableRefs
	ts, ok := from.Left.(*ast.TableSource)
	if !ok {
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", from.Left)
		return nil
	}
	var bestPlan Plan
	switch v := ts.Source.(type) {
	case *ast.TableName:
	case *ast.SelectStmt:
		bestPlan = b.buildSelect(v)
	case *ast.UnionStmt:
		bestPlan = b.buildUnion(v)
	}
	if bestPlan != nil {
		return bestPlan
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", ts.Source)
		return nil
	}
	conditions := splitWhere(sel.Where)
	path := &joinPath{table: tn, conditions: conditions}
	candidates := b.buildAllAccessMethodsPlan(path)
	var lowestCost float64
	for _, v := range candidates {
		cost := EstimateCost(b.buildPseudoSelectPlan(v, sel))
		if bestPlan == nil {
			bestPlan = v
			lowestCost = cost
		}
		if cost <= lowestCost {
			bestPlan = v
			lowestCost = cost
		}
	}
	return bestPlan
}

func (b *planBuilder) buildAllAccessMethodsPlan(path *joinPath) []Plan {
	indices, includeTableScan := availableIndices(path.table)
	var candidates []Plan
	if includeTableScan {
		p := b.buildTableScanPlan(path)
		candidates = append(candidates, p)
	}
	for _, index := range indices {
		ip := b.buildIndexScanPlan(index, path)
		candidates = append(candidates, ip)
	}
	return candidates
}

func availableIndices(table *ast.TableName) (indices []*model.IndexInfo, includeTableScan bool) {
	var usableHints []*ast.IndexHint
	for _, hint := range table.IndexHints {
		if hint.HintScope == ast.HintForScan {
			usableHints = append(usableHints, hint)
		}
	}
	if len(usableHints) == 0 {
		return table.TableInfo.Indices, true
	}
	var hasUse bool
	var ignores []*model.IndexInfo
	for _, hint := range usableHints {
		switch hint.HintType {
		case ast.HintUse, ast.HintForce:
			// Currently we don't distinguish between Force and Use because our cost estimation is not reliable.
			hasUse = true
			for _, idxName := range hint.IndexNames {
				idx := findIndexByName(table.TableInfo.Indices, idxName)
				if idx != nil {
					indices = append(indices, idx)
				}
			}
		case ast.HintIgnore:
			// Collect all the ignore index hints.
			for _, idxName := range hint.IndexNames {
				idx := findIndexByName(table.TableInfo.Indices, idxName)
				if idx != nil {
					ignores = append(ignores, idx)
				}
			}
		}
	}
	indices = removeIgnores(indices, ignores)
	// If we have got FORCE or USE index hint, table scan is excluded.
	if len(indices) != 0 {
		return indices, false
	}
	if hasUse {
		// Empty use hint means don't use any index.
		return nil, true
	}
	if len(ignores) == 0 {
		return table.TableInfo.Indices, true
	}
	for _, idx := range table.TableInfo.Indices {
		// Exclude ignored index.
		if findIndexByName(ignores, idx.Name) == nil {
			indices = append(indices, idx)
		}
	}
	return indices, true
}

func removeIgnores(indices, ignores []*model.IndexInfo) []*model.IndexInfo {
	if len(ignores) == 0 {
		return indices
	}
	var remainedIndices []*model.IndexInfo
	for _, index := range indices {
		if findIndexByName(ignores, index.Name) == nil {
			remainedIndices = append(remainedIndices, index)
		}
	}
	return remainedIndices
}

func findIndexByName(indices []*model.IndexInfo, name model.CIStr) *model.IndexInfo {
	for _, idx := range indices {
		if idx.Name.L == name.L {
			return idx
		}
	}
	return nil
}

func (b *planBuilder) buildTableDual(sel *ast.SelectStmt) Plan {
	dual := &TableDual{FilterConditions: splitWhere(sel.Where)}
	ret := ast.ResultField{}
	dual.SetFields([]*ast.ResultField{&ret})
	return dual
}

func getTableAsName(fields []*ast.ResultField) *model.CIStr {
	if len(fields) > 0 {
		return &fields[0].TableAsName
	}
	return nil
}

func (b *planBuilder) buildTableScanPlan(path *joinPath) Plan {
	tn := path.table
	p := &TableScan{
		Table:     tn.TableInfo,
		TableName: tn,
	}
	// Equal condition contains a column from previous joined table.
	p.RefAccess = len(path.eqConds) > 0
	p.SetFields(tn.GetResultFields())
	p.TableAsName = getTableAsName(p.Fields())
	var pkName model.CIStr
	if p.Table.PKIsHandle {
		for _, colInfo := range p.Table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pkName = colInfo.Name
			}
		}
	}
	for _, con := range path.conditions {
		if pkName.L != "" {
			checker := conditionChecker{tableName: tn.TableInfo.Name, pkName: pkName}
			if checker.check(con) {
				p.AccessConditions = append(p.AccessConditions, con)
			} else {
				p.FilterConditions = append(p.FilterConditions, con)
			}
		} else {
			p.FilterConditions = append(p.FilterConditions, con)
		}
	}
	return p
}

func (b *planBuilder) buildIndexScanPlan(index *model.IndexInfo, path *joinPath) Plan {
	tn := path.table
	ip := &IndexScan{Table: tn.TableInfo, Index: index, TableName: tn}
	ip.RefAccess = len(path.eqConds) > 0
	ip.SetFields(tn.GetResultFields())
	ip.TableAsName = getTableAsName(ip.Fields())

	condMap := map[ast.ExprNode]bool{}
	for _, con := range path.conditions {
		condMap[con] = true
	}
out:
	// Build equal access conditions first.
	// Starts from the first index column, if equal condition is found, add it to access conditions,
	// proceed to the next index column. until we can't find any equal condition for the column.
	for ip.AccessEqualCount < len(index.Columns) {
		for con := range condMap {
			binop, ok := con.(*ast.BinaryOperationExpr)
			if !ok || binop.Op != opcode.EQ {
				continue
			}
			if ast.IsPreEvaluable(binop.L) {
				binop.L, binop.R = binop.R, binop.L
			}
			if !ast.IsPreEvaluable(binop.R) {
				continue
			}
			cn, ok2 := binop.L.(*ast.ColumnNameExpr)
			if !ok2 || cn.Refer.Column.Name.L != index.Columns[ip.AccessEqualCount].Name.L {
				continue
			}
			ip.AccessConditions = append(ip.AccessConditions, con)
			delete(condMap, con)
			ip.AccessEqualCount++
			continue out
		}
		break
	}

	for con := range condMap {
		if ip.AccessEqualCount < len(ip.Index.Columns) {
			// Try to add non-equal access condition for index column at AccessEqualCount.
			checker := conditionChecker{tableName: tn.TableInfo.Name, idx: index, columnOffset: ip.AccessEqualCount}
			if checker.check(con) {
				ip.AccessConditions = append(ip.AccessConditions, con)
			} else {
				ip.FilterConditions = append(ip.FilterConditions, con)
			}
		} else {
			ip.FilterConditions = append(ip.FilterConditions, con)
		}
	}
	return ip
}

// buildPseudoSelectPlan pre-builds more complete plans that may affect total cost.
// Also set OutOfOrder and NoLimit property.
func (b *planBuilder) buildPseudoSelectPlan(p Plan, sel *ast.SelectStmt) Plan {
	if sel.OrderBy == nil {
		return p
	}
	if sel.GroupBy != nil {
		return p
	}
	if !pushOrder(p, sel.OrderBy.Items) {
		switch x := p.(type) {
		case *IndexScan:
			x.OutOfOrder = true
			x.NoLimit = true
		}
		np := &Sort{ByItems: sel.OrderBy.Items}
		addChild(np, p)
		p = np
	}
	if sel.Limit != nil {
		np := &Limit{Offset: sel.Limit.Offset, Count: sel.Limit.Count}
		addChild(np, p)
		np.SetLimit(0)
		p = np
	} else {
		switch x := p.(type) {
		case *IndexScan:
			x.NoLimit = true
		}
	}
	return p
}

func (b *planBuilder) buildSelectLock(src Plan, lock ast.SelectLockType) *SelectLock {
	selectLock := &SelectLock{
		Lock:            lock,
		baseLogicalPlan: newBaseLogicalPlan(Lock, b.allocator),
	}
	selectLock.initID()
	addChild(selectLock, src)
	selectLock.SetSchema(src.GetSchema())
	selectLock.SetFields(src.Fields())
	return selectLock
}

func (b *planBuilder) buildSelectFields(src Plan, fields []*ast.ResultField) Plan {
	selectFields := &SelectFields{}
	addChild(selectFields, src)
	selectFields.SetFields(fields)
	return selectFields
}

func (b *planBuilder) buildAggregate(src Plan, aggFuncs []*ast.AggregateFuncExpr, groupby *ast.GroupByClause) Plan {
	// Add aggregate plan.
	aggPlan := &Aggregate{
		AggFuncs: aggFuncs,
	}
	addChild(aggPlan, src)
	if src != nil {
		aggPlan.SetFields(src.Fields())
	}
	if groupby != nil {
		aggPlan.GroupByItems = groupby.Items
	}
	return aggPlan
}

func (b *planBuilder) buildHaving(src Plan, having *ast.HavingClause) Plan {
	p := &Having{
		Conditions: splitWhere(having.Expr),
	}
	addChild(p, src)
	p.SetFields(src.Fields())
	return p
}

func (b *planBuilder) buildSort(src Plan, byItems []*ast.ByItem) Plan {
	sort := &Sort{
		ByItems: byItems,
	}
	addChild(sort, src)
	sort.SetFields(src.Fields())
	return sort
}

func (b *planBuilder) buildLimit(src Plan, limit *ast.Limit) Plan {
	li := &Limit{
		Offset: limit.Offset,
		Count:  limit.Count,
	}
	if s, ok := src.(*Sort); ok {
		s.ExecLimit = li
		return s
	}
	addChild(li, src)
	li.SetFields(src.Fields())
	li.SetSchema(src.GetSchema().DeepCopy())
	return li
}

func (b *planBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		p.SQLText, _ = x.SQLVar.GetValue().(string)
	} else {
		p.SQLText = x.SQLText
	}
	return p
}

func (b *planBuilder) buildAdmin(as *ast.AdminStmt) Plan {
	var p Plan

	switch as.Tp {
	case ast.AdminCheckTable:
		p = &CheckTable{Tables: as.Tables}
	case ast.AdminShowDDL:
		p = &ShowDDL{}
		p.SetFields(buildShowDDLFields())
	default:
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", as)
	}

	return p
}

func buildShowDDLFields() []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, 6)
	rfs = append(rfs, buildResultField("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField("", "OWNER", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField("", "JOB", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "BG_SCHEMA_VER", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField("", "BG_OWNER", mysql.TypeVarchar, 64))
	rfs = append(rfs, buildResultField("", "BG_JOB", mysql.TypeVarchar, 128))

	return rfs
}

func buildResultField(tableName, name string, tp byte, size int) *ast.ResultField {
	cs := charset.CharsetBin
	cl := charset.CharsetBin
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		cs = mysql.DefaultCharset
		cl = mysql.DefaultCollationName
		flag = 0
	}

	fieldType := types.FieldType{
		Charset: cs,
		Collate: cl,
		Tp:      tp,
		Flen:    size,
		Flag:    uint(flag),
	}
	colInfo := &model.ColumnInfo{
		Name:      model.NewCIStr(name),
		FieldType: fieldType,
	}
	expr := &ast.ValueExpr{}
	expr.SetType(&fieldType)

	return &ast.ResultField{
		Column:       colInfo,
		ColumnAsName: colInfo.Name,
		TableAsName:  model.NewCIStr(tableName),
		DBName:       model.NewCIStr(infoschema.Name),
		Expr:         expr,
	}
}

func pushLimit(p Plan, limit *ast.Limit) {
	switch x := p.(type) {
	case *IndexScan:
		limitCount := limit.Offset + limit.Count
		if limitCount < math.MaxInt64 {
			x.LimitCount = proto.Int64(int64(limitCount))
		}
	case *TableScan:
		limitCount := limit.Offset + limit.Count
		if limitCount < math.MaxInt64 {
			x.LimitCount = proto.Int64(int64(limitCount))
		}
	default:
		child := x.GetChildByIndex(0)
		if child != nil {
			pushLimit(child, limit)
		}
	}
}

// pushOrder tries to push order by items to the plan, returns true if
// order is pushed.
func pushOrder(p Plan, items []*ast.ByItem) bool {
	switch x := p.(type) {
	case *Aggregate:
		return false
	case *IndexScan:
		if len(items) > len(x.Index.Columns) {
			return false
		}
		var hasDesc bool
		var hasAsc bool
		for i, item := range items {
			var rf *ast.ResultField
			switch y := item.Expr.(type) {
			case *ast.ColumnNameExpr:
				rf = y.Refer
			case *ast.PositionExpr:
				rf = y.Refer
			default:
				return false
			}
			if rf.Table.Name.L != x.Table.Name.L || rf.Column.Name.L != x.Index.Columns[i].Name.L {
				return false
			}
			if item.Desc {
				if hasAsc {
					return false
				}
				hasDesc = true
			} else {
				if hasDesc {
					return false
				}
				hasAsc = true
			}
		}
		x.Desc = hasDesc
		return true
	case *TableScan:
		if len(items) != 1 || !x.Table.PKIsHandle {
			return false
		}
		var refer *ast.ResultField
		switch x := items[0].Expr.(type) {
		case *ast.ColumnNameExpr:
			refer = x.Refer
		case *ast.PositionExpr:
			refer = x.Refer
		default:
			return false
		}
		if mysql.HasPriKeyFlag(refer.Column.Flag) {
			x.Desc = items[0].Desc
			return true
		}
		return false
	case *JoinOuter:
		return false
	case *JoinInner:
		return false
	case *Sort:
		// Sort plan should not be checked here as there should only be one sort plan in a plan tree.
		return false
	default:
		child := x.GetChildByIndex(0)
		if child != nil {
			return pushOrder(child, items)
		}
	}
	return false
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.AndAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

// SubQueryBuilder is the interface for building SubQuery executor.
type SubQueryBuilder interface {
	Build(p Plan) ast.SubqueryExec
}

// subqueryVisitor visits AST and handles SubqueryExpr.
type subqueryVisitor struct {
	builder *planBuilder
}

func (se *subqueryVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.SubqueryExpr:
		p := se.builder.build(x.Query)
		// The expr pointer is copied into ResultField when running name resolver.
		// So we can not just replace the expr node in AST. We need to put SubQuery into the expr.
		// See optimizer.nameResolver.createResultFields()
		x.SubqueryExec = se.builder.sb.Build(p)
		return in, true
	case *ast.Join:
		// SubSelect in from clause will be handled in buildJoin().
		return in, true
	}
	return in, false
}

func (se *subqueryVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (b *planBuilder) buildUnion(union *ast.UnionStmt) Plan {
	sels := make([]Plan, len(union.SelectList.Selects))
	for i, sel := range union.SelectList.Selects {
		sels[i] = b.buildSelect(sel)
	}
	var p Plan
	p = &Union{
		Selects: sels,
	}
	unionFields := union.GetResultFields()
	for _, sel := range sels {
		for i, f := range sel.Fields() {
			if i == len(unionFields) {
				b.err = errors.New("The used SELECT statements have a different number of columns")
				return nil
			}
			uField := unionFields[i]
			/*
			 * The lengths of the columns in the UNION result take into account the values retrieved by all of the SELECT statements
			 * SELECT REPEAT('a',1) UNION SELECT REPEAT('b',10);
			 * +---------------+
			 * | REPEAT('a',1) |
			 * +---------------+
			 * | a             |
			 * | bbbbbbbbbb    |
			 * +---------------+
			 */
			if f.Column.Flen > uField.Column.Flen {
				uField.Column.Flen = f.Column.Flen
			}
			// For select nul union select "abc", we should not convert "abc" to nil.
			// And the result field type should be VARCHAR.
			if uField.Column.Tp == 0 || uField.Column.Tp == mysql.TypeNull {
				uField.Column.Tp = f.Column.Tp
			}
		}
		addChild(p, sel)
	}
	for _, v := range unionFields {
		v.Expr.SetType(&v.Column.FieldType)
	}

	p.SetFields(unionFields)
	if union.Distinct {
		p = b.buildDistinct(p)
	}
	if union.OrderBy != nil {
		p = b.buildSort(p, union.OrderBy.Items)
	}
	if union.Limit != nil {
		p = b.buildLimit(p, union.Limit)
	}
	return p
}

func (b *planBuilder) buildDistinct(src Plan) Plan {
	d := &Distinct{}
	addChild(d, src)
	d.SetFields(src.Fields())
	return d
}

func (b *planBuilder) buildUpdate(update *ast.UpdateStmt) Plan {
	sel := &ast.SelectStmt{From: update.TableRefs, Where: update.Where, OrderBy: update.Order, Limit: update.Limit}
	p := b.buildFrom(sel)
	for _, v := range p.Fields() {
		v.Referenced = true
	}
	if sel.OrderBy != nil && !pushOrder(p, sel.OrderBy.Items) {
		p = b.buildSort(p, sel.OrderBy.Items)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		pushLimit(p, sel.Limit)
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	orderedList := b.buildUpdateLists(update.List, p.Fields())
	if b.err != nil {
		return nil
	}
	return &Update{OrderedList: orderedList, SelectPlan: p, basePlan: basePlan{children: []Plan{p}}}
}

func (b *planBuilder) buildUpdateLists(list []*ast.Assignment, fields []*ast.ResultField) []*ast.Assignment {
	newList := make([]*ast.Assignment, len(fields))
	for _, assign := range list {
		offset, err := columnOffsetInFields(assign.Column, fields)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		newList[offset] = assign
	}
	return newList
}

func (b *planBuilder) buildDelete(del *ast.DeleteStmt) Plan {
	sel := &ast.SelectStmt{From: del.TableRefs, Where: del.Where, OrderBy: del.Order, Limit: del.Limit}
	p := b.buildFrom(sel)
	for _, v := range p.Fields() {
		v.Referenced = true
	}
	if sel.OrderBy != nil && !pushOrder(p, sel.OrderBy.Items) {
		p = b.buildSort(p, sel.OrderBy.Items)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		pushLimit(p, sel.Limit)
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	var tables []*ast.TableName
	if del.Tables != nil {
		tables = del.Tables.Tables
	}
	return &Delete{
		Tables:       tables,
		IsMultiTable: del.IsMultiTable,
		SelectPlan:   p,
		basePlan:     basePlan{children: []Plan{p}},
	}
}

func columnOffsetInFields(cn *ast.ColumnName, fields []*ast.ResultField) (int, error) {
	offset := -1
	tableNameL := cn.Table.L
	columnNameL := cn.Name.L
	if tableNameL != "" {
		for i, f := range fields {
			// Check table name.
			if f.TableAsName.L != "" {
				if tableNameL != f.TableAsName.L {
					continue
				}
			} else {
				if tableNameL != f.Table.Name.L {
					continue
				}
			}
			// Check column name.
			if f.ColumnAsName.L != "" {
				if columnNameL != f.ColumnAsName.L {
					continue
				}
			} else {
				if columnNameL != f.Column.Name.L {
					continue
				}
			}

			offset = i
		}
	} else {
		for i, f := range fields {
			matchAsName := f.ColumnAsName.L != "" && f.ColumnAsName.L == columnNameL
			matchColumnName := f.ColumnAsName.L == "" && f.Column.Name.L == columnNameL
			if matchAsName || matchColumnName {
				if offset != -1 {
					return -1, errors.Errorf("column %s is ambiguous.", cn.Name.O)
				}
				offset = i
			}
		}
	}
	if offset == -1 {
		return -1, errors.Errorf("column %s not found", cn.Name.O)
	}
	return offset, nil
}

func (b *planBuilder) buildShow(show *ast.ShowStmt) Plan {
	var p Plan
	p = &Show{
		Tp:     show.Tp,
		DBName: show.DBName,
		Table:  show.Table,
		Column: show.Column,
		Flag:   show.Flag,
		Full:   show.Full,
		User:   show.User,
	}
	p.SetFields(show.GetResultFields())
	var conditions []ast.ExprNode
	if show.Pattern != nil {
		conditions = append(conditions, show.Pattern)
	}
	if show.Where != nil {
		conditions = append(conditions, show.Where)
	}
	if len(conditions) != 0 {
		filter := &Filter{Conditions: conditions}
		addChild(filter, p)
		p = filter
	}
	return p
}

func (b *planBuilder) buildSimple(node ast.StmtNode) Plan {
	return &Simple{Statement: node}
}

func (b *planBuilder) buildInsert(insert *ast.InsertStmt) Plan {
	insertPlan := &Insert{
		Table:       insert.Table,
		Columns:     insert.Columns,
		Lists:       insert.Lists,
		Setlist:     insert.Setlist,
		OnDuplicate: insert.OnDuplicate,
		IsReplace:   insert.IsReplace,
		Priority:    insert.Priority,
	}
	if insert.Select != nil {
		insertPlan.SelectPlan = b.build(insert.Select)
		addChild(insertPlan, insertPlan.SelectPlan)
		if b.err != nil {
			return nil
		}
	}
	return insertPlan
}

func (b *planBuilder) buildDDL(node ast.DDLNode) Plan {
	return &DDL{Statement: node}
}

func (b *planBuilder) buildExplain(explain *ast.ExplainStmt) Plan {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(show)
	}
	targetPlan := b.build(explain.Stmt)
	if b.err != nil {
		return nil
	}
	p := &Explain{StmtPlan: targetPlan}
	addChild(p, targetPlan)
	p.SetFields(buildExplainFields())
	return p
}

// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
func buildExplainFields() []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, 10)
	rfs = append(rfs, buildResultField("", "id", mysql.TypeLonglong, 4))
	rfs = append(rfs, buildResultField("", "select_type", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "table", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "type", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "possible_keys", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "key", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "key_len", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "ref", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "rows", mysql.TypeVarchar, 128))
	rfs = append(rfs, buildResultField("", "Extra", mysql.TypeVarchar, 128))
	return rfs
}
