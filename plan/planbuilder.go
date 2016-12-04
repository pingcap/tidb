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
	"github.com/juju/errors"
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

// planBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type planBuilder struct {
	err          error
	hasAgg       bool
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
	case *ast.LoadDataStmt:
		return b.buildLoadData(x)
	case *ast.PrepareStmt:
		return b.buildPrepare(x)
	case *ast.SelectStmt:
		return b.buildSelect(x)
	case *ast.UnionStmt:
		return b.buildUnion(x)
	case *ast.UpdateStmt:
		return b.buildUpdate(x)
	case *ast.ShowStmt:
		return b.buildShow(x)
	case *ast.AnalyzeTableStmt, *ast.BinlogStmt, *ast.FlushTableStmt, *ast.UseStmt, *ast.SetStmt, *ast.DoStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt:
		return b.buildSimple(node.(ast.StmtNode))
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
			b.err = errors.New("failed to extract agg expr")
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

func availableIndices(table *ast.TableName) (indices []*model.IndexInfo, includeTableScan bool) {
	var usableHints []*ast.IndexHint
	for _, hint := range table.IndexHints {
		if hint.HintScope == ast.HintForScan {
			usableHints = append(usableHints, hint)
		}
	}
	publicIndices := make([]*model.IndexInfo, 0, len(table.TableInfo.Indices))
	for _, index := range table.TableInfo.Indices {
		if index.State == model.StatePublic {
			publicIndices = append(publicIndices, index)
		}
	}
	if len(usableHints) == 0 {
		return publicIndices, true
	}
	var hasUse bool
	var ignores []*model.IndexInfo
	for _, hint := range usableHints {
		switch hint.HintType {
		case ast.HintUse, ast.HintForce:
			// Currently we don't distinguish between Force and Use because our cost estimation is not reliable.
			hasUse = true
			for _, idxName := range hint.IndexNames {
				idx := findIndexByName(publicIndices, idxName)
				if idx != nil {
					indices = append(indices, idx)
				}
			}
		case ast.HintIgnore:
			// Collect all the ignore index hints.
			for _, idxName := range hint.IndexNames {
				idx := findIndexByName(publicIndices, idxName)
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
	return removeIgnores(publicIndices, ignores), true
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

func (b *planBuilder) buildSelectLock(src Plan, lock ast.SelectLockType) *SelectLock {
	selectLock := &SelectLock{
		Lock:            lock,
		baseLogicalPlan: newBaseLogicalPlan(Lock, b.allocator),
	}
	selectLock.self = selectLock
	selectLock.initIDAndContext(b.ctx)
	addChild(selectLock, src)
	selectLock.SetSchema(src.GetSchema())
	return selectLock
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
		p.SetSchema(buildShowDDLFields())
	default:
		b.err = ErrUnsupportedType.Gen("Unsupported type %T", as)
	}

	return p
}

func buildShowDDLFields() expression.Schema {
	schema := make(expression.Schema, 0, 6)
	schema = append(schema, buildColumn("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema = append(schema, buildColumn("", "OWNER", mysql.TypeVarchar, 64))
	schema = append(schema, buildColumn("", "JOB", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn("", "BG_SCHEMA_VER", mysql.TypeLonglong, 4))
	schema = append(schema, buildColumn("", "BG_OWNER", mysql.TypeVarchar, 64))
	schema = append(schema, buildColumn("", "BG_JOB", mysql.TypeVarchar, 128))

	return schema
}

func buildColumn(tableName, name string, tp byte, size int) *expression.Column {
	cs := charset.CharsetBin
	cl := charset.CharsetBin
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		cs = mysql.DefaultCharset
		cl = mysql.DefaultCollationName
		flag = 0
	}

	fieldType := &types.FieldType{
		Charset: cs,
		Collate: cl,
		Tp:      tp,
		Flen:    size,
		Flag:    uint(flag),
	}
	return &expression.Column{
		ColName: model.NewCIStr(name),
		TblName: model.NewCIStr(tableName),
		DBName:  model.NewCIStr(infoschema.Name),
		RetType: fieldType,
	}
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

func (b *planBuilder) buildShow(show *ast.ShowStmt) Plan {
	var resultPlan Plan
	p := &Show{
		Tp:              show.Tp,
		DBName:          show.DBName,
		Table:           show.Table,
		Column:          show.Column,
		Flag:            show.Flag,
		Full:            show.Full,
		User:            show.User,
		baseLogicalPlan: newBaseLogicalPlan("Show", b.allocator),
	}
	resultPlan = p
	p.initIDAndContext(b.ctx)
	p.self = p
	switch show.Tp {
	case ast.ShowProcedureStatus:
		p.SetSchema(buildShowProcedureSchema())
	case ast.ShowTriggers:
		p.SetSchema(buildShowTriggerSchema())
	case ast.ShowEvents:
		p.SetSchema(buildShowEventsSchema())
	default:
		p.SetSchema(expression.ResultFieldsToSchema(show.GetResultFields()))
	}
	for i, col := range p.schema {
		col.Position = i
	}
	var conditions []expression.Expression
	if show.Pattern != nil {
		expr, _, err := b.rewrite(show.Pattern, p, nil, false)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		conditions = append(conditions, expr)
	}
	if show.Where != nil {
		conds := splitWhere(show.Where)
		for _, cond := range conds {
			expr, _, err := b.rewrite(cond, p, nil, false)
			if err != nil {
				b.err = errors.Trace(err)
				return nil
			}
			conditions = append(conditions, expr)
		}
	}
	if len(conditions) != 0 {
		sel := &Selection{
			baseLogicalPlan: newBaseLogicalPlan(Sel, b.allocator),
			Conditions:      conditions,
		}
		sel.initIDAndContext(b.ctx)
		sel.self = sel
		addChild(sel, p)
		resultPlan = sel
	}
	return resultPlan
}

func (b *planBuilder) buildSimple(node ast.StmtNode) Plan {
	return &Simple{Statement: node}
}

func (b *planBuilder) buildInsert(insert *ast.InsertStmt) Plan {
	insertPlan := &Insert{
		Table:           insert.Table,
		Columns:         insert.Columns,
		Lists:           insert.Lists,
		Setlist:         insert.Setlist,
		OnDuplicate:     insert.OnDuplicate,
		IsReplace:       insert.IsReplace,
		Priority:        insert.Priority,
		Ignore:          insert.Ignore,
		baseLogicalPlan: newBaseLogicalPlan(Ins, b.allocator),
	}
	insertPlan.initIDAndContext(b.ctx)
	insertPlan.self = insertPlan
	if insert.Select != nil {
		selectPlan := b.build(insert.Select)
		if b.err != nil {
			return nil
		}
		addChild(insertPlan, selectPlan)
	}
	return insertPlan
}

func (b *planBuilder) buildLoadData(ld *ast.LoadDataStmt) Plan {
	p := &LoadData{
		IsLocal:    ld.IsLocal,
		Path:       ld.Path,
		Table:      ld.Table,
		FieldsInfo: ld.FieldsInfo,
		LinesInfo:  ld.LinesInfo,
	}
	return p
}

func (b *planBuilder) buildDDL(node ast.DDLNode) Plan {
	return &DDL{Statement: node}
}

func (b *planBuilder) buildExplain(explain *ast.ExplainStmt) Plan {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(show)
	}
	targetPlan, err := Optimize(b.ctx, explain.Stmt, b.is)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	p := &Explain{StmtPlan: targetPlan}
	addChild(p, targetPlan)
	schema := make(expression.Schema, 0, 3)
	schema = append(schema, &expression.Column{
		ColName: model.NewCIStr("ID"),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	schema = append(schema, &expression.Column{
		ColName: model.NewCIStr("Json"),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	schema = append(schema, &expression.Column{
		ColName: model.NewCIStr("ParentID"),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	p.SetSchema(schema)
	return p
}

func buildShowProcedureSchema() expression.Schema {
	tblName := "ROUTINES"
	schema := make(expression.Schema, 0, 11)
	schema = append(schema, buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Modified", mysql.TypeDatetime, 19))
	schema = append(schema, buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema = append(schema, buildColumn(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Comment", mysql.TypeBlob, 196605))
	schema = append(schema, buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowTriggerSchema() expression.Schema {
	tblName := "TRIGGERS"
	schema := make(expression.Schema, 0, 11)
	schema = append(schema, buildColumn(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Event", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Table", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Statement", mysql.TypeBlob, 196605))
	schema = append(schema, buildColumn(tblName, "Timing", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema = append(schema, buildColumn(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema = append(schema, buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowEventsSchema() expression.Schema {
	tblName := "EVENTS"
	schema := make(expression.Schema, 0, 15)
	schema = append(schema, buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema = append(schema, buildColumn(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema = append(schema, buildColumn(tblName, "Starts", mysql.TypeDatetime, 19))
	schema = append(schema, buildColumn(tblName, "Ends", mysql.TypeDatetime, 19))
	schema = append(schema, buildColumn(tblName, "Status", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "Originator", mysql.TypeInt24, 4))
	schema = append(schema, buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema = append(schema, buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}
