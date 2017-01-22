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
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

// Error instances.
var (
	ErrUnsupportedType      = terror.ClassOptimizerPlan.New(CodeUnsupportedType, "Unsupported type")
	SystemInternalErrorType = terror.ClassOptimizerPlan.New(SystemInternalError, "System internal error")
	ErrUnknownColumn        = terror.ClassOptimizerPlan.New(CodeUnknownColumn, "Unknown column '%s' in '%s'")
	ErrWrongArguments       = terror.ClassOptimizerPlan.New(CodeWrongArguments, "Incorrect arguments to EXECUTE")
	ErrAmbiguous            = terror.ClassOptimizerPlan.New(CodeAmbiguous, "Column '%s' in field list is ambiguous")
)

// Error codes.
const (
	CodeUnsupportedType terror.ErrCode = 1
	SystemInternalError terror.ErrCode = 2
	CodeAmbiguous       terror.ErrCode = 1052
	CodeUnknownColumn   terror.ErrCode = 1054
	CodeWrongArguments  terror.ErrCode = 1210
)

func init() {
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		CodeUnknownColumn:  mysql.ErrBadField,
		CodeAmbiguous:      mysql.ErrNonUniq,
		CodeWrongArguments: mysql.ErrWrongArguments,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizerPlan] = tableMySQLErrCodes
}

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
	inUpdateStmt bool
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
		return b.buildExecute(x)
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
	case *ast.DoStmt:
		return b.buildDo(x)
	case *ast.SetStmt:
		return b.buildSet(x)
	case *ast.AnalyzeTableStmt, *ast.BinlogStmt, *ast.FlushTableStmt, *ast.UseStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt:
		return b.buildSimple(node.(ast.StmtNode))
	case *ast.TruncateTableStmt:
		return b.buildDDL(x)
	case *ast.RenameTableStmt:
		return b.buildDDL(x)
	}
	b.err = ErrUnsupportedType.Gen("Unsupported type %T", node)
	return nil
}

func (b *planBuilder) buildExecute(v *ast.ExecuteStmt) Plan {
	vars := make([]expression.Expression, 0, len(v.UsingVars))
	for _, expr := range v.UsingVars {
		newExpr, _, err := b.rewrite(expr, nil, nil, true)
		if err != nil {
			b.err = errors.Trace(err)
		}
		vars = append(vars, newExpr)
	}
	exe := &Execute{Name: v.Name, UsingVars: vars}
	return exe
}

func (b *planBuilder) buildDo(v *ast.DoStmt) Plan {
	exprs := make([]expression.Expression, 0, len(v.Exprs))
	dual := &TableDual{
		baseLogicalPlan: newBaseLogicalPlan(Dual, b.allocator),
	}
	dual.self = dual
	for _, astExpr := range v.Exprs {
		expr, _, err := b.rewrite(astExpr, dual, nil, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		exprs = append(exprs, expr)
	}
	p := &Projection{
		Exprs:           exprs,
		baseLogicalPlan: newBaseLogicalPlan(Proj, b.allocator),
	}
	p.initIDAndContext(b.ctx)
	addChild(p, dual)
	p.self = p
	return p
}

func (b *planBuilder) buildSet(v *ast.SetStmt) Plan {
	p := &Set{}
	p.tp = St
	p.allocator = b.allocator
	for _, vars := range v.Variables {
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			assign.Expr, _, b.err = b.rewrite(vars.Value, nil, nil, true)
			if b.err != nil {
				return nil
			}
		} else {
			assign.IsDefault = true
		}
		if vars.ExtendValue != nil {
			assign.ExtendValue = &expression.Constant{
				Value:   vars.ExtendValue.Datum,
				RetType: &vars.ExtendValue.Type,
			}
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	p.initIDAndContext(b.ctx)
	return p
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

func availableIndices(hints []*ast.IndexHint, tableInfo *model.TableInfo) (indices []*model.IndexInfo, includeTableScan bool) {
	var usableHints []*ast.IndexHint
	for _, hint := range hints {
		if hint.HintScope == ast.HintForScan {
			usableHints = append(usableHints, hint)
		}
	}
	publicIndices := make([]*model.IndexInfo, 0, len(tableInfo.Indices))
	for _, index := range tableInfo.Indices {
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
	schema := expression.NewSchema(make([]*expression.Column, 0, 6))
	schema.Append(buildColumn("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "OWNER", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "JOB", mysql.TypeVarchar, 128))
	schema.Append(buildColumn("", "BG_SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "BG_OWNER", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "BG_JOB", mysql.TypeVarchar, 128))

	return schema
}

func buildColumn(tableName, name string, tp byte, size int) *expression.Column {
	cs, cl := types.DefaultCharsetForType(tp)
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
		p.SetSchema(buildShowDefaultSchema(show))
	}
	for i, col := range p.schema.Columns {
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

func (b *planBuilder) getDefaultValue(col *table.Column) (*expression.Constant, error) {
	if value, ok, err := table.GetColDefaultValue(b.ctx, col.ToInfo()); ok {
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
	}
	return &expression.Constant{RetType: &col.FieldType}, nil
}

func (b *planBuilder) findDefaultValue(cols []*table.Column, name *ast.ColumnName) (*expression.Constant, error) {
	for _, col := range cols {
		if col.Name.L == name.Name.L {
			return b.getDefaultValue(col)
		}
	}
	return nil, ErrUnknownColumn.GenByArgs(name.Name.O, "field_list")
}

func (b *planBuilder) buildInsert(insert *ast.InsertStmt) Plan {
	// Get Table
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		b.err = infoschema.ErrTableNotExists.GenByArgs()
		return nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		b.err = infoschema.ErrTableNotExists.GenByArgs()
		return nil
	}
	tableInfo := tn.TableInfo
	schema := expression.TableInfo2Schema(tableInfo)
	table, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can't get table %s.", tableInfo.Name.O)
		return nil
	}
	insertPlan := &Insert{
		Table:           table,
		Columns:         insert.Columns,
		tableSchema:     schema,
		IsReplace:       insert.IsReplace,
		Priority:        insert.Priority,
		Ignore:          insert.Ignore,
		baseLogicalPlan: newBaseLogicalPlan(Ins, b.allocator),
	}
	cols := table.Cols()
	for _, valuesItem := range insert.Lists {
		exprList := make([]expression.Expression, 0, len(valuesItem))
		for i, valueItem := range valuesItem {
			var expr expression.Expression
			var err error
			if dft, ok := valueItem.(*ast.DefaultExpr); ok {
				if dft.Name != nil {
					expr, err = b.findDefaultValue(cols, dft.Name)
				} else {
					expr, err = b.getDefaultValue(cols[i])
				}
			} else if val, ok := valueItem.(*ast.ValueExpr); ok {
				expr = &expression.Constant{
					Value:   val.Datum,
					RetType: &val.Type,
				}
			} else {
				expr, _, err = b.rewrite(valueItem, nil, nil, true)
			}
			if err != nil {
				b.err = errors.Trace(err)
			}
			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	for _, assign := range insert.Setlist {
		col, err := schema.FindColumn(assign.Column)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		if col == nil {
			b.err = errors.Errorf("Can't find column %s", assign.Column)
			return nil
		}
		// Here we keep different behaviours with MySQL. MySQL allow set a = b, b = a and the result is NULL, NULL.
		// It's unreasonable.
		expr, _, err := b.rewrite(assign.Expr, nil, nil, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		insertPlan.Setlist = append(insertPlan.Setlist, &expression.Assignment{
			Col:  col,
			Expr: expr,
		})
	}
	mockTablePlan := &TableDual{}
	mockTablePlan.SetSchema(schema)
	for _, assign := range insert.OnDuplicate {
		col, err := schema.FindColumn(assign.Column)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		if col == nil {
			b.err = errors.Errorf("Can't find column %s", assign.Column)
			return nil
		}
		expr, _, err := b.rewrite(assign.Expr, mockTablePlan, nil, true)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		insertPlan.OnDuplicate = append(insertPlan.OnDuplicate, &expression.Assignment{
			Col:  col,
			Expr: expr,
		})
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
	schema := expression.NewSchema(make([]*expression.Column, 0, 3))
	schema.Append(&expression.Column{
		ColName: model.NewCIStr("ID"),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	schema.Append(&expression.Column{
		ColName: model.NewCIStr("Json"),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	schema.Append(&expression.Column{
		ColName: model.NewCIStr("ParentID"),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	p.SetSchema(schema)
	return p
}

func buildShowProcedureSchema() expression.Schema {
	tblName := "ROUTINES"
	schema := expression.NewSchema(make([]*expression.Column, 0, 11))
	schema.Append(buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Modified", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Comment", mysql.TypeBlob, 196605))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowTriggerSchema() expression.Schema {
	tblName := "TRIGGERS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 11))
	schema.Append(buildColumn(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Event", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Table", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Statement", mysql.TypeBlob, 196605))
	schema.Append(buildColumn(tblName, "Timing", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowEventsSchema() expression.Schema {
	tblName := "EVENTS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 15))
	schema.Append(buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Starts", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Ends", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Status", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Originator", mysql.TypeInt24, 4))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

// getShowColNamesAndTypes gets column names and types. If the `ftypes` is empty, every column is set to varchar type.
func getShowColNamesAndTypes(s *ast.ShowStmt) (names []string, ftypes []byte) {
	switch s.Tp {
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowDatabases:
		names = []string{"Database"}
	case ast.ShowTables:
		names = []string{fmt.Sprintf("Tables_in_%s", s.DBName)}
		if s.Full {
			names = append(names, "Table_type")
		}
	case ast.ShowTableStatus:
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "Update_time", "Check_time", "Collation", "Checksum",
			"Create_options", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowColumns:
		names = table.ColDescFieldNames(s.Full)
	case ast.ShowWarnings:
		names = []string{"Level", "Code", "Message"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar}
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowCreateTable:
		names = []string{"Table", "Create Table"}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowGrants:
		names = []string{fmt.Sprintf("Grants for %s", s.User)}
	case ast.ShowIndex:
		names = []string{"Table", "Non_unique", "Key_name", "Seq_in_index",
			"Column_name", "Collation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowProcessList:
		names = []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	}
	return
}

func buildShowDefaultSchema(s *ast.ShowStmt) expression.Schema {
	names, ftypes := getShowColNamesAndTypes(s)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(names)))
	for i, name := range names {
		col := &expression.Column{
			ColName: model.NewCIStr(name),
		}
		var retType types.FieldType
		if len(ftypes) == 0 || ftypes[i] == 0 {
			// Use varchar as the default return column type.
			retType.Tp = mysql.TypeVarchar
		} else {
			retType.Tp = ftypes[i]
		}
		retType.Charset, retType.Collate = types.DefaultCharsetForType(retType.Tp)
		col.RetType = &retType
		schema.Append(col)
	}
	return schema
}
