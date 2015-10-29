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

package optimizer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/table"
)

func convertAssignment(converter *expressionConverter, v *ast.Assignment) (*expression.Assignment, error) {
	oldAssign := &expression.Assignment{
		ColName: joinColumnName(v.Column),
	}
	oldExpr, err := convertExpr(converter, v.Expr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldAssign.Expr = oldExpr
	return oldAssign, nil
}

func convertInsert(v *ast.InsertStmt) (*stmts.InsertIntoStmt, error) {
	oldInsert := &stmts.InsertIntoStmt{
		Priority: v.Priority,
		Text:     v.Text(),
	}
	tableName := v.Table.TableRefs.Left.(*ast.TableName)
	oldInsert.TableIdent = table.Ident{Schema: tableName.Schema, Name: tableName.Name}
	for _, val := range v.Columns {
		oldInsert.ColNames = append(oldInsert.ColNames, joinColumnName(val))
	}
	converter := newExpressionConverter()
	for _, row := range v.Lists {
		var oldRow []expression.Expression
		for _, val := range row {
			oldExpr, err := convertExpr(converter, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			oldRow = append(oldRow, oldExpr)
		}
		oldInsert.Lists = append(oldInsert.Lists, oldRow)
	}
	for _, assign := range v.Setlist {
		oldAssign, err := convertAssignment(converter, assign)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldInsert.Setlist = append(oldInsert.Setlist, oldAssign)
	}
	for _, onDup := range v.OnDuplicate {
		oldOnDup, err := convertAssignment(converter, onDup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldInsert.OnDuplicate = append(oldInsert.OnDuplicate, *oldOnDup)
	}
	if v.Select != nil {
		var err error
		switch x := v.Select.(type) {
		case *ast.SelectStmt:
			oldInsert.Sel, err = convertSelect(x)
		case *ast.UnionStmt:
			oldInsert.Sel, err = convertUnion(x)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return oldInsert, nil
}

func convertDelete(v *ast.DeleteStmt) (*stmts.DeleteStmt, error) {
	oldDelete := &stmts.DeleteStmt{
		BeforeFrom:  v.BeforeFrom,
		Ignore:      v.Ignore,
		LowPriority: v.LowPriority,
		MultiTable:  v.MultiTable,
		Quick:       v.Quick,
		Text:        v.Text(),
	}
	converter := newExpressionConverter()
	oldRefs, err := convertJoin(converter, v.TableRefs.TableRefs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldDelete.Refs = oldRefs
	for _, val := range v.Tables {
		tableIdent := table.Ident{Schema: val.Schema, Name: val.Name}
		oldDelete.TableIdents = append(oldDelete.TableIdents, tableIdent)
	}
	if v.Where != nil {
		oldDelete.Where, err = convertExpr(converter, v.Where)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if v.Order != nil {
		orderRset := &rsets.OrderByRset{}
		for _, val := range v.Order {
			oldExpr, err := convertExpr(converter, val.Expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			orderItem := rsets.OrderByItem{Expr: oldExpr, Asc: !val.Desc}
			orderRset.By = append(orderRset.By, orderItem)
		}
		oldDelete.Order = orderRset
	}
	if v.Limit != nil {
		oldDelete.Limit = &rsets.LimitRset{Count: v.Limit.Count}
	}
	return oldDelete, nil
}

func convertUpdate(v *ast.UpdateStmt) (*stmts.UpdateStmt, error) {
	oldUpdate := &stmts.UpdateStmt{
		Ignore:        v.Ignore,
		MultipleTable: v.MultipleTable,
		LowPriority:   v.LowPriority,
		Text:          v.Text(),
	}
	converter := newExpressionConverter()
	var err error
	oldUpdate.TableRefs, err = convertJoin(converter, v.TableRefs.TableRefs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if v.Where != nil {
		oldUpdate.Where, err = convertExpr(converter, v.Where)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, val := range v.List {
		var oldAssign *expression.Assignment
		oldAssign, err = convertAssignment(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldUpdate.List = append(oldUpdate.List, *oldAssign)
	}
	if v.Order != nil {
		orderRset := &rsets.OrderByRset{}
		for _, val := range v.Order {
			oldExpr, err := convertExpr(converter, val.Expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			orderItem := rsets.OrderByItem{Expr: oldExpr, Asc: !val.Desc}
			orderRset.By = append(orderRset.By, orderItem)
		}
		oldUpdate.Order = orderRset
	}
	if v.Limit != nil {
		oldUpdate.Limit = &rsets.LimitRset{Count: v.Limit.Count}
	}
	return oldUpdate, nil
}

func convertSelect(s *ast.SelectStmt) (*stmts.SelectStmt, error) {
	converter := &expressionConverter{
		exprMap: map[ast.Node]expression.Expression{},
	}
	oldSelect := &stmts.SelectStmt{}
	oldSelect.Distinct = s.Distinct
	oldSelect.Fields = make([]*field.Field, len(s.Fields.Fields))
	for i, val := range s.Fields.Fields {
		oldField := &field.Field{}
		oldField.AsName = val.AsName.O
		var err error
		oldField.Expr, err = convertExpr(converter, val.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldSelect.Fields[i] = oldField
	}
	var err error
	if s.From != nil {
		oldSelect.From, err = convertJoin(converter, s.From.TableRefs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.Where != nil {
		oldSelect.Where = &rsets.WhereRset{}
		oldSelect.Where.Expr, err = convertExpr(converter, s.Where)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if s.GroupBy != nil {
		oldSelect.GroupBy, err = convertGroupBy(converter, s.GroupBy)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.Having != nil {
		oldSelect.Having, err = convertHaving(converter, s.Having)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.OrderBy != nil {
		oldSelect.OrderBy, err = convertOrderBy(converter, s.OrderBy)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.Limit != nil {
		if s.Limit.Offset > 0 {
			oldSelect.Offset = &rsets.OffsetRset{Count: s.Limit.Offset}
		}
		if s.Limit.Count > 0 {
			oldSelect.Limit = &rsets.LimitRset{Count: s.Limit.Count}
		}
	}
	switch s.LockTp {
	case ast.SelectLockForUpdate:
		oldSelect.Lock = coldef.SelectLockForUpdate
	case ast.SelectLockInShareMode:
		oldSelect.Lock = coldef.SelectLockInShareMode
	case ast.SelectLockNone:
		oldSelect.Lock = coldef.SelectLockNone
	}
	return oldSelect, nil
}

func convertUnion(u *ast.UnionStmt) (*stmts.UnionStmt, error) {
	oldUnion := &stmts.UnionStmt{}
	oldUnion.Selects = make([]*stmts.SelectStmt, len(u.Selects))
	oldUnion.Distincts = make([]bool, len(u.Selects)-1)
	if u.Distinct {
		for i := range oldUnion.Distincts {
			oldUnion.Distincts[i] = true
		}
	}
	for i, val := range u.Selects {
		oldSelect, err := convertSelect(val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldUnion.Selects[i] = oldSelect
	}
	if u.Limit != nil {
		if u.Limit.Offset > 0 {
			oldUnion.Offset = &rsets.OffsetRset{Count: u.Limit.Offset}
		}
		if u.Limit.Count > 0 {
			oldUnion.Limit = &rsets.LimitRset{Count: u.Limit.Count}
		}
	}
	// Union order by can not converted to old because it is pushed to select statements.
	return oldUnion, nil
}

func convertJoin(converter *expressionConverter, join *ast.Join) (*rsets.JoinRset, error) {
	oldJoin := &rsets.JoinRset{}
	switch join.Tp {
	case ast.CrossJoin:
		oldJoin.Type = rsets.CrossJoin
	case ast.LeftJoin:
		oldJoin.Type = rsets.LeftJoin
	case ast.RightJoin:
		oldJoin.Type = rsets.RightJoin
	}
	switch l := join.Left.(type) {
	case *ast.Join:
		oldLeft, err := convertJoin(converter, l)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldJoin.Left = oldLeft
	case *ast.TableSource:
		oldLeft, err := convertTableSource(converter, l)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldJoin.Left = oldLeft
	}

	switch r := join.Left.(type) {
	case *ast.Join:
		oldRight, err := convertJoin(converter, r)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldJoin.Right = oldRight
	case *ast.TableSource:
		oldRight, err := convertTableSource(converter, r)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldJoin.Right = oldRight
	case nil:
	}

	if join.On != nil {
		oldOn, err := convertExpr(converter, join.On.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldJoin.On = oldOn
	}
	return oldJoin, nil
}

func convertTableSource(converter *expressionConverter, ts *ast.TableSource) (*rsets.TableSource, error) {
	oldTs := &rsets.TableSource{}
	oldTs.Name = ts.AsName.O
	switch src := ts.Source.(type) {
	case *ast.TableName:
		oldTs.Source = table.Ident{Schema: src.Schema, Name: src.Name}
	case *ast.SelectStmt:
		oldSelect, err := convertSelect(src)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldTs.Source = oldSelect
	case *ast.UnionStmt:
		oldUnion, err := convertUnion(src)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldTs.Source = oldUnion
	}
	return oldTs, nil
}

func convertGroupBy(converter *expressionConverter, gb *ast.GroupByClause) (*rsets.GroupByRset, error) {
	oldGroupBy := &rsets.GroupByRset{
		By: make([]expression.Expression, len(gb.Items)),
	}
	for i, val := range gb.Items {
		oldExpr, err := convertExpr(converter, val.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldGroupBy.By[i] = oldExpr
	}
	return oldGroupBy, nil
}

func convertHaving(converter *expressionConverter, having *ast.HavingClause) (*rsets.HavingRset, error) {
	oldHaving := &rsets.HavingRset{}
	oldExpr, err := convertExpr(converter, having.Expr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldHaving.Expr = oldExpr
	return oldHaving, nil
}

func convertOrderBy(converter *expressionConverter, orderBy *ast.OrderByClause) (*rsets.OrderByRset, error) {
	oldOrderBy := &rsets.OrderByRset{}
	oldOrderBy.By = make([]rsets.OrderByItem, len(orderBy.Items))
	for i, val := range orderBy.Items {
		oldByItemExpr, err := convertExpr(converter, val.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldOrderBy.By[i].Expr = oldByItemExpr
		oldOrderBy.By[i].Asc = !val.Desc
	}
	return oldOrderBy, nil
}

func convertCreateDatabase(v *ast.CreateDatabaseStmt) (*stmts.CreateDatabaseStmt, error) {
	oldCreateDatabase := &stmts.CreateDatabaseStmt{
		IfNotExists: v.IfNotExists,
		Name:        v.Name,
		Text:        v.Text(),
	}
	if len(v.Options) != 0 {
		oldCreateDatabase.Opt = &coldef.CharsetOpt{}
		for _, val := range v.Options {
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				oldCreateDatabase.Opt.Chs = val.Value
			case ast.DatabaseOptionCollate:
				oldCreateDatabase.Opt.Col = val.Value
			}
		}
	}
	return oldCreateDatabase, nil
}

func convertDropDatabase(v *ast.DropDatabaseStmt) (*stmts.DropDatabaseStmt, error) {
	return &stmts.DropDatabaseStmt{
		IfExists: v.IfExists,
		Name:     v.Name,
		Text:     v.Text(),
	}, nil
}

func convertColumnOption(converter *expressionConverter, v *ast.ColumnOption) (*coldef.ConstraintOpt, error) {
	oldColumnOpt := &coldef.ConstraintOpt{}
	switch v.Tp {
	case ast.ColumnOptionAutoIncrement:
		oldColumnOpt.Tp = coldef.ConstrAutoIncrement
	case ast.ColumnOptionComment:
		oldColumnOpt.Tp = coldef.ConstrComment
	case ast.ColumnOptionDefaultValue:
		oldColumnOpt.Tp = coldef.ConstrDefaultValue
	case ast.ColumnOptionIndex:
		oldColumnOpt.Tp = coldef.ConstrIndex
	case ast.ColumnOptionKey:
		oldColumnOpt.Tp = coldef.ConstrKey
	case ast.ColumnOptionFulltext:
		oldColumnOpt.Tp = coldef.ConstrFulltext
	case ast.ColumnOptionNotNull:
		oldColumnOpt.Tp = coldef.ConstrNotNull
	case ast.ColumnOptionNoOption:
		oldColumnOpt.Tp = coldef.ConstrNoConstr
	case ast.ColumnOptionOnUpdate:
		oldColumnOpt.Tp = coldef.ConstrOnUpdate
	case ast.ColumnOptionPrimaryKey:
		oldColumnOpt.Tp = coldef.ConstrPrimaryKey
	case ast.ColumnOptionNull:
		oldColumnOpt.Tp = coldef.ConstrNull
	case ast.ColumnOptionUniq:
		oldColumnOpt.Tp = coldef.ConstrUniq
	case ast.ColumnOptionUniqIndex:
		oldColumnOpt.Tp = coldef.ConstrUniqIndex
	case ast.ColumnOptionUniqKey:
		oldColumnOpt.Tp = coldef.ConstrUniqKey
	}
	if v.Expr != nil {
		oldExpr, err := convertExpr(converter, v.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldColumnOpt.Evalue = oldExpr
	}
	return oldColumnOpt, nil
}

func convertColumnDef(converter *expressionConverter, v *ast.ColumnDef) (*coldef.ColumnDef, error) {
	oldColDef := &coldef.ColumnDef{
		Name: v.Name.Name.O,
		Tp:   v.Tp,
	}
	for _, val := range v.Options {
		oldOpt, err := convertColumnOption(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldColDef.Constraints = append(oldColDef.Constraints, oldOpt)
	}
	return oldColDef, nil
}

func convertIndexColNames(v []*ast.IndexColName) (out []*coldef.IndexColName) {
	for _, val := range v {
		oldIndexColKey := &coldef.IndexColName{
			ColumnName: val.Column.Name.O,
			Length:     val.Length,
		}
		out = append(out, oldIndexColKey)
	}
	return
}

func convertConstraint(converter *expressionConverter, v *ast.Constraint) (*coldef.TableConstraint, error) {
	oldConstraint := &coldef.TableConstraint{ConstrName: v.Name}
	switch v.Tp {
	case ast.ConstraintNoConstraint:
		oldConstraint.Tp = coldef.ConstrNoConstr
	case ast.ConstraintPrimaryKey:
		oldConstraint.Tp = coldef.ConstrPrimaryKey
	case ast.ConstraintKey:
		oldConstraint.Tp = coldef.ConstrKey
	case ast.ConstraintIndex:
		oldConstraint.Tp = coldef.ConstrIndex
	case ast.ConstraintUniq:
		oldConstraint.Tp = coldef.ConstrUniq
	case ast.ConstraintUniqKey:
		oldConstraint.Tp = coldef.ConstrUniqKey
	case ast.ConstraintUniqIndex:
		oldConstraint.Tp = coldef.ConstrUniqIndex
	case ast.ConstraintForeignKey:
		oldConstraint.Tp = coldef.ConstrForeignKey
	case ast.ConstraintFulltext:
		oldConstraint.Tp = coldef.ConstrFulltext
	}
	oldConstraint.Keys = convertIndexColNames(v.Keys)
	if v.Refer != nil {
		oldConstraint.Refer = &coldef.ReferenceDef{
			TableIdent:    table.Ident{Schema: v.Refer.Table.Schema, Name: v.Refer.Table.Name},
			IndexColNames: convertIndexColNames(v.Refer.IndexColNames),
		}
	}
	return oldConstraint, nil
}

func convertCreateTable(v *ast.CreateTableStmt) (*stmts.CreateTableStmt, error) {
	oldCreateTable := &stmts.CreateTableStmt{
		Ident: table.Ident{Schema: v.Table.Schema, Name: v.Table.Name},
		Text:  v.Text(),
	}
	converter := newExpressionConverter()
	for _, val := range v.Cols {
		oldColDef, err := convertColumnDef(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldCreateTable.Cols = append(oldCreateTable.Cols, oldColDef)
	}
	for _, val := range v.Constraints {
		oldConstr, err := convertConstraint(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldCreateTable.Constraints = append(oldCreateTable.Constraints, oldConstr)
	}
	if len(v.Options) != 0 {
		oldTableOpt := &coldef.TableOption{}
		for _, val := range v.Options {
			switch val.Tp {
			case ast.TableOptionEngine:
				oldTableOpt.Engine = val.StrValue
			case ast.TableOptionCharset:
				oldTableOpt.Charset = val.StrValue
			case ast.TableOptionCollate:
				oldTableOpt.Collate = val.StrValue
			case ast.TableOptionAutoIncrement:
				oldTableOpt.AutoIncrement = val.UintValue
			}
		}
		oldCreateTable.Opt = oldTableOpt
	}
	return oldCreateTable, nil
}

func convertDropTable(v *ast.DropTableStmt) (*stmts.DropTableStmt, error) {
	oldDropTable := &stmts.DropTableStmt{
		IfExists: v.IfExists,
		Text:     v.Text(),
	}
	oldDropTable.TableIdents = make([]table.Ident, len(v.Tables))
	for i, val := range v.Tables {
		oldDropTable.TableIdents[i] = table.Ident{
			Schema: val.Schema,
			Name:   val.Name,
		}
	}
	return oldDropTable, nil
}

func convertCreateIndex(v *ast.CreateIndexStmt) (*stmts.CreateIndexStmt, error) {
	oldCreateIndex := &stmts.CreateIndexStmt{
		IndexName: v.IndexName,
		Unique:    v.Unique,
		TableIdent: table.Ident{
			Schema: v.Table.Schema,
			Name:   v.Table.Name,
		},
		Text: v.Text(),
	}
	oldCreateIndex.IndexColNames = make([]*coldef.IndexColName, len(v.IndexColNames))
	for i, val := range v.IndexColNames {
		oldIndexColName := &coldef.IndexColName{
			ColumnName: joinColumnName(val.Column),
			Length:     val.Length,
		}
		oldCreateIndex.IndexColNames[i] = oldIndexColName
	}
	return oldCreateIndex, nil
}

func convertDropIndex(v *ast.DropIndexStmt) (*stmts.DropIndexStmt, error) {
	return &stmts.DropIndexStmt{
		IfExists:  v.IfExists,
		IndexName: v.IndexName,
		Text:      v.Text(),
	}, nil
}

func convertAlterTable(v *ast.AlterTableStmt) (*stmts.AlterTableStmt, error) {
	oldAlterTable := &stmts.AlterTableStmt{
		Text: v.Text(),
	}
	return oldAlterTable, nil
}

func convertTruncateTable(v *ast.TruncateTableStmt) (*stmts.TruncateTableStmt, error) {
	return &stmts.TruncateTableStmt{
		TableIdent: table.Ident{
			Schema: v.Table.Schema,
			Name:   v.Table.Name,
		},
		Text: v.Text(),
	}, nil
}

func convertExplain(v *ast.ExplainStmt) (*stmts.ExplainStmt, error) {
	oldExplain := &stmts.ExplainStmt{
		Text: v.Text(),
	}
	var err error
	switch x := v.Stmt.(type) {
	case *ast.SelectStmt:
		oldExplain.S, err = convertSelect(x)
	case *ast.UpdateStmt:
		oldExplain.S, err = convertUpdate(x)
	case *ast.DeleteStmt:
		oldExplain.S, err = convertDelete(x)
	case *ast.InsertStmt:
		oldExplain.S, err = convertInsert(x)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return oldExplain, nil
}

func convertPrepare(v *ast.PrepareStmt) (*stmts.PreparedStmt, error) {
	oldPrepare := &stmts.PreparedStmt{
		InPrepare: true,
		Name:      v.Name,
		SQLText:   v.SQLText,
		Text:      v.Text(),
	}
	if v.SQLVar != nil {
		converter := newExpressionConverter()
		oldSQLVar, err := convertExpr(converter, v.SQLVar)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldPrepare.SQLVar = oldSQLVar.(*expression.Variable)
	}
	return oldPrepare, nil
}

func convertDeallocate(v *ast.DeallocateStmt) (*stmts.DeallocateStmt, error) {
	return &stmts.DeallocateStmt{
		ID:   v.ID,
		Name: v.Name,
		Text: v.Text(),
	}, nil
}

func convertExecute(v *ast.ExecuteStmt) (*stmts.ExecuteStmt, error) {
	oldExec := &stmts.ExecuteStmt{
		ID:   v.ID,
		Name: v.Name,
		Text: v.Text(),
	}
	oldExec.UsingVars = make([]expression.Expression, len(v.UsingVars))
	converter := newExpressionConverter()
	for i, val := range v.UsingVars {
		oldVar, err := convertExpr(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldExec.UsingVars[i] = oldVar
	}
	return oldExec, nil
}

func convertShow(v *ast.ShowStmt) (*stmts.ShowStmt, error) {
	oldShow := &stmts.ShowStmt{
		DBName:      v.DBName,
		Flag:        v.Flag,
		Full:        v.Full,
		GlobalScope: v.GlobalScope,
		Text:        v.Text(),
	}
	if v.Table != nil {
		oldShow.TableIdent = table.Ident{
			Schema: v.Table.Schema,
			Name:   v.Table.Name,
		}
	}
	if v.Column != nil {
		oldShow.ColumnName = joinColumnName(v.Column)
	}
	if v.Where != nil {
		converter := newExpressionConverter()
		oldWhere, err := convertExpr(converter, v.Where)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldShow.Where = oldWhere
	}
	if v.Pattern != nil {
		converter := newExpressionConverter()
		oldPattern, err := convertExpr(converter, v.Pattern)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldShow.Pattern = oldPattern.(*expression.PatternLike)
	}
	switch v.Tp {
	case ast.ShowCharset:
		oldShow.Target = stmt.ShowCharset
	case ast.ShowCollation:
		oldShow.Target = stmt.ShowCollation
	case ast.ShowColumns:
		oldShow.Target = stmt.ShowColumns
	case ast.ShowCreateTable:
		oldShow.Target = stmt.ShowCreateTable
	case ast.ShowDatabases:
		oldShow.Target = stmt.ShowDatabases
	case ast.ShowTables:
		oldShow.Target = stmt.ShowTables
	case ast.ShowEngines:
		oldShow.Target = stmt.ShowEngines
	case ast.ShowVariables:
		oldShow.Target = stmt.ShowVariables
	case ast.ShowWarnings:
		oldShow.Target = stmt.ShowWarnings
	case ast.ShowNone:
		oldShow.Target = stmt.ShowNone
	}
	return oldShow, nil
}

func convertBegin(v *ast.BeginStmt) (*stmts.BeginStmt, error) {
	return &stmts.BeginStmt{
		Text: v.Text(),
	}, nil
}

func convertCommit(v *ast.CommitStmt) (*stmts.CommitStmt, error) {
	return &stmts.CommitStmt{
		Text: v.Text(),
	}, nil
}

func convertRollback(v *ast.RollbackStmt) (*stmts.RollbackStmt, error) {
	return &stmts.RollbackStmt{
		Text: v.Text(),
	}, nil
}

func convertUse(v *ast.UseStmt) (*stmts.UseStmt, error) {
	return &stmts.UseStmt{
		DBName: v.DBName,
		Text:   v.Text(),
	}, nil
}

func convertVariableAssignment(converter *expressionConverter, v *ast.VariableAssignment) (*stmts.VariableAssignment, error) {
	oldValue, err := convertExpr(converter, v.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &stmts.VariableAssignment{
		IsGlobal: v.IsGlobal,
		IsSystem: v.IsSystem,
		Name:     v.Name,
		Value:    oldValue,
		Text:     v.Text(),
	}, nil
}

func convertSet(v *ast.SetStmt) (*stmts.SetStmt, error) {
	oldSet := &stmts.SetStmt{
		Text:      v.Text(),
		Variables: make([]*stmts.VariableAssignment, len(v.Variables)),
	}
	converter := newExpressionConverter()
	for i, val := range v.Variables {
		oldAssign, err := convertVariableAssignment(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldSet.Variables[i] = oldAssign
	}
	return oldSet, nil
}

func convertSetCharset(v *ast.SetCharsetStmt) (*stmts.SetCharsetStmt, error) {
	return &stmts.SetCharsetStmt{
		Charset: v.Charset,
		Collate: v.Collate,
		Text:    v.Text(),
	}, nil
}

func convertSetPwd(v *ast.SetPwdStmt) (*stmts.SetPwdStmt, error) {
	return &stmts.SetPwdStmt{
		User:     v.User,
		Password: v.Password,
		Text:     v.Text(),
	}, nil
}

func convertDo(v *ast.DoStmt) (*stmts.DoStmt, error) {
	exprConverter := newExpressionConverter()
	oldDo := &stmts.DoStmt{
		Text:  v.Text(),
		Exprs: make([]expression.Expression, len(v.Exprs)),
	}
	for i, val := range v.Exprs {
		oldExpr, err := convertExpr(exprConverter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldDo.Exprs[i] = oldExpr
	}
	return oldDo, nil
}
