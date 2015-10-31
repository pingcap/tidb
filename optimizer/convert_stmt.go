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
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
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

func convertInsert(converter *expressionConverter, v *ast.InsertStmt) (stmt.Statement, error) {
	insertValues := stmts.InsertValues{}
	insertValues.Priority = v.Priority
	tableName := v.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName)
	insertValues.TableIdent = table.Ident{Schema: tableName.Schema, Name: tableName.Name}
	for _, val := range v.Columns {
		insertValues.ColNames = append(insertValues.ColNames, joinColumnName(val))
	}
	for _, row := range v.Lists {
		var oldRow []expression.Expression
		for _, val := range row {
			oldExpr, err := convertExpr(converter, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			oldRow = append(oldRow, oldExpr)
		}
		insertValues.Lists = append(insertValues.Lists, oldRow)
	}
	for _, assign := range v.Setlist {
		oldAssign, err := convertAssignment(converter, assign)
		if err != nil {
			return nil, errors.Trace(err)
		}
		insertValues.Setlist = append(insertValues.Setlist, oldAssign)
	}

	if v.Select != nil {
		var err error
		switch x := v.Select.(type) {
		case *ast.SelectStmt:
			insertValues.Sel, err = convertSelect(converter, x)
		case *ast.UnionStmt:
			insertValues.Sel, err = convertUnion(converter, x)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if v.Replace {
		return &stmts.ReplaceIntoStmt{
			InsertValues: insertValues,
			Text:         v.Text(),
		}, nil
	}
	oldInsert := &stmts.InsertIntoStmt{
		InsertValues: insertValues,
		Text:         v.Text(),
	}
	for _, onDup := range v.OnDuplicate {
		oldOnDup, err := convertAssignment(converter, onDup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldInsert.OnDuplicate = append(oldInsert.OnDuplicate, oldOnDup)
	}
	return oldInsert, nil
}

func convertDelete(converter *expressionConverter, v *ast.DeleteStmt) (*stmts.DeleteStmt, error) {
	oldDelete := &stmts.DeleteStmt{
		BeforeFrom:  v.BeforeFrom,
		Ignore:      v.Ignore,
		LowPriority: v.LowPriority,
		MultiTable:  v.MultiTable,
		Quick:       v.Quick,
		Text:        v.Text(),
	}
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

func convertUpdate(converter *expressionConverter, v *ast.UpdateStmt) (*stmts.UpdateStmt, error) {
	oldUpdate := &stmts.UpdateStmt{
		Ignore:        v.Ignore,
		MultipleTable: v.MultipleTable,
		LowPriority:   v.LowPriority,
		Text:          v.Text(),
	}
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
		oldUpdate.List = append(oldUpdate.List, oldAssign)
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

func convertSelect(converter *expressionConverter, s *ast.SelectStmt) (*stmts.SelectStmt, error) {
	oldSelect := &stmts.SelectStmt{
		Distinct: s.Distinct,
		Text:     s.Text(),
	}
	oldSelect.Fields = make([]*field.Field, len(s.Fields.Fields))
	for i, val := range s.Fields.Fields {
		oldField := &field.Field{}
		oldField.AsName = val.AsName.O
		var err error
		if val.Expr != nil {
			oldField.Expr, err = convertExpr(converter, val.Expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else if val.WildCard != nil {
			oldField.Expr = &expression.Ident{CIStr: model.NewCIStr("*")}
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

func convertUnion(converter *expressionConverter, u *ast.UnionStmt) (*stmts.UnionStmt, error) {
	oldUnion := &stmts.UnionStmt{
		Text: u.Text(),
	}
	oldUnion.Selects = make([]*stmts.SelectStmt, len(u.Selects))
	oldUnion.Distincts = make([]bool, len(u.Selects)-1)
	if u.Distinct {
		for i := range oldUnion.Distincts {
			oldUnion.Distincts[i] = true
		}
	}
	for i, val := range u.Selects {
		oldSelect, err := convertSelect(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldUnion.Selects[i] = oldSelect
	}
	if u.OrderBy != nil {
		oldOrderBy, err := convertOrderBy(converter, u.OrderBy)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldUnion.OrderBy = oldOrderBy
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

	switch r := join.Right.(type) {
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
		oldSelect, err := convertSelect(converter, src)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldTs.Source = oldSelect
	case *ast.UnionStmt:
		oldUnion, err := convertUnion(converter, src)
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

func convertCreateDatabase(converter *expressionConverter, v *ast.CreateDatabaseStmt) (*stmts.CreateDatabaseStmt, error) {
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

func convertDropDatabase(converter *expressionConverter, v *ast.DropDatabaseStmt) (*stmts.DropDatabaseStmt, error) {
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

func convertCreateTable(converter *expressionConverter, v *ast.CreateTableStmt) (*stmts.CreateTableStmt, error) {
	oldCreateTable := &stmts.CreateTableStmt{
		Ident:       table.Ident{Schema: v.Table.Schema, Name: v.Table.Name},
		IfNotExists: v.IfNotExists,
		Text:        v.Text(),
	}
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

func convertDropTable(converter *expressionConverter, v *ast.DropTableStmt) (*stmts.DropTableStmt, error) {
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

func convertCreateIndex(converter *expressionConverter, v *ast.CreateIndexStmt) (*stmts.CreateIndexStmt, error) {
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

func convertDropIndex(converter *expressionConverter, v *ast.DropIndexStmt) (*stmts.DropIndexStmt, error) {
	return &stmts.DropIndexStmt{
		IfExists:  v.IfExists,
		IndexName: v.IndexName,
		Text:      v.Text(),
	}, nil
}

func convertAlterTableSpec(converter *expressionConverter, v *ast.AlterTableSpec) (*ddl.AlterSpecification, error) {
	oldAlterSpec := &ddl.AlterSpecification{
		Name: v.Name,
	}
	switch v.Tp {
	case ast.AlterTableAddConstraint:
		oldAlterSpec.Action = ddl.AlterAddConstr
	case ast.AlterTableAddColumn:
		oldAlterSpec.Action = ddl.AlterAddColumn
	case ast.AlterTableDropColumn:
		oldAlterSpec.Action = ddl.AlterDropColumn
	case ast.AlterTableDropForeignKey:
		oldAlterSpec.Action = ddl.AlterDropForeignKey
	case ast.AlterTableDropIndex:
		oldAlterSpec.Action = ddl.AlterDropIndex
	case ast.AlterTableDropPrimaryKey:
		oldAlterSpec.Action = ddl.AlterDropPrimaryKey
	case ast.AlterTableOption:
		oldAlterSpec.Action = ddl.AlterTableOpt
	}
	if v.Column != nil {
		oldColDef, err := convertColumnDef(converter, v.Column)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldAlterSpec.Column = oldColDef
	}
	if v.Position != nil {
		oldAlterSpec.Position = &ddl.ColumnPosition{}
		switch v.Position.Tp {
		case ast.ColumnPositionNone:
			oldAlterSpec.Position.Type = ddl.ColumnPositionNone
		case ast.ColumnPositionFirst:
			oldAlterSpec.Position.Type = ddl.ColumnPositionFirst
		case ast.ColumnPositionAfter:
			oldAlterSpec.Position.Type = ddl.ColumnPositionAfter
		}
		if v.Position.RelativeColumn != nil {
			oldAlterSpec.Position.RelativeColumn = joinColumnName(v.Position.RelativeColumn)
		}
	}
	if v.DropColumn != nil {
		oldAlterSpec.Name = joinColumnName(v.DropColumn)
	}
	if v.Constraint != nil {
		oldConstraint, err := convertConstraint(converter, v.Constraint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldAlterSpec.Constraint = oldConstraint
	}
	for _, val := range v.Options {
		oldOpt := &coldef.TableOpt{
			StrValue:  val.StrValue,
			UintValue: val.UintValue,
		}
		switch val.Tp {
		case ast.TableOptionNone:
			oldOpt.Tp = coldef.TblOptNone
		case ast.TableOptionEngine:
			oldOpt.Tp = coldef.TblOptEngine
		case ast.TableOptionCharset:
			oldOpt.Tp = coldef.TblOptCharset
		case ast.TableOptionCollate:
			oldOpt.Tp = coldef.TblOptCollate
		case ast.TableOptionAutoIncrement:
			oldOpt.Tp = coldef.TblOptAutoIncrement
		case ast.TableOptionComment:
			oldOpt.Tp = coldef.TblOptComment
		case ast.TableOptionAvgRowLength:
			oldOpt.Tp = coldef.TblOptAvgRowLength
		case ast.TableOptionCheckSum:
			oldOpt.Tp = coldef.TblOptCheckSum
		case ast.TableOptionCompression:
			oldOpt.Tp = coldef.TblOptCompression
		case ast.TableOptionConnection:
			oldOpt.Tp = coldef.TblOptConnection
		case ast.TableOptionPassword:
			oldOpt.Tp = coldef.TblOptPassword
		case ast.TableOptionKeyBlockSize:
			oldOpt.Tp = coldef.TblOptKeyBlockSize
		case ast.TableOptionMaxRows:
			oldOpt.Tp = coldef.TblOptMaxRows
		case ast.TableOptionMinRows:
			oldOpt.Tp = coldef.TblOptMinRows
		}
		oldAlterSpec.TableOpts = append(oldAlterSpec.TableOpts, oldOpt)
	}
	return oldAlterSpec, nil
}

func convertAlterTable(converter *expressionConverter, v *ast.AlterTableStmt) (*stmts.AlterTableStmt, error) {
	oldAlterTable := &stmts.AlterTableStmt{
		Ident: table.Ident{Schema: v.Table.Schema, Name: v.Table.Name},
		Text:  v.Text(),
	}
	for _, val := range v.Specs {
		oldSpec, err := convertAlterTableSpec(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldAlterTable.Specs = append(oldAlterTable.Specs, oldSpec)
	}
	return oldAlterTable, nil
}

func convertTruncateTable(converter *expressionConverter, v *ast.TruncateTableStmt) (*stmts.TruncateTableStmt, error) {
	return &stmts.TruncateTableStmt{
		TableIdent: table.Ident{
			Schema: v.Table.Schema,
			Name:   v.Table.Name,
		},
		Text: v.Text(),
	}, nil
}

func convertExplain(converter *expressionConverter, v *ast.ExplainStmt) (*stmts.ExplainStmt, error) {
	oldExplain := &stmts.ExplainStmt{
		Text: v.Text(),
	}
	var err error
	switch x := v.Stmt.(type) {
	case *ast.SelectStmt:
		oldExplain.S, err = convertSelect(converter, x)
	case *ast.UpdateStmt:
		oldExplain.S, err = convertUpdate(converter, x)
	case *ast.DeleteStmt:
		oldExplain.S, err = convertDelete(converter, x)
	case *ast.InsertStmt:
		oldExplain.S, err = convertInsert(converter, x)
	case *ast.ShowStmt:
		oldExplain.S, err = convertShow(converter, x)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return oldExplain, nil
}

func convertPrepare(converter *expressionConverter, v *ast.PrepareStmt) (*stmts.PreparedStmt, error) {
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

func convertDeallocate(converter *expressionConverter, v *ast.DeallocateStmt) (*stmts.DeallocateStmt, error) {
	return &stmts.DeallocateStmt{
		ID:   v.ID,
		Name: v.Name,
		Text: v.Text(),
	}, nil
}

func convertExecute(converter *expressionConverter, v *ast.ExecuteStmt) (*stmts.ExecuteStmt, error) {
	oldExec := &stmts.ExecuteStmt{
		ID:   v.ID,
		Name: v.Name,
		Text: v.Text(),
	}
	oldExec.UsingVars = make([]expression.Expression, len(v.UsingVars))
	for i, val := range v.UsingVars {
		oldVar, err := convertExpr(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldExec.UsingVars[i] = oldVar
	}
	return oldExec, nil
}

func convertShow(converter *expressionConverter, v *ast.ShowStmt) (*stmts.ShowStmt, error) {
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

func convertBegin(converter *expressionConverter, v *ast.BeginStmt) (*stmts.BeginStmt, error) {
	return &stmts.BeginStmt{
		Text: v.Text(),
	}, nil
}

func convertCommit(converter *expressionConverter, v *ast.CommitStmt) (*stmts.CommitStmt, error) {
	return &stmts.CommitStmt{
		Text: v.Text(),
	}, nil
}

func convertRollback(converter *expressionConverter, v *ast.RollbackStmt) (*stmts.RollbackStmt, error) {
	return &stmts.RollbackStmt{
		Text: v.Text(),
	}, nil
}

func convertUse(converter *expressionConverter, v *ast.UseStmt) (*stmts.UseStmt, error) {
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

func convertSet(converter *expressionConverter, v *ast.SetStmt) (*stmts.SetStmt, error) {
	oldSet := &stmts.SetStmt{
		Text:      v.Text(),
		Variables: make([]*stmts.VariableAssignment, len(v.Variables)),
	}
	for i, val := range v.Variables {
		oldAssign, err := convertVariableAssignment(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldSet.Variables[i] = oldAssign
	}
	return oldSet, nil
}

func convertSetCharset(converter *expressionConverter, v *ast.SetCharsetStmt) (*stmts.SetCharsetStmt, error) {
	return &stmts.SetCharsetStmt{
		Charset: v.Charset,
		Collate: v.Collate,
		Text:    v.Text(),
	}, nil
}

func convertSetPwd(converter *expressionConverter, v *ast.SetPwdStmt) (*stmts.SetPwdStmt, error) {
	return &stmts.SetPwdStmt{
		User:     v.User,
		Password: v.Password,
		Text:     v.Text(),
	}, nil
}

func convertUserSpec(converter *expressionConverter, v *ast.UserSpec) (*coldef.UserSpecification, error) {
	oldSpec := &coldef.UserSpecification{
		User: v.User,
	}
	if v.AuthOpt != nil {
		oldAuthOpt := &coldef.AuthOption{
			AuthString:   v.AuthOpt.AuthString,
			ByAuthString: v.AuthOpt.ByAuthString,
			HashString:   v.AuthOpt.HashString,
		}
		oldSpec.AuthOpt = oldAuthOpt
	}
	return oldSpec, nil
}

func convertCreateUser(converter *expressionConverter, v *ast.CreateUserStmt) (*stmts.CreateUserStmt, error) {
	oldCreateUser := &stmts.CreateUserStmt{
		IfNotExists: v.IfNotExists,
		Text:        v.Text(),
	}
	for _, val := range v.Specs {
		oldSpec, err := convertUserSpec(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldCreateUser.Specs = append(oldCreateUser.Specs, oldSpec)
	}
	return oldCreateUser, nil
}

func convertDo(converter *expressionConverter, v *ast.DoStmt) (*stmts.DoStmt, error) {
	oldDo := &stmts.DoStmt{
		Text:  v.Text(),
		Exprs: make([]expression.Expression, len(v.Exprs)),
	}
	for i, val := range v.Exprs {
		oldExpr, err := convertExpr(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldDo.Exprs[i] = oldExpr
	}
	return oldDo, nil
}

func convertPrivElem(converter *expressionConverter, v *ast.PrivElem) (*coldef.PrivElem, error) {
	oldPrim := &coldef.PrivElem{
		Priv: v.Priv,
	}
	for _, val := range v.Cols {
		oldPrim.Cols = append(oldPrim.Cols, joinColumnName(val))
	}
	return oldPrim, nil
}

func convertGrant(converter *expressionConverter, v *ast.GrantStmt) (*stmts.GrantStmt, error) {
	oldGrant := &stmts.GrantStmt{
		Text: v.Text(),
	}
	for _, val := range v.Privs {
		oldPrim, err := convertPrivElem(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldGrant.Privs = append(oldGrant.Privs, oldPrim)
	}
	switch v.ObjectType {
	case ast.ObjectTypeNone:
		oldGrant.ObjectType = coldef.ObjectTypeNone
	case ast.ObjectTypeTable:
		oldGrant.ObjectType = coldef.ObjectTypeTable
	}
	if v.Level != nil {
		oldGrantLevel := &coldef.GrantLevel{
			DBName:    v.Level.DBName,
			TableName: v.Level.TableName,
		}
		switch v.Level.Level {
		case ast.GrantLevelDB:
			oldGrantLevel.Level = coldef.GrantLevelDB
		case ast.GrantLevelGlobal:
			oldGrantLevel.Level = coldef.GrantLevelGlobal
		case ast.GrantLevelNone:
			oldGrantLevel.Level = coldef.GrantLevelNone
		case ast.GrantLevelTable:
			oldGrantLevel.Level = coldef.GrantLevelTable
		}
		oldGrant.Level = oldGrantLevel
	}
	for _, val := range v.Users {
		oldUserSpec, err := convertUserSpec(converter, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldGrant.Users = append(oldGrant.Users, oldUserSpec)
	}
	return oldGrant, nil
}
