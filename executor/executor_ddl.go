// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
)

// DDLExec represents a DDL executor.
type DDLExec struct {
	Statement ast.StmtNode
	ctx       context.Context
	is        infoschema.InfoSchema
	done      bool
}

// Fields implements Executor Fields interface.
func (e *DDLExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Execution Next interface.
func (e *DDLExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}
	var err error
	switch x := e.Statement.(type) {
	case *ast.TruncateTableStmt:
		err = e.executeTruncateTable(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.DropDatabaseStmt:
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		err = e.executeDropTable(x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.AlterTableStmt:
		err = e.executeAlterTable(x)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.done = true
	return nil, nil
}

// Close implements Executor Close interface.
func (e *DDLExec) Close() error {
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	table, ok := e.is.TableByID(s.Table.TableInfo.ID)
	if !ok {
		return errors.New("table not found, should never happen")
	}
	return table.Truncate(e.ctx)
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	var opt *coldef.CharsetOpt
	if len(s.Options) != 0 {
		opt = &coldef.CharsetOpt{}
		for _, val := range s.Options {
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				opt.Chs = val.Value
			case ast.DatabaseOptionCollate:
				opt.Col = val.Value
			}
		}
	}
	err := sessionctx.GetDomain(e.ctx).DDL().CreateSchema(e.ctx, model.NewCIStr(s.Name), opt)
	if err != nil {
		if terror.ErrorEqual(err, infoschema.DatabaseExists) && s.IfNotExists {
			err = nil
		}
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	ident := table.Ident{Schema: s.Table.Schema, Name: s.Table.Name}

	var coldefs []*coldef.ColumnDef
	for _, val := range s.Cols {
		coldef, err := convertColumnDef(val)
		if err != nil {
			return errors.Trace(err)
		}
		coldefs = append(coldefs, coldef)
	}
	var constrs []*coldef.TableConstraint
	for _, val := range s.Constraints {
		constr, err := convertConstraint(val)
		if err != nil {
			return errors.Trace(err)
		}
		constrs = append(constrs, constr)
	}

	err := sessionctx.GetDomain(e.ctx).DDL().CreateTable(e.ctx, ident, coldefs, constrs)
	if terror.ErrorEqual(err, infoschema.TableExists) {
		if s.IfNotExists {
			return nil
		}
		return infoschema.TableExists.Gen("CREATE TABLE: table exists %s", ident)
	}
	return errors.Trace(err)
}

func convertColumnDef(v *ast.ColumnDef) (*coldef.ColumnDef, error) {
	oldColDef := &coldef.ColumnDef{
		Name: v.Name.Name.O,
		Tp:   v.Tp,
	}
	for _, val := range v.Options {
		oldOpt, err := convertColumnOption(val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		oldColDef.Constraints = append(oldColDef.Constraints, oldOpt)
	}
	return oldColDef, nil
}

func convertColumnOption(v *ast.ColumnOption) (*coldef.ConstraintOpt, error) {
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
	oldColumnOpt.Evalue = v.Expr
	return oldColumnOpt, nil
}

func convertConstraint(v *ast.Constraint) (*coldef.TableConstraint, error) {
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

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	ident := table.Ident{Schema: s.Table.Schema, Name: s.Table.Name}

	colNames := convertIndexColNames(s.IndexColNames)
	err := sessionctx.GetDomain(e.ctx).DDL().CreateIndex(e.ctx, ident, s.Unique, model.NewCIStr(s.IndexName), colNames)
	return errors.Trace(err)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	err := sessionctx.GetDomain(e.ctx).DDL().DropSchema(e.ctx, model.NewCIStr(s.Name))
	if terror.ErrorEqual(err, infoschema.DatabaseNotExists) {
		if s.IfExists {
			err = nil
		} else {
			err = infoschema.DatabaseDropExists.Gen("Can't drop database '%s'; database doesn't exist", s.Name)
		}
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	var notExistTables []string
	for _, tn := range s.Tables {
		fullti := table.Ident{Schema: tn.Schema, Name: tn.Name}
		schema, ok := e.is.SchemaByName(tn.Schema)
		if !ok {
			// TODO: we should return special error for table not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistTables = append(notExistTables, fullti.String())
			continue
		}
		tb, err := e.is.TableByName(tn.Schema, tn.Name)
		if err != nil && strings.HasSuffix(err.Error(), "not exist") {
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else if err != nil {
			return errors.Trace(err)
		}
		// Check Privilege
		privChecker := privilege.GetPrivilegeChecker(e.ctx)
		hasPriv, err := privChecker.Check(e.ctx, schema, tb.Meta(), mysql.DropPriv)
		if err != nil {
			return errors.Trace(err)
		}
		if !hasPriv {
			return errors.Errorf("You do not have the privilege to drop table %s.%s.", tn.Schema, tn.Name)
		}

		err = sessionctx.GetDomain(e.ctx).DDL().DropTable(e.ctx, fullti)
		if infoschema.DatabaseNotExists.Equal(err) || infoschema.TableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
		} else if err != nil {
			return errors.Trace(err)
		}
	}
	if len(notExistTables) > 0 && !s.IfExists {
		return infoschema.TableDropExists.Gen("DROP TABLE: table %s does not exist", strings.Join(notExistTables, ","))
	}
	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	ti := table.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := sessionctx.GetDomain(e.ctx).DDL().DropIndex(e.ctx, ti, model.NewCIStr(s.IndexName))
	if (infoschema.DatabaseNotExists.Equal(err) || infoschema.TableNotExists.Equal(err)) && s.IfExists {
		err = nil
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeAlterTable(s *ast.AlterTableStmt) error {
	ti := table.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	var specs []*ddl.AlterSpecification
	for _, v := range s.Specs {
		spec, err := convertAlterTableSpec(v)
		if err != nil {
			return errors.Trace(err)
		}
		specs = append(specs, spec)
	}
	err := sessionctx.GetDomain(e.ctx).DDL().AlterTable(e.ctx, ti, specs)
	return errors.Trace(err)
}

func convertAlterTableSpec(v *ast.AlterTableSpec) (*ddl.AlterSpecification, error) {
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
		oldColDef, err := convertColumnDef(v.Column)
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
		oldConstraint, err := convertConstraint(v.Constraint)
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
		case ast.TableOptionDelayKeyWrite:
			oldOpt.Tp = coldef.TblOptDelayKeyWrite
		}
		oldAlterSpec.TableOpts = append(oldAlterSpec.TableOpts, oldOpt)
	}
	return oldAlterSpec, nil
}

func joinColumnName(columnName *ast.ColumnName) string {
	var originStrs []string
	if columnName.Schema.O != "" {
		originStrs = append(originStrs, columnName.Schema.O)
	}
	if columnName.Table.O != "" {
		originStrs = append(originStrs, columnName.Table.O)
	}
	originStrs = append(originStrs, columnName.Name.O)
	return strings.Join(originStrs, ".")
}
