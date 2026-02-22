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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func checkAutoIncrementOp(colDef *ast.ColumnDef, index int) (bool, error) {
	var hasAutoIncrement bool

	if colDef.Options[index].Tp == ast.ColumnOptionAutoIncrement {
		hasAutoIncrement = true
		if len(colDef.Options) == index+1 {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.ColumnOptionDefaultValue {
				if tmp, ok := op.Expr.(*driver.ValueExpr); ok {
					if !tmp.Datum.IsNull() {
						return hasAutoIncrement, types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
					}
				}
				if tmp, ok := op.Expr.(*ast.FuncCallExpr); ok {
					if tmp.FnName.L == "current_date" {
						return hasAutoIncrement, types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
					}
				}
			}
		}
	}

	if colDef.Options[index].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != index+1 {
		if tmp, ok := colDef.Options[index].Expr.(*driver.ValueExpr); ok {
			if tmp.Datum.IsNull() {
				return hasAutoIncrement, nil
			}
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.ColumnOptionAutoIncrement {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}

	return hasAutoIncrement, nil
}

func (p *preprocessor) checkAutoIncrement(stmt *ast.CreateTableStmt) {
	autoIncrementCols := make(map[*ast.ColumnDef]bool)

	for _, colDef := range stmt.Cols {
		var hasAutoIncrement bool
		var isKey bool
		for i, op := range colDef.Options {
			ok, err := checkAutoIncrementOp(colDef, i)
			if err != nil {
				p.err = err
				return
			}
			if ok {
				hasAutoIncrement = true
			}
			switch op.Tp {
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isKey = true
			}
		}
		if hasAutoIncrement {
			autoIncrementCols[colDef] = isKey
		}
	}

	if len(autoIncrementCols) < 1 {
		return
	}
	// Only have one auto_increment col.
	if len(autoIncrementCols) > 1 {
		p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		return
	}
	for col := range autoIncrementCols {
		switch col.Tp.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
		default:
			p.err = errors.Errorf("Incorrect column specifier for column '%s'", col.Name.Name.O)
		}
	}
}

// checkSetOprSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
//
//	https://mariadb.com/kb/en/intersect/
//	https://mariadb.com/kb/en/except/
//
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkSetOprSelectList(stmt *ast.SetOprSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		switch s := sel.(type) {
		case *ast.SelectStmt:
			if s.SelectIntoOpt != nil {
				p.err = plannererrors.ErrWrongUsage.GenWithStackByArgs("UNION", "INTO")
				return
			}
			if s.IsInBraces {
				continue
			}
			if s.Limit != nil {
				p.err = plannererrors.ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
				return
			}
			if s.OrderBy != nil {
				p.err = plannererrors.ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
				return
			}
		case *ast.SetOprSelectList:
			p.checkSetOprSelectList(s)
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if util.IsInCorrectIdentifierName(stmt.Name.L) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAlterDatabaseGrammar(stmt *ast.AlterDatabaseStmt) {
	// for 'ALTER DATABASE' statement, database name can be empty to alter default database.
	if util.IsInCorrectIdentifierName(stmt.Name.L) && !stmt.AlterDefaultDatabase {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	if util.IsInCorrectIdentifierName(stmt.Name.L) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkFlashbackTableGrammar(stmt *ast.FlashBackTableStmt) {
	if util.IsInCorrectIdentifierName(stmt.NewName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(stmt.NewName)
	}
}

func (p *preprocessor) checkFlashbackDatabaseGrammar(stmt *ast.FlashBackDatabaseStmt) {
	if util.IsInCorrectIdentifierName(stmt.NewName) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.NewName)
	}
}

func (p *preprocessor) checkAdminCheckTableGrammar(stmt *ast.AdminStmt) {
	for _, table := range stmt.Tables {
		tableInfo, err := p.tableByName(table)
		if err != nil {
			p.err = err
			return
		}
		tempTableType := tableInfo.Meta().TempTableType
		if (stmt.Tp == ast.AdminCheckTable || stmt.Tp == ast.AdminChecksumTable || stmt.Tp == ast.AdminCheckIndex) && tempTableType != model.TempTableNone {
			if stmt.Tp == ast.AdminChecksumTable {
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("admin checksum table")
			} else if stmt.Tp == ast.AdminCheckTable {
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("admin check table")
			} else {
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("admin check index")
			}
			return
		}
	}
}

func (p *preprocessor) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	if stmt.ReferTable != nil {
		schema := ast.NewCIStr(p.sctx.GetSessionVars().CurrentDB)
		if stmt.ReferTable.Schema.String() != "" {
			schema = stmt.ReferTable.Schema
		}
		// get the infoschema from the context.
		tableInfo, err := p.ensureInfoSchema().TableByName(p.ctx, schema, stmt.ReferTable.Name)
		if err != nil {
			p.err = err
			return
		}
		tableMetaInfo := tableInfo.Meta()
		if tableMetaInfo.TempTableType != model.TempTableNone {
			p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like")
			return
		}
		if stmt.TemporaryKeyword != ast.TemporaryNone {
			err := checkReferInfoForTemporaryTable(tableMetaInfo)
			if err != nil {
				p.err = err
				return
			}
		}
	}
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		for _, opt := range stmt.Options {
			switch opt.Tp {
			case ast.TableOptionShardRowID:
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
				return
			case ast.TableOptionPlacementPolicy:
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("PLACEMENT")
				return
			}
		}
	}
	tName := stmt.Table.Name.String()
	if util.IsInCorrectIdentifierName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkColumn(colDef); err != nil {
			// Try to convert to BLOB or TEXT, see issue #30328
			if !terror.ErrorEqual(err, types.ErrTooBigFieldLength) || !p.hasAutoConvertWarning(colDef) {
				p.err = err
				return
			}
		}
		isPrimary, err := checkColumnOptions(stmt.TemporaryKeyword != ast.TemporaryNone, colDef.Options)
		if err != nil {
			p.err = err
			return
		}
		countPrimaryKey += isPrimary
		if countPrimaryKey > 1 {
			p.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex, ast.ConstraintForeignKey:
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
			if constraint.IsEmptyIndex {
				p.err = dbterror.ErrWrongNameForIndex.GenWithStackByArgs(constraint.Name)
				return
			}
		case ast.ConstraintPrimaryKey:
			if countPrimaryKey > 0 {
				p.err = infoschema.ErrMultiplePriKey
				return
			}
			countPrimaryKey++
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
		}
	}
	if p.err = checkUnsupportedTableOptions(stmt.Options); p.err != nil {
		return
	}
	if stmt.Select != nil {
		// FIXME: a temp error noticing 'not implemented' (issue 4754)
		// Note: if we implement it later, please clear it's MDL related tables for
		// it like what CREATE VIEW does.
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
		p.err = dbterror.ErrTableMustHaveColumns
		return
	}

	if stmt.Partition != nil {
		for _, def := range stmt.Partition.Definitions {
			pName := def.Name.String()
			if util.IsInCorrectIdentifierName(pName) {
				p.err = dbterror.ErrWrongPartitionName.GenWithStackByArgs()
				return
			}
		}
	}
}

func (p *preprocessor) checkCreateViewGrammar(stmt *ast.CreateViewStmt) {
	vName := stmt.ViewName.Name.String()
	if util.IsInCorrectIdentifierName(vName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(vName)
		return
	}
	for _, col := range stmt.Cols {
		if util.IsInCorrectIdentifierName(col.String()) {
			p.err = dbterror.ErrWrongColumnName.GenWithStackByArgs(col)
			return
		}
	}
	if len(stmt.Definer.Username) > auth.UserNameMaxLength {
		p.err = dbterror.ErrWrongStringLength.GenWithStackByArgs(stmt.Definer.Username, "user name", auth.UserNameMaxLength)
		return
	}
	if len(stmt.Definer.Hostname) > auth.HostNameMaxLength {
		p.err = dbterror.ErrWrongStringLength.GenWithStackByArgs(stmt.Definer.Hostname, "host name", auth.HostNameMaxLength)
		return
	}
}

func (p *preprocessor) checkCreateViewWithSelect(stmt ast.Node) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if s.SelectIntoOpt != nil {
			p.err = dbterror.ErrViewSelectClause.GenWithStackByArgs("INFO")
			return
		}
		if s.LockInfo != nil && s.LockInfo.LockType != ast.SelectLockNone {
			s.LockInfo.LockType = ast.SelectLockNone
			return
		}
	case *ast.SetOprSelectList:
		for _, sel := range s.Selects {
			p.checkCreateViewWithSelect(sel)
		}
	}
}

func (p *preprocessor) checkCreateViewWithSelectGrammar(stmt *ast.CreateViewStmt) {
	switch stmt := stmt.Select.(type) {
	case *ast.SelectStmt:
		p.checkCreateViewWithSelect(stmt)
	case *ast.SetOprStmt:
		for _, selectStmt := range stmt.SelectList.Selects {
			p.checkCreateViewWithSelect(selectStmt)
			if p.err != nil {
				return
			}
		}
	}
}

func (p *preprocessor) checkDropSequenceGrammar(stmt *ast.DropSequenceStmt) {
	p.checkDropTableNames(stmt.Sequences)
}

func (p *preprocessor) checkDropTableGrammar(stmt *ast.DropTableStmt) {
	p.checkDropTableNames(stmt.Tables)
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		p.checkDropTemporaryTableGrammar(stmt)
	}
}

func (p *preprocessor) checkDropTemporaryTableGrammar(stmt *ast.DropTableStmt) {
	currentDB := ast.NewCIStr(p.sctx.GetSessionVars().CurrentDB)
	for _, t := range stmt.Tables {
		if util.IsInCorrectIdentifierName(t.Name.String()) {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}

		schema := t.Schema
		if schema.L == "" {
			schema = currentDB
		}

		tbl, err := p.ensureInfoSchema().TableByName(p.ctx, schema, t.Name)
		if infoschema.ErrTableNotExists.Equal(err) {
			// Non-exist table will be checked in ddl executor
			continue
		}

		if err != nil {
			p.err = err
			return
		}

		tblInfo := tbl.Meta()
		if stmt.TemporaryKeyword == ast.TemporaryGlobal && tblInfo.TempTableType != model.TempTableGlobal {
			p.err = plannererrors.ErrDropTableOnTemporaryTable
			return
		}
	}
}
