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
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
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

func (p *preprocessor) checkDropTableNames(tables []*ast.TableName) {
	for _, t := range tables {
		if util.IsInCorrectIdentifierName(t.Name.String()) {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	if p.flag&parentIsJoin == 0 {
		p.tableAliasInJoin = append(p.tableAliasInJoin, make(map[string]any))
	}
	tableAliases := p.tableAliasInJoin[len(p.tableAliasInJoin)-1]
	isOracleMode := p.sctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
	if !isOracleMode {
		if err := isTableAliasDuplicate(stmt.Left, tableAliases); err != nil {
			p.err = err
			return
		}
		if err := isTableAliasDuplicate(stmt.Right, tableAliases); err != nil {
			p.err = err
			return
		}
	}
	p.flag |= parentIsJoin
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]any) error {
	if ts, ok := node.(*ast.TableSource); ok {
		tabName := ts.AsName
		if tabName.L == "" {
			if tableNode, ok := ts.Source.(*ast.TableName); ok {
				if tableNode.Schema.L != "" {
					tabName = ast.NewCIStr(fmt.Sprintf("%s.%s", tableNode.Schema.L, tableNode.Name.L))
				} else {
					tabName = tableNode.Name
				}
			}
		}
		_, exists := tableAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			return plannererrors.ErrNonUniqTable.GenWithStackByArgs(tabName)
		}
		tableAliases[tabName.L] = nil
	}
	return nil
}

func checkColumnOptions(isTempTable bool, ops []*ast.ColumnOption) (int, error) {
	isPrimary, isGenerated, isStored := 0, 0, false

	for _, op := range ops {
		switch op.Tp {
		case ast.ColumnOptionPrimaryKey:
			isPrimary = 1
		case ast.ColumnOptionGenerated:
			isGenerated = 1
			isStored = op.Stored
		case ast.ColumnOptionAutoRandom:
			if isTempTable {
				return isPrimary, plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
			}
		}
	}

	if isPrimary > 0 && isGenerated > 0 && !isStored {
		return isPrimary, plannererrors.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
	}

	return isPrimary, nil
}

func checkIndexOptions(isColumnar bool, indexOptions *ast.IndexOption) error {
	if isColumnar && indexOptions == nil {
		return dbterror.ErrUnsupportedIndexType.FastGen("COLUMNAR INDEX must specify 'USING <index_type>'")
	}
	if indexOptions == nil {
		return nil
	}
	if isColumnar {
		switch indexOptions.Tp {
		case ast.IndexTypeVector, ast.IndexTypeInverted:
			// Accepted
		case ast.IndexTypeFulltext:
			if indexOptions.ParserName.L != "" && model.GetFullTextParserTypeBySQLName(indexOptions.ParserName.L) == model.FullTextParserTypeInvalid {
				return dbterror.ErrUnsupportedIndexType.FastGen("Unsupported parser '%s'", indexOptions.ParserName.O)
			}
		case ast.IndexTypeInvalid:
			return dbterror.ErrUnsupportedIndexType.FastGen("COLUMNAR INDEX must specify 'USING <index_type>'")
		default:
			return dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for COLUMNAR INDEX", indexOptions.Tp)
		}
		if indexOptions.Visibility == ast.IndexVisibilityInvisible {
			return dbterror.ErrUnsupportedIndexType.FastGen("INVISIBLE can not be used in %s INDEX", indexOptions.Tp)
		}
	} else {
		switch indexOptions.Tp {
		case ast.IndexTypeHNSW:
			return dbterror.ErrUnsupportedIndexType.FastGen("'USING HNSW' can be only used for VECTOR INDEX")
		case ast.IndexTypeVector, ast.IndexTypeInverted, ast.IndexTypeFulltext:
			return dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' can be only used for COLUMNAR INDEX", indexOptions.Tp)
		default:
			// Accepted
		}
	}

	return nil
}

func checkIndexSpecs(indexOptions *ast.IndexOption, partSpecs []*ast.IndexPartSpecification) error {
	if indexOptions == nil {
		return nil
	}
	switch indexOptions.Tp {
	case ast.IndexTypeVector:
		if len(partSpecs) != 1 || partSpecs[0].Expr == nil {
			return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("VECTOR INDEX must specify an expression like ((VEC_XX_DISTANCE(<COLUMN>)))")
		}
	case ast.IndexTypeInverted:
		if len(partSpecs) != 1 || partSpecs[0].Column == nil {
			return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("COLUMNAR INDEX of INVERTED type must specify one column name")
		}
	case ast.IndexTypeFulltext:
		if len(partSpecs) != 1 || partSpecs[0].Column == nil {
			return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index must specify one column name")
		}
	}
	return nil
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Table.Name.String()
	if util.IsInCorrectIdentifierName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	if stmt.IndexName == "" {
		p.err = dbterror.ErrWrongNameForIndex.GenWithStackByArgs(stmt.IndexName)
		return
	}
	p.err = checkIndexInfo(stmt.IndexName, stmt.IndexPartSpecifications)
	if p.err != nil {
		return
	}

	// Rewrite CREATE VECTOR INDEX into CREATE COLUMNAR INDEX
	if stmt.KeyType == ast.IndexKeyTypeVector {
		if stmt.IndexOption.Tp != ast.IndexTypeInvalid && stmt.IndexOption.Tp != ast.IndexTypeHNSW {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for VECTOR INDEX", stmt.IndexOption.Tp)
			return
		}
		stmt.KeyType = ast.IndexKeyTypeColumnar
		stmt.IndexOption.Tp = ast.IndexTypeVector
	}
	// Rewrite CREATE FULLTEXT INDEX into CREATE COLUMNAR INDEX
	if stmt.KeyType == ast.IndexKeyTypeFulltext {
		if stmt.IndexOption.Tp != ast.IndexTypeInvalid {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for FULLTEXT INDEX", stmt.IndexOption.Tp)
			return
		}
		stmt.KeyType = ast.IndexKeyTypeColumnar
		stmt.IndexOption.Tp = ast.IndexTypeFulltext
	}

	p.err = checkIndexOptions(stmt.KeyType == ast.IndexKeyTypeColumnar, stmt.IndexOption)
	if p.err != nil {
		return
	}
	p.err = checkIndexSpecs(stmt.IndexOption, stmt.IndexPartSpecifications)
}

func (p *preprocessor) checkConstraintGrammar(stmt *ast.Constraint) {
	// Rewrite VECTOR INDEX into COLUMNAR INDEX
	if stmt.Tp == ast.ConstraintVector {
		if stmt.Option.Tp != ast.IndexTypeInvalid && stmt.Option.Tp != ast.IndexTypeHNSW {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for VECTOR INDEX", stmt.Option.Tp)
			return
		}
		stmt.Tp = ast.ConstraintColumnar
		stmt.Option.Tp = ast.IndexTypeVector
	}
	if stmt.Tp == ast.ConstraintFulltext {
		if stmt.Option.Tp != ast.IndexTypeInvalid {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for FULLTEXT INDEX", stmt.Option.Tp)
			return
		}
		stmt.Tp = ast.ConstraintColumnar
		stmt.Option.Tp = ast.IndexTypeFulltext
	}

	p.err = checkIndexOptions(stmt.Tp == ast.ConstraintColumnar, stmt.Option)
	if p.err != nil {
		return
	}
	p.err = checkIndexSpecs(stmt.Option, stmt.Keys)
}

func (p *preprocessor) checkSelectNoopFuncs(stmt *ast.SelectStmt) {
	noopFuncsMode := p.sctx.GetSessionVars().NoopFuncsMode
	if noopFuncsMode == variable.OnInt {
		// Set `ForShareLockEnabledByNoop` properly before returning.
		// When `tidb_enable_shared_lock_promotion` is enabled, the `for share` statements would be
		// executed as `for update` statements despite setting of noop functions.
		if stmt.LockInfo != nil && (stmt.LockInfo.LockType == ast.SelectLockForShare ||
			stmt.LockInfo.LockType == ast.SelectLockForShareNoWait) &&
			!p.sctx.GetSessionVars().SharedLockPromotion {
			p.sctx.GetSessionVars().StmtCtx.ForShareLockEnabledByNoop = true
		}
		return
	}
	if stmt.SelectStmtOpts != nil && stmt.SelectStmtOpts.CalcFoundRows {
		err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("SQL_CALC_FOUND_ROWS")
		if noopFuncsMode == variable.OffInt {
			p.err = err
			return
		}
		// NoopFuncsMode is Warn, append an error
		p.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
	}

	// When `tidb_enable_shared_lock_promotion` is enabled, the `for share` statements would be
	// executed as `for update` statements.
	if stmt.LockInfo != nil && (stmt.LockInfo.LockType == ast.SelectLockForShare ||
		stmt.LockInfo.LockType == ast.SelectLockForShareNoWait) &&
		!p.sctx.GetSessionVars().SharedLockPromotion {
		err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("LOCK IN SHARE MODE")
		if noopFuncsMode == variable.OffInt {
			p.err = err
			return
		}
		// NoopFuncsMode is Warn, append an error
		p.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		p.sctx.GetSessionVars().StmtCtx.ForShareLockEnabledByNoop = true
	}
}

func (p *preprocessor) checkGroupBy(stmt *ast.GroupByClause) {
	noopFuncsMode := p.sctx.GetSessionVars().NoopFuncsMode
	for _, item := range stmt.Items {
		if !item.NullOrder && noopFuncsMode != variable.OnInt {
			err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("GROUP BY expr ASC|DESC")
			if noopFuncsMode == variable.OffInt {
				p.err = err
				return
			}
			// NoopFuncsMode is Warn, append an error
			p.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}
}

func (p *preprocessor) checkRenameTableGrammar(stmt *ast.RenameTableStmt) {
	oldTable := stmt.TableToTables[0].OldTable.Name.String()
	newTable := stmt.TableToTables[0].NewTable.Name.String()

	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkRenameTable(oldTable, newTable string) {
	if util.IsInCorrectIdentifierName(oldTable) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(oldTable)
		return
	}

	if util.IsInCorrectIdentifierName(newTable) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(newTable)
		return
	}
}

func (p *preprocessor) checkRepairTableGrammar(stmt *ast.RepairTableStmt) {
	// Check create table stmt whether it's is in REPAIR MODE.
	if !domainutil.RepairInfo.InRepairMode() {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("TiDB is not in REPAIR MODE")
		return
	}
	if len(domainutil.RepairInfo.GetRepairTableList()) == 0 {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("repair list is empty")
		return
	}

	// Check rename action as the rename statement does.
	oldTable := stmt.Table.Name.String()
	newTable := stmt.CreateStmt.Table.Name.String()
	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	tName := stmt.Table.Name.String()
	if util.IsInCorrectIdentifierName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewTable != nil {
			ntName := spec.NewTable.Name.String()
			if util.IsInCorrectIdentifierName(ntName) {
				p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(ntName)
				return
			}
		}
		for _, colDef := range spec.NewColumns {
			if p.err = checkColumn(colDef); p.err != nil {
				return
			}
		}
		if p.err = checkUnsupportedTableOptions(spec.Options); p.err != nil {
			return
		}
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey, ast.ConstraintPrimaryKey:
				p.err = checkIndexInfo(spec.Constraint.Name, spec.Constraint.Keys)
				if p.err != nil {
					return
				}
			default:
				// Nothing to do now.
			}
		case ast.AlterTableAddStatistics, ast.AlterTableDropStatistics:
			statsName := spec.Statistics.StatsName
			if util.IsInCorrectIdentifierName(statsName) {
				msg := fmt.Sprintf("Incorrect statistics name: %s", statsName)
				p.err = plannererrors.ErrInternal.GenWithStack(msg)
				return
			}
		case ast.AlterTableAddPartitions:
			for _, def := range spec.PartDefinitions {
				pName := def.Name.String()
				if util.IsInCorrectIdentifierName(pName) {
					p.err = dbterror.ErrWrongPartitionName.GenWithStackByArgs()
					return
				}
			}
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(indexPartSpecifications []*ast.IndexPartSpecification) error {
	colNames := make(map[string]struct{}, len(indexPartSpecifications))
	for _, IndexColNameWithExpr := range indexPartSpecifications {
		if IndexColNameWithExpr.Column != nil {
			name := IndexColNameWithExpr.Column.Name
			if _, ok := colNames[name.L]; ok {
				return infoschema.ErrColumnExists.GenWithStackByArgs(name)
			}
			colNames[name.L] = struct{}{}
		}
	}
	return nil
}

// checkIndexInfo checks index name, index column names and prefix lengths.
func checkIndexInfo(indexName string, indexPartSpecifications []*ast.IndexPartSpecification) error {
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	if len(indexPartSpecifications) > mysql.MaxKeyParts {
		return infoschema.ErrTooManyKeyParts.GenWithStackByArgs(mysql.MaxKeyParts)
	}
	for _, idxSpec := range indexPartSpecifications {
		// -1 => unspecified/full, > 0 OK, 0 => error
		if idxSpec.Expr == nil && idxSpec.Length == 0 {
			return plannererrors.ErrKeyPart0.GenWithStackByArgs(idxSpec.Column.Name.O)
		}
	}
	return checkDuplicateColumnName(indexPartSpecifications)
}

// checkUnsupportedTableOptions checks if there exists unsupported table options
func checkUnsupportedTableOptions(options []*ast.TableOption) error {
	var err error
	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionUnion:
			err = dbterror.ErrTableOptionUnionUnsupported
		case ast.TableOptionInsertMethod:
			err = dbterror.ErrTableOptionInsertMethodUnsupported
		case ast.TableOptionEngine:
			err = checkTableEngine(option.StrValue)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var mysqlValidTableEngineNames = map[string]struct{}{
	"archive":    {},
	"blackhole":  {},
	"csv":        {},
	"example":    {},
	"federated":  {},
	"innodb":     {},
	"memory":     {},
	"merge":      {},
	"mgr_myisam": {},
	"myisam":     {},
	"ndb":        {},
	"heap":       {},
	"aria":       {}, // https://mariadb.com/docs/server/server-usage/storage-engines/aria/aria-storage-engine
	"myrocks":    {}, // https://mariadb.com/docs/server/server-usage/storage-engines/myrocks
	"tokudb":     {}, // https://mariadb.com/docs/server/server-usage/storage-engines/legacy-storage-engines/tokudb
}

func checkTableEngine(engineName string) error {
	if _, have := mysqlValidTableEngineNames[strings.ToLower(engineName)]; !have {
		return dbterror.ErrUnknownEngine.GenWithStackByArgs(engineName)
	}
	return nil
}

func checkReferInfoForTemporaryTable(tableMetaInfo *model.TableInfo) error {
	if tableMetaInfo.AutoRandomBits != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
	}
	if tableMetaInfo.PreSplitRegions != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions")
	}
	if tableMetaInfo.Partition != nil {
		return plannererrors.ErrPartitionNoTemporary
	}
	if tableMetaInfo.ShardRowIDBits != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
	}
	if tableMetaInfo.PlacementPolicyRef != nil {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("placement")
	}

	return nil
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if util.IsInCorrectIdentifierName(cName) {
		return dbterror.ErrWrongColumnName.GenWithStackByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.GetFlen() > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.GetType() {
	case mysql.TypeString:
		if tp.GetFlen() != types.UnspecifiedLength && tp.GetFlen() > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		if len(tp.GetCharset()) == 0 {
			// It's not easy to get the schema charset and table charset here.
			// The charset is determined by the order ColumnDefaultCharset --> TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the ddl.CreateTable.
			return nil
		}
		err := types.IsVarcharTooBigFieldLength(colDef.Tp.GetFlen(), colDef.Name.Name.O, tp.GetCharset())
		if err != nil {
			return err
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		// For FLOAT, the SQL standard permits an optional specification of the precision.
		// https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
		if tp.GetDecimal() == -1 {
			switch tp.GetType() {
			case mysql.TypeDouble:
				// For Double type flen and decimal check is moved to parser component
			default:
				if tp.GetFlen() > mysql.MaxDoublePrecisionLength {
					return types.ErrWrongFieldSpec.GenWithStackByArgs(colDef.Name.Name.O)
				}
			}
		} else {
			if tp.GetDecimal() > mysql.MaxFloatingTypeScale {
				return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
			}
			if tp.GetFlen() > mysql.MaxFloatingTypeWidth || tp.GetFlen() == 0 {
				return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
			}
			if tp.GetFlen() < tp.GetDecimal() {
				return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
			}
		}
	case mysql.TypeSet:
		if len(tp.GetElems()) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
		for _, str := range colDef.Tp.GetElems() {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.GetType()), str)
			}
		}
	case mysql.TypeNewDecimal:
		tpFlen := tp.GetFlen()
		tpDecimal := tp.GetDecimal()
		if tpDecimal > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tpDecimal, colDef.Name.Name.O, mysql.MaxDecimalScale)
		}
		if tpFlen > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tpFlen, colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}
		if tpFlen < tpDecimal {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
		}
		// If decimal and flen all equals 0, just set flen to default value.
		if tpFlen == 0 && (tpDecimal == 0 || tpDecimal == types.UnspecifiedLength) {
			defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNewDecimal)
			tp.SetFlen(defaultFlen)
			tp.SetDecimal(0)
		}
	case mysql.TypeBit:
		if tp.GetFlen() <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(colDef.Name.Name.O)
		}
		if tp.GetFlen() > mysql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxBitDisplayWidth)
		}
	case mysql.TypeTiDBVectorFloat32:
		if tp.GetFlen() != types.UnspecifiedLength {
			if err := types.CheckVectorDimValid(tp.GetFlen()); err != nil {
				return err
			}
		}
	default:
		// TODO: Add more types.
	}
	return nil
}

// isDefaultValNowSymFunc checks whether default value is a NOW() builtin function.
func isDefaultValNowSymFunc(expr ast.ExprNode) bool {
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		// Default value NOW() is transformed to CURRENT_TIMESTAMP() in parser.
		if funcCall.FnName.L == ast.CurrentTimestamp {
			return true
		}
	}
	return false
}

func isInvalidDefaultValue(colDef *ast.ColumnDef) bool {
	tp := colDef.Tp
	// Check the last default value.
	for i := len(colDef.Options) - 1; i >= 0; i-- {
		columnOpt := colDef.Options[i]
		if columnOpt.Tp == ast.ColumnOptionDefaultValue {
			if !(tp.GetType() == mysql.TypeTimestamp || tp.GetType() == mysql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				return true
			}
			break
		}
	}

	return false
}

