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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
)

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
