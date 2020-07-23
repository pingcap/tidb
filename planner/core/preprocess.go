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

package core

import (
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/domainutil"
)

// PreprocessOpt presents optional parameters to `Preprocess` method.
type PreprocessOpt func(*preprocessor)

// InPrepare is a PreprocessOpt that indicates preprocess is executing under prepare statement.
func InPrepare(p *preprocessor) {
	p.flag |= inPrepare
}

// InTxnRetry is a PreprocessOpt that indicates preprocess is executing under transaction retry.
func InTxnRetry(p *preprocessor) {
	p.flag |= inTxnRetry
}

// TryAddExtraLimit trys to add an extra limit for SELECT or UNION statement when sql_select_limit is set.
func TryAddExtraLimit(ctx sessionctx.Context, node ast.StmtNode) ast.StmtNode {
	if ctx.GetSessionVars().SelectLimit == math.MaxUint64 || ctx.GetSessionVars().InRestrictedSQL {
		return node
	}
	if explain, ok := node.(*ast.ExplainStmt); ok {
		explain.Stmt = TryAddExtraLimit(ctx, explain.Stmt)
		return explain
	} else if sel, ok := node.(*ast.SelectStmt); ok {
		if sel.Limit != nil || sel.SelectIntoOpt != nil {
			return node
		}
		newSel := *sel
		newSel.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newSel
	} else if setOprStmt, ok := node.(*ast.SetOprStmt); ok {
		if setOprStmt.Limit != nil {
			return node
		}
		newSetOpr := *setOprStmt
		newSetOpr.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newSetOpr
	}
	return node
}

// Preprocess resolves table names of the node, and checks some statements validation.
func Preprocess(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema, preprocessOpt ...PreprocessOpt) error {
	v := preprocessor{is: is, ctx: ctx, tableAliasInJoin: make([]map[string]interface{}, 0)}
	for _, optFn := range preprocessOpt {
		optFn(&v)
	}
	node.Accept(&v)
	return errors.Trace(v.err)
}

type preprocessorFlag uint8

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// inTxnRetry is set when visiting in transaction retry.
	inTxnRetry
	// inCreateOrDropTable is set when visiting create/drop table statement.
	inCreateOrDropTable
	// parentIsJoin is set when visiting node's parent is join.
	parentIsJoin
	// inRepairTable is set when visiting a repair table statement.
	inRepairTable
	// inSequenceFunction is set when visiting a sequence function.
	// This flag indicates the tableName in these function should be checked as sequence object.
	inSequenceFunction
)

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	is   infoschema.InfoSchema
	ctx  sessionctx.Context
	err  error
	flag preprocessorFlag

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]interface{}
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.CreateTableStmt:
		p.flag |= inCreateOrDropTable
		p.resolveCreateTableStmt(node)
		p.checkCreateTableGrammar(node)
	case *ast.CreateViewStmt:
		p.flag |= inCreateOrDropTable
		p.checkCreateViewGrammar(node)
		p.checkCreateViewWithSelectGrammar(node)
	case *ast.DropTableStmt:
		p.flag |= inCreateOrDropTable
		p.checkDropTableGrammar(node)
	case *ast.RenameTableStmt:
		p.flag |= inCreateOrDropTable
		p.checkRenameTableGrammar(node)
	case *ast.CreateIndexStmt:
		p.checkCreateIndexGrammar(node)
	case *ast.AlterTableStmt:
		p.resolveAlterTableStmt(node)
		p.checkAlterTableGrammar(node)
	case *ast.CreateDatabaseStmt:
		p.checkCreateDatabaseGrammar(node)
	case *ast.AlterDatabaseStmt:
		p.checkAlterDatabaseGrammar(node)
	case *ast.DropDatabaseStmt:
		p.checkDropDatabaseGrammar(node)
	case *ast.ShowStmt:
		p.resolveShowStmt(node)
	case *ast.SetOprSelectList:
		p.checkSetOprSelectList(node)
	case *ast.DeleteTableList:
		return in, true
	case *ast.Join:
		p.checkNonUniqTableAlias(node)
	case *ast.CreateBindingStmt:
		EraseLastSemicolon(node.OriginSel)
		EraseLastSemicolon(node.HintedSel)
		p.checkBindGrammar(node.OriginSel, node.HintedSel)
		return in, true
	case *ast.DropBindingStmt:
		EraseLastSemicolon(node.OriginSel)
		if node.HintedSel != nil {
			EraseLastSemicolon(node.HintedSel)
			p.checkBindGrammar(node.OriginSel, node.HintedSel)
		}
		return in, true
	case *ast.RecoverTableStmt, *ast.FlashBackTableStmt:
		// The specified table in recover table statement maybe already been dropped.
		// So skip check table name here, otherwise, recover table [table_name] syntax will return
		// table not exists error. But recover table statement is use to recover the dropped table. So skip children here.
		return in, true
	case *ast.RepairTableStmt:
		// The RepairTable should consist of the logic for creating tables and renaming tables.
		p.flag |= inRepairTable
		p.checkRepairTableGrammar(node)
	case *ast.CreateSequenceStmt:
		p.flag |= inCreateOrDropTable
		p.resolveCreateSequenceStmt(node)
	case *ast.DropSequenceStmt:
		p.flag |= inCreateOrDropTable
		p.checkDropSequenceGrammar(node)
	case *ast.FuncCallExpr:
		if node.FnName.L == ast.NextVal || node.FnName.L == ast.LastVal || node.FnName.L == ast.SetVal {
			p.flag |= inSequenceFunction
		}
	case *ast.BRIEStmt:
		if node.Kind == ast.BRIEKindRestore {
			p.flag |= inCreateOrDropTable
		}
	default:
		p.flag &= ^parentIsJoin
	}
	return in, p.err != nil
}

// EraseLastSemicolon removes last semicolon of sql.
func EraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(sql[:len(sql)-1])
	}
}

func (p *preprocessor) checkBindGrammar(originSel, hintedSel ast.StmtNode) {
	originSQL := parser.Normalize(originSel.(*ast.SelectStmt).Text())
	hintedSQL := parser.Normalize(hintedSel.(*ast.SelectStmt).Text())

	if originSQL != hintedSQL {
		p.err = errors.Errorf("hinted sql and origin sql don't match when hinted sql erase the hint info, after erase hint info, originSQL:%s, hintedSQL:%s", originSQL, hintedSQL)
	}
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.CreateTableStmt:
		p.flag &= ^inCreateOrDropTable
		p.checkAutoIncrement(x)
		p.checkContainDotColumn(x)
	case *ast.CreateViewStmt:
		p.flag &= ^inCreateOrDropTable
	case *ast.DropTableStmt, *ast.AlterTableStmt, *ast.RenameTableStmt:
		p.flag &= ^inCreateOrDropTable
	case *driver.ParamMarkerExpr:
		if p.flag&inPrepare == 0 {
			p.err = parser.ErrSyntax.GenWithStack("syntax error, unexpected '?'")
			return
		}
	case *ast.ExplainStmt:
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			break
		}
		valid := false
		for i, length := 0, len(ast.ExplainFormats); i < length; i++ {
			if strings.ToLower(x.Format) == ast.ExplainFormats[i] {
				valid = true
				break
			}
		}
		if !valid {
			p.err = ErrUnknownExplainFormat.GenWithStackByArgs(x.Format)
		}
	case *ast.TableName:
		p.handleTableName(x)
	case *ast.Join:
		if len(p.tableAliasInJoin) > 0 {
			p.tableAliasInJoin = p.tableAliasInJoin[:len(p.tableAliasInJoin)-1]
		}
	case *ast.FuncCallExpr:
		// The arguments for builtin NAME_CONST should be constants
		// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
		if x.FnName.L == ast.NameConst {
			if len(x.Args) != 2 {
				p.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(x.FnName.L)
			} else {
				_, isValueExpr1 := x.Args[0].(*driver.ValueExpr)
				isValueExpr2 := false
				switch x.Args[1].(type) {
				case *driver.ValueExpr, *ast.UnaryOperationExpr:
					isValueExpr2 = true
				}

				if !isValueExpr1 || !isValueExpr2 {
					p.err = ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
				}
			}
			break
		}

		// no need sleep when retry transaction and avoid unexpect sleep caused by retry.
		if p.flag&inTxnRetry > 0 && x.FnName.L == ast.Sleep {
			if len(x.Args) == 1 {
				x.Args[0] = ast.NewValueExpr(0, "", "")
			}
		}

		if x.FnName.L == ast.NextVal || x.FnName.L == ast.LastVal || x.FnName.L == ast.SetVal {
			p.flag &= ^inSequenceFunction
		}
	case *ast.RepairTableStmt:
		p.flag &= ^inRepairTable
	case *ast.CreateSequenceStmt:
		p.flag &= ^inCreateOrDropTable
	case *ast.BRIEStmt:
		if x.Kind == ast.BRIEKindRestore {
			p.flag &= ^inCreateOrDropTable
		}
	}

	return in, p.err == nil
}

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
						return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
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

func isConstraintKeyTp(constraints []*ast.Constraint, colDef *ast.ColumnDef) bool {
	for _, c := range constraints {
		// If the constraint as follows: primary key(c1, c2)
		// we only support c1 column can be auto_increment.
		if colDef.Name.Name.L != c.Keys[0].Column.Name.L {
			continue
		}
		switch c.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
			ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
			return true
		}
	}

	return false
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
	if len(autoIncrementCols) > 1 {
		p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		return
	}
	// Only have one auto_increment col.
	for col, isKey := range autoIncrementCols {
		if !isKey {
			isKey = isConstraintKeyTp(stmt.Constraints, col)
		}
		autoIncrementMustBeKey := true
		for _, opt := range stmt.Options {
			if opt.Tp == ast.TableOptionEngine && strings.EqualFold(opt.StrValue, "MyISAM") {
				autoIncrementMustBeKey = false
			}
		}
		if autoIncrementMustBeKey && !isKey {
			p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		}
		switch col.Tp.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
		default:
			p.err = errors.Errorf("Incorrect column specifier for column '%s'", col.Name.Name.O)
		}
	}

}

// checkSetOprSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
//        https://mariadb.com/kb/en/intersect/
//        https://mariadb.com/kb/en/except/
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkSetOprSelectList(stmt *ast.SetOprSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		if sel.IsInBraces {
			continue
		}
		if sel.Limit != nil {
			p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
			return
		}
		if sel.OrderBy != nil {
			p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
			return
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = ddl.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAlterDatabaseGrammar(stmt *ast.AlterDatabaseStmt) {
	// for 'ALTER DATABASE' statement, database name can be empty to alter default database.
	if isIncorrectName(stmt.Name) && !stmt.AlterDefaultDatabase {
		p.err = ddl.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = ddl.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkColumn(colDef); err != nil {
			p.err = err
			return
		}
		isPrimary, err := checkColumnOptions(colDef.Options)
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
		for _, spec := range constraint.Keys {
			if spec.Expr != nil {
				p.err = ErrNotSupportedYet.GenWithStackByArgs("create table with expression index")
				return
			}
		}
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
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
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
		p.err = ddl.ErrTableMustHaveColumns
		return
	}
}

func (p *preprocessor) checkCreateViewGrammar(stmt *ast.CreateViewStmt) {
	vName := stmt.ViewName.Name.String()
	if isIncorrectName(vName) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(vName)
		return
	}
	for _, col := range stmt.Cols {
		if isIncorrectName(col.String()) {
			p.err = ddl.ErrWrongColumnName.GenWithStackByArgs(col)
			return
		}
	}
}

func (p *preprocessor) checkCreateViewWithSelect(stmt *ast.SelectStmt) {
	if stmt.SelectIntoOpt != nil {
		p.err = ddl.ErrViewSelectClause.GenWithStackByArgs("INFO")
		return
	}
	if stmt.LockTp != ast.SelectLockNone {
		stmt.LockTp = ast.SelectLockNone
		return
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
}

func (p *preprocessor) checkDropTableNames(tables []*ast.TableName) {
	for _, t := range tables {
		if isIncorrectName(t.Name.String()) {
			p.err = ddl.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	if p.flag&parentIsJoin == 0 {
		p.tableAliasInJoin = append(p.tableAliasInJoin, make(map[string]interface{}))
	}
	tableAliases := p.tableAliasInJoin[len(p.tableAliasInJoin)-1]
	if err := isTableAliasDuplicate(stmt.Left, tableAliases); err != nil {
		p.err = err
		return
	}
	if err := isTableAliasDuplicate(stmt.Right, tableAliases); err != nil {
		p.err = err
		return
	}
	p.flag |= parentIsJoin
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]interface{}) error {
	if ts, ok := node.(*ast.TableSource); ok {
		tabName := ts.AsName
		if tabName.L == "" {
			if tableNode, ok := ts.Source.(*ast.TableName); ok {
				if tableNode.Schema.L != "" {
					tabName = model.NewCIStr(fmt.Sprintf("%s.%s", tableNode.Schema.L, tableNode.Name.L))
				} else {
					tabName = tableNode.Name
				}
			}
		}
		_, exists := tableAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			return ErrNonUniqTable.GenWithStackByArgs(tabName)
		}
		tableAliases[tabName.L] = nil
	}
	return nil
}

func checkColumnOptions(ops []*ast.ColumnOption) (int, error) {
	isPrimary, isGenerated, isStored := 0, 0, false

	for _, op := range ops {
		switch op.Tp {
		case ast.ColumnOptionPrimaryKey:
			isPrimary = 1
		case ast.ColumnOptionGenerated:
			isGenerated = 1
			isStored = op.Stored
		}
	}

	if isPrimary > 0 && isGenerated > 0 && !isStored {
		return isPrimary, ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
	}

	return isPrimary, nil
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	p.err = checkIndexInfo(stmt.IndexName, stmt.IndexPartSpecifications)
}

func (p *preprocessor) checkRenameTableGrammar(stmt *ast.RenameTableStmt) {
	oldTable := stmt.OldTable.Name.String()
	newTable := stmt.NewTable.Name.String()

	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkRenameTable(oldTable, newTable string) {
	if isIncorrectName(oldTable) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(oldTable)
		return
	}

	if isIncorrectName(newTable) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(newTable)
		return
	}
}

func (p *preprocessor) checkRepairTableGrammar(stmt *ast.RepairTableStmt) {
	// Check create table stmt whether it's is in REPAIR MODE.
	if !domainutil.RepairInfo.InRepairMode() {
		p.err = ddl.ErrRepairTableFail.GenWithStackByArgs("TiDB is not in REPAIR MODE")
		return
	}
	if len(domainutil.RepairInfo.GetRepairTableList()) == 0 {
		p.err = ddl.ErrRepairTableFail.GenWithStackByArgs("repair list is empty")
		return
	}

	// Check rename action as the rename statement does.
	oldTable := stmt.Table.Name.String()
	newTable := stmt.CreateStmt.Table.Name.String()
	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewTable != nil {
			ntName := spec.NewTable.Name.String()
			if isIncorrectName(ntName) {
				p.err = ddl.ErrWrongTableName.GenWithStackByArgs(ntName)
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
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(IndexPartSpecifications []*ast.IndexPartSpecification) error {
	colNames := make(map[string]struct{}, len(IndexPartSpecifications))
	for _, IndexColNameWithExpr := range IndexPartSpecifications {
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

// checkIndexInfo checks index name and index column names.
func checkIndexInfo(indexName string, IndexPartSpecifications []*ast.IndexPartSpecification) error {
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		return ddl.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	if len(IndexPartSpecifications) > mysql.MaxKeyParts {
		return infoschema.ErrTooManyKeyParts.GenWithStackByArgs(mysql.MaxKeyParts)
	}
	return checkDuplicateColumnName(IndexPartSpecifications)
}

// checkUnsupportedTableOptions checks if there exists unsupported table options
func checkUnsupportedTableOptions(options []*ast.TableOption) error {
	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionUnion:
			return ddl.ErrTableOptionUnionUnsupported
		case ast.TableOptionInsertMethod:
			return ddl.ErrTableOptionInsertMethodUnsupported
		}
	}
	return nil
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if isIncorrectName(cName) {
		return ddl.ErrWrongColumnName.GenWithStackByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.Flen > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.Tp {
	case mysql.TypeString:
		if tp.Flen != types.UnspecifiedLength && tp.Flen > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		if len(tp.Charset) == 0 {
			// It's not easy to get the schema charset and table charset here.
			// The charset is determined by the order ColumnDefaultCharset --> TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the ddl.CreateTable.
			return nil
		}
		err := ddl.IsTooBigFieldLength(colDef.Tp.Flen, colDef.Name.Name.O, tp.Charset)
		if err != nil {
			return err
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		if tp.Decimal > mysql.MaxFloatingTypeScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tp.Decimal, colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
		}
		if tp.Flen > mysql.MaxFloatingTypeWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Flen, colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
		}
	case mysql.TypeSet:
		if len(tp.Elems) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
		for _, str := range colDef.Tp.Elems {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.Tp), str)
			}
		}
	case mysql.TypeNewDecimal:
		if tp.Decimal > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tp.Decimal, colDef.Name.Name.O, mysql.MaxDecimalScale)
		}

		if tp.Flen > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Flen, colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}
	case mysql.TypeBit:
		if tp.Flen <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(colDef.Name.Name.O)
		}
		if tp.Flen > mysql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxBitDisplayWidth)
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
			if !(tp.Tp == mysql.TypeTimestamp || tp.Tp == mysql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				return true
			}
			break
		}
	}

	return false
}

// isIncorrectName checks if the identifier is incorrect.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func isIncorrectName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}

// checkContainDotColumn checks field contains the table name.
// for example :create table t (c1.c2 int default null).
func (p *preprocessor) checkContainDotColumn(stmt *ast.CreateTableStmt) {
	tName := stmt.Table.Name.String()
	sName := stmt.Table.Schema.String()

	for _, colDef := range stmt.Cols {
		// check schema and table names.
		if colDef.Name.Schema.O != sName && len(colDef.Name.Schema.O) != 0 {
			p.err = ddl.ErrWrongDBName.GenWithStackByArgs(colDef.Name.Schema.O)
			return
		}
		if colDef.Name.Table.O != tName && len(colDef.Name.Table.O) != 0 {
			p.err = ddl.ErrWrongTableName.GenWithStackByArgs(colDef.Name.Table.O)
			return
		}
	}
}

func (p *preprocessor) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {
		currentDB := p.ctx.GetSessionVars().CurrentDB
		if currentDB == "" {
			p.err = errors.Trace(ErrNoDB)
			return
		}
		tn.Schema = model.NewCIStr(currentDB)
	}
	if p.flag&inCreateOrDropTable > 0 {
		// The table may not exist in create table or drop table statement.
		if p.flag&inRepairTable > 0 {
			// Create stmt is in repair stmt, skip resolving the table to avoid error.
			return
		}
		// Create stmt is not in repair stmt, check the table not in repair list.
		if domainutil.RepairInfo.InRepairMode() {
			p.checkNotInRepair(tn)
		}
		return
	}
	// repairStmt: admin repair table A create table B ...
	// repairStmt's tableName is whether `inCreateOrDropTable` or `inRepairTable` flag.
	if p.flag&inRepairTable > 0 {
		p.handleRepairName(tn)
		return
	}

	table, err := p.is.TableByName(tn.Schema, tn.Name)
	if err != nil {
		p.err = err
		return
	}
	tableInfo := table.Meta()
	dbInfo, _ := p.is.SchemaByName(tn.Schema)
	// tableName should be checked as sequence object.
	if p.flag&inSequenceFunction > 0 {
		if !tableInfo.IsSequence() {
			p.err = infoschema.ErrWrongObject.GenWithStackByArgs(dbInfo.Name.O, tableInfo.Name.O, "SEQUENCE")
			return
		}
	}
	tn.TableInfo = tableInfo
	tn.DBInfo = dbInfo
}

func (p *preprocessor) checkNotInRepair(tn *ast.TableName) {
	tableInfo, dbInfo := domainutil.RepairInfo.GetRepairedTableInfoByTableName(tn.Schema.L, tn.Name.L)
	if dbInfo == nil {
		return
	}
	if tableInfo != nil {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tn.Name.L, "this table is in repair")
	}
}

func (p *preprocessor) handleRepairName(tn *ast.TableName) {
	// Check the whether the repaired table is system table.
	if util.IsMemOrSysDB(tn.Schema.L) {
		p.err = ddl.ErrRepairTableFail.GenWithStackByArgs("memory or system database is not for repair")
		return
	}
	tableInfo, dbInfo := domainutil.RepairInfo.GetRepairedTableInfoByTableName(tn.Schema.L, tn.Name.L)
	// tableName here only has the schema rather than DBInfo.
	if dbInfo == nil {
		p.err = ddl.ErrRepairTableFail.GenWithStackByArgs("database " + tn.Schema.L + " is not in repair")
		return
	}
	if tableInfo == nil {
		p.err = ddl.ErrRepairTableFail.GenWithStackByArgs("table " + tn.Name.L + " is not in repair")
		return
	}
	p.ctx.SetValue(domainutil.RepairedTable, tableInfo)
	p.ctx.SetValue(domainutil.RepairedDatabase, dbInfo)
}

func (p *preprocessor) resolveShowStmt(node *ast.ShowStmt) {
	if node.DBName == "" {
		if node.Table != nil && node.Table.Schema.L != "" {
			node.DBName = node.Table.Schema.O
		} else {
			node.DBName = p.ctx.GetSessionVars().CurrentDB
		}
	} else if node.Table != nil && node.Table.Schema.L == "" {
		node.Table.Schema = model.NewCIStr(node.DBName)
	}
	if node.User != nil && node.User.CurrentUser {
		// Fill the Username and Hostname with the current user.
		currentUser := p.ctx.GetSessionVars().User
		if currentUser != nil {
			node.User.Username = currentUser.Username
			node.User.Hostname = currentUser.Hostname
			node.User.AuthUsername = currentUser.AuthUsername
			node.User.AuthHostname = currentUser.AuthHostname
		}
	}
}

func (p *preprocessor) resolveCreateTableStmt(node *ast.CreateTableStmt) {
	for _, val := range node.Constraints {
		if val.Refer != nil && val.Refer.Table.Schema.String() == "" {
			val.Refer.Table.Schema = node.Table.Schema
		}
	}
}

func (p *preprocessor) resolveAlterTableStmt(node *ast.AlterTableStmt) {
	for _, spec := range node.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			p.flag |= inCreateOrDropTable
			break
		}
	}
}

func (p *preprocessor) resolveCreateSequenceStmt(stmt *ast.CreateSequenceStmt) {
	sName := stmt.Name.Name.String()
	if isIncorrectName(sName) {
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(sName)
		return
	}
}
