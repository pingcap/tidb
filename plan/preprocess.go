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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
)

// Preprocess resolves table names of the node, and checks some statements validation.
func Preprocess(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema, inPrepare bool) error {
	v := preprocessor{is: is, ctx: ctx, inPrepare: inPrepare, tableAliasInJoin: make([]map[string]interface{}, 0, 0)}
	node.Accept(&v)
	return errors.Trace(v.err)
}

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	is        infoschema.InfoSchema
	ctx       sessionctx.Context
	err       error
	inPrepare bool
	// inCreateOrDropTable is true when visiting create/drop table statement.
	inCreateOrDropTable bool

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]interface{}

	parentIsJoin bool
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.CreateTableStmt:
		p.inCreateOrDropTable = true
		p.checkCreateTableGrammar(node)
	case *ast.DropTableStmt:
		p.inCreateOrDropTable = true
		p.checkDropTableGrammar(node)
	case *ast.RenameTableStmt:
		p.inCreateOrDropTable = true
		p.checkRenameTableGrammar(node)
	case *ast.CreateIndexStmt:
		p.checkCreateIndexGrammar(node)
	case *ast.AlterTableStmt:
		p.resolveAlterTableStmt(node)
		p.checkAlterTableGrammar(node)
	case *ast.CreateDatabaseStmt:
		p.checkCreateDatabaseGrammar(node)
	case *ast.DropDatabaseStmt:
		p.checkDropDatabaseGrammar(node)
	case *ast.ShowStmt:
		p.resolveShowStmt(node)
	case *ast.UnionSelectList:
		p.checkUnionSelectList(node)
	case *ast.DeleteTableList:
		return in, true
	case *ast.Join:
		p.checkNonUniqTableAlias(node)
	default:
		p.parentIsJoin = false
	}
	return in, p.err != nil
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.CreateTableStmt:
		p.inCreateOrDropTable = false
		p.checkAutoIncrement(x)
		p.checkContainDotColumn(x)
	case *ast.DropTableStmt, *ast.AlterTableStmt, *ast.RenameTableStmt:
		p.inCreateOrDropTable = false
	case *ast.ParamMarkerExpr:
		if !p.inPrepare {
			p.err = parser.ErrSyntax.Gen("syntax error, unexpected '?'")
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
			p.err = ErrUnknownExplainFormat.GenByArgs(x.Format)
		}
	case *ast.TableName:
		p.handleTableName(x)
	case *ast.Join:
		if len(p.tableAliasInJoin) > 0 {
			p.tableAliasInJoin = p.tableAliasInJoin[:len(p.tableAliasInJoin)-1]
		}
	}

	return in, p.err == nil
}

func checkAutoIncrementOp(colDef *ast.ColumnDef, num int) (bool, error) {
	var hasAutoIncrement bool

	if colDef.Options[num].Tp == ast.ColumnOptionAutoIncrement {
		hasAutoIncrement = true
		if len(colDef.Options) == num+1 {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[num+1:] {
			if op.Tp == ast.ColumnOptionDefaultValue && !op.Expr.GetDatum().IsNull() {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}
	if colDef.Options[num].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != num+1 {
		if colDef.Options[num].Expr.GetDatum().IsNull() {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[num+1:] {
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
	var (
		isKey            bool
		count            int
		autoIncrementCol *ast.ColumnDef
	)

	for _, colDef := range stmt.Cols {
		var hasAutoIncrement bool
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
			count++
			autoIncrementCol = colDef
		}
	}

	if count < 1 {
		return
	}
	if !isKey {
		isKey = isConstraintKeyTp(stmt.Constraints, autoIncrementCol)
	}
	autoIncrementMustBeKey := true
	for _, opt := range stmt.Options {
		if opt.Tp == ast.TableOptionEngine && strings.EqualFold(opt.StrValue, "MyISAM") {
			autoIncrementMustBeKey = false
		}
	}
	if (autoIncrementMustBeKey && !isKey) || count > 1 {
		p.err = errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}

	switch autoIncrementCol.Tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
	default:
		p.err = errors.Errorf("Incorrect column specifier for column '%s'", autoIncrementCol.Name.Name.O)
	}
}

// checkUnionSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkUnionSelectList(stmt *ast.UnionSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		if sel.IsInBraces {
			continue
		}
		if sel.Limit != nil {
			p.err = ErrWrongUsage.GenByArgs("UNION", "LIMIT")
			return
		}
		if sel.OrderBy != nil {
			p.err = ErrWrongUsage.GenByArgs("UNION", "ORDER BY")
			return
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = ddl.ErrWrongDBName.GenByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = ddl.ErrWrongDBName.GenByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = ddl.ErrWrongTableName.GenByArgs(tName)
		return
	}
	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkColumn(colDef); err != nil {
			p.err = errors.Trace(err)
			return
		}
		countPrimaryKey += isPrimary(colDef.Options)
		if countPrimaryKey > 1 {
			p.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
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
	if stmt.Select != nil {
		// FIXME: a temp error noticing 'not implemented' (issue 4754)
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
		p.err = ddl.ErrTableMustHaveColumns
		return
	}
}

func (p *preprocessor) checkDropTableGrammar(stmt *ast.DropTableStmt) {
	for _, t := range stmt.Tables {
		if isIncorrectName(t.Name.String()) {
			p.err = ddl.ErrWrongTableName.GenByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	if !p.parentIsJoin {
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
	p.parentIsJoin = true
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]interface{}) error {
	if ts, ok := node.(*ast.TableSource); ok {
		_, exists := tableAliases[ts.AsName.L]
		if len(ts.AsName.L) != 0 && exists {
			return ErrNonUniqTable.GenByArgs(ts.AsName)
		}
		tableAliases[ts.AsName.L] = nil
	}
	return nil
}

func isPrimary(ops []*ast.ColumnOption) int {
	for _, op := range ops {
		if op.Tp == ast.ColumnOptionPrimaryKey {
			return 1
		}
	}
	return 0
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = ddl.ErrWrongTableName.GenByArgs(tName)
		return
	}
	p.err = checkIndexInfo(stmt.IndexName, stmt.IndexColNames)
}

func (p *preprocessor) checkRenameTableGrammar(stmt *ast.RenameTableStmt) {
	oldTable := stmt.OldTable.Name.String()
	newTable := stmt.NewTable.Name.String()

	if isIncorrectName(oldTable) {
		p.err = ddl.ErrWrongTableName.GenByArgs(oldTable)
		return
	}

	if isIncorrectName(newTable) {
		p.err = ddl.ErrWrongTableName.GenByArgs(newTable)
		return
	}
}

func (p *preprocessor) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = ddl.ErrWrongTableName.GenByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewTable != nil {
			ntName := spec.NewTable.Name.String()
			if isIncorrectName(ntName) {
				p.err = ddl.ErrWrongTableName.GenByArgs(ntName)
				return
			}
		}
		for _, colDef := range spec.NewColumns {
			if p.err = checkColumn(colDef); p.err != nil {
				return
			}
		}
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey:
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
func checkDuplicateColumnName(indexColNames []*ast.IndexColName) error {
	colNames := make(map[string]struct{}, len(indexColNames))
	for _, indexColName := range indexColNames {
		name := indexColName.Column.Name
		if _, ok := colNames[name.L]; ok {
			return infoschema.ErrColumnExists.GenByArgs(name)
		}
		colNames[name.L] = struct{}{}
	}
	return nil
}

// checkIndexInfo checks index name and index column names.
func checkIndexInfo(indexName string, indexColNames []*ast.IndexColName) error {
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		return ddl.ErrWrongNameForIndex.GenByArgs(indexName)
	}
	if len(indexColNames) > mysql.MaxKeyParts {
		return infoschema.ErrTooManyKeyParts.GenByArgs(mysql.MaxKeyParts)
	}
	return checkDuplicateColumnName(indexColNames)
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if isIncorrectName(cName) {
		return ddl.ErrWrongColumnName.GenByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.Flen > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.Gen("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.Tp {
	case mysql.TypeString:
		if tp.Flen != types.UnspecifiedLength && tp.Flen > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.Gen("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		maxFlen := mysql.MaxFieldVarCharLength
		cs := tp.Charset
		// TODO: TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
		// TODO: Change TableOption parser to parse collate.
		// Reference https://github.com/pingcap/tidb/blob/b091e828cfa1d506b014345fb8337e424a4ab905/ddl/ddl_api.go#L185-L204
		if len(tp.Charset) == 0 {
			cs = mysql.DefaultCharset
		}
		desc, err := charset.GetCharsetDesc(cs)
		if err != nil {
			return errors.Trace(err)
		}
		maxFlen /= desc.Maxlen
		if tp.Flen != types.UnspecifiedLength && tp.Flen > maxFlen {
			return types.ErrTooBigFieldLength.Gen("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, maxFlen)
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		if tp.Decimal > mysql.MaxFloatingTypeScale {
			return types.ErrTooBigScale.GenByArgs(tp.Decimal, colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
		}
		if tp.Flen > mysql.MaxFloatingTypeWidth {
			return types.ErrTooBigPrecision.GenByArgs(tp.Flen, colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
		}
	case mysql.TypeSet:
		if len(tp.Elems) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.Gen("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html .
		for _, str := range colDef.Tp.Elems {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenByArgs(types.TypeStr(tp.Tp), str)
			}
		}
	case mysql.TypeNewDecimal:
		if tp.Decimal > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenByArgs(tp.Decimal, colDef.Name.Name.O, mysql.MaxDecimalScale)
		}

		if tp.Flen > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenByArgs(tp.Flen, colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}
	default:
		// TODO: Add more types.
	}
	return nil
}

// isDefaultValNowSymFunc checks whether defaul value is a NOW() builtin function.
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
			p.err = ddl.ErrWrongDBName.GenByArgs(colDef.Name.Schema.O)
			return
		}
		if colDef.Name.Table.O != tName && len(colDef.Name.Table.O) != 0 {
			p.err = ddl.ErrWrongTableName.GenByArgs(colDef.Name.Table.O)
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
	if p.inCreateOrDropTable {
		// The table may not exist in create table or drop table statement.
		// Skip resolving the table to avoid error.
		return
	}
	table, err := p.is.TableByName(tn.Schema, tn.Name)
	if err != nil {
		p.err = errors.Trace(err)
		return
	}
	tn.TableInfo = table.Meta()
	dbInfo, _ := p.is.SchemaByName(tn.Schema)
	tn.DBInfo = dbInfo
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
		}
	}
}

func (p *preprocessor) resolveAlterTableStmt(node *ast.AlterTableStmt) {
	for _, spec := range node.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			p.inCreateOrDropTable = true
			break
		}
	}
}
