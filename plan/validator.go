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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// Validate checkes whether the node is valid.
func Validate(node ast.Node, inPrepare bool) error {
	v := validator{inPrepare: inPrepare}
	node.Accept(&v)
	return v.err
}

// validator is an ast.Visitor that validates
// ast Nodes parsed from parser.
type validator struct {
	err         error
	inPrepare   bool
	inAggregate bool
}

func (v *validator) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.AggregateFuncExpr:
		v.inAggregate = true
	case *ast.CreateTableStmt:
		v.checkCreateTableGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.DropTableStmt:
		v.checkDropTableGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.CreateIndexStmt:
		v.checkCreateIndexGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.AlterTableStmt:
		v.checkAlterTableGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.CreateDatabaseStmt:
		v.checkCreateDatabaseGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.DropDatabaseStmt:
		v.checkDropDatabaseGrammar(node)
		if v.err != nil {
			return in, true
		}
	}
	return in, false
}

func (v *validator) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.AggregateFuncExpr:
		v.inAggregate = false
	case *ast.CreateTableStmt:
		v.checkAutoIncrement(x)
	case *ast.ParamMarkerExpr:
		if !v.inPrepare {
			v.err = parser.ErrSyntax.Gen("syntax error, unexpected '?'")
			return
		}
	case *ast.Limit:
		if x.Count == nil {
			break
		}
		if _, isParamMarker := x.Count.(*ast.ParamMarkerExpr); isParamMarker {
			break
		}
		// We only accept ? and uint64 for count/offset in parser.y
		var count, offset uint64
		if x.Count != nil {
			count, _ = x.Count.GetValue().(uint64)
		}
		if x.Offset != nil {
			offset, _ = x.Offset.GetValue().(uint64)
		}
		if count > math.MaxUint64-offset {
			x.Count.SetValue(math.MaxUint64 - offset)
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
			v.err = ErrUnknownExplainFormat.GenByArgs(x.Format)
		}
	}

	return in, v.err == nil
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

func (v *validator) checkAutoIncrement(stmt *ast.CreateTableStmt) {
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
				v.err = err
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
		v.err = errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}

	switch autoIncrementCol.Tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
	default:
		v.err = errors.Errorf("Incorrect column specifier for column '%s'", autoIncrementCol.Name.Name.O)
	}
}

func (v *validator) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		v.err = ddl.ErrWrongDBName.GenByArgs(stmt.Name)
	}
}

func (v *validator) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		v.err = ddl.ErrWrongDBName.GenByArgs(stmt.Name)
	}
}

func (v *validator) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	if stmt.Table == nil {
		v.err = ddl.ErrWrongTableName.GenByArgs("")
		return
	}

	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		v.err = ddl.ErrWrongTableName.GenByArgs(tName)
		return
	}

	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkColumn(colDef); err != nil {
			v.err = errors.Trace(err)
			return
		}
		countPrimaryKey += isPrimary(colDef.Options)
		if countPrimaryKey > 1 {
			v.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				v.err = err
				return
			}
		case ast.ConstraintPrimaryKey:
			if countPrimaryKey > 0 {
				v.err = infoschema.ErrMultiplePriKey
				return
			}
			countPrimaryKey++
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				v.err = err
				return
			}
		}
	}
}

func (v *validator) checkDropTableGrammar(stmt *ast.DropTableStmt) {
	if stmt.Tables == nil {
		v.err = ddl.ErrWrongTableName.GenByArgs("")
		return
	}
	for _, t := range stmt.Tables {
		if isIncorrectName(t.Name.String()) {
			v.err = ddl.ErrWrongTableName.GenByArgs(t.Name.String())
			return
		}
	}
}

func isPrimary(ops []*ast.ColumnOption) int {
	for _, op := range ops {
		if op.Tp == ast.ColumnOptionPrimaryKey {
			return 1
		}
	}
	return 0
}

func (v *validator) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		v.err = ddl.ErrWrongTableName.GenByArgs(tName)
		return
	}
	v.err = checkIndexInfo(stmt.IndexName, stmt.IndexColNames)
}

func (v *validator) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	if stmt.Table == nil {
		v.err = ddl.ErrWrongTableName.GenByArgs("")
		return
	}

	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		v.err = ddl.ErrWrongTableName.GenByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewTable != nil {
			ntName := spec.NewTable.Name.String()
			if isIncorrectName(ntName) {
				v.err = ddl.ErrWrongTableName.GenByArgs(ntName)
				return
			}
		}
		if spec.NewColumn != nil {
			if err := checkColumn(spec.NewColumn); err != nil {
				v.err = err
				return
			}
		}
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey:
				v.err = checkIndexInfo(spec.Constraint.Name, spec.Constraint.Keys)
				if v.err != nil {
					return
				}
			default:
				// Nothing to do now.
			}
		case ast.AlterTableOption:
			for _, opt := range spec.Options {
				if opt.Tp == ast.TableOptionAutoIncrement {
					v.err = ErrAlterAutoID
					return
				}
			}
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(indexColNames []*ast.IndexColName) error {
	for i := 0; i < len(indexColNames); i++ {
		name1 := indexColNames[i].Column.Name
		for j := i + 1; j < len(indexColNames); j++ {
			name2 := indexColNames[j].Column.Name
			if name1.L == name2.L {
				return infoschema.ErrColumnExists.GenByArgs(name2)
			}
		}
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
	case mysql.TypeDouble:
		if tp.Flen != types.UnspecifiedLength && tp.Flen > mysql.PrecisionForDouble {
			return types.ErrWrongFieldSpec.Gen("Incorrect column specifier for column '%s'", colDef.Name.Name.O)
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
	default:
		// TODO: Add more types.
	}
	return nil
}

// isNowSymFunc checks whether defaul value is a NOW() builtin function.
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
