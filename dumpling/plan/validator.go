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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
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
	err           error
	wildCardCount int
	inPrepare     bool
	inAggregate   bool
}

func (v *validator) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch in.(type) {
	case *ast.AggregateFuncExpr:
		if v.inAggregate {
			// Aggregate function can not contain aggregate function.
			v.err = ErrInvalidGroupFuncUse
			return in, true
		}
		v.inAggregate = true
	case *ast.CreateTableStmt:
		v.checkCreateTableGrammar(in.(*ast.CreateTableStmt))
		if v.err != nil {
			return in, true
		}
	case *ast.CreateIndexStmt:
		v.checkCreateIndexGrammar(in.(*ast.CreateIndexStmt))
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
		}
	case *ast.Limit:
		if x.Count > math.MaxUint64-x.Offset {
			x.Count = math.MaxUint64 - x.Offset
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
			if op.Tp == ast.ColumnOptionDefaultValue {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}
	if colDef.Options[num].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != num+1 {
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
		if len(c.Keys) < 1 {
		}
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
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey, ast.ColumnOptionUniqIndex,
				ast.ColumnOptionUniq, ast.ColumnOptionKey, ast.ColumnOptionIndex:
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

func (v *validator) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	countPrimayKey := 0
	for _, colDef := range stmt.Cols {
		tp := colDef.Tp
		if tp.Tp == mysql.TypeString &&
			tp.Flen != types.UnspecifiedLength && tp.Flen > 255 {
			v.err = errors.Errorf("Column length too big for column '%s' (max = 255); use BLOB or TEXT instead", colDef.Name.Name.O)
			return
		}
		countPrimayKey += isPrimary(colDef.Options)
		if countPrimayKey > 1 {
			v.err = errors.Errorf("Multiple primary key defined")
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
	for i := 0; i < len(stmt.IndexColNames); i++ {
		name1 := stmt.IndexColNames[i].Column.Name
		for j := i + 1; j < len(stmt.IndexColNames); j++ {
			name2 := stmt.IndexColNames[j].Column.Name
			if name1.L == name2.L {
				v.err = errors.Errorf("Duplicate column name '%s'", name1.O)
				return
			}
		}
	}
}
