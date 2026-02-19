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

package ddl

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func buildColumnsAndConstraints(
	ctx *metabuild.Context,
	colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint,
	tblCharset string,
	tblCollate string,
) ([]*table.Column, []*ast.Constraint, error) {
	// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		if v.Tp == ast.ConstraintPrimaryKey {
			outPriKeyConstraint = v
			break
		}
	}
	cols := make([]*table.Column, 0, len(colDefs))
	colMap := make(map[string]*table.Column, len(colDefs))

	for i, colDef := range colDefs {
		if field_types.TiDBStrictIntegerDisplayWidth {
			switch colDef.Tp.GetType() {
			case mysql.TypeTiny:
				// No warning for BOOL-like tinyint(1)
				if colDef.Tp.GetFlen() != types.UnspecifiedLength && colDef.Tp.GetFlen() != 1 {
					ctx.AppendWarning(
						dbterror.ErrWarnDeprecatedIntegerDisplayWidth.FastGenByArgs(),
					)
				}
			case mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
				if colDef.Tp.GetFlen() != types.UnspecifiedLength {
					ctx.AppendWarning(
						dbterror.ErrWarnDeprecatedIntegerDisplayWidth.FastGenByArgs(),
					)
				}
			}
		}
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint, tblCharset, tblCollate)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		col.State = model.StatePublic
		if mysql.HasZerofillFlag(col.GetFlag()) {
			ctx.AppendWarning(
				dbterror.ErrWarnDeprecatedZerofill.FastGenByArgs(),
			)
		}
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		var colName string
		for _, keyPart := range constr.Keys {
			if keyPart.Expr != nil {
				colName = getAnonymousIndexPrefix(constr.Option != nil && constr.Option.Tp == ast.IndexTypeVector)
			}
		}
		if colName == "" {
			colName = constr.Keys[0].Column.Name.O
		}
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, mysql.PrimaryKeyName) {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(tableName ast.CIStr, constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, constr.Tp)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, constr.Tp)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	checkConstraints := make([]*ast.Constraint, 0, len(constraints))
	for _, constr := range constraints {
		if constr.Tp != ast.ConstraintForeignKey {
			setEmptyConstraintName(constrNames, constr)
		}
		if constr.Tp == ast.ConstraintCheck {
			checkConstraints = append(checkConstraints, constr)
		}
	}
	// Set check constraint name under its order.
	if len(checkConstraints) > 0 {
		setEmptyCheckConstraintName(tableName.L, constrNames, checkConstraints)
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, constraintType ast.ConstraintType) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		switch constraintType {
		case ast.ConstraintForeignKey:
			return dbterror.ErrFkDupName.GenWithStackByArgs(name)
		case ast.ConstraintCheck:
			return dbterror.ErrCheckConstraintDupName.GenWithStackByArgs(name)
		default:
			return dbterror.ErrDupKeyName.GenWithStackByArgs(name)
		}
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyCheckConstraintName(tableLowerName string, namesMap map[string]bool, constrs []*ast.Constraint) {
	cnt := 1
	constraintPrefix := tableLowerName + "_chk_"
	for _, constr := range constrs {
		if constr.Name == "" {
			constrName := fmt.Sprintf("%s%d", constraintPrefix, cnt)
			for {
				// loop until find constrName that haven't been used.
				if !namesMap[constrName] {
					namesMap[constrName] = true
					break
				}
				cnt++
				constrName = fmt.Sprintf("%s%d", constraintPrefix, cnt)
			}
			constr.Name = constrName
		}
	}
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			c.AddFlag(mysql.PriKeyFlag)
			// Primary key can not be NULL.
			c.AddFlag(mysql.NotNullFlag)
			setNoDefaultValueFlag(c, c.DefaultValue != nil)
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.AddFlag(mysql.MultipleKeyFlag)
				} else {
					c.AddFlag(mysql.UniqueKeyFlag)
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set.
				c.AddFlag(mysql.MultipleKeyFlag)
			}
		}
	}
}
