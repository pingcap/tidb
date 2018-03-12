// Copyright 2017 PingCAP, Inc.
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

package ddl

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/table"
)

// columnGenerationInDDL is a struct for validating generated columns in DDL.
type columnGenerationInDDL struct {
	position    int
	generated   bool
	dependences map[string]struct{}
}

// verifyColumnGeneration is for CREATE TABLE, because we need verify all columns in the table.
func verifyColumnGeneration(colName2Generation map[string]columnGenerationInDDL, colName string) error {
	attribute := colName2Generation[colName]
	if attribute.generated {
		for depCol := range attribute.dependences {
			if attr, ok := colName2Generation[depCol]; ok {
				if attr.generated && attribute.position <= attr.position {
					// A generated column definition can refer to other
					// generated columns occurring earilier in the table.
					err := errGeneratedColumnNonPrior.GenByArgs()
					return errors.Trace(err)
				}
			} else {
				err := errBadField.GenByArgs(depCol, "generated column function")
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// columnNamesCover checks whether dependColNames is covered by normalColNames or not.
// it's only for alter table add column because before alter, we can make sure that all
// columns in table are verified already.
func columnNamesCover(normalColNames map[string]struct{}, dependColNames map[string]struct{}) error {
	for name := range dependColNames {
		if _, ok := normalColNames[name]; !ok {
			return errBadField.GenByArgs(name, "generated column function")
		}
	}
	return nil
}

// findDependedColumnNames returns a set of string, which indicates
// the names of the columns that are depended by colDef.
func findDependedColumnNames(colDef *ast.ColumnDef) (generated bool, colsMap map[string]struct{}) {
	colsMap = make(map[string]struct{})
	for _, option := range colDef.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			generated = true
			colNames := findColumnNamesInExpr(option.Expr)
			for _, depCol := range colNames {
				colsMap[depCol.Name.L] = struct{}{}
			}
			break
		}
	}
	return
}

// findColumnNamesInExpr returns a slice of ast.ColumnName which is referred in expr.
func findColumnNamesInExpr(expr ast.ExprNode) []*ast.ColumnName {
	var c generatedColumnChecker
	expr.Accept(&c)
	return c.cols
}

type generatedColumnChecker struct {
	cols []*ast.ColumnName
}

func (c *generatedColumnChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	return inNode, false
}

func (c *generatedColumnChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch x := inNode.(type) {
	case *ast.ColumnName:
		c.cols = append(c.cols, x)
	}
	return inNode, true
}

// checkModifyGeneratedColumn checks the modification between
// old and new is valid or not by such rules:
//  1. the modification can't change stored status;
//  2. if the new is generated, check its refer rules.
func checkModifyGeneratedColumn(originCols []*table.Column, oldCol, newCol *table.Column) error {
	// rule 1.
	var stored = [2]bool{false, false}
	var cols = [2]*table.Column{oldCol, newCol}
	for i, col := range cols {
		if !col.IsGenerated() || col.GeneratedStored {
			stored[i] = true
		}
	}
	if stored[0] != stored[1] {
		return errUnsupportedOnGeneratedColumn.GenByArgs("Changing the STORED status")
	}
	// rule 2.
	var colName2Generation = make(map[string]columnGenerationInDDL, len(originCols))
	for i, column := range originCols {
		// We can compare the pointers simply.
		if column == oldCol {
			colName2Generation[newCol.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   newCol.IsGenerated(),
				dependences: newCol.Dependences,
			}
		} else if !column.IsGenerated() {
			colName2Generation[column.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[column.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: column.Dependences,
			}
		}
	}
	// We always need test all columns, even if it's not changed
	// because other can depend on it so its name can't be changed.
	for _, column := range originCols {
		var colName string
		if column == oldCol {
			colName = newCol.Name.L
		} else {
			colName = column.Name.L
		}
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
