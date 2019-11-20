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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
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
					// generated columns occurring earlier in the table.
					err := errGeneratedColumnNonPrior.GenWithStackByArgs()
					return errors.Trace(err)
				}
			} else {
				err := ErrBadField.GenWithStackByArgs(depCol, "generated column function")
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// verifyColumnGenerationSingle is for ADD GENERATED COLUMN, we just need verify one column itself.
func verifyColumnGenerationSingle(dependColNames map[string]struct{}, cols []*table.Column, position *ast.ColumnPosition) error {
	// Since the added column does not exist yet, we should derive it's offset from ColumnPosition.
	pos, err := findPositionRelativeColumn(cols, position)
	if err != nil {
		return errors.Trace(err)
	}
	// should check unknown column first, then the prior ones.
	for _, col := range cols {
		if _, ok := dependColNames[col.Name.L]; ok {
			if col.IsGenerated() && col.Offset >= pos {
				// Generated column can refer only to generated columns defined prior to it.
				return errGeneratedColumnNonPrior.GenWithStackByArgs()
			}
		}
	}
	return nil
}

// checkDependedColExist ensure all depended columns exist.
// NOTE: this will MODIFY parameter `dependCols`.
func checkDependedColExist(dependCols map[string]struct{}, cols []*table.Column) error {
	for _, col := range cols {
		delete(dependCols, col.Name.L)
	}
	if len(dependCols) != 0 {
		for arbitraryCol := range dependCols {
			return ErrBadField.GenWithStackByArgs(arbitraryCol, "generated column function")
		}
	}
	return nil
}

// findPositionRelativeColumn returns a pos relative to added generated column position.
func findPositionRelativeColumn(cols []*table.Column, pos *ast.ColumnPosition) (int, error) {
	position := len(cols)
	// Get the column position, default is cols's length means appending.
	// For "alter table ... add column(...)", the position will be nil.
	// For "alter table ... add column ... ", the position will be default one.
	if pos == nil {
		return position, nil
	}
	if pos.Tp == ast.ColumnPositionFirst {
		position = 0
	} else if pos.Tp == ast.ColumnPositionAfter {
		var col *table.Column
		for _, c := range cols {
			if c.Name.L == pos.RelativeColumn.Name.L {
				col = c
				break
			}
		}
		if col == nil {
			return -1, ErrBadField.GenWithStackByArgs(pos.RelativeColumn, "generated column function")
		}
		// Inserted position is after the mentioned column.
		position = col.Offset + 1
	}
	return position, nil
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
//  3. check if the modified expr contains non-deterministic functions
//  4. check whether new column refers to any auto-increment columns.
//  5. check if the new column is indexed or stored
func checkModifyGeneratedColumn(tbl table.Table, oldCol, newCol *table.Column, newColDef *ast.ColumnDef) error {
	// rule 1.
	oldColIsStored := !oldCol.IsGenerated() || oldCol.GeneratedStored
	newColIsStored := !newCol.IsGenerated() || newCol.GeneratedStored
	if oldColIsStored != newColIsStored {
		return errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Changing the STORED status")
	}

	// rule 2.
	originCols := tbl.Cols()
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

	if newCol.IsGenerated() {
		// rule 3.
		if err := checkIllegalFn4GeneratedColumn(newCol.Name.L, newCol.GeneratedExpr); err != nil {
			return errors.Trace(err)
		}

		// rule 4.
		if err := checkGeneratedWithAutoInc(tbl.Meta(), newColDef); err != nil {
			return errors.Trace(err)
		}

		// rule 5.
		if err := checkIndexOrStored(tbl, oldCol, newCol); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type illegalFunctionChecker struct {
	hasIllegalFunc bool
	hasAggFunc     bool
}

func (c *illegalFunctionChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	switch node := inNode.(type) {
	case *ast.FuncCallExpr:
		// Blocked functions & non-builtin functions is not allowed
		_, IsFunctionBlocked := expression.IllegalFunctions4GeneratedColumns[node.FnName.L]
		if IsFunctionBlocked || !expression.IsFunctionSupported(node.FnName.L) {
			c.hasIllegalFunc = true
			return inNode, true
		}
	case *ast.SubqueryExpr, *ast.ValuesExpr, *ast.VariableExpr:
		// Subquery & `values(x)` & variable is not allowed
		c.hasIllegalFunc = true
		return inNode, true
	case *ast.AggregateFuncExpr:
		// Aggregate function is not allowed
		c.hasAggFunc = true
		return inNode, true
	}
	return inNode, false
}

func (c *illegalFunctionChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	return inNode, true
}

func checkIllegalFn4GeneratedColumn(colName string, expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}
	var c illegalFunctionChecker
	expr.Accept(&c)
	if c.hasIllegalFunc {
		return ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs(colName)
	}
	if c.hasAggFunc {
		return ErrInvalidGroupFuncUse
	}
	return nil
}

// Check whether newColumnDef refers to any auto-increment columns.
func checkGeneratedWithAutoInc(tableInfo *model.TableInfo, newColumnDef *ast.ColumnDef) error {
	_, dependColNames := findDependedColumnNames(newColumnDef)
	if err := checkAutoIncrementRef(newColumnDef.Name.Name.L, dependColNames, tableInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkIndexOrStored(tbl table.Table, oldCol, newCol *table.Column) error {
	if oldCol.GeneratedExprString == newCol.GeneratedExprString {
		return nil
	}

	if newCol.GeneratedStored {
		return errUnsupportedOnGeneratedColumn.GenWithStackByArgs("modifying a stored column")
	}

	for _, idx := range tbl.Indices() {
		for _, col := range idx.Meta().Columns {
			if col.Name.L == newCol.Name.L {
				return errUnsupportedOnGeneratedColumn.GenWithStackByArgs("modifying an indexed column")
			}
		}
	}
	return nil
}

// checkAutoIncrementRef checks if an generated column depends on an auto-increment column and raises an error if so.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html for details.
func checkAutoIncrementRef(name string, dependencies map[string]struct{}, tbInfo *model.TableInfo) error {
	exists, autoIncrementColumn := infoschema.HasAutoIncrementColumn(tbInfo)
	if exists {
		if _, found := dependencies[autoIncrementColumn]; found {
			return ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(name)
		}
	}
	return nil
}
