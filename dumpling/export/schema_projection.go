// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

func generateProjectedSchema(
	originSQL string,
	database string,
	projected bool,
	retainedColumns map[string]struct{},
	schemaColumns map[tableName]map[string]struct{},
) (string, error) {
	stmt, err := parser.New().ParseOneStmt(originSQL, "", "")
	if err != nil {
		return "", errors.Annotate(err, "failed to parse CREATE TABLE for column projection")
	}
	createTable, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return "", errors.Errorf("expected CREATE TABLE for column projection, got %T", stmt)
	}
	columns := make([]*ast.ColumnDef, 0, len(createTable.Cols))
	for _, column := range createTable.Cols {
		if _, ok := retainedColumns[column.Name.Name.L]; !ok {
			continue
		}
		options, err := filterColumnOptions(column, retainedColumns, database, schemaColumns)
		if err != nil {
			return "", err
		}
		column.Options = options
		columns = append(columns, column)
	}
	createTable.Cols = columns

	constraints := make([]*ast.Constraint, 0, len(createTable.Constraints))
	for _, constraint := range createTable.Constraints {
		keep, err := filterTableConstraint(
			constraint,
			retainedColumns,
			database,
			schemaColumns,
		)
		if err != nil {
			return "", err
		}
		if !keep {
			continue
		}
		constraints = append(constraints, constraint)
	}
	createTable.Constraints = constraints

	if err := validatePartitionColumns(createTable.Partition, retainedColumns); err != nil {
		return "", err
	}
	if err := validateTTLColumns(createTable.Options, retainedColumns); err != nil {
		return "", err
	}
	if !projected {
		return originSQL, nil
	}

	var buffer bytes.Buffer
	err = createTable.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment,
		In:    &buffer,
	})
	if err != nil {
		return "", errors.Annotate(err, "failed to restore projected CREATE TABLE")
	}
	return buffer.String(), nil
}

func collectProjectedSchemaColumns(originSQL string, selectedColumns []string) (map[string]struct{}, error) {
	stmt, err := parser.New().ParseOneStmt(originSQL, "", "")
	if err != nil {
		return nil, errors.Annotate(err, "failed to parse CREATE TABLE for column projection")
	}
	createTable, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.Errorf("expected CREATE TABLE for column projection, got %T", stmt)
	}

	retainedColumns := make(map[string]struct{}, len(selectedColumns))
	for _, selectedColumn := range selectedColumns {
		retainedColumns[strings.ToLower(selectedColumn)] = struct{}{}
	}
	// A generated column can only depend on generated columns defined before it.
	for _, column := range createTable.Cols {
		for _, option := range column.Options {
			if option.Tp == ast.ColumnOptionGenerated && allReferencedColumnsCovered(option.Expr, retainedColumns) {
				retainedColumns[column.Name.Name.L] = struct{}{}
				break
			}
		}
	}

	return retainedColumns, nil
}

func filterColumnOptions(
	column *ast.ColumnDef,
	retained map[string]struct{},
	database string,
	schemaColumns map[tableName]map[string]struct{},
) ([]*ast.ColumnOption, error) {
	options := make([]*ast.ColumnOption, 0, len(column.Options))
	for _, option := range column.Options {
		switch option.Tp {
		case ast.ColumnOptionCheck:
			if !allReferencedColumnsCovered(option.Expr, retained) {
				continue
			}
		case ast.ColumnOptionReference:
			// MySQL 9.7 supports inline foreign keys as column-level REFERENCES options.
			if err := validateForeignKeyReference(option.Refer, database, schemaColumns); err != nil {
				return nil, err
			}
		case ast.ColumnOptionDefaultValue, ast.ColumnOptionOnUpdate:
			if !allReferencedColumnsCovered(option.Expr, retained) {
				return nil, errors.Errorf(
					"column `%s` expression references a removed column",
					column.Name.Name.O,
				)
			}
		}
		options = append(options, option)
	}
	return options, nil
}

func filterTableConstraint(
	constraint *ast.Constraint,
	retained map[string]struct{},
	database string,
	schemaColumns map[tableName]map[string]struct{},
) (bool, error) {
	for _, key := range constraint.Keys {
		if key.Column != nil {
			if _, ok := retained[key.Column.Name.L]; !ok {
				return false, nil
			}
		}
		if !allReferencedColumnsCovered(key.Expr, retained) {
			return false, nil
		}
	}
	if !allReferencedColumnsCovered(constraint.Expr, retained) {
		return false, nil
	}
	if constraint.Option != nil && !allReferencedColumnsCovered(constraint.Option.Condition, retained) {
		return false, nil
	}
	if err := validateForeignKeyReference(constraint.Refer, database, schemaColumns); err != nil {
		return false, err
	}
	return true, nil
}

func validateForeignKeyReference(
	reference *ast.ReferenceDef,
	database string,
	schemaColumns map[tableName]map[string]struct{},
) error {
	if reference == nil || reference.Table == nil {
		return nil
	}

	referenceTable := reference.Table.Name.O
	referenceDatabase := reference.Table.Schema.O
	if referenceDatabase == "" {
		referenceDatabase = database
	}

	targetColumns, ok := schemaColumns[tableName{db: referenceDatabase, table: referenceTable}]
	if !ok {
		return nil
	}
	for _, key := range reference.IndexPartSpecifications {
		if key.Column != nil {
			if _, ok := targetColumns[key.Column.Name.L]; !ok {
				return removedReferenceColumnError(referenceDatabase, referenceTable, key.Column.Name.O)
			}
		}
	}
	return nil
}

func removedReferenceColumnError(database, table, column string) error {
	return errors.Errorf(
		"foreign key references removed column `%s`.`%s`.`%s`",
		escapeString(database),
		escapeString(table),
		escapeString(column),
	)
}

func validatePartitionColumns(partition *ast.PartitionOptions, retained map[string]struct{}) error {
	if partition == nil {
		return nil
	}
	if !partitionColumnsRetained(&partition.PartitionMethod, retained) ||
		(partition.Sub != nil && !partitionColumnsRetained(partition.Sub, retained)) {
		return errors.New("partition definition references a removed column")
	}
	return nil
}

func partitionColumnsRetained(method *ast.PartitionMethod, retained map[string]struct{}) bool {
	for _, column := range method.ColumnNames {
		if _, ok := retained[column.Name.L]; !ok {
			return false
		}
	}
	return allReferencedColumnsCovered(method.Expr, retained)
}

func validateTTLColumns(options []*ast.TableOption, retained map[string]struct{}) error {
	for _, option := range options {
		if option.Tp == ast.TableOptionTTL && option.ColumnName != nil {
			if _, ok := retained[option.ColumnName.Name.L]; !ok {
				return errors.Errorf("TTL definition references removed column `%s`", option.ColumnName.Name.O)
			}
		}
	}
	return nil
}

func allReferencedColumnsCovered(expr ast.ExprNode, retained map[string]struct{}) bool {
	if expr == nil {
		return true
	}
	collector := &columnNameCollector{columns: make(map[string]struct{})}
	expr.Accept(collector)
	for column := range collector.columns {
		if _, ok := retained[column]; !ok {
			return false
		}
	}
	return true
}

type columnNameCollector struct {
	columns map[string]struct{}
}

func (c *columnNameCollector) Enter(node ast.Node) (ast.Node, bool) {
	column, ok := node.(*ast.ColumnNameExpr)
	if ok {
		c.columns[column.Name.Name.L] = struct{}{}
	}
	return node, false
}

func (*columnNameCollector) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}
