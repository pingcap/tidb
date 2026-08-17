// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

func projectTableSchema(
	originSQL string,
	database string,
	projection columnProjection,
	projections map[tableName]columnProjection,
) (string, error) {
	// An unchanged table may still reference a projected table through a foreign key.
	if !projection.projected && len(projections) == 0 {
		return originSQL, nil
	}

	createTable, err := parseCreateTableForProjection(originSQL)
	if err != nil {
		return "", err
	}
	retainedColumns, err := retainedSchemaColumnSet(createTable, projection.selectedColumns)
	if err != nil {
		return "", err
	}

	changed := false
	columns := make([]*ast.ColumnDef, 0, len(createTable.Cols))
	for _, column := range createTable.Cols {
		if _, ok := retainedColumns[column.Name.Name.L]; !ok {
			changed = true
			continue
		}
		options, optionsChanged, err := projectColumnOptions(column, retainedColumns, database, createTable.Table.Name.L, projections)
		if err != nil {
			return "", err
		}
		column.Options = options
		changed = changed || optionsChanged
		columns = append(columns, column)
	}
	createTable.Cols = columns

	constraints := make([]*ast.Constraint, 0, len(createTable.Constraints))
	for _, constraint := range createTable.Constraints {
		if !constraintColumnsRetained(constraint, retainedColumns) {
			changed = true
			continue
		}
		if err := validateReferenceColumns(constraint.Refer, database, createTable.Table.Name.L, retainedColumns, projections); err != nil {
			return "", err
		}
		constraints = append(constraints, constraint)
	}
	createTable.Constraints = constraints
	if err := validateAutomaticIDColumns(createTable.Cols, createTable.Constraints); err != nil {
		return "", err
	}

	if err := validatePartitionColumns(createTable.Partition, retainedColumns); err != nil {
		return "", err
	}
	if err := validateTTLColumns(createTable.Options, retainedColumns); err != nil {
		return "", err
	}
	if !changed {
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

func projectedTableSchemaColumns(originSQL string, projection columnProjection) ([]string, error) {
	createTable, err := parseCreateTableForProjection(originSQL)
	if err != nil {
		return nil, err
	}
	retainedColumns, err := retainedSchemaColumnSet(createTable, projection.selectedColumns)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(retainedColumns))
	for _, column := range createTable.Cols {
		if _, ok := retainedColumns[column.Name.Name.L]; ok {
			columns = append(columns, column.Name.Name.O)
		}
	}
	return columns, nil
}

func parseCreateTableForProjection(originSQL string) (*ast.CreateTableStmt, error) {
	stmt, err := parser.New().ParseOneStmt(originSQL, "", "")
	if err != nil {
		return nil, errors.Annotate(err, "failed to parse CREATE TABLE for column projection")
	}
	createTable, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.Errorf("expected CREATE TABLE for column projection, got %T", stmt)
	}
	return createTable, nil
}

func retainedSchemaColumnSet(createTable *ast.CreateTableStmt, selectedColumns []string) (map[string]struct{}, error) {
	retainedColumns := makeColumnSet(selectedColumns)
	definedColumns := make(map[string]struct{}, len(createTable.Cols))
	generatedColumns := make(map[string]*ast.ColumnDef)
	for _, column := range createTable.Cols {
		definedColumns[column.Name.Name.L] = struct{}{}
		if generatedColumnOption(column) != nil {
			generatedColumns[column.Name.Name.L] = column
		}
	}
	for selectedColumn := range retainedColumns {
		if _, ok := definedColumns[selectedColumn]; !ok {
			return nil, errors.Errorf("selected column `%s` is missing from CREATE TABLE", selectedColumn)
		}
	}
	retainGeneratedColumns(retainedColumns, generatedColumns)
	return retainedColumns, nil
}

func hasProjectedTable(projections map[tableName]columnProjection) bool {
	for _, projection := range projections {
		if projection.projected {
			return true
		}
	}
	return false
}

func makeColumnSet(columns []string) map[string]struct{} {
	set := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		set[strings.ToLower(column)] = struct{}{}
	}
	return set
}

func generatedColumnOption(column *ast.ColumnDef) *ast.ColumnOption {
	for _, option := range column.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			return option
		}
	}
	return nil
}

func retainGeneratedColumns(retained map[string]struct{}, generated map[string]*ast.ColumnDef) {
	changed := true
	for changed {
		changed = false
		for name, column := range generated {
			if _, ok := retained[name]; ok {
				continue
			}
			if expressionColumnsRetained(generatedColumnOption(column).Expr, retained) {
				retained[name] = struct{}{}
				changed = true
			}
		}
	}
}

func projectColumnOptions(
	column *ast.ColumnDef,
	retained map[string]struct{},
	database string,
	table string,
	projections map[tableName]columnProjection,
) ([]*ast.ColumnOption, bool, error) {
	options := make([]*ast.ColumnOption, 0, len(column.Options))
	changed := false
	for _, option := range column.Options {
		switch option.Tp {
		case ast.ColumnOptionCheck:
			if !expressionColumnsRetained(option.Expr, retained) {
				changed = true
				continue
			}
		case ast.ColumnOptionReference:
			if err := validateReferenceColumns(option.Refer, database, table, retained, projections); err != nil {
				return nil, false, err
			}
		case ast.ColumnOptionDefaultValue, ast.ColumnOptionOnUpdate:
			if !expressionColumnsRetained(option.Expr, retained) {
				return nil, false, errors.Errorf(
					"column `%s` expression references a removed column",
					column.Name.Name.O,
				)
			}
		}
		options = append(options, option)
	}
	return options, changed, nil
}

func constraintColumnsRetained(constraint *ast.Constraint, retained map[string]struct{}) bool {
	for _, key := range constraint.Keys {
		if key.Column != nil {
			if _, ok := retained[key.Column.Name.L]; !ok {
				return false
			}
		}
		if !expressionColumnsRetained(key.Expr, retained) {
			return false
		}
	}
	if !expressionColumnsRetained(constraint.Expr, retained) {
		return false
	}
	return constraint.Option == nil || expressionColumnsRetained(constraint.Option.Condition, retained)
}

func validateAutomaticIDColumns(columns []*ast.ColumnDef, constraints []*ast.Constraint) error {
	indexedColumns := make(map[string]struct{})
	for _, constraint := range constraints {
		if len(constraint.Keys) == 0 || constraint.Keys[0].Column == nil {
			continue
		}
		indexedColumns[constraint.Keys[0].Column.Name.L] = struct{}{}
	}

	for _, column := range columns {
		requiresKey := false
		hasInlineKey := false
		for _, option := range column.Options {
			switch option.Tp {
			case ast.ColumnOptionAutoIncrement, ast.ColumnOptionAutoRandom:
				requiresKey = true
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				hasInlineKey = true
			}
		}
		if !requiresKey || hasInlineKey {
			continue
		}
		if _, ok := indexedColumns[column.Name.Name.L]; !ok {
			return errors.Errorf("column `%s` loses the index required by its automatic ID option", column.Name.Name.O)
		}
	}
	return nil
}

func validateReferenceColumns(
	reference *ast.ReferenceDef,
	database string,
	table string,
	retained map[string]struct{},
	projections map[tableName]columnProjection,
) error {
	if reference == nil || reference.Table == nil {
		return nil
	}

	referenceDatabase := reference.Table.Schema.O
	if referenceDatabase == "" {
		referenceDatabase = database
	}
	referenceTable := reference.Table.Name.O
	if strings.EqualFold(referenceDatabase, database) && strings.EqualFold(referenceTable, table) {
		for _, key := range reference.IndexPartSpecifications {
			if key.Column != nil {
				if _, ok := retained[key.Column.Name.L]; !ok {
					return removedReferenceColumnError(referenceDatabase, referenceTable, key.Column.Name.O)
				}
			}
		}
		return nil
	}

	target, ok := projections[tableName{db: referenceDatabase, table: referenceTable}]
	if !ok || !target.projected {
		return nil
	}
	targetColumns := makeColumnSet(target.schemaColumns)
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
	missing := missingPartitionColumns(&partition.PartitionMethod, retained)
	if partition.Sub != nil {
		missing = append(missing, missingPartitionColumns(partition.Sub, retained)...)
	}
	if len(missing) == 0 {
		return nil
	}
	slices.Sort(missing)
	return errors.Errorf("partition definition references removed column `%s`", missing[0])
}

func missingPartitionColumns(method *ast.PartitionMethod, retained map[string]struct{}) []string {
	missing := missingExpressionColumns(method.Expr, retained)
	for _, column := range method.ColumnNames {
		if _, ok := retained[column.Name.L]; !ok {
			missing = append(missing, column.Name.O)
		}
	}
	return missing
}

func validateTTLColumns(options []*ast.TableOption, retained map[string]struct{}) error {
	for _, option := range options {
		if option.Tp != ast.TableOptionTTL || option.ColumnName == nil {
			continue
		}
		if _, ok := retained[option.ColumnName.Name.L]; !ok {
			return errors.Errorf("TTL definition references removed column `%s`", option.ColumnName.Name.O)
		}
	}
	return nil
}

func expressionColumnsRetained(expr ast.ExprNode, retained map[string]struct{}) bool {
	return len(missingExpressionColumns(expr, retained)) == 0
}

func missingExpressionColumns(expr ast.ExprNode, retained map[string]struct{}) []string {
	if expr == nil {
		return nil
	}
	collector := &columnNameCollector{columns: make(map[string]string)}
	expr.Accept(collector)
	missing := make([]string, 0)
	for lowerName, originalName := range collector.columns {
		if _, ok := retained[lowerName]; !ok {
			missing = append(missing, originalName)
		}
	}
	return missing
}

type columnNameCollector struct {
	columns map[string]string
}

func (c *columnNameCollector) Enter(node ast.Node) (ast.Node, bool) {
	column, ok := node.(*ast.ColumnNameExpr)
	if ok {
		c.columns[column.Name.Name.L] = column.Name.Name.O
	}
	return node, false
}

func (*columnNameCollector) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}
