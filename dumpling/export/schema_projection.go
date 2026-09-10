// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

type projectedTableSchema struct {
	createTable     *ast.CreateTableStmt
	retainedColumns map[string]struct{}
}

type projectedTableSchemas map[tableName]*projectedTableSchema

func buildProjectedTableSchema(
	p *parser.Parser,
	originSQL string,
	selectedColumns []string,
	projected bool,
) (*projectedTableSchema, error) {
	stmt, err := p.ParseOneStmt(originSQL, "", "")
	if err != nil {
		return nil, errors.Annotate(err, "failed to parse CREATE TABLE for column projection")
	}
	createTable, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.Errorf("expected CREATE TABLE for column projection, got %T", stmt)
	}
	partitionColumns, unsupportedPartition := collectPartitionColumns(createTable)

	retainedColumns := make(map[string]struct{}, len(selectedColumns))
	for _, selectedColumn := range selectedColumns {
		retainedColumns[strings.ToLower(selectedColumn)] = struct{}{}
	}
	// A generated column can only depend on generated columns defined before it.
	for _, column := range createTable.Cols {
		for _, option := range column.Options {
			if option.Tp == ast.ColumnOptionGenerated && usesOnlyRetainedColumns(option.Expr, retainedColumns) {
				retainedColumns[column.Name.Name.L] = struct{}{}
				break
			}
		}
	}

	columns := make([]*ast.ColumnDef, 0, len(createTable.Cols))
	for _, column := range createTable.Cols {
		if _, ok := retainedColumns[column.Name.Name.L]; !ok {
			continue
		}
		options, err := filterColumnOptions(column, retainedColumns)
		if err != nil {
			return nil, err
		}
		column.Options = options
		columns = append(columns, column)
	}
	createTable.Cols = columns

	constraints := make([]*ast.Constraint, 0, len(createTable.Constraints))
	for _, constraint := range createTable.Constraints {
		if filterTableConstraint(constraint, retainedColumns) {
			constraints = append(constraints, constraint)
		}
	}
	createTable.Constraints = constraints

	if projected && unsupportedPartition {
		return nil, errors.New("PARTITION BY KEY() is not supported with column filtering")
	}
	if projected {
		if err := validateAutoRandomColumns(createTable); err != nil {
			return nil, err
		}
	}
	if !allColumnsRetained(partitionColumns, retainedColumns) {
		return nil, errors.New("partition definition references a removed column")
	}
	if err := validateTTLColumns(createTable.Options, retainedColumns); err != nil {
		return nil, err
	}
	return &projectedTableSchema{
		createTable:     createTable,
		retainedColumns: retainedColumns,
	}, nil
}

func restoreProjectedSchema(createTable *ast.CreateTableStmt) (string, error) {
	var buffer bytes.Buffer
	err := createTable.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment,
		In:    &buffer,
	})
	if err != nil {
		return "", errors.Annotate(err, "failed to restore projected CREATE TABLE")
	}
	return buffer.String(), nil
}

func filterColumnOptions(
	column *ast.ColumnDef,
	retained map[string]struct{},
) ([]*ast.ColumnOption, error) {
	options := make([]*ast.ColumnOption, 0, len(column.Options))
	for _, option := range column.Options {
		switch option.Tp {
		case ast.ColumnOptionCheck:
			// CHECK constraints can be removed as a whole; expressions on retained columns cannot.
			if !usesOnlyRetainedColumns(option.Expr, retained) {
				continue
			}
		case ast.ColumnOptionDefaultValue, ast.ColumnOptionOnUpdate:
			if !usesOnlyRetainedColumns(option.Expr, retained) {
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
) bool {
	for _, key := range constraint.Keys {
		if key.Column != nil {
			if _, ok := retained[key.Column.Name.L]; !ok {
				return false
			}
		}
		if !usesOnlyRetainedColumns(key.Expr, retained) {
			return false
		}
	}
	if !usesOnlyRetainedColumns(constraint.Expr, retained) {
		return false
	}
	if constraint.Option != nil && !usesOnlyRetainedColumns(constraint.Option.Condition, retained) {
		return false
	}
	return true
}

func validateForeignKeyParents(
	childDB string,
	child *projectedTableSchema,
	schemas projectedTableSchemas,
) error {
	for _, column := range child.createTable.Cols {
		for _, option := range column.Options {
			if option.Tp != ast.ColumnOptionReference {
				continue
			}
			// MySQL 9.7 supports inline foreign keys as column-level REFERENCES options.
			if err := validateForeignKeyParent(option.Refer, childDB, schemas); err != nil {
				return err
			}
		}
	}
	for _, constraint := range child.createTable.Constraints {
		if err := validateForeignKeyParent(constraint.Refer, childDB, schemas); err != nil {
			return err
		}
	}
	return nil
}

func validateForeignKeyParent(reference *ast.ReferenceDef, childDB string, schemas projectedTableSchemas) error {
	if reference == nil || reference.Table == nil {
		return nil
	}

	parentTable := reference.Table.Name.O
	parentDB := reference.Table.Schema.O
	if parentDB == "" {
		parentDB = childDB
	}

	parent, ok, err := schemas.lookup(parentDB, parentTable)
	if err != nil {
		return err
	}
	if !ok {
		// The referenced table is outside this dump and is not rewritten here.
		return nil
	}
	parentColumns := make([]ast.CIStr, 0, len(reference.IndexPartSpecifications))
	// Foreign key references from SHOW CREATE TABLE contain only column index parts.
	for _, key := range reference.IndexPartSpecifications {
		if _, ok := parent.retainedColumns[key.Column.Name.L]; !ok {
			return errors.Errorf(
				"foreign key references removed column `%s`.`%s`.`%s`",
				escapeString(parentDB),
				escapeString(parentTable),
				escapeString(key.Column.Name.O),
			)
		}
		parentColumns = append(parentColumns, key.Column.Name)
	}
	if !hasParentIndex(parent, parentColumns) {
		return errors.Errorf(
			"foreign key referenced columns are not indexed in table `%s`.`%s`",
			escapeString(parentDB),
			escapeString(parentTable),
		)
	}
	return nil
}

func (schemas projectedTableSchemas) lookup(database, table string) (*projectedTableSchema, bool, error) {
	if schema, ok := schemas[tableName{db: database, table: table}]; ok {
		return schema, true, nil
	}

	var matched *projectedTableSchema
	for name, schema := range schemas {
		if !strings.EqualFold(name.db, database) || !strings.EqualFold(name.table, table) {
			continue
		}
		if matched != nil {
			return nil, false, errors.Errorf(
				"foreign key reference `%s`.`%s` is ambiguous under case-insensitive matching",
				escapeString(database),
				escapeString(table),
			)
		}
		matched = schema
	}
	return matched, matched != nil, nil
}

func hasParentIndex(parent *projectedTableSchema, parentColumns []ast.CIStr) bool {
	for _, column := range parent.createTable.Cols {
		if len(parentColumns) != 1 || column.Name.Name.L != parentColumns[0].L {
			continue
		}
		for _, option := range column.Options {
			if option.Tp == ast.ColumnOptionPrimaryKey || option.Tp == ast.ColumnOptionUniqKey {
				return true
			}
		}
	}

	for _, constraint := range parent.createTable.Constraints {
		switch constraint.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
			ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			if indexCoversColumns(constraint.Keys, parentColumns) {
				return true
			}
		}
	}
	return false
}

func indexCoversColumns(indexColumns []*ast.IndexPartSpecification, columns []ast.CIStr) bool {
	if len(indexColumns) < len(columns) {
		return false
	}
	for i, column := range columns {
		indexColumn := indexColumns[i]
		if indexColumn.Column == nil || indexColumn.Length > 0 || indexColumn.Column.Name.L != column.L {
			return false
		}
	}
	return true
}

func validateAutoRandomColumns(createTable *ast.CreateTableStmt) error {
	for _, column := range createTable.Cols {
		hasAutoRandom := false
		for _, option := range column.Options {
			hasAutoRandom = hasAutoRandom || option.Tp == ast.ColumnOptionAutoRandom
		}
		if !hasAutoRandom || hasClusteredPrimaryKey(createTable, column.Name.Name.L) {
			continue
		}
		return errors.New("auto_random is only supported on the tables with clustered primary key")
	}
	return nil
}

func hasClusteredPrimaryKey(createTable *ast.CreateTableStmt, columnName string) bool {
	for _, column := range createTable.Cols {
		if column.Name.Name.L != columnName {
			continue
		}
		for _, option := range column.Options {
			if option.Tp == ast.ColumnOptionPrimaryKey {
				return option.PrimaryKeyTp != ast.PrimaryKeyTypeNonClustered
			}
		}
	}
	for _, constraint := range createTable.Constraints {
		if constraint.Tp != ast.ConstraintPrimaryKey {
			continue
		}
		for _, key := range constraint.Keys {
			if key.Column != nil && key.Column.Name.L == columnName {
				return constraint.Option == nil || constraint.Option.PrimaryKeyTp != ast.PrimaryKeyTypeNonClustered
			}
		}
	}
	return false
}

func collectPartitionColumns(createTable *ast.CreateTableStmt) (map[string]struct{}, bool) {
	columns := make(map[string]struct{})
	if createTable.Partition == nil {
		return columns, false
	}

	methods := []*ast.PartitionMethod{&createTable.Partition.PartitionMethod}
	if createTable.Partition.Sub != nil {
		methods = append(methods, createTable.Partition.Sub)
	}
	for _, method := range methods {
		for _, column := range method.ColumnNames {
			columns[column.Name.L] = struct{}{}
		}
		if method.Expr != nil {
			for _, column := range ddl.FindColumnNamesInExpr(method.Expr) {
				columns[column.Name.L] = struct{}{}
			}
		}
		if method.Tp != ast.PartitionTypeKey || len(method.ColumnNames) != 0 {
			continue
		}
		return columns, true
	}
	return columns, false
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

func usesOnlyRetainedColumns(expr ast.ExprNode, retainedColumns map[string]struct{}) bool {
	if expr == nil {
		return true
	}
	for _, column := range ddl.FindColumnNamesInExpr(expr) {
		if _, ok := retainedColumns[column.Name.L]; !ok {
			return false
		}
	}
	return true
}

func allColumnsRetained(columns, retained map[string]struct{}) bool {
	for column := range columns {
		if _, ok := retained[column]; !ok {
			return false
		}
	}
	return true
}
