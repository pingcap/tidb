// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type projectedTableSchema struct {
	createTable     *ast.CreateTableStmt
	retainedColumns map[string]struct{}
	tableInfo       *model.TableInfo
}

type projectedTableSchemas map[tableName]*projectedTableSchema

func buildProjectedTableSchema(
	p *parser.Parser,
	originSQL string,
	selectedColumns []string,
) (*projectedTableSchema, error) {
	stmt, err := p.ParseOneStmt(originSQL, "", "")
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

	if err := validatePartitionColumns(createTable.Partition, retainedColumns); err != nil {
		return nil, err
	}
	if err := validateTTLColumns(createTable.Options, retainedColumns); err != nil {
		return nil, err
	}
	return &projectedTableSchema{
		createTable:     createTable,
		retainedColumns: retainedColumns,
	}, nil
}

func (s *projectedTableSchema) getTableInfo() (*model.TableInfo, error) {
	if s.tableInfo != nil {
		return s.tableInfo, nil
	}

	createTable := *s.createTable
	createTable.Constraints = make([]*ast.Constraint, 0, len(s.createTable.Constraints))
	// Columnar indexes cannot support foreign key lookups.
	for _, constraint := range s.createTable.Constraints {
		switch constraint.Tp {
		case ast.ConstraintColumnar, ast.ConstraintFulltext:
			continue
		default:
			createTable.Constraints = append(createTable.Constraints, constraint)
		}
	}
	tableInfo, err := ddl.BuildTableInfoWithStmt(
		metabuild.NewContext(),
		&createTable,
		mysql.DefaultCharset,
		"",
		nil,
	)
	if err != nil {
		return nil, err
	}
	s.tableInfo = tableInfo
	return tableInfo, nil
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
			if !allReferencedColumnsCovered(option.Expr, retained) {
				continue
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
) bool {
	for _, key := range constraint.Keys {
		if key.Column != nil {
			if _, ok := retained[key.Column.Name.L]; !ok {
				return false
			}
		}
		if !allReferencedColumnsCovered(key.Expr, retained) {
			return false
		}
	}
	if !allReferencedColumnsCovered(constraint.Expr, retained) {
		return false
	}
	if constraint.Option != nil && !allReferencedColumnsCovered(constraint.Option.Condition, retained) {
		return false
	}
	return true
}

func validateForeignKeys(
	database string,
	schema *projectedTableSchema,
	schemas projectedTableSchemas,
) error {
	for _, column := range schema.createTable.Cols {
		for _, option := range column.Options {
			if option.Tp != ast.ColumnOptionReference {
				continue
			}
			// MySQL 9.7 supports inline foreign keys as column-level REFERENCES options.
			if err := validateForeignKeyReference(option.Refer, database, schemas); err != nil {
				return err
			}
		}
	}
	for _, constraint := range schema.createTable.Constraints {
		if err := validateForeignKeyReference(constraint.Refer, database, schemas); err != nil {
			return err
		}
	}
	return nil
}

func validateForeignKeyReference(reference *ast.ReferenceDef, database string, schemas projectedTableSchemas) error {
	if reference == nil || reference.Table == nil {
		return nil
	}

	referenceTable := reference.Table.Name.O
	referenceDatabase := reference.Table.Schema.O
	if referenceDatabase == "" {
		referenceDatabase = database
	}

	targetSchema, ok, err := schemas.lookup(referenceDatabase, referenceTable)
	if err != nil {
		return err
	}
	if !ok {
		// The referenced table is outside this dump and is not rewritten here.
		return nil
	}
	referencedColumns := make([]ast.CIStr, 0, len(reference.IndexPartSpecifications))
	// Foreign key references from SHOW CREATE TABLE contain only column index parts.
	for _, key := range reference.IndexPartSpecifications {
		if _, ok := targetSchema.retainedColumns[key.Column.Name.L]; !ok {
			return removedReferenceColumnError(referenceDatabase, referenceTable, key.Column.Name.O)
		}
		referencedColumns = append(referencedColumns, key.Column.Name)
	}
	targetTableInfo, err := targetSchema.getTableInfo()
	if err != nil {
		return err
	}
	if !hasForeignKeyIndex(targetTableInfo, referencedColumns) {
		return errors.Errorf(
			"foreign key referenced columns are not indexed in table `%s`.`%s`",
			escapeString(referenceDatabase),
			escapeString(referenceTable),
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

func hasForeignKeyIndex(tableInfo *model.TableInfo, referencedColumns []ast.CIStr) bool {
	if len(referencedColumns) == 1 {
		column := model.FindColumnInfo(tableInfo.Columns, referencedColumns[0].L)
		if column != nil && tableInfo.PKIsHandle && mysql.HasPriKeyFlag(column.GetFlag()) {
			return true
		}
	}
	return model.FindIndexByColumnsForForeignKey(tableInfo, tableInfo.Indices, referencedColumns...) != nil
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
