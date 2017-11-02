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

package dashbase

import (
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func Execute(stmt ast.Node) (bool, ast.RecordSet, error) {
	switch node := stmt.(type) {
	case *ast.InsertStmt:
		return executeInsert(node)
	case *ast.SelectStmt:
		return executeSelect(node)
	default:
		return false, nil, nil
	}
}

func executeInsert(node *ast.InsertStmt) (bool, ast.RecordSet, error) {
	ts, ok := node.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return false, nil, nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return false, nil, nil
	}
	tableName := tn.Name.L
	tableSchema := GetTableSchema(tableName)
	if tableSchema == nil {
		return false, nil, nil
	}

	if node.IsReplace {
		return true, nil, errors.Errorf("REPLACE is not supported for Dashbase table")
	}
	if len(node.OnDuplicate) > 0 {
		return true, nil, errors.Errorf("ON DUPLICATE is not supported for Dashbase table")
	}

	columnsByName := make(map[string]*ColumnDefinition)
	for _, col := range tableSchema.columns {
		columnsByName[strings.ToLower(col.name)] = col
	}

	// Ensure column exists: check INSERT ... SET ...
	for _, assignment := range node.Setlist {
		if _, exists := columnsByName[assignment.Column.Name.L]; !exists {
			return true, nil, fmt.Errorf("unknown column %s", assignment.Column.Name.O)
		}
	}
	// Ensure column exists: check INSERT ... (...) VALUES (...)
	if len(node.Columns) > 0 {
		if len(node.Columns) != len(node.Lists) {
			return true, nil, fmt.Errorf("column count doesn't match value count")
		}
		for _, column := range node.Columns {
			if _, exists := columnsByName[column.Name.L]; !exists {
				return true, nil, fmt.Errorf("unknown column %s", column.Name.O)
			}
		}
	}

	var rows [][]*expression.Constant
	var columns []*ColumnDefinition

	if len(node.Setlist) > 0 {
		// Assign values based on INSERT ... SET ...
		row := make([]*expression.Constant, 0)
		for _, assignment := range node.Setlist {
			if val, ok := assignment.Expr.(*ast.ValueExpr); ok {
				row = append(row, &expression.Constant{
					Value:   val.Datum,
					RetType: &val.Type,
				})
				columns = append(columns, columnsByName[assignment.Column.Name.L])
			} else {
				return true, nil, fmt.Errorf("Only support constants for Dashbase table")
			}
		}
		rows = append(rows, row)
	} else {
		// Assign values based on INSERT ... VALUES (....),(....)
		hasNamedColumns := true
		for _, insertRow := range node.Lists {
			maxListItems := len(insertRow)
			if len(node.Columns) == 0 {
				hasNamedColumns = false
				if len(tableSchema.columns) < maxListItems {
					maxListItems = len(tableSchema.columns)
				}
			}
			row := make([]*expression.Constant, maxListItems)
			for i, item := range insertRow {
				if i >= maxListItems {
					break
				}
				var columnName string
				if hasNamedColumns {
					columnName = node.Columns[i].Name.L
				} else {
					columnName = strings.ToLower(tableSchema.columns[i].name)
				}
				if val, ok := item.(*ast.ValueExpr); ok {
					row[i] = &expression.Constant{
						Value:   val.Datum,
						RetType: &val.Type,
					}
					columns = append(columns, columnsByName[columnName])
				} else {
					return true, nil, fmt.Errorf("Only support constants for Dashbase table")
				}
			}
			rows = append(rows, row)
		}
	}

	var columnEncoders []kafkaEncoder

	for _, column := range columns {
		encoder, err := getEncoder(column.dataType)
		if err != nil {
			return true, nil, errors.Trace(err)
		}
		columnEncoders = append(columnEncoders, encoder)
	}

	for _, row := range rows {
		encodedRow := make([]interface{}, len(columns))
		for idx, data := range row {
			encodedRow[idx] = columnEncoders[idx](data.Value)
		}

		message, err := AvroEncode(columns, encodedRow)
		if err != nil {
			return true, nil, errors.Trace(err)
		}
		PublishKafka(strings.ToLower(tableName), message)
	}

	return true, nil, nil
}

type selectResultSet struct {
	sql             string
	tableName       string
	fields          []*ColumnDefinition
	resultSetFields []*ast.ResultField // will be assigned after first Next()
	fetched         bool
	rows            []executor.Row
	cursor          int
}

func (e *selectResultSet) Next() (*ast.Row, error) {
	if !e.fetched {
		err := e.fetchAll()
		e.fetched = true
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return &ast.Row{Data: row}, nil
}

func (e *selectResultSet) Fields() ([]*ast.ResultField, error) {
	return e.resultSetFields, nil
}

func (e *selectResultSet) Close() error {
	return nil
}

func (e *selectResultSet) fetchAll() error {
	client := ApiClient{
		URL: config.GetGlobalConfig().Dashbase.APIURL,
	}

	result, err := client.Query(e.sql)
	if err != nil {
		return errors.Trace(err)
	}

	numberOfTargetColumns := len(e.fields)
	resultSetFields := make([]*ast.ResultField, 0)

	// selection columns
	for _, field := range e.fields {
		var fieldType *types.FieldType
		if field.dataType == TypeTime {
			fieldType = types.NewFieldType(mysql.TypeDatetime)
		} else {
			fieldType = types.NewFieldType(mysql.TypeString)
		}
		rf := ast.ResultField{
			ColumnAsName: model.NewCIStr(field.name),
			TableAsName:  model.NewCIStr(e.tableName),
			DBName:       model.NewCIStr(""),
			Table:        &model.TableInfo{Name: model.NewCIStr(e.tableName)},
			Column: &model.ColumnInfo{
				FieldType: *fieldType,
				Name:      model.NewCIStr(field.name),
			},
		}
		resultSetFields = append(resultSetFields, &rf)
	}

	// aggregation columns
	aggregations := make([]string, 0)
	for key := range result.Request.Aggregations {
		aggregations = append(aggregations, key)
		numberOfTargetColumns++
		rf := ast.ResultField{
			ColumnAsName: model.NewCIStr(key),
			TableAsName:  model.NewCIStr(e.tableName),
			DBName:       model.NewCIStr(""),
			Table:        &model.TableInfo{Name: model.NewCIStr(e.tableName)},
			Column: &model.ColumnInfo{
				FieldType: *types.NewFieldType(mysql.TypeDouble),
				Name:      model.NewCIStr(key),
			},
		}
		resultSetFields = append(resultSetFields, &rf)
	}

	e.resultSetFields = resultSetFields

	var iterateHitMax int // how many rows should we iterate
	if len(aggregations) > 0 {
		iterateHitMax = 1
	} else {
		iterateHitMax = len(result.Hits)
	}

	for i := 0; i < iterateHitMax; i++ {
		datums := make([]types.Datum, 0)

		// append field selections
		if i >= len(result.Hits) {
			for range e.fields {
				datums = append(datums, types.NewDatum(nil))
			}
		} else {
			hit := result.Hits[i]
			loweredKeyRow := make(map[string]string)
			for key, field := range hit.Payload.Fields {
				if len(field) > 0 {
					loweredKeyRow[strings.ToLower(key)] = field[0]
				}
			}
			for _, field := range e.fields {
				if field.dataType == TypeTime {
					datums = append(datums, types.NewDatum(types.Time{
						Time:     types.FromGoTime(time.Unix(hit.TimeInSeconds, 0)),
						Type:     mysql.TypeDatetime,
						TimeZone: time.Local,
					}))
				} else {
					data, ok := loweredKeyRow[strings.ToLower(field.name)]
					if !ok {
						datums = append(datums, types.NewDatum(nil))
					} else {
						datums = append(datums, types.NewDatum(data))
					}
				}
			}
		}

		// append aggregation selections
		for _, key := range aggregations {
			data, ok := result.Aggregations[key]
			if !ok {
				datums = append(datums, types.NewDatum(nil))
			} else {
				datums = append(datums, types.NewDatum(data.Value))
			}
		}
		e.rows = append(e.rows, datums)
	}

	return nil
}

func executeSelect(node *ast.SelectStmt) (bool, ast.RecordSet, error) {
	if node.From == nil {
		return false, nil, nil
	}
	ts, ok := node.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return false, nil, nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return false, nil, nil
	}
	tableName := tn.Name.L
	tableSchema := GetTableSchema(tableName)
	if tableSchema == nil {
		return false, nil, nil
	}

	columnsByName := make(map[string]*ColumnDefinition)
	for _, column := range tableSchema.columns {
		columnsByName[strings.ToLower(column.name)] = column
	}

	hasWildcard := false

	for _, field := range node.Fields.Fields {
		if field.WildCard != nil {
			hasWildcard = true
			break
		}
	}

	fields := make([]*ColumnDefinition, 0)

	// field selections
	if hasWildcard {
		for _, col := range tableSchema.columns {
			fields = append(fields, col)
		}
	} else {
		for _, field := range node.Fields.Fields {
			if field.WildCard != nil {
				continue
			}
			switch expr := field.Expr.(type) {
			case *ast.ColumnNameExpr:
				lowerColumnName := expr.Name.Name.L
				col, ok := columnsByName[lowerColumnName]
				if !ok {
					return true, nil, fmt.Errorf("Column %s not found", expr.Name.Name.O)
				}
				fields = append(fields, col)
			default:
				// ignore non-column-name ref
			}
		}
	}

	return true, &selectResultSet{
		sql:       node.Text(),
		tableName: tableName,
		fields:    fields,
	}, nil
}
