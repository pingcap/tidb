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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
)

func Execute(stmt ast.Node) (bool, ast.RecordSet, error) {
	switch node := stmt.(type) {
	case *ast.InsertStmt:
		return executeInsert(node)
	default:
		return false, nil, nil
	}
}

type insertRecordSet struct {
	insertedRows int
}

func (rs insertRecordSet) Next() (*ast.Row, error) {
	return nil, nil
}

func (rs insertRecordSet) Fields() ([]*ast.ResultField, error) {
	return nil, nil
}

func (rs insertRecordSet) Close() error {
	return nil
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

	return true, insertRecordSet{insertedRows: len(rows)}, nil
}
