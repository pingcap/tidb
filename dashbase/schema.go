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

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type ColumnDefinition struct {
	name     string
	dataType string
}

type TableDefinition struct {
	name    string
	columns []*ColumnDefinition
}

var tableDefinitions map[string]*TableDefinition

func init() {
	clearTableDefinitions()
}

func clearTableDefinitions() {
	tableDefinitions = make(map[string]*TableDefinition)
}

// LoadSchemaFromFile loads Dashbase schema definitions from a file.
func LoadSchemaFromFile(path string) error {
	schemas := make(map[string]interface{})
	_, err := toml.DecodeFile(path, &schemas)
	if err != nil {
		return errors.Trace(err)
	}

	tableDefinitions = make(map[string]*TableDefinition)

	for tableName, _tableColumns := range schemas {
		var table TableDefinition
		table.name = tableName
		table.columns = make([]*ColumnDefinition, 0)
		tableColumns := _tableColumns.(map[string]interface{})
		for columnName, _columnType := range tableColumns {
			columnType := _columnType.(string)
			var column ColumnDefinition
			column.name = columnName
			column.dataType = columnType
			table.columns = append(table.columns, &column)
		}
		lowerTableName := strings.ToLower(tableName)
		_, existTable := tableDefinitions[lowerTableName]
		if existTable {
			return fmt.Errorf("Duplicate table schema definition %s", lowerTableName)
		}
		tableDefinitions[lowerTableName] = &table
	}

	return nil
}

// GetTableSchema gets defined schema for a given table.
func GetTableSchema(tableName string) *TableDefinition {
	table, ok := tableDefinitions[strings.ToLower(tableName)]
	if !ok {
		return nil
	}
	return table
}
