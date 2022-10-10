// Copyright 2022 PingCAP, Inc.
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

package connection

import (
	"fmt"
	"regexp"

	"github.com/juju/errors"
)

const (
	indexColumnName = "Key_name"
)

var (
	binlogSyncTablePattern = regexp.MustCompile(`^t[0-9]+$`)
	columnTypePattern      = regexp.MustCompile(`^([a-z]+)\(?\d*?\)?$`)
)

// FetchDatabases database list
func (c *Connection) FetchDatabases() ([]string, error) {
	databaseSlice := []string{}

	databases, err := c.db.Query(showDatabasesSQL)
	if err != nil {
		return []string{}, errors.Trace(err)
	}
	if databases.Err() != nil {
		return []string{}, errors.Trace(databases.Err())
	}
	for databases.Next() {
		var dbname string
		if err := databases.Scan(&dbname); err != nil {
			return []string{}, errors.Trace(err)
		}
		databaseSlice = append(databaseSlice, dbname)
	}
	return databaseSlice, nil
}

// FetchTables table list
func (c *Connection) FetchTables(db string) ([]string, error) {
	tableSlice := []string{}

	tables, err := c.db.Query(schemaSQL)
	if err != nil {
		return []string{}, errors.Trace(err)
	}
	if tables.Err() != nil {
		return []string{}, errors.Trace(tables.Err())
	}
	// fetch tables need to be described
	for tables.Next() {
		var schemaName, tableName string
		if err := tables.Scan(&schemaName, &tableName, new(interface{})); err != nil {
			return []string{}, errors.Trace(err)
		}
		if schemaName == db {
			tableSlice = append(tableSlice, tableName)
		}
	}
	return tableSlice, nil
}

// FetchSchema get schema of given database from database
func (c *Connection) FetchSchema(db string) ([][6]string, error) {
	var (
		schema     [][6]string
		tablesInDB [][3]string
	)
	tables, err := c.db.Query(schemaSQL)
	if err != nil {
		return schema, errors.Trace(err)
	}
	if tables.Err() != nil {
		return schema, errors.Trace(tables.Err())
	}

	// fetch tables need to be described
	for tables.Next() {
		var schemaName, tableName, tableType string
		if err = tables.Scan(&schemaName, &tableName, &tableType); err != nil {
			return nil, errors.Trace(err)
		}
		// do not generate SQL about sync table
		// because this can not be reproduced from logs
		if schemaName == db && !ifBinlogSyncTable(tableName) {
			tablesInDB = append(tablesInDB, [3]string{schemaName, tableName, tableType})
		}
	}

	// desc tables
	for _, table := range tablesInDB {
		var (
			schemaName = table[0]
			tableName  = table[1]
			tableType  = table[2]
		)
		columns, err := c.FetchColumns(schemaName, tableName)
		if err != nil {
			return [][6]string{}, errors.Trace(err)
		}
		for _, column := range columns {
			schema = append(schema, [6]string{schemaName, tableName, tableType, column[0], column[1], column[2]})
		}
	}
	return schema, nil
}

// FetchColumns get columns for given table
func (c *Connection) FetchColumns(db, table string) ([][3]string, error) {
	var columns [][3]string
	res, err := c.db.Query(fmt.Sprintf(tableSQL, db, table))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res.Err() != nil {
		return nil, errors.Trace(res.Err())
	}
	for res.Next() {
		var columnName, columnType, nullValue, index string
		var defaultValue, extra interface{}
		if err = res.Scan(&columnName, &columnType, &nullValue, &index, &defaultValue, &extra); err != nil {
			return nil, errors.Trace(err)
		}

		columnType = columnTypePattern.FindStringSubmatch(columnType)[1]
		// columns = append(columns, [3]string{columnName, columnType, nullValue})
		columns = append(columns, [3]string{columnName, columnType, nullValue})
	}
	return columns, nil
}

// FetchIndexes get indexes for given table
func (c *Connection) FetchIndexes(db, table string) ([]string, error) {
	var indexes []string
	res, err := c.db.Query(fmt.Sprintf(indexSQL, db, table))
	if err != nil {
		return []string{}, errors.Trace(err)
	}
	if res.Err() != nil {
		return []string{}, errors.Trace(res.Err())
	}

	columnTypes, err := res.ColumnTypes()
	if err != nil {
		return indexes, errors.Trace(err)
	}
	for res.Next() {
		var (
			keyname       string
			rowResultSets []interface{}
		)

		for range columnTypes {
			rowResultSets = append(rowResultSets, new(interface{}))
		}
		if err = res.Scan(rowResultSets...); err != nil {
			return []string{}, errors.Trace(err)
		}

		for index, resultItem := range rowResultSets {
			if columnTypes[index].Name() != indexColumnName {
				continue
			}
			r := *resultItem.(*interface{})
			if r != nil {
				bytes := r.([]byte)
				keyname = string(bytes)
			}
		}

		if keyname != "" {
			indexes = append(indexes, keyname)
		}
	}
	return indexes, nil
}

func ifBinlogSyncTable(t string) bool {
	return binlogSyncTablePattern.MatchString(t)
}
