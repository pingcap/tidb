// Copyright 2024 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/set"
)

// InfoSchemaColumnsExtractor is the predicate extractor for information_schema.columns.
type InfoSchemaColumnsExtractor struct {
	InfoSchemaBaseExtractor
	predColNames       set.StringSet
	predColNamesInited bool
}

// NewInfoSchemaColumnsExtractor creates a new InfoSchemaColumnsExtractor.
func NewInfoSchemaColumnsExtractor() *InfoSchemaColumnsExtractor {
	e := &InfoSchemaColumnsExtractor{}
	e.extractableColumns = extractableCols{
		schema:     TableSchema,
		table:      TableName,
		columnName: ColumnName,
	}
	e.colNames = []string{TableSchema, TableName, ColumnName}
	return e
}

// ListTables lists related tables for given schema from predicate.
// If no table found in predicate, it return all tables.
// TODO(tangenta): remove this after streaming interface is supported.
func (e *InfoSchemaColumnsExtractor) ListTables(
	ctx context.Context,
	s ast.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	ec := e.extractableColumns
	base := e.GetBase()
	schemas := []ast.CIStr{s}
	var tableNames []ast.CIStr
	if ec.table != "" {
		tableNames = e.getSchemaObjectNames(ec.table)
	}
	if len(tableNames) > 0 {
		tableNames = filterSchemaObjectByRegexp(base, ec.table, tableNames, extractStrCIStr)
		_, tblInfos, err := findTableAndSchemaByName(ctx, is, schemas, tableNames)
		return tblInfos, err
	}
	_, tblInfos, err := listTablesForEachSchema(ctx, base, is, schemas)
	return tblInfos, err
}

// ListColumns lists unhidden columns and corresponding ordinal positions for given table from predicates.
// If no column found in predicate, it return all visible columns.
func (e *InfoSchemaColumnsExtractor) ListColumns(
	tbl *model.TableInfo,
) ([]*model.ColumnInfo, []int) {
	ec := e.extractableColumns
	if !e.predColNamesInited {
		e.predColNames = set.NewStringSet()
		colNames := e.getSchemaObjectNames(ec.columnName)
		for _, name := range colNames {
			e.predColNames.Insert(name.L)
		}
		e.predColNamesInited = true
	}
	predCol := e.predColNames
	regexp := e.GetBase().colsRegexp[ec.columnName]

	columns := make([]*model.ColumnInfo, 0, len(predCol))
	ordinalPos := make([]int, 0, len(predCol))
	ord := 0
ForLoop:
	for _, column := range tbl.Columns {
		if column.Hidden {
			continue
		}
		ord++
		if len(predCol) > 0 && !predCol.Exist(column.Name.L) {
			continue
		}
		for _, re := range regexp {
			if !re.MatchString(column.Name.L) {
				continue ForLoop
			}
		}
		columns = append(columns, column)
		ordinalPos = append(ordinalPos, ord)
	}

	return columns, ordinalPos
}

// InfoSchemaTiDBIndexUsageExtractor is the predicate extractor for information_schema.tidb_index_usage.
type InfoSchemaTiDBIndexUsageExtractor struct {
	InfoSchemaBaseExtractor
	predIdxNames       set.StringSet
	predIdxNamesInited bool
}

// IndexUsageIndexInfo is the necessary index info for information_schema.tidb_index_usage. It only includes the index name
// and ID in lower case.
type IndexUsageIndexInfo struct {
	Name string
	ID   int64
}

// NewInfoSchemaTiDBIndexUsageExtractor creates a new InfoSchemaTiDBIndexUsageExtractor.
func NewInfoSchemaTiDBIndexUsageExtractor() *InfoSchemaTiDBIndexUsageExtractor {
	e := &InfoSchemaTiDBIndexUsageExtractor{}
	e.extractableColumns = extractableCols{
		schema:    TableSchema,
		table:     TableName,
		indexName: IndexName,
	}
	e.colNames = []string{TableSchema, TableName, IndexName}
	return e
}

// ListIndexes lists related indexes for given table from predicate.
// If no index found in predicate, it return all indexes.
func (e *InfoSchemaTiDBIndexUsageExtractor) ListIndexes(
	tbl *model.TableInfo,
) []IndexUsageIndexInfo {
	ec := e.extractableColumns
	if !e.predIdxNamesInited {
		e.predIdxNames = set.NewStringSet()
		colNames := e.getSchemaObjectNames(ec.indexName)
		for _, name := range colNames {
			e.predIdxNames.Insert(name.L)
		}
		e.predIdxNamesInited = true
	}
	predCol := e.predIdxNames
	regexp := e.GetBase().colsRegexp[ec.indexName]

	indexes := make([]IndexUsageIndexInfo, 0, len(tbl.Indices))
	// Append the int primary key. The clustered index is already included in the `tbl.Indices`, but the int primary key is not.
	if tbl.PKIsHandle {
		indexes = append(indexes, IndexUsageIndexInfo{Name: primaryKeyName, ID: 0})
	}
	for _, index := range tbl.Indices {
		indexes = append(indexes, IndexUsageIndexInfo{Name: index.Name.L, ID: index.ID})
	}
	if len(predCol) == 0 && len(regexp) == 0 {
		return indexes
	}

	retIndexes := make([]IndexUsageIndexInfo, 0, len(indexes))
ForLoop:
	for _, index := range indexes {
		if len(predCol) > 0 && !predCol.Exist(index.Name) {
			continue
		}
		for _, re := range regexp {
			if !re.MatchString(index.Name) {
				continue ForLoop
			}
		}
		retIndexes = append(retIndexes, IndexUsageIndexInfo{Name: index.Name, ID: index.ID})
	}

	return retIndexes
}
