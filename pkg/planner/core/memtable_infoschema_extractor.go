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
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/set"
	"golang.org/x/exp/maps"
)

const (
	_tableSchema      = "table_schema"
	_tableName        = "table_name"
	_tidbTableID      = "tidb_table_id"
	_partitionName    = "partition_name"
	_tidbPartitionID  = "tidb_partition_id"
	_indexName        = "index_name"
	_schemaName       = "schema_name"
	_constraintSchema = "constraint_schema"
	_constraintName   = "constraint_name"
	_columnName       = "column_name"
)

var extractableColumns = map[string][]string{
	// See infoschema.tablesCols for full columns.
	// Used by InfoSchemaTablesExtractor and setDataFromTables.
	infoschema.TableTables: {
		_tableSchema, _tableName, _tidbTableID,
	},
	// See infoschema.partitionsCols for full columns.
	// Used by InfoSchemaPartitionsExtractor and setDataFromPartitions.
	infoschema.TablePartitions: {
		_tableSchema, _tableName, _tidbPartitionID,
		_partitionName,
	},
	// See infoschema.statisticsCols for full columns.
	// Used by InfoSchemaStatisticsExtractor and setDataForStatistics.
	infoschema.TableStatistics: {
		_tableSchema, _tableName,
		_indexName,
	},
	// See infoschema.columns for full columns.
	// Used by InfoSchemaStatisticsExtractor and setDataFromColumns.
	infoschema.TableColumns: {
		_tableSchema, _tableName,
		_columnName,
	},
	// See infoschema.tidb_index_usage for full columns.
	// Used by InfoSchemaIndexesExtractor and setDataFromIndexUsage.
	infoschema.TableTiDBIndexUsage: {
		_tableSchema, _tableName,
		_indexName,
	},
	// See infoschema.schemataCols for full columns.
	// Used by InfoSchemaSchemataExtractor and setDataFromSchemata.
	infoschema.TableSchemata: {
		_schemaName,
	},
	// See infoschema.tableTiDBIndexesCols for full columns.
	// Used by InfoSchemaIndexesExtractor and setDataFromIndexes.
	infoschema.TableTiDBIndexes: {
		_tableSchema,
		_tableName,
	},
	// See infoschema.tableViewsCols for full columns.
	// Used by InfoSchemaViewsExtractor and setDataFromViews.
	infoschema.TableViews: {
		_tableSchema,
		_tableName,
	},
	// See infoschema.keyColumnUsageCols for full columns.
	// Used by InfoSchemaViewsExtractor and setDataFromKeyColumn
	infoschema.TableKeyColumn: {
		_tableSchema,
		_constraintSchema,
		_tableName,
		_constraintName,
	},
	// See infoschema.tableConstraintsCols for full columns.
	// Used by InfoSchemaTableConstraintsExtractor and setDataFromTableConstraints.
	infoschema.TableConstraints: {
		_tableSchema,
		_constraintSchema,
		_tableName,
		_constraintName,
	},
}

// InfoSchemaBaseExtractor is used to extract infoSchema tables related predicates.
type InfoSchemaBaseExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest   bool
	ColPredicates map[string]set.StringSet
	// columns occurs in predicate will be extracted.
	colNames []string
}

func (e *InfoSchemaBaseExtractor) initExtractableColNames(systableName string) {
	cols, ok := extractableColumns[systableName]
	if ok {
		e.colNames = cols
	} else {
		// TODO: remove this after all system tables are supported.
		e.colNames = []string{
			"table_schema",
			"constraint_schema",
			"table_name",
			"constraint_name",
			"sequence_schema",
			"sequence_name",
			"partition_name",
			"schema_name",
			"index_name",
			"tidb_table_id",
		}
	}
}

// SetExtractColNames sets the columns that need to be extracted.
func (e *InfoSchemaBaseExtractor) SetExtractColNames(colNames ...string) {
	e.colNames = colNames
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *InfoSchemaBaseExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	var resultSet, resultSet1 set.StringSet
	e.ColPredicates = make(map[string]set.StringSet)
	remained = predicates
	for _, colName := range e.colNames {
		remained, e.SkipRequest, resultSet = e.extractColWithLower(ctx, schema, names, remained, colName)
		if e.SkipRequest {
			break
		}
		remained, e.SkipRequest, resultSet1 = e.extractCol(ctx, schema, names, remained, colName, true)
		if e.SkipRequest {
			break
		}
		for elt := range resultSet1 {
			resultSet.Insert(elt)
		}
		if len(resultSet) == 0 {
			continue
		}
		e.ColPredicates[colName] = resultSet
	}
	return remained
}

// ExplainInfo implements base.MemTablePredicateExtractor interface.
func (e *InfoSchemaBaseExtractor) ExplainInfo(_ base.PhysicalPlan) string {
	if e.SkipRequest {
		return "skip_request:true"
	}

	r := new(bytes.Buffer)
	colNames := maps.Keys(e.ColPredicates)
	sort.Strings(colNames)
	for _, colName := range colNames {
		if len(e.ColPredicates[colName]) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(e.ColPredicates[colName]))
		}
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// Filter use the col predicates to filter records.
// Return true if the underlying row does not match predicate,
// then it should be filtered and not shown in the result.
func (e *InfoSchemaBaseExtractor) Filter(colName string, val string) bool {
	if e.SkipRequest {
		return true
	}
	predVals, ok := e.ColPredicates[colName]
	if ok && len(predVals) > 0 {
		lower, ok := e.isLower[colName]
		if ok {
			var valStr string
			// only have varchar string type, safe to do that.
			if lower {
				valStr = strings.ToLower(val)
			} else {
				valStr = strings.ToUpper(val)
			}
			return !predVals.Exist(valStr)
		}
		return !predVals.Exist(val)
	}
	// No need to filter records since no predicate for the column exists.
	return false
}

// InfoSchemaIndexesExtractor is the predicate extractor for information_schema.tidb_indexes.
type InfoSchemaIndexesExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaIndexesExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaTablesExtractor is the predicate extractor for information_schema.tables.
type InfoSchemaTablesExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaTablesExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)

	tableIDs := e.getSchemaObjectNames(_tidbTableID)
	tableNames := e.getSchemaObjectNames(_tableName)

	if len(tableIDs) > 0 {
		tableMap := make(map[int64]*model.TableInfo, len(tableIDs))
		findTablesByID(is, tableIDs, tableNames, tableMap)
		return findSchemasForTables(ctx, is, schemas, maps.Values(tableMap))
	}
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaViewsExtractor is the predicate extractor for information_schema.views.
type InfoSchemaViewsExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaViewsExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaKeyColumnUsageExtractor is the predicate extractor for information_schema.key_column_usage.
type InfoSchemaKeyColumnUsageExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaKeyColumnUsageExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaTableConstraintsExtractor is the predicate extractor for information_schema.constraints.
type InfoSchemaTableConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaTableConstraintsExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaPartitionsExtractor is the predicate extractor for information_schema.partitions.
type InfoSchemaPartitionsExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaPartitionsExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)

	partIDs := e.getSchemaObjectNames(_tidbPartitionID)
	tableNames := e.getSchemaObjectNames(_tableName)

	if len(partIDs) > 0 {
		tableMap := make(map[int64]*model.TableInfo, len(partIDs))
		findTablesByPartID(is, partIDs, tableNames, tableMap)
		return findSchemasForTables(ctx, is, schemas, maps.Values(tableMap))
	}
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaStatisticsExtractor is the predicate extractor for  information_schema.statistics.
type InfoSchemaStatisticsExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaStatisticsExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _tableSchema)
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// ListTables lists related tables from predicate.
// If no table is found in predicate, it return all tables.
func (e *InfoSchemaStatisticsExtractor) ListTables(
	ctx context.Context,
	is infoschema.InfoSchema,
	schema model.CIStr,
) ([]*model.TableInfo, error) {
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) == 0 {
		return is.SchemaTableInfos(ctx, schema)
	}

	tables := make(map[int64]*model.TableInfo, 8)
	err := findNameAndAppendToTableMap(ctx, is, schema, tableNames, tables)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return maps.Values(tables), nil
}

// InfoSchemaSchemataExtractor is the predicate extractor for information_schema.schemata.
type InfoSchemaSchemataExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemas lists related schemas from predicate.
// If no schema found in predicate, it return all schemas.
func (e *InfoSchemaSchemataExtractor) ListSchemas(is infoschema.InfoSchema) []model.CIStr {
	return e.listSchemas(is, _schemaName)
}

func (e *InfoSchemaBaseExtractor) listSchemas(is infoschema.InfoSchema, schemaCol string) []model.CIStr {
	schemas := e.getSchemaObjectNames(schemaCol)
	if len(schemas) == 0 {
		ret := is.AllSchemaNames()
		slices.SortFunc(ret, func(a, b model.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return ret
	}
	ret := schemas[:0]
	for _, s := range schemas {
		if n, ok := is.SchemaByName(s); ok {
			ret = append(ret, n.Name)
		}
	}
	return ret
}

func findNameAndAppendToTableMap(
	ctx context.Context,
	is infoschema.InfoSchema,
	schema model.CIStr,
	tableNames []model.CIStr,
	tables map[int64]*model.TableInfo,
) error {
	for _, n := range tableNames {
		tbl, err := is.TableByName(ctx, schema, n)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				continue
			}
			return errors.Trace(err)
		}
		tblInfo := tbl.Meta()
		if tblInfo.TempTableType != model.TempTableNone {
			continue
		}
		tables[tblInfo.ID] = tblInfo
	}
	return nil
}

// findTablesByID finds tables by table IDs and append them to table map.
func findTablesByID(
	is infoschema.InfoSchema,
	tableIDs []model.CIStr,
	tableNames []model.CIStr,
	tables map[int64]*model.TableInfo,
) {
	tblNameMap := make(map[string]struct{}, len(tableNames))
	for _, n := range tableNames {
		tblNameMap[n.L] = struct{}{}
	}
	for _, tid := range parseIDs(tableIDs) {
		tbl, ok := is.TableByID(context.Background(), tid)
		if !ok {
			continue
		}
		tblInfo := tbl.Meta()
		if tblInfo.TempTableType != model.TempTableNone {
			continue
		}
		if len(tableNames) > 0 {
			if _, found := tblNameMap[tblInfo.Name.L]; !found {
				// table_id does not match table_name, skip it.
				continue
			}
		}
		tables[tblInfo.ID] = tblInfo
	}
}

// findTablesByPartID finds tables by partition IDs and append them to table map.
func findTablesByPartID(
	is infoschema.InfoSchema,
	partIDs []model.CIStr,
	tableNames []model.CIStr,
	tables map[int64]*model.TableInfo,
) {
	tblNameMap := make(map[string]struct{}, len(tableNames))
	for _, n := range tableNames {
		tblNameMap[n.L] = struct{}{}
	}
	for _, pid := range parseIDs(partIDs) {
		tbl, _, _ := is.FindTableByPartitionID(pid)
		if tbl == nil {
			continue
		}
		if len(tableNames) > 0 {
			if _, found := tblNameMap[tbl.Meta().Name.L]; !found {
				// partition_id does not match table_name, skip it.
				continue
			}
		}
		tblInfo := tbl.Meta()
		tables[tblInfo.ID] = tblInfo
	}
}

func findTableAndSchemaByName(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemas []model.CIStr,
	tableNames []model.CIStr,
) ([]model.CIStr, []*model.TableInfo, error) {
	type schemaAndTable struct {
		schema model.CIStr
		table  *model.TableInfo
	}
	tableMap := make(map[int64]schemaAndTable, len(tableNames))
	for _, n := range tableNames {
		for _, s := range schemas {
			tbl, err := is.TableByName(ctx, s, n)
			if err != nil {
				if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
					continue
				}
				return nil, nil, errors.Trace(err)
			}
			tblInfo := tbl.Meta()
			if tblInfo.TempTableType != model.TempTableNone {
				continue
			}
			tableMap[tblInfo.ID] = schemaAndTable{s, tblInfo}
		}
	}
	schemaSlice := make([]model.CIStr, 0, len(tableMap))
	tableSlice := make([]*model.TableInfo, 0, len(tableMap))
	for _, st := range tableMap {
		schemaSlice = append(schemaSlice, st.schema)
		tableSlice = append(tableSlice, st.table)
	}
	return schemaSlice, tableSlice, nil
}

func listTablesForEachSchema(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemas []model.CIStr,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemaSlice := make([]model.CIStr, 0, 8)
	tableSlice := make([]*model.TableInfo, 0, 8)
	for _, s := range schemas {
		tables, err := is.SchemaTableInfos(ctx, s)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for _, t := range tables {
			schemaSlice = append(schemaSlice, s)
			tableSlice = append(tableSlice, t)
		}
	}
	return schemaSlice, tableSlice, nil
}

func findSchemasForTables(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemas []model.CIStr,
	tableSlice []*model.TableInfo,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemaSlice := make([]model.CIStr, 0, len(tableSlice))
	for i, tbl := range tableSlice {
		found := false
		for _, s := range schemas {
			isTbl, err := is.TableByName(ctx, s, tbl.Name)
			if err != nil {
				if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
					continue
				}
				return nil, nil, errors.Trace(err)
			}
			if isTbl.Meta().ID == tbl.ID {
				schemaSlice = append(schemaSlice, s)
				found = true
				break
			}
		}
		if !found {
			tableSlice[i] = nil
		}
	}
	// Remove nil elements in tableSlice.
	remains := tableSlice[:0]
	for _, tbl := range tableSlice {
		if tbl != nil {
			remains = append(remains, tbl)
		}
	}
	return schemaSlice, remains, nil
}

func parseIDs(ids []model.CIStr) []int64 {
	tableIDs := make([]int64, 0, len(ids))
	for _, s := range ids {
		v, err := strconv.ParseInt(s.L, 10, 64)
		if err != nil {
			continue
		}
		tableIDs = append(tableIDs, v)
	}
	slices.Sort(tableIDs)
	return tableIDs
}

// getSchemaObjectNames gets the schema object names specified in predicate of given column name.
func (e *InfoSchemaBaseExtractor) getSchemaObjectNames(colName string) []model.CIStr {
	predVals, ok := e.ColPredicates[colName]
	if ok && len(predVals) > 0 {
		tableNames := make([]model.CIStr, 0, len(predVals))
		predVals.IterateWith(func(n string) {
			tableNames = append(tableNames, model.NewCIStr(n))
		})
		slices.SortFunc(tableNames, func(a, b model.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return tableNames
	}
	return nil
}

// InfoSchemaTableNameExtractor is a base struct used to list schema and tables by predicates.
type InfoSchemaTableNameExtractor struct {
	InfoSchemaSchemataExtractor

	listTableFunc func(
		ctx context.Context,
		s model.CIStr,
		is infoschema.InfoSchema,
	) ([]*model.TableInfo, error)

	tableNames []model.CIStr

	predColsLower map[string]set.StringSet

	LikePatterns map[string][]string

	colRegexp map[string][]collate.WildcardPattern
}

// Extract all object names and like operator in predicates
func (e *InfoSchemaTableNameExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained := e.InfoSchemaBaseExtractor.Extract(ctx, schema, names, predicates)
	if e.SkipRequest {
		return remained
	}

	e.LikePatterns = make(map[string][]string, len(e.colNames))
	e.colRegexp = make(map[string][]collate.WildcardPattern, len(e.colNames))
	e.predColsLower = make(map[string]set.StringSet, len(e.colNames))
	var likePatterns []string
	for _, colName := range e.colNames {
		remained, likePatterns = e.extractLikePatternCol(ctx, schema, names, remained, colName, true, false)
		regexp := make([]collate.WildcardPattern, len(likePatterns))
		predColLower := set.StringSet{}
		for i, pattern := range likePatterns {
			regexp[i] = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
			regexp[i].Compile(pattern, byte('\\'))
		}
		if vals, ok := e.ColPredicates[colName]; ok {
			vals.IterateWith(func(n string) {
				predColLower.Insert(strings.ToLower(n))
			})
		}
		e.predColsLower[colName] = predColLower
		e.LikePatterns[colName] = likePatterns
		e.colRegexp[colName] = regexp
	}

	return remained
}

// ListSchemas lists related schemas from predicate.
// If no schema found in predicate, it return all schemas.
func (e *InfoSchemaTableNameExtractor) ListSchemas(
	is infoschema.InfoSchema,
) []model.CIStr {
	allSchemas := e.InfoSchemaSchemataExtractor.listSchemas(is, _tableSchema)

	if regexp, ok := e.colRegexp[_tableSchema]; ok {
		schemas := make([]model.CIStr, 0, len(allSchemas))
	ForLoop:
		for _, schema := range allSchemas {
			for _, re := range regexp {
				if !re.DoMatch(schema.L) {
					continue ForLoop
				}
			}
			schemas = append(schemas, schema)
		}
		allSchemas = schemas
	}

	// TODO: add table_id here
	tableNames := e.getSchemaObjectNames(_tableName)
	e.tableNames = tableNames
	if len(tableNames) > 0 {
		e.listTableFunc = e.listSchemaTablesByName
	} else {
		e.listTableFunc = listSchemaTables
	}

	return allSchemas
}

// ListTables lists related tables for given schema from predicate.
// If no table found in predicate, it return all tables.
func (e *InfoSchemaTableNameExtractor) ListTables(
	ctx context.Context,
	s model.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	allTbls, err := e.listTableFunc(ctx, s, is)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if regexp, ok := e.colRegexp[_tableName]; ok {
		tbls := make([]*model.TableInfo, 0, len(allTbls))
	ForLoop:
		for _, tbl := range allTbls {
			for _, re := range regexp {
				if !re.DoMatch(tbl.Name.L) {
					continue ForLoop
				}
			}
			tbls = append(tbls, tbl)
		}
		allTbls = tbls
	}

	return allTbls, nil
}

func (e *InfoSchemaTableNameExtractor) listSchemaTablesByName(
	ctx context.Context,
	s model.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	tbls := make([]*model.TableInfo, 0, len(e.tableNames))
	for _, n := range e.tableNames {
		tbl, err := is.TableByName(ctx, s, n)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				continue
			}
			return nil, errors.Trace(err)
		}
		tbls = append(tbls, tbl.Meta())
	}

	return tbls, nil
}

func listSchemaTables(
	ctx context.Context,
	s model.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	return is.SchemaTableInfos(ctx, s)
}

// getPredicates gets the object names and like pattern specified in predicate of given column name.
func (e *InfoSchemaTableNameExtractor) getPredicates(colName string) (
	set.StringSet, []collate.WildcardPattern) {
	if predCol, ok := e.predColsLower[colName]; ok {
		return predCol, e.colRegexp[colName]
	}
	return nil, nil
}

// ExplainInfo implements base.MemTablePredicateExtractor interface.
func (e *InfoSchemaTableNameExtractor) ExplainInfo(_ base.PhysicalPlan) string {
	if e.SkipRequest {
		return "skip_request:true"
	}

	r := new(bytes.Buffer)

	colNames := make([]string, len(e.colNames))
	copy(colNames, e.colNames)
	sort.Strings(colNames)

	for _, colName := range colNames {
		if pred, ok := e.ColPredicates[colName]; ok && len(pred) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(pred))
		}
	}

	for _, colName := range colNames {
		if patterns, ok := e.LikePatterns[colName]; ok && len(patterns) > 0 {
			fmt.Fprintf(r, "%s_pattern:[%s], ", colName, extractStringFromStringSlice(patterns))
		}
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// ColumnsTableExtractor is the predicate extractor for information_schema.columns.
type ColumnsTableExtractor struct {
	InfoSchemaTableNameExtractor
}

// ListColumns lists related columns for given table from predicate.
// If no column found in predicate, it return all columns.
func (e *InfoSchemaTableNameExtractor) ListColumns(
	tbl *model.TableInfo,
) []*model.ColumnInfo {
	predCol, regexp := e.getPredicates(_columnName)
	if len(predCol) == 0 && len(regexp) == 0 {
		return tbl.Columns
	}

	columns := make([]*model.ColumnInfo, 0, len(predCol))
ForLoop:
	for _, column := range tbl.Columns {
		if len(predCol) > 0 && !predCol.Exist(column.Name.L) {
			continue
		}
		for _, re := range regexp {
			if !re.DoMatch(column.Name.L) {
				continue ForLoop
			}
		}
		columns = append(columns, column)
	}

	return columns
}

// InfoSchemaIndexesExtractor is the predicate extractor for information_schema.tidb_index_usage.
type InfoSchemaIndexesExtractor struct {
	InfoSchemaTableNameExtractor
}

// ListIndexes lists related indexes for given table from predicate.
// If no index found in predicate, it return all indexes.
func (e *InfoSchemaIndexesExtractor) ListIndexes(
	tbl *model.TableInfo,
) []*model.IndexInfo {
	predCol, regexp := e.getPredicates(_indexName)
	if len(predCol) == 0 && len(regexp) == 0 {
		return tbl.Indices
	}

	indexes := make([]*model.IndexInfo, 0, len(predCol))
ForLoop:
	for _, index := range tbl.Indices {
		if len(predCol) > 0 && !predCol.Exist(index.Name.L) {
			continue
		}
		for _, re := range regexp {
			if !re.DoMatch(index.Name.L) {
				continue ForLoop
			}
		}
		indexes = append(indexes, index)
	}

	return indexes
}
