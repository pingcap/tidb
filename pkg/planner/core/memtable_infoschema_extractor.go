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
	_tableID          = "table_id"
	_sequenceSchema   = "sequence_schema"
	_sequenceName     = "sequence_name"
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
	// Used by InfoSchemaColumnsExtractor and setDataFromColumns.
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
	// See infoschema.tableCheckConstraintsCols for full columns.
	// Used by InfoSchemaCheckConstraintsExtractor and setDataFromCheckConstraints.
	infoschema.TableCheckConstraints: {
		_constraintSchema,
		_constraintName,
	},
	// See infoschema.tableTiDBCheckConstraintsCols for full columns.
	// Used by InfoSchemaTiDBCheckConstraintsExtractor and setDataFromTiDBCheckConstraints.
	infoschema.TableTiDBCheckConstraints: {
		_constraintSchema, _tableName, _tableID,
		_constraintName,
	},
	// See infoschema.referConstCols for full columns.
	// Used by InfoSchemaReferConstExtractor and setDataFromReferConst.
	infoschema.TableReferConst: {
		_constraintSchema, _tableName,
		_constraintName,
	},
	// See infoschema.tableSequencesCols for full columns.
	// Used by InfoSchemaSequenceExtractor and setDataFromSequences.
	infoschema.TableSequences: {
		_sequenceSchema, _sequenceName,
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

// InfoSchemaCheckConstraintsExtractor is the predicate extractor for information_schema.check_constraints.
type InfoSchemaCheckConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemas lists related schemas from predicate.
func (e *InfoSchemaCheckConstraintsExtractor) ListSchemas(is infoschema.InfoSchema) []model.CIStr {
	return e.listSchemas(is, _constraintSchema)
}

// InfoSchemaTiDBCheckConstraintsExtractor is the predicate extractor for information_schema.tidb_check_constraints.
type InfoSchemaTiDBCheckConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
func (e *InfoSchemaTiDBCheckConstraintsExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _constraintSchema)

	tableIDs := e.getSchemaObjectNames(_tableID)
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

// InfoSchemaReferConstExtractor is the predicate extractor for information_schema.referential_constraints.
type InfoSchemaReferConstExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
func (e *InfoSchemaReferConstExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _constraintSchema)
	tableNames := e.getSchemaObjectNames(_tableName)
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// InfoSchemaSequenceExtractor is the predicate extractor for information_schema.sequences.
type InfoSchemaSequenceExtractor struct {
	InfoSchemaBaseExtractor
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
func (e *InfoSchemaSequenceExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemas := e.listSchemas(is, _sequenceSchema)
	seqNames := e.getSchemaObjectNames(_sequenceName)
	if len(seqNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, seqNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
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
		if tblInfo.TempTableType == model.TempTableLocal {
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
		if tblInfo.TempTableType == model.TempTableLocal {
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
	ctx = infoschema.WithRefillOption(ctx, false)
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
			if tblInfo.TempTableType == model.TempTableLocal {
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

// InfoSchemaTableNameExtractor is a base struct to list matching schemas and tables in predicates,
// so there is no need to call `Filter` for returns from `ListSchemas` and `ListTables`.
// But for other columns, Subclass **must** reimplement `Filter` method to use like operators for filtering.
// Currently, table_id is not taken into consideration.
type InfoSchemaTableNameExtractor struct {
	InfoSchemaSchemataExtractor

	listTableFunc func(
		ctx context.Context,
		s model.CIStr,
		is infoschema.InfoSchema,
	) ([]*model.TableInfo, error)

	// table names from predicate, used by `ListTables`
	tableNames []model.CIStr

	// all predicates in lower case
	colsPredLower map[string]set.StringSet

	// all built regexp in predicates
	colsRegexp map[string][]collate.WildcardPattern

	// used for EXPLAIN only
	LikePatterns map[string][]string
}

// Extract all names and like operators in predicates
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
	e.colsRegexp = make(map[string][]collate.WildcardPattern, len(e.colNames))
	e.colsPredLower = make(map[string]set.StringSet, len(e.colNames))
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
		e.colsPredLower[colName] = predColLower
		e.LikePatterns[colName] = likePatterns
		e.colsRegexp[colName] = regexp
	}

	return remained
}

// getPredicates gets all names and regexps related to given column names.
func (e *InfoSchemaTableNameExtractor) getPredicates(colNames ...string) (
	set.StringSet, []collate.WildcardPattern, bool) {
	filters := set.StringSet{}
	regexp := []collate.WildcardPattern{}
	hasPredicates := false

	// Extract all filters and like patterns
	for _, col := range colNames {
		if rs, ok := e.colsRegexp[col]; ok && len(rs) > 0 {
			regexp = append(regexp, rs...)
		}
		if f, ok := e.colsPredLower[col]; ok && len(f) > 0 {
			if !hasPredicates {
				filters = f
				hasPredicates = true
			} else {
				filters = filters.Intersection(f)
			}
		}
	}

	return filters, regexp, hasPredicates
}

// Get all predicates related to schema extraction.
// Add more columns if necessary.
func (e *InfoSchemaTableNameExtractor) getSchemaNames() (
	set.StringSet, []collate.WildcardPattern, bool) {
	return e.getPredicates(_tableSchema, _schemaName, _constraintSchema)
}

// ListSchemas lists related schemas from predicates.
// Returned schemas is examined by like operators, so there is no need to call Filter again.
func (e *InfoSchemaTableNameExtractor) ListSchemas(
	is infoschema.InfoSchema,
) []model.CIStr {
	schemaFilters, schemaRegexp, hasPredicates := e.getSchemaNames()

	// Get all schema names
	var schemas []model.CIStr
	if hasPredicates {
		schemas = make([]model.CIStr, 0, len(schemaFilters))
		schemaFilters.IterateWith(func(n string) {
			s := model.CIStr{O: n, L: n}
			if n, ok := is.SchemaByName(s); ok {
				schemas = append(schemas, n.Name)
			}
		})
	} else {
		schemas = is.AllSchemaNames()
	}
	slices.SortFunc(schemas, func(a, b model.CIStr) int {
		return strings.Compare(a.L, b.L)
	})

	// Filter with regexp
	filteredSchemas := make([]model.CIStr, 0, len(schemas))
ForLoop:
	for _, schema := range schemas {
		for _, re := range schemaRegexp {
			if !re.DoMatch(schema.L) {
				continue ForLoop
			}
		}
		filteredSchemas = append(filteredSchemas, schema)
	}

	// TODO: add table_id here
	tableNames := e.getSchemaObjectNames(_tableName)
	e.tableNames = tableNames
	if len(tableNames) > 0 {
		e.listTableFunc = e.listSchemaTablesByName
	} else {
		e.listTableFunc = listSchemaTables
	}

	return filteredSchemas
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

	if regexp, ok := e.colsRegexp[_tableName]; ok {
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

// ExplainInfo implements base.MemTablePredicateExtractor interface.
func (e *InfoSchemaTableNameExtractor) ExplainInfo(_ base.PhysicalPlan) string {
	if e.SkipRequest {
		return "skip_request:true"
	}

	r := new(bytes.Buffer)

	for _, colName := range e.colNames {
		if pred, ok := e.ColPredicates[colName]; ok && len(pred) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(pred))
		}
	}

	for _, colName := range e.colNames {
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

// InfoSchemaColumnsExtractor is the predicate extractor for information_schema.columns.
type InfoSchemaColumnsExtractor struct {
	InfoSchemaTableNameExtractor
}

// ListColumns lists unhidden columns and corresponding ordinal positions for given table from predicates.
// If no column found in predicate, it return all visible columns.
func (e *InfoSchemaTableNameExtractor) ListColumns(
	tbl *model.TableInfo,
) ([]*model.ColumnInfo, []int) {
	predCol, regexp, _ := e.getPredicates(_columnName)

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
			if !re.DoMatch(column.Name.L) {
				continue ForLoop
			}
		}
		columns = append(columns, column)
		ordinalPos = append(ordinalPos, ord)
	}

	return columns, ordinalPos
}

// InfoSchemaIndexUsageExtractor is the predicate extractor for information_schema.tidb_index_usage.
type InfoSchemaIndexUsageExtractor struct {
	InfoSchemaTableNameExtractor
}

// ListIndexes lists related indexes for given table from predicate.
// If no index found in predicate, it return all indexes.
func (e *InfoSchemaIndexUsageExtractor) ListIndexes(
	tbl *model.TableInfo,
) []*model.IndexInfo {
	predCol, regexp, _ := e.getPredicates(_indexName)
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
