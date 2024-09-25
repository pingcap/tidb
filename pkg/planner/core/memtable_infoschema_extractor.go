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
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/set"
	"golang.org/x/exp/maps"
)

// extractableCols records the column names used by tables in information_schema.
type extractableCols struct {
	schema      string
	table       string
	tableID     string
	partitionID string

	partitionName string
	indexName     string
	columnName    string
	constrName    string
	constrSchema  string
}

//revive:disable:exported
const (
	TableSchema      = "table_schema"
	TableName        = "table_name"
	TidbTableID      = "tidb_table_id"
	PartitionName    = "partition_name"
	TidbPartitionID  = "tidb_partition_id"
	IndexName        = "index_name"
	SchemaName       = "schema_name"
	DBName           = "db_name"
	ConstraintSchema = "constraint_schema"
	ConstraintName   = "constraint_name"
	TableID          = "table_id"
	SequenceSchema   = "sequence_schema"
	SequenceName     = "sequence_name"
	ColumnName       = "column_name"
	DDLStateName     = "state"
)

//revive:enable:exported

var patternMatchable = map[string]struct{}{
	TableSchema:      {},
	TableName:        {},
	IndexName:        {},
	SchemaName:       {},
	ConstraintSchema: {},
	SequenceSchema:   {},
	SequenceName:     {},
	ColumnName:       {},
}

const (
	primaryKeyName = "primary"
)

// InfoSchemaBaseExtractor is used to extract infoSchema tables related predicates.
type InfoSchemaBaseExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool
	// ColPredicates records the columns that can be extracted from the predicates.
	// For example, `select * from information_schema.SCHEMATA where schema_name='mysql' or schema_name='INFORMATION_SCHEMA'`
	// {"schema_name": ["mysql", "INFORMATION_SCHEMA"]}
	ColPredicates map[string]set.StringSet
	// all built regexp in predicates
	colsRegexp map[string][]collate.WildcardPattern
	// used for EXPLAIN only
	LikePatterns map[string][]string
	// columns occurs in predicate will be extracted.
	colNames           []string
	extractableColumns extractableCols
}

// GetBase is only used for test.
func (e *InfoSchemaBaseExtractor) GetBase() *InfoSchemaBaseExtractor {
	return e
}

// ListSchemas lists related schemas from predicate.
func (e *InfoSchemaBaseExtractor) ListSchemas(is infoschema.InfoSchema) []pmodel.CIStr {
	ec := e.extractableColumns
	schemas := e.getSchemaObjectNames(ec.schema)
	if len(schemas) == 0 {
		ret := is.AllSchemaNames()
		slices.SortFunc(ret, func(a, b pmodel.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return filterSchemaObjectByRegexp(e, ec.schema, ret, extractStrCIStr)
	}
	ret := schemas[:0]
	for _, s := range schemas {
		if n, ok := is.SchemaByName(s); ok {
			ret = append(ret, n.Name)
		}
	}
	return filterSchemaObjectByRegexp(e, ec.schema, ret, extractStrCIStr)
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaBaseExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]pmodel.CIStr, []*model.TableInfo, error) {
	ec := e.extractableColumns
	schemas := e.ListSchemas(is)
	var tableNames []pmodel.CIStr
	if ec.table != "" {
		tableNames = e.getSchemaObjectNames(ec.table)
	}
	var tableIDs []pmodel.CIStr
	if ec.tableID != "" {
		tableIDs = e.getSchemaObjectNames(ec.tableID)
		if len(tableIDs) > 0 {
			tableMap := make(map[int64]*model.TableInfo, len(tableIDs))
			findTablesByID(is, tableIDs, tableNames, tableMap)
			tableSlice := maps.Values(tableMap)
			tableSlice = filterSchemaObjectByRegexp(e, ec.table, tableSlice, extractStrTableInfo)
			return findSchemasForTables(ctx, is, schemas, tableSlice)
		}
	}
	if ec.partitionID != "" {
		partIDs := e.getSchemaObjectNames(ec.partitionID)
		if len(partIDs) > 0 {
			tableMap := make(map[int64]*model.TableInfo, len(partIDs))
			findTablesByPartID(is, partIDs, tableNames, tableMap)
			tableSlice := maps.Values(tableMap)
			tableSlice = filterSchemaObjectByRegexp(e, ec.table, tableSlice, extractStrTableInfo)
			return findSchemasForTables(ctx, is, schemas, tableSlice)
		}
	}
	if len(tableNames) > 0 {
		tableNames = filterSchemaObjectByRegexp(e, ec.table, tableNames, extractStrCIStr)
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, e, is, schemas)
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *InfoSchemaBaseExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	e.ColPredicates = make(map[string]set.StringSet)
	e.LikePatterns = make(map[string][]string, len(e.colNames))
	e.colsRegexp = make(map[string][]collate.WildcardPattern, len(e.colNames))
	remained = predicates
	for _, colName := range e.colNames {
		var resultSet set.StringSet
		remained, e.SkipRequest, resultSet = e.extractCol(ctx, schema, names, remained, colName, true)
		if e.SkipRequest {
			break
		}
		if len(resultSet) == 0 {
			continue
		}
		e.ColPredicates[colName] = resultSet
	}
	for _, colName := range e.colNames {
		if _, ok := patternMatchable[colName]; !ok {
			continue
		}
		var likePatterns []string
		remained, likePatterns = e.extractLikePatternCol(ctx, schema, names, remained, colName, true, false)
		regexp := make([]collate.WildcardPattern, len(likePatterns))
		for i, pattern := range likePatterns {
			regexp[i] = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
			regexp[i].Compile(pattern, byte('\\'))
		}
		e.LikePatterns[colName] = likePatterns
		e.colsRegexp[colName] = regexp
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
		preds := e.ColPredicates[colName]
		if len(preds) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(preds))
		}
	}

	colNames = maps.Keys(e.LikePatterns)
	sort.Strings(colNames)
	for _, colName := range colNames {
		patterns := e.LikePatterns[colName]
		if len(patterns) > 0 {
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

// Filter use the col predicates to filter records.
// Return true if the underlying row does not match predicate,
// then it should be filtered and not shown in the result.
func (e *InfoSchemaBaseExtractor) filter(colName string, val string) bool {
	if e.SkipRequest {
		return true
	}
	predVals, ok := e.ColPredicates[colName]
	if ok && len(predVals) > 0 {
		fn, ok := e.pushedDownFuncs[colName]
		if ok {
			return !predVals.Exist(fn(val))
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

// NewInfoSchemaIndexesExtractor creates a new InfoSchemaIndexesExtractor.
func NewInfoSchemaIndexesExtractor() *InfoSchemaIndexesExtractor {
	e := &InfoSchemaIndexesExtractor{}
	e.extractableColumns = extractableCols{
		schema: TableSchema,
		table:  TableName,
	}
	e.colNames = []string{TableSchema, TableName}
	return e
}

// InfoSchemaTablesExtractor is the predicate extractor for information_schema.tables.
type InfoSchemaTablesExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaTablesExtractor creates a new InfoSchemaTablesExtractor.
func NewInfoSchemaTablesExtractor() *InfoSchemaTablesExtractor {
	e := &InfoSchemaTablesExtractor{}
	e.extractableColumns = extractableCols{
		schema:  TableSchema,
		table:   TableName,
		tableID: TidbTableID,
	}
	e.colNames = []string{TableSchema, TableName, TidbTableID}
	return e
}

// HasTableName returns true if table name is specified in predicates.
func (e *InfoSchemaTablesExtractor) HasTableName(name string) bool {
	return !e.filter(TableName, name)
}

// HasTableSchema returns true if table schema is specified in predicates.
func (e *InfoSchemaTablesExtractor) HasTableSchema(name string) bool {
	return !e.filter(TableSchema, name)
}

// InfoSchemaDDLExtractor is the predicate extractor for information_schema.ddl_jobs.
type InfoSchemaDDLExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaDDLExtractor creates a new InfoSchemaDDLExtractor.
func NewInfoSchemaDDLExtractor() *InfoSchemaDDLExtractor {
	e := &InfoSchemaDDLExtractor{}
	e.extractableColumns = extractableCols{
		schema: DBName,
		table:  TableName,
	}
	e.colNames = []string{DBName, TableName, DDLStateName}
	return e
}

// Extract implements the MemTablePredicateExtractor Extract interface
//
// Different from other extractor, input predicates will not be pruned.
// For example, we will use state to determine whether to scan history ddl jobs,
// but we don't not use these predicates to do filtering.
// So the Selection Operator is still needed.
func (e *InfoSchemaDDLExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	e.InfoSchemaBaseExtractor.Extract(ctx, schema, names, predicates)
	return predicates
}

// InfoSchemaViewsExtractor is the predicate extractor for information_schema.views.
type InfoSchemaViewsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaViewsExtractor creates a new InfoSchemaViewsExtractor.
func NewInfoSchemaViewsExtractor() *InfoSchemaViewsExtractor {
	e := &InfoSchemaViewsExtractor{}
	e.extractableColumns = extractableCols{
		schema: TableSchema,
		table:  TableName,
	}
	e.colNames = []string{TableSchema, TableName}
	return e
}

// InfoSchemaKeyColumnUsageExtractor is the predicate extractor for information_schema.key_column_usage.
type InfoSchemaKeyColumnUsageExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaKeyColumnUsageExtractor creates a new InfoSchemaKeyColumnUsageExtractor.
func NewInfoSchemaKeyColumnUsageExtractor() *InfoSchemaKeyColumnUsageExtractor {
	e := &InfoSchemaKeyColumnUsageExtractor{}
	e.extractableColumns = extractableCols{
		schema:       TableSchema,
		table:        TableName,
		constrName:   ConstraintName,
		constrSchema: ConstraintSchema,
	}
	e.colNames = []string{TableSchema, TableName, ConstraintName, ConstraintSchema}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaKeyColumnUsageExtractor) HasConstraint(name string) bool {
	return !e.filter(ConstraintName, name)
}

// HasPrimaryKey returns true if primary key is specified in predicates.
func (e *InfoSchemaKeyColumnUsageExtractor) HasPrimaryKey() bool {
	return !e.filter(ConstraintName, primaryKeyName)
}

// HasConstraintSchema returns true if constraint schema is specified in predicates.
func (e *InfoSchemaKeyColumnUsageExtractor) HasConstraintSchema(name string) bool {
	return !e.filter(ConstraintSchema, name)
}

// InfoSchemaTableConstraintsExtractor is the predicate extractor for information_schema.constraints.
type InfoSchemaTableConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaTableConstraintsExtractor creates a new InfoSchemaTableConstraintsExtractor.
func NewInfoSchemaTableConstraintsExtractor() *InfoSchemaTableConstraintsExtractor {
	e := &InfoSchemaTableConstraintsExtractor{}
	e.extractableColumns = extractableCols{
		schema:       TableSchema,
		table:        TableName,
		constrName:   ConstraintName,
		constrSchema: ConstraintSchema,
	}
	e.colNames = []string{TableSchema, TableName, ConstraintName, ConstraintSchema}
	return e
}

// HasConstraintSchema returns true if constraint schema is specified in predicates.
func (e *InfoSchemaTableConstraintsExtractor) HasConstraintSchema(name string) bool {
	return !e.filter(ConstraintSchema, name)
}

// HasConstraint returns true if constraint is specified in predicates.
func (e *InfoSchemaTableConstraintsExtractor) HasConstraint(name string) bool {
	return !e.filter(ConstraintName, name)
}

// HasPrimaryKey returns true if primary key is specified in predicates.
func (e *InfoSchemaTableConstraintsExtractor) HasPrimaryKey() bool {
	return !e.filter(ConstraintName, primaryKeyName)
}

// InfoSchemaPartitionsExtractor is the predicate extractor for information_schema.partitions.
type InfoSchemaPartitionsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaPartitionsExtractor creates a new InfoSchemaPartitionsExtractor.
func NewInfoSchemaPartitionsExtractor() *InfoSchemaPartitionsExtractor {
	e := &InfoSchemaPartitionsExtractor{}
	e.extractableColumns = extractableCols{
		schema:        TableSchema,
		table:         TableName,
		partitionID:   TidbPartitionID,
		partitionName: PartitionName,
	}
	e.colNames = []string{TableSchema, TableName, TidbPartitionID, PartitionName}
	return e
}

// HasPartition returns true if partition name matches the one in predicates.
func (e *InfoSchemaPartitionsExtractor) HasPartition(name string) bool {
	return !e.filter(PartitionName, name)
}

// HasPartitionPred returns true if partition name is specified in predicates.
func (e *InfoSchemaPartitionsExtractor) HasPartitionPred() bool {
	return len(e.ColPredicates[PartitionName]) > 0
}

// InfoSchemaStatisticsExtractor is the predicate extractor for  information_schema.statistics.
type InfoSchemaStatisticsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaStatisticsExtractor creates a new InfoSchemaStatisticsExtractor.
func NewInfoSchemaStatisticsExtractor() *InfoSchemaStatisticsExtractor {
	e := &InfoSchemaStatisticsExtractor{}
	e.extractableColumns = extractableCols{
		schema:    TableSchema,
		table:     TableName,
		indexName: IndexName,
	}
	e.colNames = []string{TableSchema, TableName, IndexName}
	return e
}

// HasIndex returns true if index name is specified in predicates.
func (e *InfoSchemaStatisticsExtractor) HasIndex(val string) bool {
	return !e.filter(IndexName, val)
}

// HasPrimaryKey returns true if primary key is specified in predicates.
func (e *InfoSchemaStatisticsExtractor) HasPrimaryKey() bool {
	return !e.filter(IndexName, primaryKeyName)
}

// InfoSchemaSchemataExtractor is the predicate extractor for information_schema.schemata.
type InfoSchemaSchemataExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaSchemataExtractor creates a new InfoSchemaSchemataExtractor.
func NewInfoSchemaSchemataExtractor() *InfoSchemaSchemataExtractor {
	e := &InfoSchemaSchemataExtractor{}
	e.extractableColumns = extractableCols{
		schema: SchemaName,
	}
	e.colNames = []string{SchemaName}
	return e
}

// InfoSchemaCheckConstraintsExtractor is the predicate extractor for information_schema.check_constraints.
type InfoSchemaCheckConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaCheckConstraintsExtractor creates a new InfoSchemaCheckConstraintsExtractor.
func NewInfoSchemaCheckConstraintsExtractor() *InfoSchemaCheckConstraintsExtractor {
	e := &InfoSchemaCheckConstraintsExtractor{}
	e.extractableColumns = extractableCols{
		schema:     ConstraintSchema,
		constrName: ConstraintName,
	}
	e.colNames = []string{ConstraintSchema, ConstraintName}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaCheckConstraintsExtractor) HasConstraint(name string) bool {
	return !e.filter(ConstraintName, name)
}

// InfoSchemaTiDBCheckConstraintsExtractor is the predicate extractor for information_schema.tidb_check_constraints.
type InfoSchemaTiDBCheckConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaTiDBCheckConstraintsExtractor creates a new InfoSchemaTiDBCheckConstraintsExtractor.
func NewInfoSchemaTiDBCheckConstraintsExtractor() *InfoSchemaTiDBCheckConstraintsExtractor {
	e := &InfoSchemaTiDBCheckConstraintsExtractor{}
	e.extractableColumns = extractableCols{
		schema:     ConstraintSchema,
		table:      TableName,
		tableID:    TableID,
		constrName: ConstraintName,
	}
	e.colNames = []string{ConstraintSchema, TableName, TableID, ConstraintName}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaTiDBCheckConstraintsExtractor) HasConstraint(name string) bool {
	return !e.filter(ConstraintName, name)
}

// InfoSchemaReferConstExtractor is the predicate extractor for information_schema.referential_constraints.
type InfoSchemaReferConstExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaReferConstExtractor creates a new InfoSchemaReferConstExtractor.
func NewInfoSchemaReferConstExtractor() *InfoSchemaReferConstExtractor {
	e := &InfoSchemaReferConstExtractor{}
	e.extractableColumns = extractableCols{
		schema:     ConstraintSchema,
		table:      TableName,
		constrName: ConstraintName,
	}
	e.colNames = []string{ConstraintSchema, TableName, ConstraintName}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaReferConstExtractor) HasConstraint(name string) bool {
	return !e.filter(ConstraintName, name)
}

// InfoSchemaSequenceExtractor is the predicate extractor for information_schema.sequences.
type InfoSchemaSequenceExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaSequenceExtractor creates a new InfoSchemaSequenceExtractor.
func NewInfoSchemaSequenceExtractor() *InfoSchemaSequenceExtractor {
	e := &InfoSchemaSequenceExtractor{}
	e.extractableColumns = extractableCols{
		schema: SequenceSchema,
		table:  SequenceName,
	}
	e.colNames = []string{SequenceSchema, SequenceName}
	return e
}

// findTablesByID finds tables by table IDs and append them to table map.
func findTablesByID(
	is infoschema.InfoSchema,
	tableIDs []pmodel.CIStr,
	tableNames []pmodel.CIStr,
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
	partIDs []pmodel.CIStr,
	tableNames []pmodel.CIStr,
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
	schemas []pmodel.CIStr,
	tableNames []pmodel.CIStr,
) ([]pmodel.CIStr, []*model.TableInfo, error) {
	type schemaAndTable struct {
		schema pmodel.CIStr
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
	schemaSlice := make([]pmodel.CIStr, 0, len(tableMap))
	tableSlice := make([]*model.TableInfo, 0, len(tableMap))
	for _, st := range tableMap {
		schemaSlice = append(schemaSlice, st.schema)
		tableSlice = append(tableSlice, st.table)
	}
	return schemaSlice, tableSlice, nil
}

func listTablesForEachSchema(
	ctx context.Context,
	e *InfoSchemaBaseExtractor,
	is infoschema.InfoSchema,
	schemas []pmodel.CIStr,
) ([]pmodel.CIStr, []*model.TableInfo, error) {
	ec := e.extractableColumns
	schemaSlice := make([]pmodel.CIStr, 0, 8)
	tableSlice := make([]*model.TableInfo, 0, 8)
	for _, s := range schemas {
		tables, err := is.SchemaTableInfos(ctx, s)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		tables = filterSchemaObjectByRegexp(e, ec.table, tables, extractStrTableInfo)
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
	schemas []pmodel.CIStr,
	tableSlice []*model.TableInfo,
) ([]pmodel.CIStr, []*model.TableInfo, error) {
	schemaSlice := make([]pmodel.CIStr, 0, len(tableSlice))
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

func parseIDs(ids []pmodel.CIStr) []int64 {
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
func (e *InfoSchemaBaseExtractor) getSchemaObjectNames(colName string) []pmodel.CIStr {
	predVals, ok := e.ColPredicates[colName]
	if ok && len(predVals) > 0 {
		objNames := make([]pmodel.CIStr, 0, len(predVals))
		predVals.IterateWith(func(n string) {
			objNames = append(objNames, pmodel.NewCIStr(n))
		})
		slices.SortFunc(objNames, func(a, b pmodel.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return objNames
	}
	return nil
}

func extractStrCIStr(str pmodel.CIStr) string {
	return str.O
}

func extractStrTableInfo(tblInfo *model.TableInfo) string {
	return tblInfo.Name.O
}

// filterSchemaObjectByRegexp filter the targets by regexp.
// Note that targets will be modified.
func filterSchemaObjectByRegexp[targetTp any](
	e *InfoSchemaBaseExtractor,
	colName string,
	targets []targetTp,
	strFn func(targetTp) string,
) []targetTp {
	regs := e.colsRegexp[colName]
	if len(regs) == 0 {
		return targets
	}
	filtered := targets[:0]
	for _, target := range targets {
		for _, re := range regs {
			if re.DoMatch(strFn(target)) {
				filtered = append(filtered, target)
			}
		}
	}
	return filtered
}

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
	s pmodel.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	ec := e.extractableColumns
	base := e.GetBase()
	schemas := []pmodel.CIStr{s}
	var tableNames []pmodel.CIStr
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
			if !re.DoMatch(column.Name.L) {
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
) []*model.IndexInfo {
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
