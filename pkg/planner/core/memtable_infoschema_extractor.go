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
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
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
	colsRegexp map[string][]*regexp.Regexp
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

// ListSchemas lists all schemas from predicate. If no schema is specified, it lists
// all schemas in the storage.
func (e *InfoSchemaBaseExtractor) ListSchemas(is infoschema.InfoSchema) []ast.CIStr {
	extractedSchemas, unspecified := e.listPredicateSchemas(is)
	if unspecified {
		ret := is.AllSchemaNames()
		slices.SortFunc(ret, func(a, b ast.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return filterSchemaObjectByRegexp(e, e.extractableColumns.schema, ret, extractStrCIStr)
	}
	return extractedSchemas
}

// listPredicateSchemas lists all schemas specified in predicates.
// If no schema is specified in predicates, return `unspecified` as true.
func (e *InfoSchemaBaseExtractor) listPredicateSchemas(
	is infoschema.InfoSchema,
) (schemas []ast.CIStr, unspecified bool) {
	ec := e.extractableColumns
	schemas = e.getSchemaObjectNames(ec.schema)
	if len(schemas) == 0 {
		return nil, true
	}
	ret := schemas[:0]
	for _, s := range schemas {
		if n, ok := is.SchemaByName(s); ok {
			ret = append(ret, n.Name)
		}
	}
	return filterSchemaObjectByRegexp(e, ec.schema, ret, extractStrCIStr), false
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaBaseExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]ast.CIStr, []*model.TableInfo, error) {
	ec := e.extractableColumns
	var tableNames []ast.CIStr
	if ec.table != "" {
		tableNames = e.getSchemaObjectNames(ec.table)
	}
	var tableIDs []ast.CIStr
	if ec.tableID != "" {
		tableIDs = e.getSchemaObjectNames(ec.tableID)
		if len(tableIDs) > 0 {
			tableMap := make(map[int64]*model.TableInfo, len(tableIDs))
			findTablesByID(is, tableIDs, tableNames, tableMap)
			tableSlice := slices.Collect(maps.Values(tableMap))
			tableSlice = filterSchemaObjectByRegexp(e, ec.table, tableSlice, extractStrTableInfo)
			return findSchemasForTables(e, is, tableSlice)
		}
	}
	if ec.partitionID != "" {
		partIDs := e.getSchemaObjectNames(ec.partitionID)
		if len(partIDs) > 0 {
			tableMap := make(map[int64]*model.TableInfo, len(partIDs))
			findTablesByPartID(is, partIDs, tableNames, tableMap)
			tableSlice := slices.Collect(maps.Values(tableMap))
			tableSlice = filterSchemaObjectByRegexp(e, ec.table, tableSlice, extractStrTableInfo)
			return findSchemasForTables(e, is, tableSlice)
		}
	}
	if len(tableNames) > 0 {
		tableNames = filterSchemaObjectByRegexp(e, ec.table, tableNames, extractStrCIStr)
		schemas := e.ListSchemas(is)
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	schemas := e.ListSchemas(is)
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
	e.colsRegexp = make(map[string][]*regexp.Regexp, len(e.colNames))
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
		newRemained, likePatterns := e.extractLikePatternCol(ctx, schema, names, remained, colName, true, true)
		if len(likePatterns) == 0 {
			continue
		}
		// Use the original pattern to display.
		_, oldLikePatterns := e.extractLikePatternCol(ctx, schema, names, remained, colName, true, false)
		regs := make([]*regexp.Regexp, len(likePatterns))
		meetError := false
		for i, pattern := range likePatterns {
			reg, err := regexp.Compile(fmt.Sprintf("(?i)%s", pattern))
			if err != nil {
				logutil.BgLogger().Warn("compile regexp failed in infoSchema extractor", zap.String("pattern", pattern), zap.Error(err))
				meetError = true
				break
			}
			regs[i] = reg
		}
		if !meetError {
			remained = newRemained
			e.LikePatterns[colName] = oldLikePatterns
			e.colsRegexp[colName] = regs
		}
	}
	return remained
}

// ExplainInfo implements base.MemTablePredicateExtractor interface.
func (e *InfoSchemaBaseExtractor) ExplainInfo(_ base.PhysicalPlan) string {
	if e.SkipRequest {
		return "skip_request:true"
	}

	r := new(bytes.Buffer)
	colNames := slices.Collect(maps.Keys(e.ColPredicates))
	slices.Sort(colNames)
	for _, colName := range colNames {
		preds := e.ColPredicates[colName]
		if len(preds) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(preds))
		}
	}

	colNames = slices.Collect(maps.Keys(e.LikePatterns))
	slices.Sort(colNames)
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
	for _, re := range e.colsRegexp[colName] {
		if !re.MatchString(val) {
			return true
		}
	}

	toLower := false
	if e.extractLowerString != nil {
		toLower = e.extractLowerString[colName]
	}

	predVals, ok := e.ColPredicates[colName]
	if ok && len(predVals) > 0 {
		if toLower {
			return !predVals.Exist(strings.ToLower(val))
		}
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
