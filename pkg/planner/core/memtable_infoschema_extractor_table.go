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
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
	tableIDs []ast.CIStr,
	tableNames []ast.CIStr,
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
	partIDs []ast.CIStr,
	tableNames []ast.CIStr,
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
	schemas []ast.CIStr,
	tableNames []ast.CIStr,
) ([]ast.CIStr, []*model.TableInfo, error) {
	type schemaAndTable struct {
		schema ast.CIStr
		table  *model.TableInfo
	}
	schemaAndTbls := make([]schemaAndTable, 0, len(tableNames))
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
			schemaAndTbls = append(schemaAndTbls, schemaAndTable{s, tblInfo})
		}
	}

	slices.SortFunc(schemaAndTbls, func(a, b schemaAndTable) int {
		if a.schema.L == b.schema.L {
			return strings.Compare(a.table.Name.L, b.table.Name.L)
		}
		return strings.Compare(a.schema.L, b.schema.L)
	})
	schemaSlice := make([]ast.CIStr, 0, len(schemaAndTbls))
	tableSlice := make([]*model.TableInfo, 0, len(schemaAndTbls))
	for _, st := range schemaAndTbls {
		schemaSlice = append(schemaSlice, st.schema)
		tableSlice = append(tableSlice, st.table)
	}
	return schemaSlice, tableSlice, nil
}

func listTablesForEachSchema(
	ctx context.Context,
	e *InfoSchemaBaseExtractor,
	is infoschema.InfoSchema,
	schemas []ast.CIStr,
) ([]ast.CIStr, []*model.TableInfo, error) {
	ec := e.extractableColumns
	schemaSlice := make([]ast.CIStr, 0, 8)
	tableSlice := make([]*model.TableInfo, 0, 8)
	for _, s := range schemas {
		tables, err := is.SchemaTableInfos(ctx, s)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if ctx.Err() != nil {
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

// findSchemasForTables finds a schema for each tableInfo, and it
// returns a schema slice and a table slice that has the same length.
// Note that input arg "tableSlice" will be changed in place.
func findSchemasForTables(
	e *InfoSchemaBaseExtractor,
	is infoschema.InfoSchema,
	tableSlice []*model.TableInfo,
) ([]ast.CIStr, []*model.TableInfo, error) {
	schemas, unspecified := e.listPredicateSchemas(is)
	schemaSlice := make([]ast.CIStr, 0, len(tableSlice))
	for i, tbl := range tableSlice {
		dbInfo, ok := is.SchemaByID(tbl.DBID)
		intest.Assert(ok)
		if !ok {
			logutil.BgLogger().Warn("schema not found for table info",
				zap.Int64("tableID", tbl.ID), zap.Int64("dbID", tbl.DBID))
			continue
		}
		if unspecified { // all schemas should be included.
			schemaSlice = append(schemaSlice, dbInfo.Name)
			continue
		}

		found := false
		for _, s := range schemas {
			if s.L == dbInfo.Name.L {
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
	sort.Slice(schemaSlice, func(i, j int) bool {
		iSchema, jSchema := schemaSlice[i].L, schemaSlice[j].L
		less := iSchema < jSchema ||
			(iSchema == jSchema && remains[i].Name.L < remains[j].Name.L)
		if less {
			remains[i], remains[j] = remains[j], remains[i]
		}
		return less
	})
	return schemaSlice, remains, nil
}

func parseIDs(ids []ast.CIStr) []int64 {
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
func (e *InfoSchemaBaseExtractor) getSchemaObjectNames(colName string) []ast.CIStr {
	predVals, ok := e.ColPredicates[colName]
	if ok && len(predVals) > 0 {
		objNames := make([]ast.CIStr, 0, len(predVals))
		predVals.IterateWith(func(n string) {
			objNames = append(objNames, ast.NewCIStr(n))
		})
		slices.SortFunc(objNames, func(a, b ast.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return objNames
	}
	return nil
}

func extractStrCIStr(str ast.CIStr) string {
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
		allMatch := true
		for _, re := range regs {
			if !re.MatchString(strFn(target)) {
				allMatch = false
				break
			}
		}
		if allMatch {
			filtered = append(filtered, target)
		}
	}
	return filtered
}

