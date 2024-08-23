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

package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
)

type colPredicates struct {
	// eg. table_schema
	colName string
	// eg. [db1, db2]
	values []string
	// for table_id, tidb_table_id etc.
	isInteger bool
}

// input: ["a", "b", "c"]
// output: [[], ["a"], ["b"], ["c"], ["a", "b"], ["a", "c"], ["b", "c"], ["a", "b", "c"]]
func (cp colPredicates) generateCombinations(val []string) (res [][]string) {
	switch l := len(val); l {
	case 0:
	case 1:
		res = append(res, []string{})
		if cp.isInteger {
			res = append(res, []string{val[0]})
		} else {
			res = append(res, []string{fmt.Sprintf("'%s'", val[0])})
		}
	default:
		tmp := cp.generateCombinations(val[:l-1])
		for _, t := range tmp {
			res = append(res, t)
			if cp.isInteger {
				t = append(t, val[l-1])
			} else {
				t = append(t, fmt.Sprintf("'%s'", val[l-1]))
			}
			res = append(res, t)
		}
	}
	return
}

func (cp colPredicates) enumerateAll() (res []string) {
	if len(cp.values) == 0 {
		return
	}

	// EQ
	if cp.isInteger {
		for _, value := range cp.values {
			res = append(res, fmt.Sprintf("%s = %s", cp.colName, value))
		}
	} else {
		for _, value := range cp.values {
			res = append(res, fmt.Sprintf("%s = '%s'", cp.colName, value))
			res = append(res, fmt.Sprintf("lower(%s) = '%s'", cp.colName, value))
		}
	}

	combs := cp.generateCombinations(cp.values)
	for _, comb := range combs {
		if len(comb) == 0 {
			continue
		}

		// In
		res = append(res, fmt.Sprintf("%s in (%s)", cp.colName, strings.Join(comb, ",")))
		if !cp.isInteger {
			res = append(res, fmt.Sprintf("lower(%s) in (%s)", cp.colName, strings.Join(comb, ",")))
		}

		// LogicOr
		for i := range comb {
			comb[i] = fmt.Sprintf("%s = %s", cp.colName, comb[i])
		}
		res = append(res, fmt.Sprintf("(%s)", strings.Join(comb, " or ")))
	}
	return
}

type testCase struct {
	memTableName string
	prepareData  func(tk *testkit.TestKit) (names []colPredicates)
	cleanData    func(tk *testkit.TestKit)
}

func prepareDataTables(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_tables1", "schema_tables2"}
	tableNames := []string{"t1", "t2"}
	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})
	ids := []string{}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (a int)", schemaName, tableName))
			tableID := tk.MustQuery(fmt.Sprintf("select tidb_table_id from information_schema.tables where table_schema = '%s' and table_name = '%s'", schemaName, tableName)).String()
			ids = append(ids, tableID)
		}
	}
	names = append(names, colPredicates{colName: core.TidbTableID, values: ids, isInteger: true})
	return
}

func cleanDataTables(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_tables1")
	tk.MustExec("drop database schema_tables2")
}

func prepareDataTiDBIndexes(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_tidb_indexes1", "schema_tidb_indexes2", "schema_tidb_indexes3"}
	tableNames := []string{"t1", "t2", "t3"}
	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})

	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (a int, index i(a))", schemaName, tableName))
		}
	}
	return
}

func cleanDataTiDBIndexes(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_tidb_indexes1")
	tk.MustExec("drop database schema_tidb_indexes2")
	tk.MustExec("drop database schema_tidb_indexes3")
}

func prepareDataViews(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_views1", "schema_views2", "schema_views3"}
	viewNames := []string{"v1", "v2", "v3"}
	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: viewNames})

	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		tk.MustExec(fmt.Sprintf("create table %s.t (c int)", schemaName))
		for _, viewName := range viewNames {
			tk.MustExec(fmt.Sprintf("create view %s.%s as select * from %s.t", schemaName, viewName, schemaName))
		}
	}
	return
}

func cleanDataViews(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_views1")
	tk.MustExec("drop database schema_views2")
	tk.MustExec("drop database schema_views3")
}

func prepareDataKeyColumnUsage(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_key_column_usage1", "schema_key_column_usage2"}
	tableNames := []string{"t1", "t2"}
	constraintNames := []string{"c1", "c2"}
	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})
	names = append(names, colPredicates{colName: core.ConstraintSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.ConstraintName, values: constraintNames})
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (a int)", schemaName, tableName))
			for _, constraintName := range constraintNames {
				tk.MustExec(fmt.Sprintf("alter table %s.%s add constraint %s unique(a)", schemaName, tableName, constraintName))
			}
		}
	}
	return
}

func cleanDataKeyColumnUsage(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_key_column_usage1")
	tk.MustExec("drop database schema_key_column_usage2")
}

func prepareDataPartitions(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_partition1", "schema_partition2"}
	tableNames := []string{"t1", "t2"}
	partitionNames := []string{"p1", "p2"}

	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (a int) partition by list (a) (partition p1 values in (1, 2, 3), partition p2 default)", schemaName, tableName))
		}
	}

	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})
	names = append(names, colPredicates{colName: core.PartitionName, values: partitionNames})
	return
}

func cleanDataPartitions(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_partition1")
	tk.MustExec("drop database schema_partition2")
}

func prepareDataStatistics(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_statistics1", "schema_statistics2"}
	tableNames := []string{"t1", "t2"}
	indexNames := []string{"i1", "i2"}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (i1 int, i2 int, unique key(i1), unique key(i2))", schemaName, tableName))
		}
	}
	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})
	names = append(names, colPredicates{colName: core.IndexName, values: indexNames})
	return
}

func cleanDataStatistics(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_statistics1")
	tk.MustExec("drop database schema_statistics2")
}

func prepareDataSchemata(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_schemata1", "schema_schemata2", "schema_schemata3"}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
	}
	names = append(names, colPredicates{colName: core.SchemaName, values: schemaNames})
	return
}

func cleanDataSchemata(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_schemata1")
	tk.MustExec("drop database schema_schemata2")
	tk.MustExec("drop database schema_schemata3")
}

func prepareDataCheckConstraints(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_check_constraints1", "schema_check_constraints2", "schema_check_constraints3"}
	constrainNames := []string{"c1", "c2", "c3"}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		tk.MustExec(fmt.Sprintf("create table %s.t (a int, constraint c1 check (a > 0), constraint c2 check (a = 0), constraint c3 check (a < 0))", schemaName))
	}
	names = append(names, colPredicates{colName: core.ConstraintSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.ConstraintName, values: constrainNames})
	return
}

func cleanDataCheckConstraints(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_check_constraints1")
	tk.MustExec("drop database schema_check_constraints2")
	tk.MustExec("drop database schema_check_constraints3")
}

func prepareDataTidbCheckConstraints(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_tidb_check_constraints1", "schema_tidb_check_constraints2"}
	tableNames := []string{"t1", "t2"}
	constrainNames := []string{"t1_c", "t2_c"}
	tableIDs := []string{}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (a int, constraint %s_c check (a != 0))", schemaName, tableName, tableName))
			sql := fmt.Sprintf("select distinct table_id from information_schema.tidb_check_constraints where CONSTRAINT_SCHEMA = '%s' and TABLE_NAME = '%s'", schemaName, tableName)
			id := tk.MustQuery(sql).String()
			tableIDs = append(tableIDs, id)
		}
	}
	names = append(names, colPredicates{colName: core.ConstraintSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})
	names = append(names, colPredicates{colName: core.ConstraintName, values: constrainNames})
	names = append(names, colPredicates{colName: core.TableID, values: tableIDs, isInteger: true})
	return
}

func cleanDataTidbCheckConstraints(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_tidb_check_constraints1")
	tk.MustExec("drop database schema_tidb_check_constraints2")
}

func prepareDataSequences(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_sequences1", "schema_sequences2"}
	sequenceNames := []string{"s1", "s2"}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, sequenceName := range sequenceNames {
			tk.MustExec(fmt.Sprintf("create sequence %s.%s", schemaName, sequenceName))
		}
	}
	names = append(names, colPredicates{colName: core.SequenceSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.SequenceName, values: sequenceNames})
	return
}

func cleanDataSequences(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_sequences1")
	tk.MustExec("drop database schema_sequences2")
}

func prepareDataColumns(tk *testkit.TestKit) (names []colPredicates) {
	schemaNames := []string{"schema_columns1", "schema_columns2"}
	tableNames := []string{"t1", "t2"}
	columnNames := []string{"c1", "c2"}
	for _, schemaName := range schemaNames {
		tk.MustExec(fmt.Sprintf("create database %s", schemaName))
		for _, tableName := range tableNames {
			tk.MustExec(fmt.Sprintf("create table %s.%s (c1 int, c2 int)", schemaName, tableName))
		}
	}
	names = append(names, colPredicates{colName: core.TableSchema, values: schemaNames})
	names = append(names, colPredicates{colName: core.TableName, values: tableNames})
	names = append(names, colPredicates{colName: core.ColumnName, values: columnNames})
	return
}

func cleanDataColumns(tk *testkit.TestKit) {
	tk.MustExec("drop database schema_columns1")
	tk.MustExec("drop database schema_columns2")
}

func TestMemtableInfoschemaExtractor(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global tidb_enable_check_constraint = true")
	tcs := []testCase{
		{
			memTableName: infoschema.TableTiDBIndexes,
			prepareData:  prepareDataTiDBIndexes,
			cleanData:    cleanDataTiDBIndexes,
		},
		{
			memTableName: infoschema.TableTables,
			prepareData:  prepareDataTables,
			cleanData:    cleanDataTables,
		},
		{
			memTableName: infoschema.TableViews,
			prepareData:  prepareDataViews,
			cleanData:    cleanDataViews,
		},
		{
			memTableName: infoschema.TableKeyColumn,
			prepareData:  prepareDataKeyColumnUsage,
			cleanData:    cleanDataKeyColumnUsage,
		},
		{
			memTableName: infoschema.TableConstraints,
			prepareData:  prepareDataKeyColumnUsage,
			cleanData:    cleanDataKeyColumnUsage,
		},
		{
			memTableName: infoschema.TablePartitions,
			prepareData:  prepareDataPartitions,
			cleanData:    cleanDataPartitions,
		},
		{
			memTableName: infoschema.TableStatistics,
			prepareData:  prepareDataStatistics,
			cleanData:    cleanDataStatistics,
		},
		{
			memTableName: infoschema.TableSchemata,
			prepareData:  prepareDataSchemata,
			cleanData:    cleanDataSchemata,
		},
		{
			memTableName: infoschema.TableCheckConstraints,
			prepareData:  prepareDataCheckConstraints,
			cleanData:    cleanDataCheckConstraints,
		},
		{
			memTableName: infoschema.TableTiDBCheckConstraints,
			prepareData:  prepareDataTidbCheckConstraints,
			cleanData:    cleanDataTidbCheckConstraints,
		},
		{
			memTableName: infoschema.TableSequences,
			prepareData:  prepareDataSequences,
			cleanData:    cleanDataSequences,
		},
		{
			memTableName: infoschema.TableTiDBIndexUsage,
			prepareData:  prepareDataStatistics,
			cleanData:    cleanDataStatistics,
		},
	}

	countSQL := 0
	for _, tc := range tcs {
		names := tc.prepareData(tk)
		conditions := []string{}
		for i := len(names) - 1; i >= 0; i-- {
			all := names[i].enumerateAll()
			if len(conditions) == 0 {
				conditions = all
			} else {
				newConditions := []string{}
				for _, a := range all {
					for _, b := range conditions {
						newConditions = append(newConditions, fmt.Sprintf("%s and %s", a, b))
					}
				}
				conditions = newConditions
			}
		}

		countSQL += len(conditions)
		for _, c := range conditions {
			sql := fmt.Sprintf("select * from information_schema.%s where %s", tc.memTableName, c)
			tk.MustQuery(sql)
		}

		tc.cleanData(tk)
	}
	fmt.Printf("Total SQLs: %d\n", countSQL)
}
