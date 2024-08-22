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
	prepareData  func(t *testing.T, tk *testkit.TestKit) (names []colPredicates)
	cleanData    func(t *testing.T, tk *testkit.TestKit)
}

func prepareDataTables(_ *testing.T, tk *testkit.TestKit) (names []colPredicates) {
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

func cleanDataTables(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("drop database schema_tables1")
	tk.MustExec("drop database schema_tables2")
}

func prepareDataTiDBIndexes(_ *testing.T, tk *testkit.TestKit) (names []colPredicates) {
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

func cleanDataTiDBIndexes(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("drop database schema_tidb_indexes1")
	tk.MustExec("drop database schema_tidb_indexes2")
	tk.MustExec("drop database schema_tidb_indexes3")
}

func TestMemtableInfoschemaExtractor(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

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

		// TODO: add more infoschema tables
	}

	countSQL := 0
	for _, tc := range tcs {
		names := tc.prepareData(t, tk)
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

		tc.cleanData(t, tk)
	}
	fmt.Printf("Total SQLs: %d\n", countSQL)
}
