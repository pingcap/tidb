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

package regexprrouter

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/util/filter"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/stretchr/testify/require"
)

func TestCreateRouter(t *testing.T) {
	_, err := NewRegExprRouter(true, []*router.TableRule{})
	require.Equal(t, nil, err)
	_, err = NewRegExprRouter(false, []*router.TableRule{})
	require.Equal(t, nil, err)
}

func TestAddRule(t *testing.T) {
	r, err := NewRegExprRouter(true, []*router.TableRule{})
	require.Equal(t, nil, err)
	rules := []*router.TableRule{
		{
			SchemaPattern: "test1",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "test2",
			TablePattern:  "table2",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
	}
	for _, rule := range rules {
		err = r.AddRule(rule)
		require.Equal(t, nil, err)
	}
	r, err = NewRegExprRouter(false, []*router.TableRule{})
	require.Equal(t, nil, err)
	for _, rule := range rules {
		err := r.AddRule(rule)
		require.Equal(t, nil, err)
	}
}

func TestSchemaRoute(t *testing.T) {
	rules := []*router.TableRule{
		{
			SchemaPattern: "test1",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "gtest*",
			TargetSchema:  "dtest",
		},
	}
	oldRouter, err := router.NewTableRouter(true, rules)
	require.Equal(t, nil, err)
	newRouter, err := NewRegExprRouter(true, rules)
	require.Equal(t, nil, err)
	inputTables := []filter.Table{
		{
			Schema: "test1", // match rule 1
			Name:   "table1",
		},
		{
			Schema: "gtesttest", // match rule 2
			Name:   "atable",
		},
		{
			Schema: "ptest", // match neither
			Name:   "atableg",
		},
	}
	expectedResult := []filter.Table{
		{
			Schema: "dtest1",
			Name:   "table1",
		},
		{
			Schema: "dtest",
			Name:   "atable",
		},
		{
			Schema: "ptest",
			Name:   "atableg",
		},
	}
	for idx := range inputTables {
		schema, table := inputTables[idx].Schema, inputTables[idx].Name
		expSchema, expTable := expectedResult[idx].Schema, expectedResult[idx].Name
		oldSchema, oldTable, err := oldRouter.Route(schema, table)
		require.Equal(t, nil, err)
		newSchema, newTable, err := newRouter.Route(schema, table)
		require.Equal(t, nil, err)
		require.Equal(t, expSchema, oldSchema)
		require.Equal(t, expTable, oldTable)
		require.Equal(t, expSchema, newSchema)
		require.Equal(t, expTable, newTable)
	}
}

func TestTableRoute(t *testing.T) {
	rules := []*router.TableRule{
		{
			SchemaPattern: "test1",
			TablePattern:  "table1",
			TargetSchema:  "dtest1",
			TargetTable:   "dtable1",
		},
		{
			SchemaPattern: "test*",
			TablePattern:  "table2",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "test3",
			TablePattern:  "table*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
	}
	inputTables := []*filter.Table{}
	expTables := []*filter.Table{}
	for i := 1; i <= 3; i++ {
		inputTables = append(inputTables, &filter.Table{
			Schema: fmt.Sprintf("test%d", i),
			Name:   fmt.Sprintf("table%d", i),
		})
		expTables = append(expTables, &filter.Table{
			Schema: fmt.Sprintf("dtest%d", i),
			Name:   fmt.Sprintf("dtable%d", i),
		})
	}
	oldRouter, err := router.NewTableRouter(true, rules)
	require.Equal(t, nil, err)
	newRouter, err := NewRegExprRouter(true, rules)
	require.Equal(t, nil, err)
	for i := range inputTables {
		schema, table := inputTables[i].Schema, inputTables[i].Name
		expSchema, expTable := expTables[i].Schema, expTables[i].Name
		oldSch, oldTbl, _ := oldRouter.Route(schema, table)
		newSch, newTbl, _ := newRouter.Route(schema, table)
		require.Equal(t, expSchema, newSch)
		require.Equal(t, expTable, newTbl)
		require.Equal(t, expSchema, oldSch)
		require.Equal(t, expTable, oldTbl)
	}
}

func TestRegExprRoute(t *testing.T) {
	rules := []*router.TableRule{
		{
			SchemaPattern: "~test.[0-9]+",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[animal|human]",
			TablePattern:  "~tbl.*[cat|dog]+",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "~test3_(schema)?.*",
			TablePattern:  "test3_*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
		{
			SchemaPattern: "test4s_*",
			TablePattern:  "~testtable_[donot_delete]?",
			TargetSchema:  "dtest4",
			TargetTable:   "dtable4",
		},
	}
	inputTable := []filter.Table{
		{
			Schema: "tests100",
			Name:   "table1", // match rule 1
		},
		{
			Schema: "test2animal",
			Name:   "tbl_animal_dogcat", // match rule 2
		},
		{
			Schema: "test3_schema_meta",
			Name:   "test3_tail", // match rule 3
		},
		{
			Schema: "test4s_2022",
			Name:   "testtable_donot_delete", // match rule 4
		},
		{
			Schema: "mytst5566",
			Name:   "gtable", // match nothing
		},
	}
	expectedOutput := []filter.Table{
		{
			Schema: "dtest1",
			Name:   "table1",
		},
		{
			Schema: "dtest2",
			Name:   "dtable2",
		},
		{
			Schema: "dtest3",
			Name:   "dtable3",
		},
		{
			Schema: "dtest4",
			Name:   "dtable4",
		},
		{
			Schema: "mytst5566",
			Name:   "gtable",
		},
	}
	newRouter, err := NewRegExprRouter(true, rules)
	require.Equal(t, nil, err)
	for idx := range inputTable {
		s, n := inputTable[idx].Schema, inputTable[idx].Name
		expSchm, expName := expectedOutput[idx].Schema, expectedOutput[idx].Name
		newSchm, newName, err := newRouter.Route(s, n)
		require.Equal(t, nil, err)
		require.Equal(t, expSchm, newSchm)
		require.Equal(t, expName, newName)
	}
}

func TestFetchExtendColumn(t *testing.T) {
	rules := []*router.TableRule{
		{
			SchemaPattern: "schema*",
			TablePattern:  "t*",
			TargetSchema:  "test",
			TargetTable:   "t",
			TableExtractor: &router.TableExtractor{
				TargetColumn: "table_name",
				TableRegexp:  "table_(.*)",
			},
			SchemaExtractor: &router.SchemaExtractor{
				TargetColumn: "schema_name",
				SchemaRegexp: "schema_(.*)",
			},
			SourceExtractor: &router.SourceExtractor{
				TargetColumn: "source_name",
				SourceRegexp: "source_(.*)_(.*)",
			},
		},
		{
			SchemaPattern: "~s?chema.*",
			TargetSchema:  "test",
			TargetTable:   "t2",
			SchemaExtractor: &router.SchemaExtractor{
				TargetColumn: "schema_name",
				SchemaRegexp: "(.*)",
			},
			SourceExtractor: &router.SourceExtractor{
				TargetColumn: "source_name",
				SourceRegexp: "(.*)",
			},
		},
	}
	r, err := NewRegExprRouter(false, rules)
	require.NoError(t, err)
	expected := [][]string{
		{"table_name", "schema_name", "source_name"},
		{"t1", "s1", "s1s1"},

		{"schema_name", "source_name"},
		{"schema_s2", "source_s2"},
	}

	// table level rules have highest priority
	extendCol, extendVal := r.FetchExtendColumn("schema_s1", "table_t1", "source_s1_s1")
	require.Equal(t, extendCol, expected[0])
	require.Equal(t, extendVal, expected[1])

	// only schema rules
	extendCol2, extendVal2 := r.FetchExtendColumn("schema_s2", "a_table_t2", "source_s2")
	require.Equal(t, extendCol2, expected[2])
	require.Equal(t, extendVal2, expected[3])
}

func TestAllRule(t *testing.T) {
	rules := []*router.TableRule{
		{
			SchemaPattern: "~test.[0-9]+",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[animal|human]",
			TablePattern:  "~tbl.*[cat|dog]+",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "~test3_(schema)?.*",
			TablePattern:  "test3_*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
		{
			SchemaPattern: "test4s_*",
			TablePattern:  "~testtable_[donot_delete]?",
			TargetSchema:  "dtest4",
			TargetTable:   "dtable4",
		},
	}
	r, err := NewRegExprRouter(true, rules)
	require.Equal(t, nil, err)
	schemaRules, tableRules := r.AllRules()
	require.Equal(t, 1, len(schemaRules))
	require.Equal(t, 3, len(tableRules))
	require.Equal(t, rules[0].SchemaPattern, schemaRules[0].SchemaPattern)
	for i := 0; i < 3; i++ {
		require.Equal(t, rules[i+1].SchemaPattern, tableRules[i].SchemaPattern)
		require.Equal(t, rules[i+1].TablePattern, tableRules[i].TablePattern)
	}
}

func TestDupMatch(t *testing.T) {
	rules := []*router.TableRule{
		{
			SchemaPattern: "~test[0-9]+.*",
			TablePattern:  "~.*",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[a|b]",
			TablePattern:  "~tbl2",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "mytest*",
			TargetSchema:  "mytest",
		},
		{
			SchemaPattern: "~mytest(_meta)?_schema",
			TargetSchema:  "test",
		},
	}
	inputTables := []filter.Table{
		{
			Schema: "test2a", // match rule1 and rule2
			Name:   "tbl2",
		},
		{
			Schema: "mytest_meta_schema", // match rule3 and rule4
			Name:   "",
		},
	}
	r, err := NewRegExprRouter(true, rules)
	require.Equal(t, nil, err)
	for i := range inputTables {
		targetSchm, targetTbl, err := r.Route(inputTables[i].Schema, inputTables[i].Name)
		require.Equal(t, "", targetSchm)
		require.Equal(t, "", targetTbl)
		require.Regexp(t, ".*matches more than one rule.*", err.Error())
	}
}
