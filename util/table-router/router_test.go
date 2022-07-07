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

package router

import (
	"testing"

	selector "github.com/pingcap/tidb/util/table-rule-selector"
	"github.com/stretchr/testify/require"
)

func TestRoute(t *testing.T) {
	rules := []*TableRule{
		{SchemaPattern: "Test_1_*", TablePattern: "abc*", TargetSchema: "t1", TargetTable: "abc"},
		{SchemaPattern: "test_1_*", TablePattern: "test*", TargetSchema: "t2", TargetTable: "test"},
		{SchemaPattern: "test_1_*", TablePattern: "", TargetSchema: "test", TargetTable: ""},
		{SchemaPattern: "test_2_*", TablePattern: "abc*", TargetSchema: "t1", TargetTable: "abc"},
		{SchemaPattern: "test_2_*", TablePattern: "test*", TargetSchema: "t2", TargetTable: "test"},
	}

	cases := [][]string{
		{"test_1_a", "abc1", "t1", "abc"},
		{"test_2_a", "abc2", "t1", "abc"},
		{"test_1_a", "test1", "t2", "test"},
		{"test_2_a", "test2", "t2", "test"},
		{"test_1_a", "xyz", "test", "xyz"},
	}

	// initial table router
	router, err := NewTableRouter(false, rules)
	require.NoError(t, err)

	// insert duplicate rules
	for _, rule := range rules {
		err = router.AddRule(rule)
		require.Error(t, err)
	}
	for _, cs := range cases {
		schema, table, err := router.Route(cs[0], cs[1])
		require.NoError(t, err)
		require.Equal(t, cs[2], schema)
		require.Equal(t, cs[3], table)
	}

	// update rules
	rules[0].TargetTable = "xxx"
	cases[0][3] = "xxx"
	err = router.UpdateRule(rules[0])
	require.NoError(t, err)
	for _, cs := range cases {
		schema, table, err := router.Route(cs[0], cs[1])
		require.NoError(t, err)
		require.Equal(t, cs[2], schema)
		require.Equal(t, cs[3], table)
	}

	// remove rule
	err = router.RemoveRule(rules[0])
	require.NoError(t, err)
	// remove not existing rule
	err = router.RemoveRule(rules[0])
	require.Error(t, err)
	schema, table, err := router.Route(cases[0][0], cases[0][1])
	require.NoError(t, err)
	require.Equal(t, "test", schema)
	require.Equal(t, "abc1", table)
	// delete removed rule
	rules = rules[1:]
	cases = cases[1:]

	// mismatched
	schema, _, err = router.Route("test_3_a", "")
	require.NoError(t, err)
	require.Equal(t, "test_3_a", schema)
	// test multiple schema level rules
	err = router.AddRule(&TableRule{SchemaPattern: "test_*", TablePattern: "", TargetSchema: "error", TargetTable: ""})
	require.NoError(t, err)
	_, _, err = router.Route("test_1_a", "")
	require.Error(t, err)
	// test multiple table level rules
	err = router.AddRule(&TableRule{SchemaPattern: "test_1_*", TablePattern: "tes*", TargetSchema: "error", TargetTable: "error"})
	require.NoError(t, err)
	_, _, err = router.Route("test_1_a", "test")
	require.Error(t, err)
	// invalid rule
	err = router.Selector.Insert("test_1_*", "abc*", "error", selector.Insert)
	require.NoError(t, err)
	_, _, err = router.Route("test_1_a", "abc")
	require.Error(t, err)

	// Add/Update invalid table route rule
	inValidRule := &TableRule{
		SchemaPattern: "test*",
		TablePattern:  "abc*",
	}
	err = router.AddRule(inValidRule)
	require.Error(t, err)
	err = router.UpdateRule(inValidRule)
	require.Error(t, err)
}

func TestCaseSensitive(t *testing.T) {
	// we test case insensitive in TestRoute
	rules := []*TableRule{
		{SchemaPattern: "Test_1_*", TablePattern: "abc*", TargetSchema: "t1", TargetTable: "abc"},
		{SchemaPattern: "test_1_*", TablePattern: "test*", TargetSchema: "t2", TargetTable: "test"},
		{SchemaPattern: "test_1_*", TablePattern: "", TargetSchema: "test", TargetTable: ""},
		{SchemaPattern: "test_2_*", TablePattern: "abc*", TargetSchema: "t1", TargetTable: "abc"},
		{SchemaPattern: "test_2_*", TablePattern: "test*", TargetSchema: "t2", TargetTable: "test"},
	}

	cases := [][]string{
		{"test_1_a", "abc1", "test", "abc1"},
		{"test_2_a", "abc2", "t1", "abc"},
		{"test_1_a", "test1", "t2", "test"},
		{"test_2_a", "test2", "t2", "test"},
		{"test_1_a", "xyz", "test", "xyz"},
	}

	// initial table router
	router, err := NewTableRouter(true, rules)
	require.NoError(t, err)

	// insert duplicate rules
	for _, rule := range rules {
		err = router.AddRule(rule)
		require.Error(t, err)
	}
	for _, cs := range cases {
		schema, table, err := router.Route(cs[0], cs[1])
		require.NoError(t, err)
		require.Equal(t, cs[2], schema)
		require.Equal(t, cs[3], table)
	}
}

func TestFetchExtendColumn(t *testing.T) {
	rules := []*TableRule{
		{
			SchemaPattern: "schema*",
			TablePattern:  "t*",
			TargetSchema:  "test",
			TargetTable:   "t",
			TableExtractor: &TableExtractor{
				TargetColumn: "table_name",
				TableRegexp:  "table_(.*)",
			},
			SchemaExtractor: &SchemaExtractor{
				TargetColumn: "schema_name",
				SchemaRegexp: "schema_(.*)",
			},
			SourceExtractor: &SourceExtractor{
				TargetColumn: "source_name",
				SourceRegexp: "source_(.*)_(.*)",
			},
		},
		{
			SchemaPattern: "schema*",
			TargetSchema:  "test",
			TargetTable:   "t2",
			SchemaExtractor: &SchemaExtractor{
				TargetColumn: "schema_name",
				SchemaRegexp: "(.*)",
			},
			SourceExtractor: &SourceExtractor{
				TargetColumn: "source_name",
				SourceRegexp: "(.*)",
			},
		},
	}
	r, err := NewTableRouter(false, rules)
	require.NoError(t, err)
	expected := [][]string{
		{"table_name", "schema_name", "source_name"},
		{"t1", "s1", "s1s1"},

		{"schema_name", "source_name"},
		{"schema_s2", "source_s2"},
	}

	// table level rules have highest priority
	extendCol, extendVal := r.FetchExtendColumn("schema_s1", "table_t1", "source_s1_s1")
	require.Equal(t, expected[0], extendCol)
	require.Equal(t, expected[1], extendVal)

	// only schema rules
	extendCol2, extendVal2 := r.FetchExtendColumn("schema_s2", "a_table_t2", "source_s2")
	require.Equal(t, expected[2], extendCol2)
	require.Equal(t, expected[3], extendVal2)
}
