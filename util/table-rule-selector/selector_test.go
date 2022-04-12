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

package selector

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

var ts = &testSelectorSuite{
	tables: map[string][]string{
		"t*":          {"test*"},
		"schema*":     {"", "test*", "abc*", "xyz"},
		"?bc":         {"t1_abc", "t1_ab?", "abc*"},
		"a?c":         {"t2_abc", "t2_ab*", "a?b"},
		"ab?":         {"t3_ab?", "t3_ab*", "ab?"},
		"ab*":         {"t4_abc", "t4_abc*", "ab*"},
		"abc":         {"abc"},
		"abd":         {"abc"},
		"ik[hjkl]":    {"ik[!zxc]"},
		"ik[f-h]":     {"ik[!a-ce-g]"},
		"i[x-z][1-3]": {"i?[x-z]", "ix*"},
		// [\!-\!], [a-a\--\-], [a-c\--\-f-f].
		"[!]": {"[a-]", "[a-c-f]"},
		// [!a-c\!-\!f-g]
		"[!a-c!f-g]": {"*"},
		// [] match nothing.
		"[]*": {"*"},
	},
	matchCase: []struct {
		schema, table string
		matchedNum    int
		matchedRules  []string //schema, table, schema, table...
	}{
		// test one level
		{"dbc", "t1_abc", 2, []string{"?bc", "t1_ab?", "?bc", "t1_abc"}},
		{"adc", "t2_abc", 2, []string{"a?c", "t2_ab*", "a?c", "t2_abc"}},
		{"abd", "t3_abc", 2, []string{"ab?", "t3_ab*", "ab?", "t3_ab?"}},
		{"abc", "t4_abc", 2, []string{"ab*", "t4_abc", "ab*", "t4_abc*"}},
		{"abc", "abc", 4, []string{"?bc", "abc*", "ab*", "ab*", "ab?", "ab?", "abc", "abc"}},
		// test only schema rule
		{"schema1", "xxx", 1, []string{"schema*", ""}},
		{"schema1", "", 1, []string{"schema*", ""}},
		// test table rule
		{"schema1", "test1", 2, []string{"schema*", "", "schema*", "test*"}},
		{"t1", "test1", 1, []string{"t*", "test*"}},
		{"schema1", "abc1", 2, []string{"schema*", "", "schema*", "abc*"}},
		{"ikj", "ikb", 1, []string{"ik[hjkl]", "ik[!zxc]"}},
		{"ikh", "iky", 2, []string{"ik[hjkl]", "ik[!zxc]", "ik[f-h]", "ik[!a-ce-g]"}},
		{"iz3", "ixz", 2, []string{"i[x-z][1-3]", "i?[x-z]", "i[x-z][1-3]", "ix*"}},
		{"!", "-", 2, []string{"[!]", "[a-]", "[!]", "[a-c-f]"}},
		{"!", "c", 1, []string{"[!]", "[a-c-f]"}},
		{"d", "zxcv", 1, []string{"[!a-c!f-g]", "*"}},
	},
	removeCases: []string{"schema*", "", "a?c", "t2_ab*", "i[x-z][1-3]", "i?[x-z]", "[!]", "[a-c-f]"},
}

type testSelectorSuite struct {
	// for generate rules,
	// we use dummy rule which contains schema and table pattern information to simplify test
	tables map[string][]string

	matchCase []struct {
		schema, table string
		matchedNum    int
		// // for generate rules,
		// we use dummy rule which contains schema(matchedRules[2*i]) and table(matchedRules[2*i+1]) pattern information to simplify test
		matchedRules []string //schema, table, schema, table...
	}

	removeCases []string //schema, table, schema, table ...

	expectedSchemaRules map[string][]interface{}
	expectedTableRules  map[string]map[string][]interface{}
}

func TestSelector(t *testing.T) {
	s := NewTrieSelector()
	ts.expectedSchemaRules, ts.expectedTableRules = testGenerateExpectedRules()

	testInsert(t, s)
	testMatch(t, s)
	testAppend(t, s)
	testReplace(t, s)
	testRemove(t, s)
}

type dummyRule struct {
	description string
}

func testInsert(t *testing.T, s Selector) {
	var err error
	for schema, rules := range ts.expectedSchemaRules {
		err = s.Insert(schema, "", rules[0], Insert)
		require.NoError(t, err)
		// test duplicate error
		err = s.Insert(schema, "", rules[0], Insert)
		require.Error(t, err)
		// test simple replace
		err = s.Insert(schema, "", rules[0], Replace)
		require.NoError(t, err)
	}

	for schema, tables := range ts.expectedTableRules {
		for table, rules := range tables {
			err = s.Insert(schema, table, rules[0], Insert)
			require.NoError(t, err)
			// test duplicate error
			err = s.Insert(schema, table, rules[0], Insert)
			require.Error(t, err)
			// test simple replace
			err = s.Insert(schema, table, rules[0], Replace)
			require.NoError(t, err)
		}
	}

	// insert wrong pattern
	// rule can't be nil
	err = s.Insert("schema", "", nil, Replace)
	require.Error(t, err)
	// asterisk must be the last character of pattern
	err = s.Insert("ab**", "", &dummyRule{"error"}, Replace)
	require.Error(t, err)
	err = s.Insert("abcd", "ab**", &dummyRule{"error"}, Replace)
	require.Error(t, err)

	schemas, tables := s.AllRules()
	require.EqualValues(t, schemas, ts.expectedSchemaRules)
	require.EqualValues(t, tables, ts.expectedTableRules)
}

func testRemove(t *testing.T, s Selector) {
	for i := 0; i < len(ts.removeCases); i += 2 {
		schema, table := ts.removeCases[i], ts.removeCases[i+1]
		err := s.Remove(schema, table)
		require.NoError(t, err)
		err = s.Remove(schema, table)
		require.Error(t, err)

		if len(table) == 0 {
			delete(ts.expectedSchemaRules, schema)
		} else {
			rules, ok := ts.expectedTableRules[schema]
			require.True(t, ok)
			delete(rules, table)
		}
	}

	schemas, tables := s.AllRules()
	require.EqualValues(t, schemas, ts.expectedSchemaRules)
	require.EqualValues(t, tables, ts.expectedTableRules)
}

func testAppend(t *testing.T, s Selector) {
	var (
		err          error
		appendedRule = &dummyRule{description: "append"}
	)
	for schema := range ts.expectedSchemaRules {
		ts.expectedSchemaRules[schema] = append(ts.expectedSchemaRules[schema], appendedRule)
		err = s.Insert(schema, "", appendedRule, Append)
		require.NoError(t, err)
	}
	schemas, tables := s.AllRules()
	require.EqualValues(t, schemas, ts.expectedSchemaRules)
	require.EqualValues(t, tables, ts.expectedTableRules)
}

func testReplace(t *testing.T, s Selector) {
	var (
		err          error
		replacedRule = &dummyRule{"replace"}
	)
	for schema := range ts.expectedSchemaRules {
		ts.expectedSchemaRules[schema] = []interface{}{replacedRule}
		// to prevent it doesn't exist
		err = s.Insert(schema, "", replacedRule, Replace)
		require.NoError(t, err)
		// test replace
		err = s.Insert(schema, "", replacedRule, Replace)
		require.NoError(t, err)
		err = s.Insert(schema, "", replacedRule, Insert)
		require.Error(t, err)

	}

	schemas, tables := s.AllRules()
	require.EqualValues(t, schemas, ts.expectedSchemaRules)
	require.EqualValues(t, tables, ts.expectedTableRules)
}

func testMatch(t *testing.T, s Selector) {
	cache := make(map[string]RuleSet)
	for _, mc := range ts.matchCase {
		rules := s.Match(mc.schema, mc.table)
		expectedRules := make(RuleSet, 0, mc.matchedNum)
		for i := 0; i < mc.matchedNum; i++ {
			rule := &dummyRule{quoteSchemaTable(mc.matchedRules[2*i], mc.matchedRules[2*i+1])}
			expectedRules = append(expectedRules, rule)
		}
		sort.Slice(expectedRules, func(i, j int) bool {
			return expectedRules[i].(*dummyRule).description < expectedRules[j].(*dummyRule).description
		})
		sort.Slice(rules, func(i, j int) bool {
			return rules[i].(*dummyRule).description < rules[j].(*dummyRule).description
		})
		require.EqualValues(t, rules, expectedRules)
		cache[quoteSchemaTable(mc.schema, mc.table)] = expectedRules
	}

	// test cache
	trie, ok := s.(*trieSelector)
	require.True(t, ok)
	for _, cacheItem := range trie.cache {
		sort.Slice(cacheItem, func(i, j int) bool {
			return cacheItem[i].(*dummyRule).description < cacheItem[j].(*dummyRule).description
		})
	}
	require.EqualValues(t, trie.cache, cache)

	// test not mathced
	rule := s.Match("t1", "")
	require.Nil(t, rule)
	cache[quoteSchemaTable("t1", "")] = rule

	rule = s.Match("t1", "abc")
	require.Nil(t, rule)
	cache[quoteSchemaTable("t1", "abc")] = rule

	rule = s.Match("xxx", "abc")
	require.Nil(t, rule)
	cache[quoteSchemaTable("xxx", "abc")] = rule
	require.EqualValues(t, trie.cache, cache)
}

func testGenerateExpectedRules() (map[string][]interface{}, map[string]map[string][]interface{}) {
	schemaRules := make(map[string][]interface{})
	tableRules := make(map[string]map[string][]interface{})
	for schema, tables := range ts.tables {
		_, ok := tableRules[schema]
		if !ok {
			tableRules[schema] = make(map[string][]interface{})
		}
		for _, table := range tables {
			if len(table) == 0 {
				schemaRules[schema] = []interface{}{&dummyRule{quoteSchemaTable(schema, "")}}
			} else {
				tableRules[schema][table] = []interface{}{&dummyRule{quoteSchemaTable(schema, table)}}
			}
		}
	}

	return schemaRules, tableRules
}
