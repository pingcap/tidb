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

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSelectorSuite{
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
})

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

func (t *testSelectorSuite) TestSelector(c *C) {
	s := NewTrieSelector()
	t.expectedSchemaRules, t.expectedTableRules = t.testGenerateExpectedRules()

	t.testInsert(c, s)
	t.testMatch(c, s)
	t.testAppend(c, s)
	t.testReplace(c, s)
	t.testRemove(c, s)
}

type dummyRule struct {
	description string
}

func (t *testSelectorSuite) testInsert(c *C, s Selector) {
	var err error
	for schema, rules := range t.expectedSchemaRules {
		err = s.Insert(schema, "", rules[0], Insert)
		c.Assert(err, IsNil)
		// test duplicate error
		err = s.Insert(schema, "", rules[0], Insert)
		c.Assert(err, NotNil)
		// test simple replace
		err = s.Insert(schema, "", rules[0], Replace)
		c.Assert(err, IsNil)
	}

	for schema, tables := range t.expectedTableRules {
		for table, rules := range tables {
			err = s.Insert(schema, table, rules[0], Insert)
			c.Assert(err, IsNil)
			// test duplicate error
			err = s.Insert(schema, table, rules[0], Insert)
			c.Assert(err, NotNil)
			// test simple replace
			err = s.Insert(schema, table, rules[0], Replace)
			c.Assert(err, IsNil)
		}
	}

	// insert wrong pattern
	// rule can't be nil
	err = s.Insert("schema", "", nil, Replace)
	c.Assert(err, NotNil)
	// asterisk must be the last character of pattern
	err = s.Insert("ab**", "", &dummyRule{"error"}, Replace)
	c.Assert(err, NotNil)
	err = s.Insert("abcd", "ab**", &dummyRule{"error"}, Replace)
	c.Assert(err, NotNil)

	schemas, tables := s.AllRules()
	c.Assert(schemas, DeepEquals, t.expectedSchemaRules)
	c.Assert(tables, DeepEquals, t.expectedTableRules)
}

func (t *testSelectorSuite) testRemove(c *C, s Selector) {
	for i := 0; i < len(t.removeCases); i += 2 {
		schema, table := t.removeCases[i], t.removeCases[i+1]
		err := s.Remove(schema, table)
		c.Assert(err, IsNil)
		err = s.Remove(schema, table)
		c.Assert(err, NotNil)

		if len(table) == 0 {
			delete(t.expectedSchemaRules, schema)
		} else {
			rules, ok := t.expectedTableRules[schema]
			c.Assert(ok, IsTrue)
			delete(rules, table)
		}
	}

	schemas, tables := s.AllRules()
	c.Assert(schemas, DeepEquals, t.expectedSchemaRules)
	c.Assert(tables, DeepEquals, t.expectedTableRules)
}

func (t *testSelectorSuite) testAppend(c *C, s Selector) {
	var (
		err          error
		appendedRule = &dummyRule{description: "append"}
	)
	for schema := range t.expectedSchemaRules {
		t.expectedSchemaRules[schema] = append(t.expectedSchemaRules[schema], appendedRule)
		err = s.Insert(schema, "", appendedRule, Append)
		c.Assert(err, IsNil)
	}
	schemas, tables := s.AllRules()
	c.Assert(schemas, DeepEquals, t.expectedSchemaRules)
	c.Assert(tables, DeepEquals, t.expectedTableRules)
}

func (t *testSelectorSuite) testReplace(c *C, s Selector) {
	var (
		err          error
		replacedRule = &dummyRule{"replace"}
	)
	for schema := range t.expectedSchemaRules {
		t.expectedSchemaRules[schema] = []interface{}{replacedRule}
		// to prevent it doesn't exist
		err = s.Insert(schema, "", replacedRule, Replace)
		c.Assert(err, IsNil)
		// test replace
		err = s.Insert(schema, "", replacedRule, Replace)
		c.Assert(err, IsNil)
		err = s.Insert(schema, "", replacedRule, Insert)
		c.Assert(err, NotNil)

	}

	schemas, tables := s.AllRules()
	c.Assert(schemas, DeepEquals, t.expectedSchemaRules)
	c.Assert(tables, DeepEquals, t.expectedTableRules)
}

func (t *testSelectorSuite) testMatch(c *C, s Selector) {
	cache := make(map[string]RuleSet)
	for _, mc := range t.matchCase {
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
		c.Assert(rules, DeepEquals, expectedRules)
		cache[quoteSchemaTable(mc.schema, mc.table)] = expectedRules
	}

	// test cache
	trie, ok := s.(*trieSelector)
	c.Assert(ok, IsTrue)
	for _, cacheItem := range trie.cache {
		sort.Slice(cacheItem, func(i, j int) bool {
			return cacheItem[i].(*dummyRule).description < cacheItem[j].(*dummyRule).description
		})
	}
	c.Assert(trie.cache, DeepEquals, cache)

	// test not mathced
	rule := s.Match("t1", "")
	c.Assert(rule, IsNil)
	cache[quoteSchemaTable("t1", "")] = rule

	rule = s.Match("t1", "abc")
	c.Assert(rule, IsNil)
	cache[quoteSchemaTable("t1", "abc")] = rule

	rule = s.Match("xxx", "abc")
	c.Assert(rule, IsNil)
	cache[quoteSchemaTable("xxx", "abc")] = rule
	c.Assert(trie.cache, DeepEquals, cache)
}

func (t *testSelectorSuite) testGenerateExpectedRules() (map[string][]interface{}, map[string]map[string][]interface{}) {
	schemaRules := make(map[string][]interface{})
	tableRules := make(map[string]map[string][]interface{})
	for schema, tables := range t.tables {
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
