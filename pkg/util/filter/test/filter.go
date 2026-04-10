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

package filter_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/stretchr/testify/require"
)

func cloneTables(tbs []*filter.ExportTable) []*filter.ExportTable {
	if tbs == nil {
		return nil
	}
	newTbs := make([]*filter.ExportTable, 0, len(tbs))
	for _, tb := range tbs {
		newTbs = append(newTbs, tb.Clone())
	}
	return newTbs
}

func RunFilterOnSchema(t *testing.T) {
	cases := []struct {
		rules         *filter.ExportRules
		Input         []*filter.ExportTable
		Output        []*filter.ExportTable
		caseSensitive bool
	}{
		// empty rules
		{
			rules: &filter.ExportRules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  nil,
			Output: nil,
		},
		{
			rules: &filter.ExportRules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		// schema-only rules
		{
			rules: &filter.ExportRules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		{
			rules: &filter.ExportRules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		// DoTable rules(Without regex)
		{
			rules: &filter.ExportRules{
				DoTables: []*filter.ExportTable{{Schema: "foo", Name: "bar1"}},
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}},
		},
		// ignoreTable rules(Without regex)
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "foo", Name: "bar"}},
				DoTables:     nil,
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
		},
		{
			// all regexp
			rules: &filter.ExportRules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*filter.ExportTable{{Schema: "~^foo", Name: "~^sbtest-\\d"}},
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "sbtest"}, {Schema: "foo1", Name: "sbtest-1"}, {Schema: "foo2", Name: ""}, {Schema: "fff", Name: "bar"}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "sbtest"}, {Schema: "foo2", Name: ""}},
		},
		// test rule with * or ?
		{
			rules: &filter.ExportRules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*filter.ExportTable{{Schema: "foor", Name: "a"}, {Schema: "foo[bar]", Name: "b"}, {Schema: "fo", Name: "c"}, {Schema: "foo?", Name: "d"}, {Schema: "special\\", Name: "e"}},
			Output: []*filter.ExportTable{{Schema: "foo[bar]", Name: "b"}, {Schema: "fo", Name: "c"}},
		},
		// ensure non case-insensitive
		{
			rules: &filter.ExportRules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*filter.ExportTable{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:  []*filter.ExportTable{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoO"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output: []*filter.ExportTable{{Schema: "5", Name: "5"}},
		},
		// ensure case-insensitive
		{
			rules: &filter.ExportRules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*filter.ExportTable{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:         []*filter.ExportTable{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output:        []*filter.ExportTable{{Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			caseSensitive: true,
		},
		// test the rule whose schema part is not regex and the table part is regex.
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "a?b?", Name: "~f[0-9]"}},
			},
			Input:  []*filter.ExportTable{{Schema: "abbd", Name: "f1"}, {Schema: "aaaa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "fa"}},
			Output: []*filter.ExportTable{{Schema: "aaaa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "fa"}},
		},
		// test the rule whose schema part is regex and the table part is not regex.
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "~t[0-8]", Name: "a??"}},
			},
			Input:  []*filter.ExportTable{{Schema: "t1", Name: "a01"}, {Schema: "t9", Name: "a02"}, {Schema: "5", Name: "5"}, {Schema: "t9", Name: "a001"}},
			Output: []*filter.ExportTable{{Schema: "t9", Name: "a02"}, {Schema: "5", Name: "5"}, {Schema: "t9", Name: "a001"}},
		},
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "a*", Name: "A*"}},
			},
			Input:         []*filter.ExportTable{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output:        []*filter.ExportTable{{Schema: "AaB", Name: "aab"}},
			caseSensitive: true,
		},
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "a*", Name: "A*"}},
			},
			Input:  []*filter.ExportTable{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output: nil,
		},
	}

	for _, tt := range cases {
		ft, err := filter.New(tt.caseSensitive, tt.rules)
		require.NoError(t, err)
		originInput := cloneTables(tt.Input)
		got := ft.ApplyOn(tt.Input)
		t.Logf("got %+v, expected %+v", got, tt.Output)
		require.Equal(t, tt.Input, originInput)
		require.Equal(t, tt.Output, got)
	}
}

func RunCaseSensitiveApply(t *testing.T) {
	cases := []struct {
		rules         *filter.ExportRules
		Input         []*filter.ExportTable
		Output        []*filter.ExportTable
		caseSensitive bool
	}{
		{
			rules: &filter.ExportRules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		{
			rules: &filter.ExportRules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		// ignoreTable rules(Without regex)
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "Foo", Name: "bAr"}},
				DoTables:     nil,
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
		},
		{
			// all regexp
			rules: &filter.ExportRules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*filter.ExportTable{{Schema: "~^foo", Name: "~^sbtest-\\d"}},
			},
			Input:  []*filter.ExportTable{{Schema: "foo", Name: "sbtest"}, {Schema: "foo1", Name: "sbtest-1"}, {Schema: "foo2", Name: ""}, {Schema: "fff", Name: "bar"}},
			Output: []*filter.ExportTable{{Schema: "foo", Name: "sbtest"}, {Schema: "foo2", Name: ""}},
		},
		// test rule with * or ?
		{
			rules: &filter.ExportRules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*filter.ExportTable{{Schema: "foor", Name: "a"}, {Schema: "foo[bar]", Name: "b"}, {Schema: "Fo", Name: "c"}, {Schema: "foo?", Name: "d"}, {Schema: "special\\", Name: "e"}},
			Output: []*filter.ExportTable{{Schema: "foo[bar]", Name: "b"}, {Schema: "Fo", Name: "c"}},
		},
		// ensure non case-insensitive
		{
			rules: &filter.ExportRules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*filter.ExportTable{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:  []*filter.ExportTable{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoO"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output: []*filter.ExportTable{{Schema: "5", Name: "5"}},
		},
		// ensure case-insensitive
		{
			rules: &filter.ExportRules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*filter.ExportTable{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:         []*filter.ExportTable{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output:        []*filter.ExportTable{{Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			caseSensitive: true,
		},
		// test the rule whose schema part is not regex and the table part is regex.
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "a?b?", Name: "~f[0-9]"}},
			},
			Input:  []*filter.ExportTable{{Schema: "abBd", Name: "f1"}, {Schema: "aAAa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "FA"}},
			Output: []*filter.ExportTable{{Schema: "aAAa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "FA"}},
		},
		// test the rule whose schema part is regex and the table part is not regex.
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "~t[0-8]", Name: "A??"}},
			},
			Input:  []*filter.ExportTable{{Schema: "t1", Name: "a01"}, {Schema: "t9", Name: "A02"}, {Schema: "5", Name: "5"}, {Schema: "T9", Name: "a001"}},
			Output: []*filter.ExportTable{{Schema: "t9", Name: "A02"}, {Schema: "5", Name: "5"}, {Schema: "T9", Name: "a001"}},
		},
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "a*", Name: "A*"}},
			},
			Input:         []*filter.ExportTable{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output:        []*filter.ExportTable{{Schema: "AaB", Name: "aab"}},
			caseSensitive: true,
		},
		{
			rules: &filter.ExportRules{
				IgnoreTables: []*filter.ExportTable{{Schema: "a*", Name: "A*"}},
			},
			Input:  []*filter.ExportTable{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output: []*filter.ExportTable{},
		},
	}

	for _, tt := range cases {
		ft, err := filter.New(tt.caseSensitive, tt.rules)
		require.NoError(t, err)
		originInput := cloneTables(tt.Input)
		got := ft.Apply(tt.Input)
		t.Logf("got %+v, expected %+v", got, tt.Output)
		require.Equal(t, tt.Input, originInput)
		require.Equal(t, tt.Output, got)
	}
}

func RunMaxBox(t *testing.T) {
	rules := &filter.ExportRules{
		DoTables: []*filter.ExportTable{
			{Schema: "test1", Name: "t1"},
		},
		IgnoreTables: []*filter.ExportTable{
			{Schema: "test1", Name: "t2"},
		},
	}

	r, err := filter.New(false, rules)
	require.NoError(t, err)

	x := &filter.ExportTable{Schema: "test1"}
	res := r.ApplyOn([]*filter.ExportTable{x})
	require.Len(t, res, 1)
	require.Equal(t, x, res[0])
}

func RunCaseSensitive(t *testing.T) {
	// ensure case-sensitive rules are really case-sensitive
	rules := &filter.ExportRules{
		IgnoreDBs:    []string{"~^FOO"},
		IgnoreTables: []*filter.ExportTable{{Schema: "~.*", Name: "~FoO$"}},
	}
	r, err := filter.New(true, rules)
	require.NoError(t, err)

	input := []*filter.ExportTable{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoO"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}}
	actual := r.ApplyOn(input)
	expected := []*filter.ExportTable{{Schema: "foo2", Name: "b"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}}
	t.Logf("got %+v, expected %+v", actual, expected)
	require.Equal(t, expected, actual)

	inputTable := &filter.ExportTable{Schema: "FOO", Name: "a"}
	require.False(t, r.Match(inputTable))

	rules = &filter.ExportRules{
		DoDBs: []string{"BAR"},
	}

	r, err = filter.New(false, rules)
	require.NoError(t, err)
	inputTable = &filter.ExportTable{Schema: "bar", Name: "a"}
	require.True(t, r.Match(inputTable))

	require.NoError(t, err)

	inputTable = &filter.ExportTable{Schema: "BAR", Name: "a"}
	originInputTable := inputTable.Clone()
	require.True(t, r.Match(inputTable))
	require.Equal(t, inputTable, originInputTable)
}

func RunInvalidRegex(t *testing.T) {
	cases := []struct {
		rules *filter.ExportRules
	}{
		{
			rules: &filter.ExportRules{
				DoDBs: []string{"~^t[0-9]+((?!_copy).)*$"},
			},
		},
		{
			rules: &filter.ExportRules{
				DoDBs: []string{"~^t[0-9]+sp(?=copy).*"},
			},
		},
	}
	for _, tc := range cases {
		_, err := filter.New(true, tc.rules)
		require.Error(t, err)
	}
}

func RunMatchReturnsBool(t *testing.T) {
	rules := &filter.ExportRules{
		DoDBs: []string{"sns"},
	}
	f, err := filter.New(true, rules)
	require.NoError(t, err)
	require.True(t, f.Match(&filter.ExportTable{Schema: "sns"}))
	require.False(t, f.Match(&filter.ExportTable{Schema: "other"}))
	f, err = filter.New(true, nil)
	require.NoError(t, err)
	require.True(t, f.Match(&filter.ExportTable{Schema: "other"}))
}
