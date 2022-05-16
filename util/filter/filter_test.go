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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func cloneTables(tbs []*Table) []*Table {
	if tbs == nil {
		return nil
	}
	newTbs := make([]*Table, 0, len(tbs))
	for _, tb := range tbs {
		newTbs = append(newTbs, tb.Clone())
	}
	return newTbs
}

func TestFilterOnSchema(t *testing.T) {
	cases := []struct {
		rules         *Rules
		Input         []*Table
		Output        []*Table
		caseSensitive bool
	}{
		// empty rules
		{
			rules: &Rules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  nil,
			Output: nil,
		},
		{
			rules: &Rules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
			Output: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		// schema-only rules
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		// DoTable rules(Without regex)
		{
			rules: &Rules{
				DoTables: []*Table{{Schema: "foo", Name: "bar1"}},
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
			Output: []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}},
		},
		// ignoreTable rules(Without regex)
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "foo", Name: "bar"}},
				DoTables:     nil,
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
			Output: []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
		},
		{
			// all regexp
			rules: &Rules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*Table{{Schema: "~^foo", Name: "~^sbtest-\\d"}},
			},
			Input:  []*Table{{Schema: "foo", Name: "sbtest"}, {Schema: "foo1", Name: "sbtest-1"}, {Schema: "foo2", Name: ""}, {Schema: "fff", Name: "bar"}},
			Output: []*Table{{Schema: "foo", Name: "sbtest"}, {Schema: "foo2", Name: ""}},
		},
		// test rule with * or ?
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*Table{{Schema: "foor", Name: "a"}, {Schema: "foo[bar]", Name: "b"}, {Schema: "fo", Name: "c"}, {Schema: "foo?", Name: "d"}, {Schema: "special\\", Name: "e"}},
			Output: []*Table{{Schema: "foo[bar]", Name: "b"}, {Schema: "fo", Name: "c"}},
		},
		// ensure non case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:  []*Table{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoO"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output: []*Table{{Schema: "5", Name: "5"}},
		},
		// ensure case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:         []*Table{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output:        []*Table{{Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			caseSensitive: true,
		},
		// test the rule whose schema part is not regex and the table part is regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "a?b?", Name: "~f[0-9]"}},
			},
			Input:  []*Table{{Schema: "abbd", Name: "f1"}, {Schema: "aaaa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "fa"}},
			Output: []*Table{{Schema: "aaaa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "fa"}},
		},
		// test the rule whose schema part is regex and the table part is not regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "~t[0-8]", Name: "a??"}},
			},
			Input:  []*Table{{Schema: "t1", Name: "a01"}, {Schema: "t9", Name: "a02"}, {Schema: "5", Name: "5"}, {Schema: "t9", Name: "a001"}},
			Output: []*Table{{Schema: "t9", Name: "a02"}, {Schema: "5", Name: "5"}, {Schema: "t9", Name: "a001"}},
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "a*", Name: "A*"}},
			},
			Input:         []*Table{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output:        []*Table{{Schema: "AaB", Name: "aab"}},
			caseSensitive: true,
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "a*", Name: "A*"}},
			},
			Input:  []*Table{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output: nil,
		},
	}

	for _, tt := range cases {
		ft, err := New(tt.caseSensitive, tt.rules)
		require.NoError(t, err)
		originInput := cloneTables(tt.Input)
		got := ft.ApplyOn(tt.Input)
		t.Logf("got %+v, expected %+v", got, tt.Output)
		require.Equal(t, tt.Input, originInput)
		require.Equal(t, tt.Output, got)
	}
}

func TestCaseSensitiveApply(t *testing.T) {
	cases := []struct {
		rules         *Rules
		Input         []*Table
		Output        []*Table
		caseSensitive bool
	}{
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}, {Schema: "foo1", Name: "bar"}, {Schema: "foo1", Name: ""}},
			Output: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: ""}},
		},
		// ignoreTable rules(Without regex)
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "Foo", Name: "bAr"}},
				DoTables:     nil,
			},
			Input:  []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
			Output: []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo", Name: ""}, {Schema: "fff", Name: "bar1"}},
		},
		{
			// all regexp
			rules: &Rules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*Table{{Schema: "~^foo", Name: "~^sbtest-\\d"}},
			},
			Input:  []*Table{{Schema: "foo", Name: "sbtest"}, {Schema: "foo1", Name: "sbtest-1"}, {Schema: "foo2", Name: ""}, {Schema: "fff", Name: "bar"}},
			Output: []*Table{{Schema: "foo", Name: "sbtest"}, {Schema: "foo2", Name: ""}},
		},
		// test rule with * or ?
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*Table{{Schema: "foor", Name: "a"}, {Schema: "foo[bar]", Name: "b"}, {Schema: "Fo", Name: "c"}, {Schema: "foo?", Name: "d"}, {Schema: "special\\", Name: "e"}},
			Output: []*Table{{Schema: "foo[bar]", Name: "b"}, {Schema: "Fo", Name: "c"}},
		},
		// ensure non case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:  []*Table{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoO"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output: []*Table{{Schema: "5", Name: "5"}},
		},
		// ensure case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{Schema: "~.*", Name: "~FoO$"}},
			},
			Input:         []*Table{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			Output:        []*Table{{Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoo"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}},
			caseSensitive: true,
		},
		// test the rule whose schema part is not regex and the table part is regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "a?b?", Name: "~f[0-9]"}},
			},
			Input:  []*Table{{Schema: "abBd", Name: "f1"}, {Schema: "aAAa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "FA"}},
			Output: []*Table{{Schema: "aAAa", Name: "f2"}, {Schema: "5", Name: "5"}, {Schema: "abbc", Name: "FA"}},
		},
		// test the rule whose schema part is regex and the table part is not regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "~t[0-8]", Name: "A??"}},
			},
			Input:  []*Table{{Schema: "t1", Name: "a01"}, {Schema: "t9", Name: "A02"}, {Schema: "5", Name: "5"}, {Schema: "T9", Name: "a001"}},
			Output: []*Table{{Schema: "t9", Name: "A02"}, {Schema: "5", Name: "5"}, {Schema: "T9", Name: "a001"}},
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "a*", Name: "A*"}},
			},
			Input:         []*Table{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output:        []*Table{{Schema: "AaB", Name: "aab"}},
			caseSensitive: true,
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{Schema: "a*", Name: "A*"}},
			},
			Input:  []*Table{{Schema: "aB", Name: "Ab"}, {Schema: "AaB", Name: "aab"}, {Schema: "acB", Name: "Afb"}},
			Output: []*Table{},
		},
	}

	for _, tt := range cases {
		ft, err := New(tt.caseSensitive, tt.rules)
		require.NoError(t, err)
		originInput := cloneTables(tt.Input)
		got := ft.Apply(tt.Input)
		t.Logf("got %+v, expected %+v", got, tt.Output)
		require.Equal(t, tt.Input, originInput)
		require.Equal(t, tt.Output, got)
	}
}

func TestMaxBox(t *testing.T) {
	rules := &Rules{
		DoTables: []*Table{
			{Schema: "test1", Name: "t1"},
		},
		IgnoreTables: []*Table{
			{Schema: "test1", Name: "t2"},
		},
	}

	r, err := New(false, rules)
	require.NoError(t, err)

	x := &Table{Schema: "test1"}
	res := r.ApplyOn([]*Table{x})
	require.Len(t, res, 1)
	require.Equal(t, x, res[0])
}

func TestCaseSensitive(t *testing.T) {
	// ensure case-sensitive rules are really case-sensitive
	rules := &Rules{
		IgnoreDBs:    []string{"~^FOO"},
		IgnoreTables: []*Table{{Schema: "~.*", Name: "~FoO$"}},
	}
	r, err := New(true, rules)
	require.NoError(t, err)

	input := []*Table{{Schema: "FOO1", Name: "a"}, {Schema: "foo2", Name: "b"}, {Schema: "BoO3", Name: "cFoO"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}}
	actual := r.ApplyOn(input)
	expected := []*Table{{Schema: "foo2", Name: "b"}, {Schema: "Foo4", Name: "dfoo"}, {Schema: "5", Name: "5"}}
	t.Logf("got %+v, expected %+v", actual, expected)
	require.Equal(t, expected, actual)

	inputTable := &Table{Schema: "FOO", Name: "a"}
	require.False(t, r.Match(inputTable))

	rules = &Rules{
		DoDBs: []string{"BAR"},
	}

	r, err = New(false, rules)
	require.NoError(t, err)
	inputTable = &Table{Schema: "bar", Name: "a"}
	require.True(t, r.Match(inputTable))

	require.NoError(t, err)

	inputTable = &Table{Schema: "BAR", Name: "a"}
	originInputTable := inputTable.Clone()
	require.True(t, r.Match(inputTable))
	require.Equal(t, inputTable, originInputTable)
}

func TestInvalidRegex(t *testing.T) {
	cases := []struct {
		rules *Rules
	}{
		{
			rules: &Rules{
				DoDBs: []string{"~^t[0-9]+((?!_copy).)*$"},
			},
		},
		{
			rules: &Rules{
				DoDBs: []string{"~^t[0-9]+sp(?=copy).*"},
			},
		},
	}
	for _, tc := range cases {
		_, err := New(true, tc.rules)
		require.Error(t, err)
	}
}

func TestMatchReturnsBool(t *testing.T) {
	rules := &Rules{
		DoDBs: []string{"sns"},
	}
	f, err := New(true, rules)
	require.NoError(t, err)
	require.True(t, f.Match(&Table{Schema: "sns"}))
	require.False(t, f.Match(&Table{Schema: "other"}))
	f, err = New(true, nil)
	require.NoError(t, err)
	require.True(t, f.Match(&Table{Schema: "other"}))
}
