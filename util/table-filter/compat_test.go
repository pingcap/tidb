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

	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestSchemaFilter(t *testing.T) {
	sf0 := filter.CaseInsensitive(filter.NewSchemasFilter("foo?", "bar"))
	require.True(t, sf0.MatchTable("foo?", "a"))
	require.False(t, sf0.MatchTable("food", "a"))
	require.True(t, sf0.MatchTable("bar", "b"))
	require.True(t, sf0.MatchTable("BAR", "b"))

	sf1 := filter.NewSchemasFilter(`\baz`)
	require.False(t, sf1.MatchSchema("baz"))
	require.False(t, sf1.MatchSchema("Baz"))
	require.True(t, sf1.MatchSchema(`\baz`))
	require.False(t, sf1.MatchSchema(`\Baz`))

	sf2 := filter.NewSchemasFilter()
	require.False(t, sf2.MatchTable("aaa", "bbb"))
}

func TestTableFilter(t *testing.T) {
	tf0 := filter.CaseInsensitive(filter.NewTablesFilter(
		filter.Table{Schema: "foo?", Name: "bar*"},
		filter.Table{Schema: "BAR?", Name: "FOO*"},
	))
	require.True(t, tf0.MatchTable("foo?", "bar*"))
	require.True(t, tf0.MatchTable("bar?", "foo*"))
	require.True(t, tf0.MatchTable("FOO?", "BAR*"))
	require.False(t, tf0.MatchTable("foo?", "bar"))
	require.False(t, tf0.MatchTable("BARD", "FOO*"))

	tf1 := filter.NewTablesFilter(
		filter.Table{Schema: `\baz`, Name: `BAR`},
	)
	require.False(t, tf1.MatchSchema("baz"))
	require.False(t, tf1.MatchSchema("Baz"))
	require.True(t, tf1.MatchSchema(`\baz`))
	require.False(t, tf1.MatchSchema(`\Baz`))

	tf2 := filter.NewTablesFilter()
	require.False(t, tf2.MatchTable("aaa", "bbb"))
}

func TestLegacyFilter(t *testing.T) {
	cases := []struct {
		rules    filter.MySQLReplicationRules
		accepted []filter.Table
		rejected []filter.Table
	}{
		{
			rules: filter.MySQLReplicationRules{},
			accepted: []filter.Table{
				{Schema: "foo", Name: "bar"},
			},
			rejected: nil,
		},
		{
			rules: filter.MySQLReplicationRules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			accepted: []filter.Table{
				{Schema: "foo", Name: "bar"},
			},
			rejected: []filter.Table{
				{Schema: "foo1", Name: "bar"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				IgnoreDBs: []string{"foo1"},
			},
			accepted: []filter.Table{
				{Schema: "foo", Name: "bar"},
			},
			rejected: []filter.Table{
				{Schema: "foo1", Name: "bar"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				DoTables: []*filter.Table{{Schema: "foo", Name: "bar1"}},
			},
			accepted: []filter.Table{
				{Schema: "foo", Name: "bar1"},
			},
			rejected: []filter.Table{
				{Schema: "foo", Name: "bar"},
				{Schema: "foo1", Name: "bar"},
				{Schema: "foo1", Name: "bar1"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				IgnoreTables: []*filter.Table{{Schema: "foo", Name: "bar"}},
			},
			accepted: []filter.Table{
				{Schema: "foo", Name: "bar1"},
				{Schema: "foo1", Name: "bar"},
				{Schema: "foo1", Name: "bar1"},
			},
			rejected: []filter.Table{
				{Schema: "foo", Name: "bar"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*filter.Table{{Schema: "~^foo", Name: `~^sbtest-\d`}},
			},
			accepted: []filter.Table{
				{Schema: "foo", Name: "sbtest"},
				{Schema: "foo", Name: `sbtest-\d`},
			},
			rejected: []filter.Table{
				{Schema: "fff", Name: "bar"},
				{Schema: "foo1", Name: "sbtest-1"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				IgnoreDBs: []string{"foo[bar]", "baz?", `special\`},
			},
			accepted: []filter.Table{
				{Schema: "foo[bar]", Name: "1"},
				{Schema: "food", Name: "2"},
				{Schema: "fo", Name: "3"},
				{Schema: `special\\`, Name: "4"},
				{Schema: "bazzz", Name: "9"},
				{Schema: `special\$`, Name: "10"},
				{Schema: `afooa`, Name: "11"},
			},
			rejected: []filter.Table{
				{Schema: "foor", Name: "5"},
				{Schema: "baz?", Name: "6"},
				{Schema: "baza", Name: "7"},
				{Schema: `special\`, Name: "8"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				DoDBs: []string{`!@#$%^&*\?`},
			},
			accepted: []filter.Table{
				{Schema: `!@#$%^&abcdef\g`, Name: "1"},
			},
			rejected: []filter.Table{
				{Schema: "abcdef", Name: "2"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				DoDBs: []string{"1[!abc]", "2[^abc]", `3[\d]`},
			},
			accepted: []filter.Table{
				{Schema: "1!", Name: "1"},
				{Schema: "1z", Name: "4"},
				{Schema: "2^", Name: "3"},
				{Schema: "2a", Name: "5"},
				{Schema: "3d", Name: "6"},
				{Schema: `3\`, Name: "8"},
			},
			rejected: []filter.Table{
				{Schema: "1a", Name: "2"},
				{Schema: "30", Name: "7"},
			},
		},
		{
			rules: filter.MySQLReplicationRules{
				DoDBs:    []string{"foo", "bar"},
				DoTables: []*filter.Table{{Schema: "*", Name: "a"}, {Schema: "*", Name: "b"}},
			},
			accepted: []filter.Table{
				{Schema: "foo", Name: "a"},
				{Schema: "foo", Name: "b"},
				{Schema: "bar", Name: "a"},
				{Schema: "bar", Name: "b"},
			},
			rejected: []filter.Table{
				{Schema: "foo", Name: "c"},
				{Schema: "baz", Name: "a"},
			},
		},
	}

	f, err := filter.ParseMySQLReplicationRules(nil)
	require.NoError(t, err)
	require.True(t, f.MatchTable("foo", "bar"))

	for _, tc := range cases {
		t.Log("test case =", tc.rules)
		f, err := filter.ParseMySQLReplicationRules(&tc.rules)
		f = filter.CaseInsensitive(f)
		require.NoError(t, err)
		for _, tbl := range tc.accepted {
			require.True(t, f.MatchTable(tbl.Schema, tbl.Name), "accept case %v", tbl)
		}
		for _, tbl := range tc.rejected {
			require.False(t, f.MatchTable(tbl.Schema, tbl.Name), "reject case %v", tbl)
		}
	}
}

func TestParseLegacyFailures(t *testing.T) {
	cases := []struct {
		arg string
		msg string
	}{
		{
			arg: "[a",
			msg: `error parsing regexp: missing closing \]:.*`,
		},
		{
			arg: "",
			msg: "pattern cannot be empty",
		},
	}

	for _, tc := range cases {
		_, err := filter.ParseMySQLReplicationRules(&filter.MySQLReplicationRules{
			DoDBs: []string{tc.arg},
		})
		require.Regexpf(t, tc.msg, err.Error(), "test case = %s", tc.arg)
	}
}
