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
	"os"
	"path/filepath"
	"testing"

	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestMatchColumns(t *testing.T) {
	cases := []struct {
		args     []string
		columns  []string
		accepted []bool
	}{
		{
			args:     nil,
			columns:  []string{"foo"},
			accepted: []bool{false},
		},
		{
			args:     []string{"*"},
			columns:  []string{"foo"},
			accepted: []bool{true},
		},
		{
			args:     []string{"foo*"},
			columns:  []string{"foo", "foo1", "foo2"},
			accepted: []bool{true, true, true},
		},
		{
			args:     []string{"*", "!foo1*"},
			columns:  []string{"foo", "foo1", "foo2"},
			accepted: []bool{true, false, true},
		},
		{
			args: []string{"/^foo/"},
			columns: []string{
				"foo",
				"foo1",
				"fff",
			},
			accepted: []bool{true, true, false},
		},
		{
			args: []string{"*", "!foo[bar]", "!bar?", `!special\\`},
			columns: []string{
				"food",
				"foor",
				"foo[bar]",
				"ba",
				"bar?",
				`special\`,
				`special\\`,
				"bazzz",
				`special\$`,
				`afooa`,
			},
			accepted: []bool{true, false, true, true, false, false, true, true, true, true},
		},
		{
			args: []string{"*", "!/a?b?f[0-9]/"},
			columns: []string{
				"abbdf1",
				"aaaaf2",
				"55",
				"abbcfa",
			},
			accepted: []bool{false, false, true, true},
		},
		{
			args: []string{"BAR"},
			columns: []string{
				"bar",
				"BAR",
			},
			accepted: []bool{true, true},
		},
		{
			args: []string{"# comment", "x", "   \t"},
			columns: []string{
				"x",
				"y",
			},
			accepted: []bool{true, false},
		},
		{
			args: []string{"p_123$", "中文"},
			columns: []string{
				"p_123",
				"p_123$",
				"英文",
				"中文",
			},
			accepted: []bool{false, true, false, true},
		},
		{
			args: []string{`\\\.`},
			columns: []string{
				`\.`,
				`\\\.`,
				`\a`,
			},
			accepted: []bool{true, false, false},
		},
		{
			args: []string{"[!a-z]"},
			columns: []string{
				"!",
				"a",
				"1",
			},
			accepted: []bool{true, false, true},
		},
		{
			args: []string{"\"some \"\"quoted\"\"\""},
			columns: []string{
				`some "quoted"`,
				`some ""quoted""`,
				`SOME "QUOTED"`,
				"some\t\"quoted\"",
			},
			accepted: []bool{true, false, true, false},
		},
		{
			args: []string{"db*", "!cfg*", "cfgsample", "a\\.b\\.c"},
			columns: []string{
				"irrelevant",
				"db1",
				"cfg1",
				"cfgsample",
				"a.b.c",
			},
			accepted: []bool{false, true, false, true, true},
		},
		{
			args: []string{"*", "!D[!a-d]"},
			columns: []string{
				"S",
				"Da",
				"Db",
				"Daa",
				"dD",
				"de",
			},
			accepted: []bool{true, true, true, true, true, false},
		},
		{
			args: []string{"?\\.?"},
			columns: []string{
				"a",
				"a.b",
				"中文.英文",
				"我.你",
			},
			accepted: []bool{false, true, false, true},
		},
		{
			args: []string{"*", "!?\\.?"},
			columns: []string{
				"a",
				".b",
				".英文",
				"我.你",
				"我.你.他",
			},
			accepted: []bool{true, true, true, false, true},
		},
	}

	for _, tc := range cases {
		t.Log("test case =", tc.args)
		columnFilter, err := filter.ParseColumnFilter(tc.args)
		require.NoError(t, err)
		for i, column := range tc.columns {
			require.Equalf(t, tc.accepted[i], columnFilter.MatchColumn(column), "col %s", column)
		}
	}
}

func TestParseFailures(t *testing.T) {
	cases := []struct {
		arg string
		msg string
	}{
		{
			arg: "/^t[0-9]+((?!_copy))*$/",
			msg: ".*: invalid pattern: error parsing regexp:.*",
		},
		{
			arg: "a%b\\.c",
			msg: ".*: unexpected special character '%'",
		},
		{
			arg: `a\tb\.c`,
			msg: `.*: cannot escape a letter or number \(\\t\), it is reserved for future extension`,
		},
		{
			arg: "[]\\.*",
			msg: ".*: syntax error: failed to parse character class",
		},
		{
			arg: "[!]\\.*",
			msg: `.*: invalid pattern: error parsing regexp: missing closing \]:.*`,
		},
		{
			arg: "[.*",
			msg: `.*: syntax error: failed to parse character class`,
		},
		{
			arg: `[\d\D].*`,
			msg: `.*: syntax error: failed to parse character class`,
		},
		{
			arg: "db.",
			msg: ".*: unexpected special character '.'",
		},
		{
			arg: "/db\\.*",
			msg: `.*: syntax error: incomplete regexp`,
		},
		{
			arg: "`db\\.*",
			msg: `.*: syntax error: incomplete quoted identifier`,
		},
		{
			arg: `"db\.*`,
			msg: `.*: syntax error: incomplete quoted identifier`,
		},
		{
			arg: `db\`,
			msg: `.*: syntax error: cannot place \\ at end of line`,
		},
		{
			arg: "db\\.tbl#not comment",
			msg: `.*: unexpected special character '#'`,
		},
	}

	for _, tc := range cases {
		_, err := filter.ParseColumnFilter([]string{tc.arg})
		require.Regexpf(t, tc.msg, err.Error(), "test case = %s", tc.arg)
	}
}

func TestImport(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "1.txt")
	path2 := filepath.Join(dir, "2.txt")
	os.WriteFile(path1, []byte(`
		col?tql?
		col?\.tql?
		col02\.tql02
	`), 0644)
	os.WriteFile(path2, []byte(`
		col03\.tql03
		!col4\.tql4
	`), 0644)

	f, err := filter.ParseColumnFilter([]string{"@" + path1, "@" + path2, "col04\\.tql04"})
	require.NoError(t, err)

	require.True(t, f.MatchColumn("col1tql1"))
	require.True(t, f.MatchColumn("col2.tql2"))
	require.True(t, f.MatchColumn("col3.tql3"))
	require.False(t, f.MatchColumn("col4.tql4"))
	require.False(t, f.MatchColumn("col01tql01"))
	require.False(t, f.MatchColumn("col01.tql01"))
	require.True(t, f.MatchColumn("col02.tql02"))
	require.True(t, f.MatchColumn("col03.tql03"))
	require.True(t, f.MatchColumn("col04.tql04"))
}

func TestRecursiveImport(t *testing.T) {
	dir := t.TempDir()
	path3 := filepath.Join(dir, "3.txt")
	path4 := filepath.Join(dir, "4.txt")
	os.WriteFile(path3, []byte("col1"), 0644)
	os.WriteFile(path4, []byte("# comment\n\n@"+path3), 0644)

	_, err := filter.ParseColumnFilter([]string{"@" + path4})
	require.Regexp(t, `.*4\.txt:3: importing filter files recursively is not allowed`, err.Error())

	_, err = filter.ParseColumnFilter([]string{"@" + filepath.Join(dir, "5.txt")})
	require.Regexp(t, `.*: cannot open filter file: open .*5\.txt: .*`, err.Error())
}
