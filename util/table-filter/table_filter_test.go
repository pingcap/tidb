// Copyright 2021 PingCAP, Inc.
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

func TestMatchTables(t *testing.T) {
	cases := []struct {
		args       []string
		tables     []filter.Table
		acceptedCS []bool
		acceptedCI []bool
	}{
		{
			args: nil,
			tables: []filter.Table{
				{Schema: "foo", Name: "bar"},
			},
			acceptedCS: []bool{false},
			acceptedCI: []bool{false},
		},
		{
			args: []string{"*.*"},
			tables: []filter.Table{
				{Schema: "foo", Name: "bar"},
			},
			acceptedCS: []bool{true},
			acceptedCI: []bool{true},
		},
		{
			args: []string{"foo.*"},
			tables: []filter.Table{
				{Schema: "foo", Name: "bar"},
				{Schema: "foo1", Name: "bar"},
				{Schema: "foo2", Name: "bar"},
			},
			acceptedCS: []bool{true, false, false},
			acceptedCI: []bool{true, false, false},
		},
		{
			args: []string{"*.*", "!foo1.*"},
			tables: []filter.Table{
				{Schema: "foo", Name: "bar"},
				{Schema: "foo1", Name: "bar"},
				{Schema: "foo2", Name: "bar"},
			},
			acceptedCS: []bool{true, false, true},
			acceptedCI: []bool{true, false, true},
		},
		{
			args: []string{"foo.bar1"},
			tables: []filter.Table{
				{Schema: "foo", Name: "bar"},
				{Schema: "foo", Name: "bar1"},
				{Schema: "fff", Name: "bar1"},
			},
			acceptedCS: []bool{false, true, false},
			acceptedCI: []bool{false, true, false},
		},
		{
			args: []string{"*.*", "!foo.bar"},
			tables: []filter.Table{
				{Schema: "foo", Name: "bar"},
				{Schema: "foo", Name: "bar1"},
				{Schema: "fff", Name: "bar1"},
			},
			acceptedCS: []bool{false, true, true},
			acceptedCI: []bool{false, true, true},
		},
		{
			args: []string{"/^foo/.*", `!/^foo/./^sbtest-\d/`},
			tables: []filter.Table{
				{Schema: "foo", Name: "sbtest"},
				{Schema: "foo1", Name: "sbtest-1"},
				{Schema: "fff", Name: "bar"},
			},
			acceptedCS: []bool{true, false, false},
			acceptedCI: []bool{true, false, false},
		},
		{
			args: []string{"*.*", "!foo[bar].*", "!bar?.*", `!special\\.*`},
			tables: []filter.Table{
				{Schema: "foor", Name: "a"},
				{Schema: "foo[bar]", Name: "b"},
				{Schema: "ba", Name: "c"},
				{Schema: "bar?", Name: "d"},
				{Schema: `special\`, Name: "e"},
				{Schema: `special\\`, Name: "f"},
				{Schema: "bazzz", Name: "g"},
				{Schema: `special\$`, Name: "h"},
				{Schema: `afooa`, Name: "i"},
			},
			acceptedCS: []bool{false, true, true, false, false, true, true, true, true},
			acceptedCI: []bool{false, true, true, false, false, true, true, true, true},
		},
		{
			args: []string{"*.*", "!/^FOO/.*", "!*./FoO$/"},
			tables: []filter.Table{
				{Schema: "FOO1", Name: "a"},
				{Schema: "foo2", Name: "b"},
				{Schema: "BoO3", Name: "cFoO"},
				{Schema: "Foo4", Name: "dfoo"},
				{Schema: "5", Name: "5"},
			},
			acceptedCS: []bool{false, true, false, true, true},
			acceptedCI: []bool{false, false, false, false, true},
		},
		{
			args: []string{"*.*", "!a?b?./f[0-9]/"},
			tables: []filter.Table{
				{Schema: "abbd", Name: "f1"},
				{Schema: "aaaa", Name: "f2"},
				{Schema: "5", Name: "5"},
				{Schema: "abbc", Name: "fa"},
			},
			acceptedCS: []bool{false, true, true, true},
			acceptedCI: []bool{false, true, true, true},
		},
		{
			args: []string{"*.*", "!/t[0-8]/.a??"},
			tables: []filter.Table{
				{Schema: "t1", Name: "a01"},
				{Schema: "t9", Name: "a02"},
				{Schema: "5", Name: "5"},
				{Schema: "t8", Name: "a001"},
			},
			acceptedCS: []bool{false, true, true, true},
			acceptedCI: []bool{false, true, true, true},
		},
		{
			args: []string{"*.*", "!a*.A*"},
			tables: []filter.Table{
				{Schema: "aB", Name: "Ab"},
				{Schema: "AaB", Name: "aab"},
				{Schema: "acB", Name: "Afb"},
			},
			acceptedCS: []bool{false, true, false},
			acceptedCI: []bool{false, false, false},
		},
		{
			args: []string{"BAR.*"},
			tables: []filter.Table{
				{Schema: "bar", Name: "a"},
				{Schema: "BAR", Name: "a"},
			},
			acceptedCS: []bool{false, true},
			acceptedCI: []bool{true, true},
		},
		{
			args: []string{"# comment", "x.y", "   \t"},
			tables: []filter.Table{
				{Schema: "x", Name: "y"},
				{Schema: "y", Name: "y"},
			},
			acceptedCS: []bool{true, false},
			acceptedCI: []bool{true, false},
		},
		{
			args: []string{"p_123$.45", "中文.表名"},
			tables: []filter.Table{
				{Schema: "p_123", Name: "45"},
				{Schema: "p_123$", Name: "45"},
				{Schema: "英文", Name: "表名"},
				{Schema: "中文", Name: "表名"},
			},
			acceptedCS: []bool{false, true, false, true},
			acceptedCI: []bool{false, true, false, true},
		},
		{
			args: []string{`\\\..*`},
			tables: []filter.Table{
				{Schema: `\.`, Name: "a"},
				{Schema: `\\\.`, Name: "b"},
				{Schema: `\a`, Name: "c"},
			},
			acceptedCS: []bool{true, false, false},
			acceptedCI: []bool{true, false, false},
		},
		{
			args: []string{"[!a-z].[^a-z]"},
			tables: []filter.Table{
				{Schema: "!", Name: "z"},
				{Schema: "!", Name: "^"},
				{Schema: "!", Name: "9"},
				{Schema: "a", Name: "z"},
				{Schema: "a", Name: "^"},
				{Schema: "a", Name: "9"},
				{Schema: "1", Name: "z"},
				{Schema: "1", Name: "^"},
				{Schema: "1", Name: "9"},
			},
			acceptedCS: []bool{true, true, false, false, false, false, true, true, false},
			acceptedCI: []bool{true, true, false, false, false, false, true, true, false},
		},
		{
			args: []string{"\"some \"\"quoted\"\"\".`identifiers?`"},
			tables: []filter.Table{
				{Schema: `some "quoted"`, Name: "identifiers?"},
				{Schema: `some "quoted"`, Name: "identifiers!"},
				{Schema: `some ""quoted""`, Name: "identifiers?"},
				{Schema: `SOME "QUOTED"`, Name: "IDENTIFIERS?"},
				{Schema: "some\t\"quoted\"", Name: "identifiers?"},
			},
			acceptedCS: []bool{true, false, false, false, false},
			acceptedCI: []bool{true, false, false, true, false},
		},
		{
			args: []string{"db*.*", "!*.cfg*", "*.cfgsample"},
			tables: []filter.Table{
				{Schema: "irrelevant", Name: "table"},
				{Schema: "db1", Name: "tbl1"},
				{Schema: "db1", Name: "cfg1"},
				{Schema: "db1", Name: "cfgsample"},
				{Schema: "else", Name: "cfgsample"},
			},
			acceptedCS: []bool{false, true, false, true, true},
			acceptedCI: []bool{false, true, false, true, true},
		},
		{
			args: []string{"*.*", "!S.D[!a-d]"},
			tables: []filter.Table{
				{Schema: "S", Name: "D1"},
				{Schema: "S", Name: "Da"},
				{Schema: "S", Name: "Db"},
				{Schema: "S", Name: "Daa"},
			},
			acceptedCS: []bool{false, true, true, true},
			acceptedCI: []bool{false, true, true, true},
		},
		{
			args: []string{"*.*", "!S.D[a-d]"},
			tables: []filter.Table{
				{Schema: "S", Name: "D1"},
				{Schema: "S", Name: "Da"},
				{Schema: "S", Name: "Db"},
				{Schema: "S", Name: "Daa"},
			},
			acceptedCS: []bool{true, false, false, true},
			acceptedCI: []bool{true, false, false, true},
		},
	}

	for _, tc := range cases {
		t.Log("test case =", tc.args)
		fcs, err := filter.Parse(tc.args)
		require.NoError(t, err)
		fci := filter.CaseInsensitive(fcs)
		for i, tbl := range tc.tables {
			require.Equalf(t, fcs.MatchTable(tbl.Schema, tbl.Name), tc.acceptedCS[i], "cs tbl %v", tbl)
			require.Equalf(t, fci.MatchTable(tbl.Schema, tbl.Name), tc.acceptedCI[i], "ci tbl %v", tbl)
		}
	}
}

func TestMatchSchemas(t *testing.T) {
	cases := []struct {
		args       []string
		schemas    []string
		acceptedCS []bool
		acceptedCI []bool
	}{
		{
			args:       nil,
			schemas:    []string{"foo"},
			acceptedCS: []bool{false},
			acceptedCI: []bool{false},
		},
		{
			args:       []string{"*.*"},
			schemas:    []string{"foo"},
			acceptedCS: []bool{true},
			acceptedCI: []bool{true},
		},
		{
			args:       []string{"foo.*"},
			schemas:    []string{"foo", "foo1"},
			acceptedCS: []bool{true, false},
			acceptedCI: []bool{true, false},
		},
		{
			args:       []string{"*.*", "!foo1.*"},
			schemas:    []string{"foo", "foo1"},
			acceptedCS: []bool{true, false},
			acceptedCI: []bool{true, false},
		},
		{
			args:       []string{"foo.bar1"},
			schemas:    []string{"foo", "foo1"},
			acceptedCS: []bool{true, false},
			acceptedCI: []bool{true, false},
		},
		{
			args:       []string{"*.*", "!foo.bar"},
			schemas:    []string{"foo", "foo1"},
			acceptedCS: []bool{true, true},
			acceptedCI: []bool{true, true},
		},
		{
			args:       []string{"/^foo/.*", `!/^foo/./^sbtest-\d/`},
			schemas:    []string{"foo", "foo2"},
			acceptedCS: []bool{true, true},
			acceptedCI: []bool{true, true},
		},
		{
			args:       []string{"*.*", "!FOO*.*", "!*.*FoO"},
			schemas:    []string{"foo", "FOO", "foobar", "FOOBAR", "bar", "BAR"},
			acceptedCS: []bool{true, false, true, false, true, true},
			acceptedCI: []bool{false, false, false, false, true, true},
		},
	}

	for _, tc := range cases {
		t.Log("test case =", tc.args)
		fcs, err := filter.Parse(tc.args)
		require.NoError(t, err)
		fci := filter.CaseInsensitive(fcs)
		for i, schema := range tc.schemas {
			require.Equalf(t, tc.acceptedCS[i], fcs.MatchSchema(schema), "cs schema %s", schema)
			require.Equalf(t, tc.acceptedCI[i], fci.MatchSchema(schema), "ci schema %s", schema)
		}
	}
}

func TestParseFailures2(t *testing.T) {
	cases := []struct {
		arg string
		msg string
	}{
		{
			arg: "/^t[0-9]+((?!_copy).)*$/.*",
			msg: ".*: invalid pattern: error parsing regexp:.*",
		},
		{
			arg: "/^t[0-9]+sp(?=copy).*/.*",
			msg: ".*: invalid pattern: error parsing regexp:.*",
		},
		{
			arg: "a.b.c",
			msg: ".*: syntax error: stray characters after table pattern",
		},
		{
			arg: "a%b.c",
			msg: ".*: unexpected special character '%'",
		},
		{
			arg: `a\tb.c`,
			msg: `.*: cannot escape a letter or number \(\\t\), it is reserved for future extension`,
		},
		{
			arg: "[].*",
			msg: ".*: syntax error: failed to parse character class",
		},
		{
			arg: "[!].*",
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
			arg: "db",
			msg: `.*: missing table pattern`,
		},
		{
			arg: "db.",
			msg: `.*: syntax error: missing pattern`,
		},
		{
			arg: "`db`*.*",
			msg: `.*: syntax error: missing '\.' between schema and table patterns`,
		},
		{
			arg: "/db.*",
			msg: `.*: syntax error: incomplete regexp`,
		},
		{
			arg: "`db.*",
			msg: `.*: syntax error: incomplete quoted identifier`,
		},
		{
			arg: `"db.*`,
			msg: `.*: syntax error: incomplete quoted identifier`,
		},
		{
			arg: `db\`,
			msg: `.*: syntax error: cannot place \\ at end of line`,
		},
		{
			arg: "db.tbl#not comment",
			msg: `.*: unexpected special character '#'`,
		},
	}

	for _, tc := range cases {
		_, err := filter.Parse([]string{tc.arg})
		require.Regexpf(t, tc.msg, err, "test case = %s", tc.arg)
	}
}

func TestImport2(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "1.txt")
	path2 := filepath.Join(dir, "2.txt")
	os.WriteFile(path1, []byte(`
		db?.tbl?
		db02.tbl02
	`), 0644)
	os.WriteFile(path2, []byte(`
		db03.tbl03
		!db4.tbl4
	`), 0644)

	f, err := filter.Parse([]string{"@" + path1, "@" + path2, "db04.tbl04"})
	require.NoError(t, err)

	require.True(t, f.MatchTable("db1", "tbl1"))
	require.True(t, f.MatchTable("db2", "tbl2"))
	require.True(t, f.MatchTable("db3", "tbl3"))
	require.False(t, f.MatchTable("db4", "tbl4"))
	require.False(t, f.MatchTable("db01", "tbl01"))
	require.True(t, f.MatchTable("db02", "tbl02"))
	require.True(t, f.MatchTable("db03", "tbl03"))
	require.True(t, f.MatchTable("db04", "tbl04"))
}

func TestRecursiveImport2(t *testing.T) {
	dir := t.TempDir()
	path3 := filepath.Join(dir, "3.txt")
	path4 := filepath.Join(dir, "4.txt")
	os.WriteFile(path3, []byte("db1.tbl1"), 0644)
	os.WriteFile(path4, []byte("# comment\n\n@"+path3), 0644)

	_, err := filter.Parse([]string{"@" + path4})
	require.Regexp(t, `.*4\.txt:3: importing filter files recursively is not allowed`, err.Error())

	_, err = filter.Parse([]string{"@" + filepath.Join(dir, "5.txt")})
	require.Regexp(t, `.*: cannot open filter file: open .*5\.txt: .*`, err.Error())
}

func TestAll(t *testing.T) {
	f := filter.All()
	require.True(t, f.MatchTable("db1", "tbl1"))
	require.True(t, f.MatchSchema("db1"))

	f = filter.CaseInsensitive(f)
	require.True(t, f.MatchTable("db1", "tbl1"))
	require.True(t, f.MatchSchema("db1"))
}
