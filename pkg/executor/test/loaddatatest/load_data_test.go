// Copyright 2023 PingCAP, Inc.
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

package loaddatatest

import (
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	data        []byte
	expected    []string
	expectedMsg string
}

func checkCases(
	tests []testCase,
	loadSQL string,
	t *testing.T,
	tk *testkit.TestKit,
	ctx sessionctx.Context,
	selectSQL, deleteSQL string,
) {
	for _, tt := range tests {
		var reader io.ReadCloser = mydump.NewStringReader(string(tt.data))
		var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (
			r io.ReadCloser, err error,
		) {
			return reader, nil
		}

		ctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
		tk.MustExec(loadSQL)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		for _, w := range warnings {
			fmt.Printf("warnnig: %#v\n", w.Err.Error())
		}
		require.Equal(t, tt.expectedMsg, tk.Session().LastMessage(), tt.expected)
		tk.MustQuery(selectSQL).Check(testkit.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func TestLoadDataInitParam(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists load_data_test")
	tk.MustExec("create table load_data_test (a varchar(10), b varchar(10))")

	require.ErrorIs(t, tk.ExecToErr("load data infile '' into table load_data_test"),
		exeerrors.ErrLoadDataEmptyPath)
	require.ErrorContains(t, tk.ExecToErr("load data infile '/a' into table load_data_test fields defined null by 'a' optionally enclosed"),
		"must specify FIELDS [OPTIONALLY] ENCLOSED BY")
	require.ErrorContains(t, tk.ExecToErr("load data infile '/a' into table load_data_test lines terminated by ''"),
		"LINES TERMINATED BY is empty")
	require.ErrorContains(t, tk.ExecToErr("load data infile '/a' into table load_data_test fields enclosed by 'a' terminated by 'a'"),
		"must not be prefix of each other")

	// null def values
	testFunc := func(sql string, expectedNullDef []string, expectedNullOptEnclosed bool) {
		require.ErrorContains(t, tk.ExecToErr(sql), "reader is nil")
		defer ctx.SetValue(executor.LoadDataVarKey, nil)
		ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataWorker)
		require.True(t, ok)
		require.NotNil(t, ld)
		require.Equal(t, expectedNullDef, ld.GetController().FieldNullDef)
		require.Equal(t, expectedNullOptEnclosed, ld.GetController().NullValueOptEnclosed)
	}
	testFunc("load data local infile '/a' into table load_data_test",
		[]string{"\\N"}, false)
	testFunc("load data local infile '/a' into table load_data_test fields enclosed by ''",
		[]string{"\\N"}, false)
	testFunc("load data local infile '/a' into table load_data_test fields defined null by 'a'",
		[]string{"a", "\\N"}, false)
	testFunc("load data local infile '/a' into table load_data_test fields enclosed by 'b' defined null by 'a' optionally enclosed",
		[]string{"a", "\\N"}, true)
	testFunc("load data local infile '/a' into table load_data_test fields enclosed by 'b'",
		[]string{"NULL", "\\N"}, false)
	testFunc("load data local infile '/a' into table load_data_test fields enclosed by 'b' escaped by ''",
		[]string{"NULL"}, false)

	// positive case
	require.ErrorContains(
		t, tk.ExecToErr(
			"load data local infile '/a' format 'sql file' into table"+
				" load_data_test",
		), "reader is nil",
	)
	ctx.SetValue(executor.LoadDataVarKey, nil)
	require.ErrorContains(
		t, tk.ExecToErr(
			"load data local infile '/a' into table load_data_test fields"+
				" terminated by 'a'",
		), "reader is nil",
	)
	ctx.SetValue(executor.LoadDataVarKey, nil)
	require.ErrorContains(
		t, tk.ExecToErr(
			"load data local infile '/a' format 'delimited data' into"+
				" table load_data_test fields terminated by 'a'",
		), "reader is nil",
	)
	ctx.SetValue(executor.LoadDataVarKey, nil)

	// According to https://dev.mysql.com/doc/refman/8.0/en/load-data.html , fixed-row format should be used when fields
	// terminated by '' and enclosed by ''. However, tidb doesn't support it yet and empty terminator leads to infinite
	// loop in `indexOfTerminator` (see https://github.com/pingcap/tidb/issues/33298).
	require.ErrorIs(t, tk.ExecToErr("load data local infile '/tmp/nonexistence.csv' into table load_data_test fields terminated by ''"),
		exeerrors.ErrLoadDataWrongFormatConfig)
	require.ErrorIs(t, tk.ExecToErr("load data local infile '/tmp/nonexistence.csv' into table load_data_test fields terminated by '' enclosed by ''"),
		exeerrors.ErrLoadDataWrongFormatConfig)
}

func TestLoadData(t *testing.T) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createSQL := `drop table if exists load_data_test;
		create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int);`
	err := tk.ExecToErr("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	require.Error(t, err)
	tk.MustExec(createSQL)
	err = tk.ExecToErr("load data infile '/tmp/nonexistence.csv' into table load_data_test")
	require.Error(t, err)
	loadSQL := "load data local infile '/tmp/nonexistence.csv' ignore into table load_data_test"
	ctx := tk.Session().(sessionctx.Context)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"

	sc := ctx.GetSessionVars().StmtCtx
	oldFlags := sc.TypeFlags()
	defer func() {
		sc.SetTypeFlags(oldFlags)
	}()
	sc.SetTypeFlags(oldFlags.WithIgnoreTruncateErr(false))
	// fields and lines are default, ReadOneBatchRows returns data is nil
	tests := []testCase{
		// In MySQL we have 4 warnings: 1*"Incorrect integer value: '' for column 'id' at row", 3*"Row 1 doesn't contain data for all columns"
		{[]byte("\n"), []string{"1|<nil>|<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("\t\n"), []string{"2|0|<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 3"},
		{[]byte("3\t2\t3\t4\n"), []string{"3|2|3|4"}, trivialMsg},
		{[]byte("3*1\t2\t3\t4\n"), []string{"3|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("4\t2\t\t3\t4\n"), []string{"4|2||3"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\t1\t2\t3\t4\n"), []string{"5|1|2|3"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("6\t2\t3\n"), []string{"6|2|3|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\t2\t3\t4\n\t22\t33\t44\n"), []string{"7|2|3|4", "8|22|33|44"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("7\t2\t3\t4\n7\t22\t33\t44\n"), []string{"7|2|3|4"}, "Records: 2  Deleted: 0  Skipped: 1  Warnings: 1"},

		// outdated test but still increase AUTO_INCREMENT
		{[]byte("\t2\t3\t4"), []string{"9|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\t2\t3\t4\t5\n"), []string{"10|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("\t2\t34\t5\n"), []string{"11|2|34|5"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)

	// lines starting symbol is "" and terminated symbol length is 2, ReadOneBatchRows returns data is nil
	loadSQL = "load data local infile '/tmp/nonexistence." +
		"csv' ignore into table load_data_test lines terminated by '||'"
	tests = []testCase{
		{[]byte("0\t2\t3\t4\t5||"), []string{"12|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("1\t2\t3\t4\t5||"), []string{"1|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("2\t2\t3\t4\t5||3\t22\t33\t44\t55||"),
			[]string{"2|2|3|4", "3|22|33|44"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("3\t2\t3\t4\t5||4\t22\t33||"), []string{
			"3|2|3|4", "4|22|33|<nil>"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("4\t2\t3\t4\t5||5\t22\t33||6\t222||"),
			[]string{"4|2|3|4", "5|22|33|<nil>", "6|222|<nil>|<nil>"}, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 3"},
		{[]byte("6\t2\t34\t5||"), []string{"6|2|34|5"}, trivialMsg},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)

	// fields and lines aren't default, ReadOneBatchRows returns data is nil
	loadSQL = "load data local infile '/tmp/nonexistence.csv' " +
		`ignore into table load_data_test fields terminated by '\\' lines starting by 'xxx' terminated by '|!#^'`
	tests = []testCase{
		{[]byte("xxx|!#^"), []string{"13|<nil>|<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xxx\\|!#^"), []string{"14|0|<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 3"},
		{[]byte("xxx3\\2\\3\\4|!#^"), []string{"3|2|3|4"}, trivialMsg},
		{[]byte("xxx4\\2\\\\3\\4|!#^"), []string{"4|2||3"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("xxx\\1\\2\\3\\4|!#^"), []string{"15|1|2|3"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xxx6\\2\\3|!#^"), []string{"6|2|3|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("xxx\\2\\3\\4|!#^xxx\\22\\33\\44|!#^"), []string{
			"16|2|3|4",
			"17|22|33|44"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("\\2\\3\\4|!#^\\22\\33\\44|!#^xxx\\222\\333\\444|!#^"), []string{
			"18|222|333|444"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		{[]byte("xxx\\2\\3\\4"), []string{"19|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\\2\\3\\4|!#^"), []string{}, "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3\\4|!#^xxx18\\22\\33\\44|!#^"),
			[]string{"18|22|33|44"}, trivialMsg},

		{[]byte("xxx10\\2\\3\\4|!#^"),
			[]string{"10|2|3|4"}, trivialMsg},
		{[]byte("10\\2\\3xxx11\\4\\5|!#^"),
			[]string{"11|4|5|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("xxx21\\2\\3\\4\\5|!#^"),
			[]string{"21|2|3|4"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("xxx22\\2\\3\\4\\5|!#^xxx23\\22\\33\\44\\55|!#^"),
			[]string{"22|2|3|4", "23|22|33|44"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xxx23\\2\\3\\4\\5|!#^xxx24\\22\\33|!#^"),
			[]string{"23|2|3|4", "24|22|33|<nil>"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xxx24\\2\\3\\4\\5|!#^xxx25\\22\\33|!#^xxx26\\222|!#^"),
			[]string{"24|2|3|4", "25|22|33|<nil>", "26|222|<nil>|<nil>"}, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 3"},
		{[]byte("xxx25\\2\\3\\4\\5|!#^26\\22\\33|!#^xxx27\\222|!#^"),
			[]string{"25|2|3|4", "27|222|<nil>|<nil>"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xxx\\2\\34\\5|!#^"), []string{"28|2|34|5"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)

	// TODO: not support it now
	// lines starting symbol is the same as terminated symbol, ReadOneBatchRows returns data is nil
	//ld.LinesInfo.Terminated = "xxx"
	//tests = []testCase{
	//	// data1 = nil, data2 != nil
	//	{[]byte("xxxxxx"), []string{"29|<nil>|<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	//	{[]byte("xxx3\\2\\3\\4xxx"), []string{"3|2|3|4"}, nil, trivialMsg},
	//	{[]byte("xxx\\2\\3\\4xxxxxx\\22\\33\\44xxx"),
	//		[]string{"30|2|3|4", "31|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
	//
	//	// data1 != nil, data2 = nil
	//	{[]byte("xxx\\2\\3\\4"), nil, []string{"32|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	//
	//	// data1 != nil, data2 != nil
	//	{[]byte("xxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"10|2|3|4"}, nil, trivialMsg},
	//	{[]byte("xxxxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"33|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	//	{[]byte("xxx21\\2\\3\\4\\5xx"), []byte("x"), []string{"21|2|3|4"}, nil, trivialMsg},
	//	{[]byte("xxx32\\2\\3\\4\\5x"), []byte("xxxxx33\\22\\33\\44\\55xxx"),
	//		[]string{"32|2|3|4", "33|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	//	{[]byte("xxx33\\2\\3\\4\\5xxx"), []byte("xxx34\\22\\33xxx"),
	//		[]string{"33|2|3|4", "34|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	//	{[]byte("xxx34\\2\\3\\4\\5xx"), []byte("xxxx35\\22\\33xxxxxx36\\222xxx"),
	//		[]string{"34|2|3|4", "35|22|33|<nil>", "36|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},
	//
	//	// ReadOneBatchRows returns data isn't nil
	//	{[]byte("\\2\\3\\4xxxx"), nil, []byte("xxxx"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
	//	{[]byte("\\2\\3\\4xxx"), nil, []string{"37|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	//	{[]byte("\\2\\3\\4xxxxxx11\\22\\33\\44xxx"), nil,
	//		[]string{"38|<nil>|<nil>|<nil>", "39|<nil>|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
	//	{[]byte("xx10\\2\\3"), []byte("\\4\\5xxx"), nil, []byte("xxx"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
	//	{[]byte("xxx10\\2\\3"), []byte("\\4xxxx"), []string{"10|2|3|4"}, []byte("x"), trivialMsg},
	//	{[]byte("xxx10\\2\\3\\4\\5x"), []byte("xx11\\22\\33xxxxxx12\\222xxx"),
	//		[]string{"10|2|3|4", "40|<nil>|<nil>|<nil>"}, []byte("xxx"), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 1"},
	//}
	//checkCases(tests, ld, t, tk, ctx, selectSQL, deleteSQL)

	// test line terminator in field quoter
	loadSQL = "load data local infile '/tmp/nonexistence.csv' " +
		"ignore into table load_data_test " +
		"fields terminated by '\\\\' enclosed by '\\\"' " +
		"lines starting by 'xxx' terminated by '\\n'"
	tests = []testCase{
		{[]byte("xxx1\\1\\\"2\n\"\\3\nxxx4\\4\\\"5\n5\"\\6"), []string{"1|1|2\n|3", "4|4|5\n5|6"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)

	loadSQL = "load data local infile '/tmp/nonexistence.csv' " +
		"ignore into table load_data_test " +
		"fields terminated by '#' enclosed by '\\\"' " +
		"lines starting by 'xxx' terminated by '#\\n'"
	tests = []testCase{
		{[]byte("xxx1#\nxxx2#\n"), []string{"1|<nil>|<nil>|<nil>", "2|<nil>|<nil>|<nil>"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xxx1#2#3#4#\nnxxx2#3#4#5#\n"), []string{"1|2|3|4", "2|3|4|5"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx1#2#\"3#\"#\"4\n\"#\nxxx2#3#\"#4#\n\"#5#\n"), []string{"1|2|3#|4", "2|3|#4#\n|5"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)

	// TODO: now support it now
	//ld.LinesInfo.Terminated = "#"
	//ld.FieldsInfo.Terminated = "##"
	//ld.LinesInfo.Starting = ""
	//tests = []testCase{
	//	{[]byte("1#2#"), []string{"1|<nil>|<nil>|<nil>", "2|<nil>|<nil>|<nil>"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	//	// TODO: WTF?
	//	{[]byte("1##2##3##4#2##3##4##5#"), []string{"1|2|3|4", "2|3|4|5"}, "Records: 14  Deleted: 0  Skipped: 3  Warnings: 9"},
	//	{[]byte("1##2##\"3##\"##\"4\n\"#2##3##\"##4#\"##5#"), []string{"1|2|3##|4", "2|3|##4#|5"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	//}
	//checkCases(tests, ld, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataEscape(t *testing.T) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table load_data_test"
	ctx := tk.Session().(sessionctx.Context)
	// test escape
	tests := []testCase{
		// data1 = nil, data2 != nil
		{[]byte("1\ta string\n"), []string{"1|a string"}, trivialMsg},
		{[]byte("2\tstr \\t\n"), []string{"2|str \t"}, trivialMsg},
		{[]byte("3\tstr \\n\n"), []string{"3|str \n"}, trivialMsg},
		{[]byte("4\tboth \\t\\n\n"), []string{"4|both \t\n"}, trivialMsg},
		{[]byte("5\tstr \\\\\n"), []string{"5|str \\"}, trivialMsg},
		{[]byte("6\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'})}, trivialMsg},
		{[]byte("7\trtn0ZbN\n"), []string{"7|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, trivialMsg},
		{[]byte("8\trtn0Zb\\N\n"), []string{"8|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, trivialMsg},
		{[]byte("9\ttab\\	tab\n"), []string{"9|tab	tab"}, trivialMsg},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

// TestLoadDataSpecifiedColumns reuse TestLoadDataEscape's test case :-)
func TestLoadDataSpecifiedColumns(t *testing.T) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec(`create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int default 0);`)
	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table load_data_test (c1, c2)"
	ctx := tk.Session().(sessionctx.Context)
	// test
	tests := []testCase{
		{[]byte("7\ta string\n"), []string{"1|7|a string|0"}, trivialMsg},
		{[]byte("8\tstr \\t\n"), []string{"2|8|str \t|0"}, trivialMsg},
		{[]byte("9\tstr \\n\n"), []string{"3|9|str \n|0"}, trivialMsg},
		{[]byte("10\tboth \\t\\n\n"), []string{"4|10|both \t\n|0"}, trivialMsg},
		{[]byte("11\tstr \\\\\n"), []string{"5|11|str \\|0"}, trivialMsg},
		{[]byte("12\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|12|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'}) + "|0"}, trivialMsg},
		{[]byte("\\N\ta string\n"), []string{"7|<nil>|a string|0"}, trivialMsg},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataIgnoreLines(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table load_data_test ignore 1 lines"
	ctx := tk.Session().(sessionctx.Context)
	tests := []testCase{
		{[]byte("1\tline1\n2\tline2\n"), []string{"2|line2"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("1\tline1\n2\tline2\n3\tline3\n"), []string{"2|line2", "3|line3"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataNULL(t *testing.T) {
	// https://dev.mysql.com/doc/refman/8.0/en/load-data.html
	// - For the default FIELDS and LINES values, NULL is written as a field value of \N for output, and a field value of \N is read as NULL for input (assuming that the ESCAPED BY character is \).
	// - If FIELDS ENCLOSED BY is not empty, a field containing the literal word NULL as its value is read as a NULL value. This differs from the word NULL enclosed within FIELDS ENCLOSED BY characters, which is read as the string 'NULL'.
	// - If FIELDS ESCAPED BY is empty, NULL is written as the word NULL.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id VARCHAR(20), value VARCHAR(20)) CHARACTER SET utf8")
	loadSQL := `load data local infile '/tmp/nonexistence.csv' into table load_data_test
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';`
	ctx := tk.Session().(sessionctx.Context)
	tests := []testCase{
		{
			[]byte(`NULL,"NULL"
\N,"\N"
"\\N"`),
			[]string{"<nil>|NULL", "<nil>|<nil>", "\\N|<nil>"},
			"Records: 3  Deleted: 0  Skipped: 0  Warnings: 1",
		},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataReplace(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test; DROP TABLE IF EXISTS load_data_replace;")
	tk.MustExec("CREATE TABLE load_data_replace (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL)")
	tk.MustExec("INSERT INTO load_data_replace VALUES(1,'val 1'),(2,'val 2')")
	loadSQL := "LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' REPLACE INTO TABLE load_data_replace"
	ctx := tk.Session().(sessionctx.Context)
	tests := []testCase{
		{[]byte("1\tline1\n2\tline2\n"), []string{"1|line1", "2|line2"}, "Records: 2  Deleted: 2  Skipped: 0  Warnings: 0"},
		{[]byte("2\tnew line2\n3\tnew line3\n"), []string{"1|line1", "2|new line2", "3|new line3"}, "Records: 2  Deleted: 1  Skipped: 0  Warnings: 0"},
	}
	deleteSQL := "DO 1"
	selectSQL := "TABLE load_data_replace;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

// TestLoadDataOverflowBigintUnsigned related to issue 6360
func TestLoadDataOverflowBigintUnsigned(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (a bigint unsigned);")
	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table load_data_test"
	ctx := tk.Session().(sessionctx.Context)
	tests := []testCase{
		{[]byte("-1\n-18446744073709551615\n-18446744073709551616\n"), []string{"0", "0", "0"}, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 3"},
		{[]byte("-9223372036854775809\n18446744073709551616\n"), []string{"0", "18446744073709551615"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataWithUppercaseUserVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (a int, b int);")
	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table load_data_test (@V1)" +
		" set a = @V1, b = @V1*100"
	ctx := tk.Session().(sessionctx.Context)
	tests := []testCase{
		{[]byte("1\n2\n"), []string{"1|100", "2|200"}, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataIntoPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table range_t (a int, b int) partition by range (a) ( " +
		"partition p0 values less than (4)," +
		"partition p1 values less than (7)," +
		"partition p2 values less than (11))")
	ctx := tk.Session().(sessionctx.Context)
	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table range_t fields terminated by ','"
	tests := []testCase{
		{[]byte("1,2\n3,4\n5,6\n7,8\n9,10\n"), []string{"1|2", "3|4", "5|6", "7|8", "9|10"}, "Records: 5  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	deleteSQL := "delete from range_t"
	selectSQL := "select * from range_t order by a;"
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestLoadDataFromServerFile(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table load_data_test (a int)")
	err := tk.ExecToErr("load data infile 'remote.csv' into table load_data_test")
	require.ErrorContains(t, err, "[executor:8154]Don't support load data from tidb-server's disk.")
}
