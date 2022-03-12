// Copyright 2017 PingCAP, Inc.
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

package types_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestGetSQLMode(t *testing.T) {
	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{",,NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
	}

	for _, test := range positiveCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(test.arg))
		require.NoError(t, err)
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE,adfadsdfasdfads"},
		{", ,NO_ZERO_DATE"},
		{" ,"},
	}

	for _, test := range negativeCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(test.arg))
		require.Error(t, err)
	}
}

func TestSQLMode(t *testing.T) {
	tests := []struct {
		arg                           string
		hasNoZeroDateMode             bool
		hasNoZeroInDateMode           bool
		hasErrorForDivisionByZeroMode bool
	}{
		{"NO_ZERO_DATE", true, false, false},
		{"NO_ZERO_IN_DATE", false, true, false},
		{"ERROR_FOR_DIVISION_BY_ZERO", false, false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, true, true},
		{"", false, false, false},
	}

	for _, test := range tests {
		sqlMode, _ := mysql.GetSQLMode(test.arg)
		require.Equal(t, test.hasNoZeroDateMode, sqlMode.HasNoZeroDateMode())
		require.Equal(t, test.hasNoZeroInDateMode, sqlMode.HasNoZeroInDateMode())
		require.Equal(t, test.hasErrorForDivisionByZeroMode, sqlMode.HasErrorForDivisionByZeroMode())
	}
}

func TestRealAsFloatMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a real);")
	result := tk.MustQuery("desc t")
	require.Len(t, result.Rows(), 1)
	row := result.Rows()[0]
	require.Equal(t, "double", row[1])

	tk.MustExec("drop table if exists t;")
	tk.MustExec("set sql_mode='REAL_AS_FLOAT'")
	tk.MustExec("create table t (a real)")
	result = tk.MustQuery("desc t")
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	require.Equal(t, "float", row[1])
}

func TestPipesAsConcatMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("SET sql_mode='PIPES_AS_CONCAT';")
	r := tk.MustQuery(`SELECT 'hello' || 'world';`)
	r.Check(testkit.Rows("helloworld"))
}

func TestIssue22387(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode=''")
	err := tk.QueryToErr("select 12 - cast(15 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(12 - 15)'")

	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustQuery("select 12 - cast(15 as unsigned);").Check(testkit.Rows("-3"))
}

func TestIssue22389(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustExec("DROP TABLE IF EXISTS tb5")
	tk.MustExec("create table tb5(a bigint, b bigint);")
	tk.MustExec("insert into tb5 values (10, -9223372036854775808);")
	err := tk.QueryToErr("select a - b from tb5;")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")
	tk.MustExec("set sql_mode=''")
	err = tk.QueryToErr("select a - b from tb5;")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")
}

func TestIssue22390(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='';")
	tk.MustExec("DROP TABLE IF EXISTS tb5")
	tk.MustExec("create table tb5(a bigint, b bigint);")
	tk.MustExec("insert into tb5 values (10, -9223372036854775808);")
	err := tk.QueryToErr("select a - b from tb5;")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")

	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	err = tk.QueryToErr("select a - b from tb5;")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")
}

func TestIssue22442(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode='';")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-1 as unsigned);").Check(testkit.Rows("0"))

	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-1 as unsigned);").Check(testkit.Rows("0"))
}

func TestIssue22444(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-10000 as unsigned); ").Check(testkit.Rows("9999"))

	tk.MustExec("set sql_mode='';")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-10000 as unsigned); ").Check(testkit.Rows("9999"))
}

func TestIssue22445(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select cast(-12 as unsigned) - cast(-1 as unsigned);").Check(testkit.Rows("-11"))

	tk.MustExec("set sql_mode='';")
	err := tk.QueryToErr("select cast(-12 as unsigned) - cast(-1 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(18446744073709551604 - 18446744073709551615)'")
}

func TestIssue22446(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select cast(-1 as unsigned) - 9223372036854775808").Check(testkit.Rows("9223372036854775807"))

	tk.MustExec("set sql_mode=''; ")
	tk.MustQuery("select cast(-1 as unsigned) - 9223372036854775808").Check(testkit.Rows("9223372036854775807"))
}

func TestIssue22447(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select 9223372036854775808 - cast(-1 as unsigned)").Check(testkit.Rows("-9223372036854775807"))

	tk.MustExec("set sql_mode='';")
	err := tk.QueryToErr("select 9223372036854775808 - cast(-1 as unsigned)")
	require.Error(t, err)
	require.EqualError(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(9223372036854775808 - 18446744073709551615)'")
}

func TestNoUnsignedSubtractionMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'")
	r := tk.MustQuery("SELECT CAST(0 as UNSIGNED) - 1;")
	r.Check(testkit.Rows("-1"))

	// 1. minusFUU
	err := tk.QueryToErr("SELECT CAST(-1 as UNSIGNED) - cast(9223372036854775807 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(18446744073709551615 - 9223372036854775807)'")

	err = tk.QueryToErr("SELECT CAST(0 as UNSIGNED) - cast(9223372036854775809 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(0 - 9223372036854775809)'")

	tk.MustQuery("SELECT CAST(0 as UNSIGNED) - cast(9223372036854775808 as unsigned);").Check(testkit.Rows("-9223372036854775808"))
	tk.MustQuery("SELECT CAST(-1 as UNSIGNED) - cast(-9223372036854775808 as unsigned);").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("SELECT cast(0 as unsigned) - cast(9223372036854775808 as unsigned);").Check(testkit.Rows("-9223372036854775808"))

	// 2. minusSS
	err = tk.QueryToErr("SELECT -9223372036854775808 - (1);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(-9223372036854775808 - 1)'")

	err = tk.QueryToErr("SELECT 1 - (-9223372036854775808);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775808)'")

	err = tk.QueryToErr("SELECT 1 - (-9223372036854775807);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775807)'")

	// 3. minusFUS
	err = tk.QueryToErr("SELECT CAST(-12 as UNSIGNED) - (-1);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(18446744073709551604 - -1)'")

	err = tk.QueryToErr("SELECT CAST(9223372036854775808 as UNSIGNED) - (0);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(9223372036854775808 - 0)'")

	err = tk.QueryToErr("SELECT CAST(-1 as UNSIGNED) - (9223372036854775807);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(18446744073709551615 - 9223372036854775807)'")

	err = tk.QueryToErr("SELECT CAST(9223372036854775808 as UNSIGNED) - 0;")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(9223372036854775808 - 0)'")

	tk.MustQuery("SELECT CAST(-1 as UNSIGNED) - (9223372036854775808);").Check(testkit.Rows("9223372036854775807"))

	err = tk.QueryToErr("SELECT CAST(1 as UNSIGNED) - (-9223372036854775808);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775808)'")

	err = tk.QueryToErr("SELECT CAST(1 as UNSIGNED) - (-9223372036854775807);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775807)'")

	tk.MustQuery("SELECT CAST(1 as UNSIGNED) - (-9223372036854775806)").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select cast(0 as unsigned) - 9223372036854775807").Check(testkit.Rows("-9223372036854775807"))

	// 4. minusFSU
	err = tk.QueryToErr("SELECT CAST(1 as SIGNED) - cast(9223372036854775810 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(1 - 9223372036854775810)'")

	err = tk.QueryToErr("SELECT CAST(-1 as SIGNED) - cast(9223372036854775808 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(-1 - 9223372036854775808)'")

	err = tk.QueryToErr("SELECT CAST(-9223372036854775807 as SIGNED) - cast(-1 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(-9223372036854775807 - 18446744073709551615)'")

	err = tk.QueryToErr("SELECT CAST(-1 as SIGNED) - cast(9223372036854775808 as unsigned);")
	require.EqualError(t, err, "[types:1690]BIGINT value is out of range in '(-1 - 9223372036854775808)'")

	tk.MustQuery("select 0 - cast(9223372036854775807 as unsigned)").Check(testkit.Rows("-9223372036854775807"))
	tk.MustQuery("SELECT CAST(1 as SIGNED) - cast(9223372036854775809 as unsigned)").Check(testkit.Rows("-9223372036854775808"))
	tk.MustQuery("SELECT CAST(-1 as SIGNED) - cast(9223372036854775807 as unsigned)").Check(testkit.Rows("-9223372036854775808"))

	rs, _ := tk.Exec("SELECT 1 - CAST(18446744073709551615 as UNSIGNED);")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.NoError(t, rs.Close())
	rs, _ = tk.Exec("SELECT CAST(-1 as UNSIGNED) - 1")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.NoError(t, rs.Close())
	rs, _ = tk.Exec("SELECT CAST(9223372036854775808 as UNSIGNED) - 1")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.NoError(t, err)
	require.NoError(t, rs.Close())
}

func TestHighNotPrecedenceMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (0),(1),(NULL);")
	r := tk.MustQuery(`SELECT * FROM t1 WHERE NOT a BETWEEN 2 AND 3;`)
	r.Check(testkit.Rows("0", "1"))
	r = tk.MustQuery(`SELECT NOT 1 BETWEEN -5 AND 5;`)
	r.Check(testkit.Rows("0"))
	tk.MustExec("set sql_mode='high_not_precedence';")
	r = tk.MustQuery(`SELECT * FROM t1 WHERE NOT a BETWEEN 2 AND 3;`)
	r.Check(testkit.Rows())
	r = tk.MustQuery(`SELECT NOT 1 BETWEEN -5 AND 5;`)
	r.Check(testkit.Rows("1"))
}

func TestIgnoreSpaceMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("CREATE TABLE COUNT (a bigint);")
	tk.MustExec("DROP TABLE COUNT;")
	tk.MustExec("CREATE TABLE `COUNT` (a bigint);")
	tk.MustExec("DROP TABLE COUNT;")
	_, err := tk.Exec("CREATE TABLE COUNT(a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE test.COUNT(a bigint);")
	tk.MustExec("DROP TABLE COUNT;")

	tk.MustExec("CREATE TABLE BIT_AND (a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")
	tk.MustExec("CREATE TABLE `BIT_AND` (a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")
	_, err = tk.Exec("CREATE TABLE BIT_AND(a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE test.BIT_AND(a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")

	tk.MustExec("CREATE TABLE NOW (a bigint);")
	tk.MustExec("DROP TABLE NOW;")
	tk.MustExec("CREATE TABLE `NOW` (a bigint);")
	tk.MustExec("DROP TABLE NOW;")
	_, err = tk.Exec("CREATE TABLE NOW(a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE test.NOW(a bigint);")
	tk.MustExec("DROP TABLE NOW;")

	tk.MustExec("set sql_mode='IGNORE_SPACE'")
	_, err = tk.Exec("CREATE TABLE COUNT (a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE `COUNT` (a bigint);")
	tk.MustExec("DROP TABLE COUNT;")
	_, err = tk.Exec("CREATE TABLE COUNT(a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE test.COUNT(a bigint);")
	tk.MustExec("DROP TABLE COUNT;")

	_, err = tk.Exec("CREATE TABLE BIT_AND (a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE `BIT_AND` (a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")
	_, err = tk.Exec("CREATE TABLE BIT_AND(a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE test.BIT_AND(a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")

	_, err = tk.Exec("CREATE TABLE NOW (a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE `NOW` (a bigint);")
	tk.MustExec("DROP TABLE NOW;")
	_, err = tk.Exec("CREATE TABLE NOW(a bigint);")
	require.Error(t, err)
	tk.MustExec("CREATE TABLE test.NOW(a bigint);")
	tk.MustExec("DROP TABLE NOW;")

}

func TestNoBackslashEscapesMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set sql_mode=''")
	r := tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\"))
	tk.MustExec("set sql_mode='NO_BACKSLASH_ESCAPES'")
	r = tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\\\"))
}

func TestServerStatus(t *testing.T) {
	tests := []struct {
		arg            uint16
		IsCursorExists bool
	}{
		{0, false},
		{mysql.ServerStatusInTrans | mysql.ServerStatusNoBackslashEscaped, false},
		{mysql.ServerStatusCursorExists, true},
		{mysql.ServerStatusCursorExists | mysql.ServerStatusLastRowSend, true},
	}

	for _, test := range tests {
		require.Equal(t, test.IsCursorExists, mysql.HasCursorExistsFlag(test.arg))
	}
}
