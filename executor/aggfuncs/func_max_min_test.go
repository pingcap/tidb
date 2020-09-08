// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestMergePartialResult4MaxMin(c *C) {
	elems := []string{"a", "b", "c", "d", "e"}
	enumA, _ := types.ParseEnumName(elems, "a", mysql.DefaultCollationName)
	enumC, _ := types.ParseEnumName(elems, "c", mysql.DefaultCollationName)
	enumE, _ := types.ParseEnumName(elems, "e", mysql.DefaultCollationName)

	setA, _ := types.ParseSetName(elems, "a", mysql.DefaultCollationName)    // setA.Value == 1
	setAB, _ := types.ParseSetName(elems, "a,b", mysql.DefaultCollationName) // setAB.Value == 3
	setAC, _ := types.ParseSetName(elems, "a,c", mysql.DefaultCollationName) // setAC.Value == 5

	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, 4, 4, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, 4, 4, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, types.NewDecFromInt(4), types.NewDecFromInt(4), types.NewDecFromInt(4)),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, "4", "4", "4"),
		buildAggTester(ast.AggFuncMax, mysql.TypeDate, 5, types.TimeFromDays(369), types.TimeFromDays(369), types.TimeFromDays(369)),
		buildAggTester(ast.AggFuncMax, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}),
		buildAggTester(ast.AggFuncMax, mysql.TypeJSON, 5, json.CreateBinary(int64(4)), json.CreateBinary(int64(4)), json.CreateBinary(int64(4))),
		buildAggTester(ast.AggFuncMax, mysql.TypeEnum, 5, enumE, enumE, enumE),
		buildAggTester(ast.AggFuncMax, mysql.TypeSet, 5, setAC, setAC, setAC),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, "0", "2", "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		buildAggTester(ast.AggFuncMin, mysql.TypeEnum, 5, enumA, enumC, enumA),
		buildAggTester(ast.AggFuncMin, mysql.TypeSet, 5, setA, setAB, setA),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMaxMin(c *C) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, nil, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, nil, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(4)),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, nil, "4", "4"),
		buildAggTester(ast.AggFuncMax, mysql.TypeDate, 5, nil, types.TimeFromDays(369)),
		buildAggTester(ast.AggFuncMax, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(4)}),
		buildAggTester(ast.AggFuncMax, mysql.TypeJSON, 5, nil, json.CreateBinary(int64(4))),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, nil, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, nil, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, nil, "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, nil, types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, nil, json.CreateBinary(int64(0))),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMaxSlidingWindowWithoutOrderBy(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a BIGINT);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result := tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a float);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a double);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a decimal);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a text);")
	tk.MustExec("insert into t values ('1'), ('2'), ('3')")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a time);")
	tk.MustExec("insert into t values ('00:00:00'), ('01:00:00'), ('02:00:00')")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("02:00:00", "02:00:00", "02:00:00"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a date);")
	tk.MustExec("insert into t values ('2020-09-08'), ('2020-09-09'), ('2020-09-10')")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("2020-09-10", "2020-09-10", "2020-09-10"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a datetime);")
	tk.MustExec("insert into t values ('2020-09-08 02:00:00'), ('2020-09-09 01:00:00'), ('2020-09-10 00:00:00')")
	result = tk.MustQuery("SELECT max(a) OVER () FROM t;")
	result.Check(testkit.Rows("2020-09-10 00:00:00", "2020-09-10 00:00:00", "2020-09-10 00:00:00"))
}

func (s *testSuite) TestMaxSlidingWindowWithOrderByAndWholeFrame(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a BIGINT);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result := tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a float);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a double);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a decimal);")
	tk.MustExec("insert into t values (1), (2), (3)")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a text);")
	tk.MustExec("insert into t values ('1'), ('2'), ('3')")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("3", "3", "3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a time);")
	tk.MustExec("insert into t values ('00:00:00'), ('01:00:00'), ('02:00:00')")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("02:00:00", "02:00:00", "02:00:00"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("02:00:00", "02:00:00", "02:00:00"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a date);")
	tk.MustExec("insert into t values ('2020-09-08'), ('2020-09-09'), ('2020-09-10')")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("2020-09-10", "2020-09-10", "2020-09-10"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("2020-09-10", "2020-09-10", "2020-09-10"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a datetime);")
	tk.MustExec("insert into t values ('2020-09-08 02:00:00'), ('2020-09-09 01:00:00'), ('2020-09-10 00:00:00')")
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("2020-09-10 00:00:00", "2020-09-10 00:00:00", "2020-09-10 00:00:00"))
	result = tk.MustQuery("SELECT max(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;")
	result.Check(testkit.Rows("2020-09-10 00:00:00", "2020-09-10 00:00:00", "2020-09-10 00:00:00"))
}
