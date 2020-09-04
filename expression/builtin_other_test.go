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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
)

func (s *testEvaluatorSuite) TestBitCount(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()
	fc := funcs[ast.BitCount]
	var bitCountCases = []struct {
		origin interface{}
		count  interface{}
	}{
		{int64(8), int64(1)},
		{int64(29), int64(4)},
		{int64(0), int64(0)},
		{int64(-1), int64(64)},
		{int64(-11), int64(62)},
		{int64(-1000), int64(56)},
		{float64(1.1), int64(1)},
		{float64(3.1), int64(2)},
		{float64(-1.1), int64(64)},
		{float64(-3.1), int64(63)},
		{uint64(math.MaxUint64), int64(64)},
		{"xxx", int64(0)},
		{nil, nil},
	}
	for _, test := range bitCountCases {
		in := types.NewDatum(test.origin)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{in}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		count, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if count.IsNull() {
			c.Assert(test.count, IsNil)
			continue
		}
		sc := new(stmtctx.StatementContext)
		sc.IgnoreTruncate = true
		res, err := count.ToInt64(sc)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.count)
	}
}

func (s *testEvaluatorSuite) TestInFunc(c *C) {
	fc := funcs[ast.In]
	decimal1 := types.NewDecFromFloatForTest(123.121)
	decimal2 := types.NewDecFromFloatForTest(123.122)
	decimal3 := types.NewDecFromFloatForTest(123.123)
	decimal4 := types.NewDecFromFloatForTest(123.124)
	time1 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 1, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time2 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 2, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time3 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 3, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time4 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 4, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	duration1 := types.Duration{Duration: 12*time.Hour + 1*time.Minute + 1*time.Second}
	duration2 := types.Duration{Duration: 12*time.Hour + 1*time.Minute}
	duration3 := types.Duration{Duration: 12*time.Hour + 1*time.Second}
	duration4 := types.Duration{Duration: 12 * time.Hour}
	json1 := json.CreateBinary("123")
	json2 := json.CreateBinary("123.1")
	json3 := json.CreateBinary("123.2")
	json4 := json.CreateBinary("123.3")
	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{1, 1, 2, 3}, int64(1)},
		{[]interface{}{1, 0, 2, 3}, int64(0)},
		{[]interface{}{1, nil, 2, 3}, nil},
		{[]interface{}{nil, nil, 2, 3}, nil},
		{[]interface{}{uint64(0), 0, 2, 3}, int64(1)},
		{[]interface{}{uint64(math.MaxUint64), uint64(math.MaxUint64), 2, 3}, int64(1)},
		{[]interface{}{-1, uint64(math.MaxUint64), 2, 3}, int64(0)},
		{[]interface{}{uint64(math.MaxUint64), -1, 2, 3}, int64(0)},
		{[]interface{}{1, 0, 2, 3}, int64(0)},
		{[]interface{}{1.1, 1.2, 1.3}, int64(0)},
		{[]interface{}{1.1, 1.1, 1.2, 1.3}, int64(1)},
		{[]interface{}{decimal1, decimal2, decimal3, decimal4}, int64(0)},
		{[]interface{}{decimal1, decimal2, decimal3, decimal1}, int64(1)},
		{[]interface{}{"1.1", "1.1", "1.2", "1.3"}, int64(1)},
		{[]interface{}{"1.1", hack.Slice("1.1"), "1.2", "1.3"}, int64(1)},
		{[]interface{}{hack.Slice("1.1"), "1.1", "1.2", "1.3"}, int64(1)},
		{[]interface{}{time1, time2, time3, time1}, int64(1)},
		{[]interface{}{time1, time2, time3, time4}, int64(0)},
		{[]interface{}{duration1, duration2, duration3, duration4}, int64(0)},
		{[]interface{}{duration1, duration2, duration1, duration4}, int64(1)},
		{[]interface{}{json1, json2, json3, json4}, int64(0)},
		{[]interface{}{json1, json1, json3, json4}, int64(1)},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(fn, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		c.Assert(err, IsNil)
		c.Assert(d.GetValue(), Equals, tc.res, Commentf("%v", types.MakeDatums(tc.args)))
	}
	collate.SetNewCollationEnabledForTest(true)
	strD1 := types.NewCollationStringDatum("a", "utf8_general_ci", 0)
	strD2 := types.NewCollationStringDatum("Á", "utf8_general_ci", 0)
	fn, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{strD1, strD2}))
	c.Assert(err, IsNil)
	d, isNull, err := fn.evalInt(chunk.Row{})
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(d, Equals, int64(1), Commentf("%v, %v", strD1, strD2))
	chk1 := chunk.NewChunkWithCapacity(nil, 1)
	chk1.SetNumVirtualRows(1)
	chk2 := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeTiny)}, 1)
	err = fn.vecEvalInt(chk1, chk2.Column(0))
	c.Assert(err, IsNil)
	c.Assert(chk2.Column(0).GetInt64(0), Equals, int64(1))
	collate.SetNewCollationEnabledForTest(false)
}

func (s *testEvaluatorSuite) TestRowFunc(c *C) {
	fc := funcs[ast.RowFunc]
	_, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums([]interface{}{"1", 1.2, true, 120}...)))
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSetVar(c *C) {
	fc := funcs[ast.SetVar]
	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{"a", "12"}, "12"},
		{[]interface{}{"b", "34"}, "34"},
		{[]interface{}{"c", nil}, ""},
		{[]interface{}{"c", "ABC"}, "ABC"},
		{[]interface{}{"c", "dEf"}, "dEf"},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(fn, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, tc.res)
		if tc.args[1] != nil {
			key, ok := tc.args[0].(string)
			c.Assert(ok, Equals, true)
			val, ok := tc.res.(string)
			c.Assert(ok, Equals, true)
			sessionVar, ok := s.ctx.GetSessionVars().Users[key]
			c.Assert(ok, Equals, true)
			c.Assert(sessionVar.GetString(), Equals, val)
		}
	}
}

func (s *testEvaluatorSuite) TestGetVar(c *C) {
	fc := funcs[ast.GetVar]

	sessionVars := []struct {
		key string
		val string
	}{
		{"a", "中"},
		{"b", "文字符chuan"},
		{"c", ""},
	}
	for _, kv := range sessionVars {
		s.ctx.GetSessionVars().Users[kv.key] = types.NewStringDatum(kv.val)
	}

	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{"a"}, "中"},
		{[]interface{}{"b"}, "文字符chuan"},
		{[]interface{}{"c"}, ""},
		{[]interface{}{"d"}, ""},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(fn, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, tc.res)
	}
}

func (s *testEvaluatorSuite) TestValues(c *C) {
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, 1, types.NewFieldType(mysql.TypeVarchar)}
	_, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums("")))
	c.Assert(err, ErrorMatches, "*Incorrect parameter count in the call to native function 'values'")

	sig, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums()))
	c.Assert(err, IsNil)

	ret, err := evalBuiltinFunc(sig, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(ret.IsNull(), IsTrue)

	s.ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(types.MakeDatums("1")).ToRow()
	ret, err = evalBuiltinFunc(sig, chunk.Row{})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Session current insert values len.*")

	currInsertValues := types.MakeDatums("1", "2")
	s.ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(currInsertValues).ToRow()
	ret, err = evalBuiltinFunc(sig, chunk.Row{})
	c.Assert(err, IsNil)

	cmp, err := ret.CompareDatum(nil, &currInsertValues[1])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (s *testEvaluatorSuite) TestSetVarFromColumn(c *C) {
	// Construct arguments.
	argVarName := &Constant{
		Value:   types.NewStringDatum("a"),
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 20},
	}
	argCol := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 20},
		Index:   0,
	}

	// Construct SetVar function.
	funcSetVar, err := NewFunction(
		s.ctx,
		ast.SetVar,
		&types.FieldType{Tp: mysql.TypeVarString, Flen: 20},
		[]Expression{argVarName, argCol}...,
	)
	c.Assert(err, IsNil)

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{argCol.RetType}, 1)
	inputChunk.AppendString(0, "a")
	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{argCol.RetType}, 1)

	// Evaluate the SetVar function.
	err = evalOneCell(s.ctx, funcSetVar, inputChunk.GetRow(0), outputChunk, 0)
	c.Assert(err, IsNil)
	c.Assert(outputChunk.GetRow(0).GetString(0), Equals, "a")

	// Change the content of the underlying Chunk.
	inputChunk.Reset()
	inputChunk.AppendString(0, "b")

	// Check whether the user variable changed.
	sessionVars := s.ctx.GetSessionVars()
	sessionVars.UsersLock.RLock()
	defer sessionVars.UsersLock.RUnlock()
	sessionVar, ok := sessionVars.Users["a"]
	c.Assert(ok, Equals, true)
	c.Assert(sessionVar.GetString(), Equals, "a")
}
