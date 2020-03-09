// Copyright 2019 PingCAP, Inc.
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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var vecBuiltinMiscellaneousCases = map[string][]vecExprBenchCase{
	ast.Inet6Aton: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv6StrGener{newDefaultRandGen()}}},
	},
	ast.IsIPv6: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Sleep: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.UUID: {},
	ast.Inet6Ntoa: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{
			newSelectStringGener(
				[]string{
					"192.168.0.1",
					"2001:db8::68", //ipv6
				},
			)}},
	},
	ast.InetAton: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4StrGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{
			newSelectStringGener(
				[]string{
					"11.11.11.11.",    // last char is .
					"266.266.266.266", // int in string exceed 255
					"127",
					".122",
					".123.123",
					"127.255",
					"127.2.1",
				},
			)}},
	},
	ast.IsIPv4Mapped: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4MappedByteGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv6ByteGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4ByteGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDefaultGener(1.0, types.ETString)}},
	},
	ast.IsIPv4Compat: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4CompatByteGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv6ByteGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4ByteGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDefaultGener(1.0, types.ETString)}},
	},
	ast.InetNtoa: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.IsIPv4: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.AnyValue: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
	},
	ast.NameConst: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDuration}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETString, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETString, types.ETReal}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETString, types.ETJson}},
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETString, types.ETTimestamp}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMiscellaneousEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinMiscellaneousCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMiscellaneousFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinMiscellaneousCases)
}

func BenchmarkVectorizedBuiltinMiscellaneousEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinMiscellaneousCases)
}

func BenchmarkVectorizedBuiltinMiscellaneousFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinMiscellaneousCases)
}

func (s *testEvaluatorSuite) TestSleepVectorized(c *C) {
	// TODO(ziyi) Refer to expression/builtin_op_vec_test.go:159
	ctx := mock.NewContext()
	sessVars := ctx.GetSessionVars()

	fc := funcs[ast.Sleep]
	ft := eType2FieldType(types.ETInt)
	col0 := &Column{RetType: ft, Index: 0}
	f, err := fc.getFunction(ctx, []Expression{col0})
	c.Assert(err, IsNil)
	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1024)
	result := chunk.NewColumn(ft, 1024)

	// non-strict model
	sessVars.StrictSQLMode = false
	input.AppendInt64(0, 1)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, 0)
	input.Reset()
	input.AppendInt64(0, -1)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, 1)
	input.Reset()
	input.AppendNull(0)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, 1)
	input.Reset()
	input.AppendNull(0)
	input.AppendInt64(0, 1)
	input.AppendInt64(0, -1)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(result.GetInt64(1), Equals, int64(0))
	c.Assert(result.GetInt64(2), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, 2)

	// for error case under the strict model
	//sessVars.StrictSQLMode = true
	//d[0].SetNull()
	//_, err = fc.getFunction(ctx, s.datumsToConstants(d))
	//c.Assert(err, IsNil)
	//_, isNull, err = f.evalInt(chunk.Row{})
	//c.Assert(err, NotNil)
	//c.Assert(isNull, IsFalse)
	//d[0].SetFloat64(-2.5)
	//_, err = fc.getFunction(ctx, s.datumsToConstants(d))
	//c.Assert(err, IsNil)
	//_, isNull, err = f.evalInt(chunk.Row{})
	//c.Assert(err, NotNil)
	//c.Assert(isNull, IsFalse)
	//
	//// strict model
	//d[0].SetFloat64(0.5)
	//start := time.Now()
	//f, err = fc.getFunction(ctx, s.datumsToConstants(d))
	//c.Assert(err, IsNil)
	//ret, isNull, err = f.evalInt(chunk.Row{})
	//c.Assert(err, IsNil)
	//c.Assert(isNull, IsFalse)
	//c.Assert(ret, Equals, int64(0))
	//sub := time.Since(start)
	//c.Assert(sub.Nanoseconds(), GreaterEqual, int64(0.5*1e9))
	//
	//d[0].SetFloat64(3)
	//f, err = fc.getFunction(ctx, s.datumsToConstants(d))
	//c.Assert(err, IsNil)
	//start = time.Now()
	//go func() {
	//	time.Sleep(1 * time.Second)
	//	atomic.CompareAndSwapUint32(&ctx.GetSessionVars().Killed, 0, 1)
	//}()
	//ret, isNull, err = f.evalInt(chunk.Row{})
	//sub = time.Since(start)
	//c.Assert(err, IsNil)
	//c.Assert(isNull, IsFalse)
	//c.Assert(ret, Equals, int64(1))
	//c.Assert(sub.Nanoseconds(), LessEqual, int64(2*1e9))
	//c.Assert(sub.Nanoseconds(), GreaterEqual, int64(1*1e9))

}
