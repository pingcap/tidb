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
	"sync/atomic"
	"testing"
	"time"

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
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}, geners: []dataGenerator{
			newSelectRealGener([]float64{0, 0.000001}),
		}},
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

type counter struct {
	count int
}

func (c *counter) add(diff int) int {
	c.count += diff
	return c.count
}

func (s *testEvaluatorSuite) TestSleepVectorized(c *C) {
	ctx := mock.NewContext()
	sessVars := ctx.GetSessionVars()

	fc := funcs[ast.Sleep]
	ft := eType2FieldType(types.ETReal)
	col0 := &Column{RetType: ft, Index: 0}
	f, err := fc.getFunction(ctx, []Expression{col0})
	c.Assert(err, IsNil)
	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1024)
	result := chunk.NewColumn(ft, 1024)
	warnCnt := counter{}

	// non-strict model
	sessVars.StrictSQLMode = false
	input.AppendFloat64(0, 1)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, uint16(warnCnt.add(0)))

	input.Reset()
	input.AppendFloat64(0, -1)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, uint16(warnCnt.add(1)))

	input.Reset()
	input.AppendNull(0)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, uint16(warnCnt.add(1)))

	input.Reset()
	input.AppendNull(0)
	input.AppendFloat64(0, 1)
	input.AppendFloat64(0, -1)
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(result.GetInt64(1), Equals, int64(0))
	c.Assert(result.GetInt64(2), Equals, int64(0))
	c.Assert(sessVars.StmtCtx.WarningCount(), Equals, uint16(warnCnt.add(2)))

	// for error case under the strict model
	sessVars.StrictSQLMode = true
	input.Reset()
	input.AppendNull(0)
	err = f.vecEvalInt(input, result)
	c.Assert(err, NotNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))

	sessVars.StmtCtx.SetWarnings(nil)
	input.Reset()
	input.AppendFloat64(0, -2.5)
	err = f.vecEvalInt(input, result)
	c.Assert(err, NotNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))

	//// strict model
	input.Reset()
	input.AppendFloat64(0, 0.5)
	start := time.Now()
	err = f.vecEvalInt(input, result)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	sub := time.Since(start)
	c.Assert(sub.Nanoseconds(), GreaterEqual, int64(0.5*1e9))

	input.Reset()
	input.AppendFloat64(0, 0.01)
	input.AppendFloat64(0, 2)
	input.AppendFloat64(0, 2)
	start = time.Now()
	go func() {
		time.Sleep(1 * time.Second)
		atomic.CompareAndSwapUint32(&ctx.GetSessionVars().Killed, 0, 1)
	}()
	err = f.vecEvalInt(input, result)
	sub = time.Since(start)
	c.Assert(err, IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(0))
	c.Assert(result.GetInt64(1), Equals, int64(1))
	c.Assert(result.GetInt64(2), Equals, int64(1))
	c.Assert(sub.Nanoseconds(), LessEqual, int64(2*1e9))
	c.Assert(sub.Nanoseconds(), GreaterEqual, int64(1*1e9))
}
