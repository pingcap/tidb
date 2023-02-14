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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
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
					"2001:db8::68", // ipv6
				},
			)}},
	},
	ast.InetAton: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&ipv4StrGener{newDefaultRandGen()}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{
			newSelectStringGener(
				[]string{
					"11.11.11.11",
					"255.255.255.255",
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
	ast.UUIDToBin: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&uuidStrGener{newDefaultRandGen()}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&uuidStrGener{newDefaultRandGen()}}},
	},
	ast.BinToUUID: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&uuidBinGener{newDefaultRandGen()}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&uuidBinGener{newDefaultRandGen()}}},
	},
	ast.IsUUID: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&uuidStrGener{newDefaultRandGen()}}},
	},
}

func TestVectorizedBuiltinMiscellaneousEvalOneVec(t *testing.T) {
	testVectorizedEvalOneVec(t, vecBuiltinMiscellaneousCases)
}

func TestVectorizedBuiltinMiscellaneousFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinMiscellaneousCases)
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

func TestSleepVectorized(t *testing.T) {
	ctx := mock.NewContext()
	sessVars := ctx.GetSessionVars()

	fc := funcs[ast.Sleep]
	ft := eType2FieldType(types.ETReal)
	col0 := &Column{RetType: ft, Index: 0}
	f, err := fc.getFunction(ctx, []Expression{col0})
	require.NoError(t, err)
	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1024)
	result := chunk.NewColumn(ft, 1024)
	warnCnt := counter{}

	// non-strict model
	sessVars.StmtCtx.BadNullAsWarning = true
	input.AppendFloat64(0, 1)
	err = f.vecEvalInt(input, result)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))
	require.Equal(t, uint16(warnCnt.add(0)), sessVars.StmtCtx.WarningCount())

	input.Reset()
	input.AppendFloat64(0, -1)
	err = f.vecEvalInt(input, result)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))
	require.Equal(t, uint16(warnCnt.add(1)), sessVars.StmtCtx.WarningCount())

	input.Reset()
	input.AppendNull(0)
	err = f.vecEvalInt(input, result)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))
	require.Equal(t, uint16(warnCnt.add(1)), sessVars.StmtCtx.WarningCount())

	input.Reset()
	input.AppendNull(0)
	input.AppendFloat64(0, 1)
	input.AppendFloat64(0, -1)
	err = f.vecEvalInt(input, result)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))
	require.Equal(t, int64(0), result.GetInt64(1))
	require.Equal(t, int64(0), result.GetInt64(2))
	require.Equal(t, uint16(warnCnt.add(2)), sessVars.StmtCtx.WarningCount())

	// for error case under the strict model
	sessVars.StmtCtx.BadNullAsWarning = false
	input.Reset()
	input.AppendNull(0)
	err = f.vecEvalInt(input, result)
	require.Error(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))

	sessVars.StmtCtx.SetWarnings(nil)
	input.Reset()
	input.AppendFloat64(0, -2.5)
	err = f.vecEvalInt(input, result)
	require.Error(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))

	// strict model
	input.Reset()
	input.AppendFloat64(0, 0.5)
	start := time.Now()
	err = f.vecEvalInt(input, result)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))
	sub := time.Since(start)
	require.GreaterOrEqual(t, sub.Nanoseconds(), int64(0.5*1e9))

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
	require.NoError(t, err)
	require.Equal(t, int64(0), result.GetInt64(0))
	require.Equal(t, int64(1), result.GetInt64(1))
	require.Equal(t, int64(1), result.GetInt64(2))
	require.LessOrEqual(t, sub.Nanoseconds(), int64(2*1e9))
	require.GreaterOrEqual(t, sub.Nanoseconds(), int64(1*1e9))
}
