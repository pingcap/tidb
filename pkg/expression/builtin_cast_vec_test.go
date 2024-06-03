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
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var vecBuiltinCastCases = map[string][]vecExprBenchCase{
	ast.Cast: {
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{newDecimalJSONGener(0)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDecimalStringGener()}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{&constJSONGener{strconv.Itoa(rand.Int())}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{&numStrGener{*newRangeInt64Gener(math.MinInt64+1, 0)}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{&numStrGener{*newRangeInt64Gener(0, math.MaxInt64)}},
		},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{newRandDurInt()}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETReal}, geners: []dataGenerator{newRandDurReal()}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDecimal}, geners: []dataGenerator{newRandDurDecimal()}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{newDecimalJSONGener(0)}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRealStringGener()}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{newRangeDurationGener(0.5)}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime},
			geners: []dataGenerator{&dateTimeGenerWithFsp{
				defaultGener: *newDefaultGener(0.2, types.ETDatetime),
				fsp:          1,
			}},
		},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{&randJSONDuration{}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newJSONStringGener()}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{&datetimeJSONGener{}}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString},
			geners: []dataGenerator{
				&dateTimeStrGener{randGen: newDefaultRandGen()},
				&dateStrGener{randGen: newDefaultRandGen()},
				&timeStrGener{randGen: newDefaultRandGen()},
			}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETJson},
			geners: []dataGenerator{
				newJSONTimeGener(),
			}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETInt}},
	},
}

type dateTimeGenerWithFsp struct {
	defaultGener
	fsp int
}

func (g *dateTimeGenerWithFsp) gen() any {
	result := g.defaultGener.gen()
	if t, ok := result.(types.Time); ok {
		t.SetFsp(g.fsp)
		return t
	}
	return result
}

type randJSONDuration struct{}

func (g *randJSONDuration) gen() any {
	d := types.Duration{
		Duration: time.Duration(rand.Intn(12))*time.Hour + time.Duration(rand.Intn(60))*time.Minute + time.Duration(rand.Intn(60))*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond,
		Fsp:      3}
	return types.CreateBinaryJSON(d)
}

type datetimeJSONGener struct{}

func (g *datetimeJSONGener) gen() any {
	year := rand.Intn(2200)
	month := rand.Intn(10) + 1
	day := rand.Intn(20) + 1
	hour := rand.Intn(12)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microsecond := rand.Intn(1000000)
	d := types.NewTime(
		types.FromDate(year, month, day, hour, minute, second, microsecond),
		0,
		3,
	)
	return types.CreateBinaryJSON(d)
}

func TestVectorizedBuiltinCastEvalOneVec(t *testing.T) {
	testVectorizedEvalOneVec(t, vecBuiltinCastCases)
}

func TestVectorizedBuiltinCastFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinCastCases)
}

func TestVectorizedCastRealAsTime(t *testing.T) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}
	ctx := createContext(t)
	baseFunc, err := newBaseBuiltinFunc(ctx, "", []Expression{col}, types.NewFieldType(mysql.TypeDatetime))
	if err != nil {
		panic(err)
	}
	cast := &builtinCastRealAsTimeSig{baseFunc}

	inputChunk, expect := genCastRealAsTime()
	inputs := []*chunk.Chunk{
		inputChunk,
	}

	for _, input := range inputs {
		result := chunk.NewColumn(types.NewFieldType(mysql.TypeDatetime), input.NumRows())
		require.NoError(t, cast.vecEvalTime(ctx, input, result))
		for i := 0; i < input.NumRows(); i++ {
			res, isNull, err := cast.evalTime(ctx, input.GetRow(i))
			require.NoError(t, err)
			if expect[i] == nil {
				require.True(t, result.IsNull(i))
				require.True(t, isNull)
				continue
			}
			require.Equal(t, result.GetTime(i), *expect[i])
			require.Equal(t, res, *expect[i])
		}
	}
}

func getTime(year int, month int, day int, hour int, minute int, second int) *types.Time {
	retTime := types.NewTime(types.FromDate(year, month, day, hour, minute, second, 0), mysql.TypeDatetime, types.DefaultFsp)
	return &retTime
}

func genCastRealAsTime() (*chunk.Chunk, []*types.Time) {
	input := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 20)
	expect := make([]*types.Time, 0, 20)

	// valid
	input.AppendFloat64(0, 0)
	input.AppendFloat64(0, 101.1)
	input.AppendFloat64(0, 111.1)
	input.AppendFloat64(0, 1122.1)
	input.AppendFloat64(0, 31212.111)
	input.AppendFloat64(0, 121212.1111)
	input.AppendFloat64(0, 1121212.111111)
	input.AppendFloat64(0, 11121212.111111)
	input.AppendFloat64(0, 99991111.1111111)
	input.AppendFloat64(0, 201212121212.1111111)
	input.AppendFloat64(0, 20121212121212.1111111)
	// invalid
	input.AppendFloat64(0, 1.1)
	input.AppendFloat64(0, 48.1)
	input.AppendFloat64(0, 100.1)
	input.AppendFloat64(0, 1301.11)
	input.AppendFloat64(0, 1131.111)
	input.AppendFloat64(0, 100001111.111)
	input.AppendFloat64(0, 20121212121260.1111111)
	input.AppendFloat64(0, 20121212126012.1111111)
	input.AppendFloat64(0, 20121212241212.1111111)

	expect = append(expect, getTime(0, 0, 0, 0, 0, 0))
	expect = append(expect, getTime(2000, 1, 1, 0, 0, 0))
	expect = append(expect, getTime(2000, 1, 11, 0, 0, 0))
	expect = append(expect, getTime(2000, 11, 22, 0, 0, 0))
	expect = append(expect, getTime(2003, 12, 12, 0, 0, 0))
	expect = append(expect, getTime(2012, 12, 12, 0, 0, 0))
	expect = append(expect, getTime(112, 12, 12, 0, 0, 0))
	expect = append(expect, getTime(1112, 12, 12, 0, 0, 0))
	expect = append(expect, getTime(9999, 11, 11, 0, 0, 0))
	expect = append(expect, getTime(2020, 12, 12, 12, 12, 12))
	expect = append(expect, getTime(2012, 12, 12, 12, 12, 12))
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)
	expect = append(expect, nil)

	return input, expect
}

// for issue https://github.com/pingcap/tidb/issues/16825
func TestVectorizedCastStringAsDecimalWithUnsignedFlagInUnion(t *testing.T) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0}
	ctx := mock.NewContext()
	baseFunc, err := newBaseBuiltinFunc(ctx, "", []Expression{col}, types.NewFieldType(mysql.TypeNewDecimal))
	if err != nil {
		panic(err)
	}
	// set `inUnion` to `true`
	baseCast := newBaseBuiltinCastFunc(baseFunc, true)
	// set the `UnsignedFlag` bit
	baseCast.tp.AddFlag(mysql.UnsignedFlag)
	cast := &builtinCastStringAsDecimalSig{baseCast}

	inputs := []*chunk.Chunk{
		genCastStringAsDecimal(false),
		genCastStringAsDecimal(true),
	}

	for _, input := range inputs {
		result := chunk.NewColumn(types.NewFieldType(mysql.TypeNewDecimal), input.NumRows())
		require.NoError(t, cast.vecEvalDecimal(ctx, input, result))
		for i := 0; i < input.NumRows(); i++ {
			res, isNull, err := cast.evalDecimal(ctx, input.GetRow(i))
			require.False(t, isNull)
			require.NoError(t, err)
			require.Zero(t, result.GetDecimal(i).Compare(res))
		}
	}
}

func genCastStringAsDecimal(isNegative bool) *chunk.Chunk {
	var sign float64
	if isNegative {
		sign = -1
	} else {
		sign = 1
	}

	input := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 1024)
	for i := 0; i < 1024; i++ {
		d := new(types.MyDecimal)
		f := sign * rand.Float64() * 100000
		if err := d.FromFloat64(f); err != nil {
			panic(err)
		}
		input.AppendString(0, d.String())
	}

	return input
}

func BenchmarkVectorizedBuiltinCastEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinCastCases)
}

func BenchmarkVectorizedBuiltinCastFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinCastCases)
}
