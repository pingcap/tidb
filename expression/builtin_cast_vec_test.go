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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
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

func (g *dateTimeGenerWithFsp) gen() interface{} {
	result := g.defaultGener.gen()
	if t, ok := result.(types.Time); ok {
		t.SetFsp(g.fsp)
		return t
	}
	return result
}

type randJSONDuration struct{}

func (g *randJSONDuration) gen() interface{} {
	d := types.Duration{
		Duration: time.Duration(rand.Intn(12))*time.Hour + time.Duration(rand.Intn(60))*time.Minute + time.Duration(rand.Intn(60))*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond,
		Fsp:      3}
	return json.CreateBinary(d.String())
}

type datetimeJSONGener struct{}

func (g *datetimeJSONGener) gen() interface{} {
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
	return json.CreateBinary(d.String())
}

func TestVectorizedBuiltinCastEvalOneVec(t *testing.T) {
	testVectorizedEvalOneVec(t, vecBuiltinCastCases)
}

func TestVectorizedBuiltinCastFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinCastCases)
}

func TestVectorizedCastRealAsTime(t *testing.T) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}
	baseFunc, err := newBaseBuiltinFunc(mock.NewContext(), "", []Expression{col}, 0)
	if err != nil {
		panic(err)
	}
	cast := &builtinCastRealAsTimeSig{baseFunc}

	inputs := []*chunk.Chunk{
		genCastRealAsTime(),
	}

	for _, input := range inputs {
		result := chunk.NewColumn(types.NewFieldType(mysql.TypeDatetime), input.NumRows())
		require.NoError(t, cast.vecEvalTime(input, result))
		for i := 0; i < input.NumRows(); i++ {
			res, isNull, err := cast.evalTime(input.GetRow(i))
			require.NoError(t, err)
			if isNull {
				require.True(t, result.IsNull(i))
				continue
			}
			require.False(t, result.IsNull(i))
			require.Zero(t, result.GetTime(i).Compare(res))
		}
	}
}

func genCastRealAsTime() *chunk.Chunk {
	input := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 10)
	gen := newDefaultRandGen()
	for i := 0; i < 10; i++ {
		if i < 5 {
			input.AppendFloat64(0, 0)
		} else {
			input.AppendFloat64(0, gen.Float64()*100000)
		}
	}
	return input
}

// for issue https://github.com/pingcap/tidb/issues/16825
func TestVectorizedCastStringAsDecimalWithUnsignedFlagInUnion(t *testing.T) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0}
	baseFunc, err := newBaseBuiltinFunc(mock.NewContext(), "", []Expression{col}, 0)
	if err != nil {
		panic(err)
	}
	// set `inUnion` to `true`
	baseCast := newBaseBuiltinCastFunc(baseFunc, true)
	baseCast.tp = types.NewFieldType(mysql.TypeNewDecimal)
	// set the `UnsignedFlag` bit
	baseCast.tp.AddFlag(mysql.UnsignedFlag)
	cast := &builtinCastStringAsDecimalSig{baseCast}

	inputs := []*chunk.Chunk{
		genCastStringAsDecimal(false),
		genCastStringAsDecimal(true),
	}

	for _, input := range inputs {
		result := chunk.NewColumn(types.NewFieldType(mysql.TypeNewDecimal), input.NumRows())
		require.NoError(t, cast.vecEvalDecimal(input, result))
		for i := 0; i < input.NumRows(); i++ {
			res, isNull, err := cast.evalDecimal(input.GetRow(i))
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
