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

package expression

// This file contains benchmarks of our expression evaluation.

import (
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/mock"
)

type benchHelper struct {
	ctx   sessionctx.Context
	exprs []Expression

	inputTypes  []*types.FieldType
	outputTypes []*types.FieldType
	inputChunk  *chunk.Chunk
	outputChunk *chunk.Chunk
}

func (h *benchHelper) init() {
	numRows := 4 * 1024

	h.ctx = mock.NewContext()
	h.ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	h.ctx.GetSessionVars().InitChunkSize = 32
	h.ctx.GetSessionVars().MaxChunkSize = numRows

	h.inputTypes = make([]*types.FieldType, 0, 10)
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	})
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      mysql.TypeDouble,
		Flen:    mysql.MaxRealWidth,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	})
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      mysql.TypeNewDecimal,
		Flen:    11,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	})

	// Use 20 string columns to show the cache performance.
	for i := 0; i < 20; i++ {
		h.inputTypes = append(h.inputTypes, &types.FieldType{
			Tp:      mysql.TypeVarString,
			Flen:    0,
			Decimal: types.UnspecifiedLength,
			Charset: charset.CharsetUTF8,
			Collate: charset.CollationUTF8,
		})
	}

	h.inputChunk = chunk.NewChunkWithCapacity(h.inputTypes, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		h.inputChunk.AppendInt64(0, 4)
		h.inputChunk.AppendFloat64(1, 2.019)
		h.inputChunk.AppendMyDecimal(2, types.NewDecFromFloatForTest(5.9101))
		for i := 0; i < 20; i++ {
			h.inputChunk.AppendString(3+i, `abcdefughasfjsaljal1321798273528791!&(*#&@&^%&%^&!)sadfashqwer`)
		}
	}

	cols := make([]*Column, 0, len(h.inputTypes))
	for i := 0; i < len(h.inputTypes); i++ {
		cols = append(cols, &Column{
			ColName: model.NewCIStr(fmt.Sprintf("col_%v", i)),
			RetType: h.inputTypes[i],
			Index:   i,
		})
	}

	h.exprs = make([]Expression, 0, 10)
	if expr, err := NewFunction(h.ctx, ast.Substr, h.inputTypes[3], []Expression{cols[3], cols[2]}...); err != nil {
		panic("create SUBSTR function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.Plus, h.inputTypes[0], []Expression{cols[1], cols[2]}...); err != nil {
		panic("create PLUS function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[11], cols[8]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[19], cols[10]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[17], cols[4]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[18], cols[5]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.LE, h.inputTypes[2], []Expression{cols[19], cols[4]}...); err != nil {
		panic("create LE function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.EQ, h.inputTypes[2], []Expression{cols[20], cols[3]}...); err != nil {
		panic("create EQ function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}
	h.exprs = append(h.exprs, cols[2])
	h.exprs = append(h.exprs, cols[2])

	h.outputTypes = make([]*types.FieldType, 0, len(h.exprs))
	for i := 0; i < len(h.exprs); i++ {
		h.outputTypes = append(h.outputTypes, h.exprs[i].GetType())
	}

	h.outputChunk = chunk.NewChunkWithCapacity(h.outputTypes, numRows)
}

func BenchmarkVectorizedExecute(b *testing.B) {
	h := benchHelper{}
	h.init()
	inputIter := chunk.NewIterator4Chunk(h.inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.outputChunk.Reset()
		if err := VectorizedExecute(h.ctx, h.exprs, inputIter, h.outputChunk); err != nil {
			panic("errors happened during \"VectorizedExecute\"")
		}
	}
}

func BenchmarkScalarFunctionClone(b *testing.B) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	con1 := One.Clone()
	con2 := Zero.Clone()
	add := NewFunctionInternal(mock.NewContext(), ast.Plus, types.NewFieldType(mysql.TypeLonglong), col, con1)
	sub := NewFunctionInternal(mock.NewContext(), ast.Plus, types.NewFieldType(mysql.TypeLonglong), add, con2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub.Clone()
	}
	b.ReportAllocs()
}

// dataGenerator is used to generate data for test.
type dataGenerator interface {
	gen() interface{}
}

type defaultGener struct {
	nullRation float64
	eType      types.EvalType
}

func (g *defaultGener) gen() interface{} {
	if rand.Float64() < g.nullRation {
		return nil
	}
	switch g.eType {
	case types.ETInt:
		if rand.Float64() < 0.5 {
			return -rand.Int63()
		}
		return rand.Int63()
	case types.ETReal:
		if rand.Float64() < 0.5 {
			return -rand.Float64()
		}
		return rand.Float64()
	case types.ETDecimal:
		d := new(types.MyDecimal)
		f := rand.Float64() * 100000
		if err := d.FromFloat64(f); err != nil {
			panic(err)
		}
		return d
	case types.ETDatetime, types.ETTimestamp:
		gt := types.FromDate(rand.Intn(2200), rand.Intn(10)+1, rand.Intn(20)+1, rand.Intn(12), rand.Intn(60), rand.Intn(60), rand.Intn(1000000))
		t := types.Time{Time: gt, Type: convertETType(g.eType)}
		return t
	case types.ETDuration:
		d := types.Duration{
			// use rand.Int32() to make it not overflow when AddDuration
			Duration: time.Duration(rand.Int31()),
		}
		return d
	case types.ETJson:
		j := new(json.BinaryJSON)
		if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"key":%v}`, rand.Int()))); err != nil {
			panic(err)
		}
		return *j
	case types.ETString:
		return randString()
	}
	return nil
}

type jsonStringGener struct{}

func (g *jsonStringGener) gen() interface{} {
	j := new(json.BinaryJSON)
	if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"key":%v}`, rand.Int()))); err != nil {
		panic(err)
	}
	return j.String()
}

type rangeDurationGener struct {
	nullRation float64
}

func (g *rangeDurationGener) gen() interface{} {
	if rand.Float64() < g.nullRation {
		return nil
	}
	tm := (math.Abs(rand.Int63n(12))*3600 + math.Abs(rand.Int63n(60))*60 + math.Abs(rand.Int63n(60))) * 1000
	tu := (tm + math.Abs(rand.Int63n(1000))) * 1000
	return types.Duration{
		Duration: time.Duration(tu * 1000)}
}

type timeFormatGener struct {
	nullRation float64
}

func (g *timeFormatGener) gen() interface{} {
	if rand.Float64() < g.nullRation {
		return nil
	}
	switch rand.Uint32() % 4 {
	case 0:
		return "%H %i %S"
	case 1:
		return "%l %i %s"
	case 2:
		return "%p %i %s"
	case 3:
		return "%I %i %S %f"
	case 4:
		return "%T"
	default:
		return nil
	}
}

// rangeRealGener is used to generate float64 items in [begin, end].
type rangeRealGener struct {
	begin float64
	end   float64

	nullRation float64
}

func (g *rangeRealGener) gen() interface{} {
	if rand.Float64() < g.nullRation {
		return nil
	}
	if g.end < g.begin {
		g.begin = -100
		g.end = 100
	}
	return rand.Float64()*(g.end-g.begin) + g.begin
}

// rangeDecimalGener is used to generate decimal items in [begin, end].
type rangeDecimalGener struct {
	begin float64
	end   float64

	nullRation float64
}

func (g *rangeDecimalGener) gen() interface{} {
	if rand.Float64() < g.nullRation {
		return nil
	}
	if g.end < g.begin {
		g.begin = -100000
		g.end = 100000
	}
	d := new(types.MyDecimal)
	f := rand.Float64()*(g.end-g.begin) + g.begin
	if err := d.FromFloat64(f); err != nil {
		panic(err)
	}
	return d
}

// rangeInt64Gener is used to generate int64 items in [begin, end).
type rangeInt64Gener struct {
	begin int
	end   int
}

func (rig *rangeInt64Gener) gen() interface{} {
	return int64(rand.Intn(rig.end-rig.begin) + rig.begin)
}

// numStrGener is used to generate number strings.
type numStrGener struct {
	rangeInt64Gener
}

func (g *numStrGener) gen() interface{} {
	return fmt.Sprintf("%v", g.rangeInt64Gener.gen())
}

// randLenStrGener is used to generate strings whose lengths are in [lenBegin, lenEnd).
type randLenStrGener struct {
	lenBegin int
	lenEnd   int
}

func (g *randLenStrGener) gen() interface{} {
	n := rand.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := rand.Intn(62)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else {
			buf[i] = byte('A' + x - 10 - 26)
		}
	}
	return string(buf)
}

type randHexStrGener struct {
	lenBegin int
	lenEnd   int
}

func (g *randHexStrGener) gen() interface{} {
	n := rand.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := rand.Intn(16)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else {
			if x%2 == 0 {
				buf[i] = byte('a' + x - 10)
			} else {
				buf[i] = byte('A' + x - 10)
			}
		}
	}
	return string(buf)
}

// dataTimeStrGener is used to generate strings which are dataTime format
type dataTimeStrGener struct{}

func (g *dataTimeStrGener) gen() interface{} {
	year := rand.Intn(2200)
	month := rand.Intn(10) + 1
	day := rand.Intn(20) + 1
	hour := rand.Intn(12)
	minute := rand.Intn(60)
	second := rand.Intn(60)

	return fmt.Sprintf("%d-%d-%d %d:%d:%d",
		year, month, day, hour, minute, second)
}

// timeStrGener is used to generate strings which are time format
type timeStrGener struct{}

func (g *timeStrGener) gen() interface{} {
	year := rand.Intn(2200)
	month := rand.Intn(10) + 1
	day := rand.Intn(20) + 1

	return fmt.Sprintf("%d-%d-%d", year, month, day)
}

// dataStrGener is used to generate strings which are data format
type dataStrGener struct{}

func (g *dataStrGener) gen() interface{} {
	hour := rand.Intn(12)
	minute := rand.Intn(60)
	second := rand.Intn(60)

	return fmt.Sprintf("%d:%d:%d", hour, minute, second)
}

type randDurInt struct{}

func (g *randDurInt) gen() interface{} {
	return int64(rand.Intn(types.TimeMaxHour)*10000 + rand.Intn(60)*100 + rand.Intn(60))
}

type vecExprBenchCase struct {
	// retEvalType is the EvalType of the expression result.
	// This field is required.
	retEvalType types.EvalType
	// childrenTypes is the EvalTypes of the expression children(arguments).
	// This field is required.
	childrenTypes []types.EvalType
	// childrenFieldTypes is the field types of the expression children(arguments).
	// If childrenFieldTypes is not set, it will be converted from childrenTypes.
	// This field is optional.
	childrenFieldTypes []*types.FieldType
	// geners are used to generate data for children and geners[i] generates data for children[i].
	// If geners[i] is nil, the default dataGenerator will be used for its corresponding child.
	// The geners slice can be shorter than the children slice, if it has 3 children, then
	// geners[gen1, gen2] will be regarded as geners[gen1, gen2, nil].
	// This field is optional.
	geners []dataGenerator
}

type vecExprBenchCases map[string][]vecExprBenchCase

func fillColumn(eType types.EvalType, chk *chunk.Chunk, colIdx int, testCase vecExprBenchCase) {
	batchSize := 1024
	var gen dataGenerator
	if len(testCase.geners) > colIdx && testCase.geners[colIdx] != nil {
		gen = testCase.geners[colIdx]
	} else {
		gen = &defaultGener{0.2, eType}
	}

	col := chk.Column(colIdx)
	col.Reset()
	for i := 0; i < batchSize; i++ {
		v := gen.gen()
		if v == nil {
			col.AppendNull()
			continue
		}
		switch eType {
		case types.ETInt:
			col.AppendInt64(v.(int64))
		case types.ETReal:
			col.AppendFloat64(v.(float64))
		case types.ETDecimal:
			col.AppendMyDecimal(v.(*types.MyDecimal))
		case types.ETDatetime, types.ETTimestamp:
			col.AppendTime(v.(types.Time))
		case types.ETDuration:
			col.AppendDuration(v.(types.Duration))
		case types.ETJson:
			col.AppendJSON(v.(json.BinaryJSON))
		case types.ETString:
			col.AppendString(v.(string))
		}
	}
}

func randString() string {
	n := 10 + rand.Intn(10)
	buf := make([]byte, n)
	for i := range buf {
		x := rand.Intn(62)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else {
			buf[i] = byte('A' + x - 10 - 26)
		}
	}
	return string(buf)
}

func eType2FieldType(eType types.EvalType) *types.FieldType {
	switch eType {
	case types.ETInt:
		return types.NewFieldType(mysql.TypeLonglong)
	case types.ETReal:
		return types.NewFieldType(mysql.TypeDouble)
	case types.ETDecimal:
		return types.NewFieldType(mysql.TypeNewDecimal)
	case types.ETDatetime, types.ETTimestamp:
		return types.NewFieldType(mysql.TypeDatetime)
	case types.ETDuration:
		return types.NewFieldType(mysql.TypeDuration)
	case types.ETJson:
		return types.NewFieldType(mysql.TypeJSON)
	case types.ETString:
		return types.NewFieldType(mysql.TypeVarString)
	default:
		panic(fmt.Sprintf("EvalType=%v is not supported.", eType))
	}
}

func genVecExprBenchCase(ctx sessionctx.Context, funcName string, testCase vecExprBenchCase) (expr Expression, fts []*types.FieldType, input *chunk.Chunk, output *chunk.Chunk) {
	fts = make([]*types.FieldType, len(testCase.childrenTypes))
	for i := range fts {
		if i < len(testCase.childrenFieldTypes) && testCase.childrenFieldTypes[i] != nil {
			fts[i] = testCase.childrenFieldTypes[i]
		} else {
			fts[i] = eType2FieldType(testCase.childrenTypes[i])
		}
	}
	cols := make([]Expression, len(testCase.childrenTypes))
	input = chunk.New(fts, 1024, 1024)
	for i, eType := range testCase.childrenTypes {
		fillColumn(eType, input, i, testCase)
		cols[i] = &Column{Index: i, RetType: fts[i]}
	}

	expr, err := NewFunction(ctx, funcName, eType2FieldType(testCase.retEvalType), cols...)
	if err != nil {
		panic(err)
	}

	output = chunk.New([]*types.FieldType{eType2FieldType(testCase.retEvalType)}, 1024, 1024)
	return expr, fts, input, output
}

// testVectorizedEvalOneVec is used to verify that the vectorized
// expression is evaluated correctly during projection
func testVectorizedEvalOneVec(c *C, vecExprCases vecExprBenchCases) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			expr, fts, input, output := genVecExprBenchCase(ctx, funcName, testCase)
			commentf := func(row int) CommentInterface {
				return Commentf("case %+v, row: %v, rowData: %v", testCase, row, input.GetRow(row).GetDatumRow(fts))
			}
			output2 := output.CopyConstruct()
			c.Assert(evalOneVec(ctx, expr, input, output, 0), IsNil)
			it := chunk.NewIterator4Chunk(input)
			c.Assert(evalOneColumn(ctx, expr, it, output2, 0), IsNil)

			c1, c2 := output.Column(0), output2.Column(0)
			switch testCase.retEvalType {
			case types.ETInt:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetInt64(i), Equals, c2.GetInt64(i), commentf(i))
					}
				}
			case types.ETReal:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetFloat64(i), Equals, c2.GetFloat64(i), commentf(i))
					}
				}
			case types.ETDecimal:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetDecimal(i), DeepEquals, c2.GetDecimal(i), commentf(i))
					}
				}
			case types.ETDatetime, types.ETTimestamp:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetTime(i), DeepEquals, c2.GetTime(i), commentf(i))
					}
				}
			case types.ETDuration:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetDuration(i, 0), Equals, c2.GetDuration(i, 0), commentf(i))
					}
				}
			case types.ETJson:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetJSON(i), DeepEquals, c2.GetJSON(i), commentf(i))
					}
				}
			case types.ETString:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetString(i), Equals, c2.GetString(i), commentf(i))
					}
				}
			}
		}
	}
}

// benchmarkVectorizedEvalOneVec is used to get the effect of
// using the vectorized expression evaluations during projection
func benchmarkVectorizedEvalOneVec(b *testing.B, vecExprCases vecExprBenchCases) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			expr, _, input, output := genVecExprBenchCase(ctx, funcName, testCase)
			exprName := expr.String()
			if sf, ok := expr.(*ScalarFunction); ok {
				exprName = fmt.Sprintf("%v", reflect.TypeOf(sf.Function))
				tmp := strings.Split(exprName, ".")
				exprName = tmp[len(tmp)-1]
			}

			b.Run(exprName+"-EvalOneVec", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := evalOneVec(ctx, expr, input, output, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(exprName+"-EvalOneCol", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it := chunk.NewIterator4Chunk(input)
					if err := evalOneColumn(ctx, expr, it, output, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func genVecBuiltinFuncBenchCase(ctx sessionctx.Context, funcName string, testCase vecExprBenchCase) (baseFunc builtinFunc, fts []*types.FieldType, input *chunk.Chunk, result *chunk.Column) {
	childrenNumber := len(testCase.childrenTypes)
	fts = make([]*types.FieldType, childrenNumber)
	for i := range fts {
		if i < len(testCase.childrenFieldTypes) && testCase.childrenFieldTypes[i] != nil {
			fts[i] = testCase.childrenFieldTypes[i]
		} else {
			fts[i] = eType2FieldType(testCase.childrenTypes[i])
		}
	}
	cols := make([]Expression, childrenNumber)
	input = chunk.New(fts, 1024, 1024)
	for i, eType := range testCase.childrenTypes {
		fillColumn(eType, input, i, testCase)
		cols[i] = &Column{Index: i, RetType: fts[i]}
	}
	if len(cols) == 0 {
		input.SetNumVirtualRows(1024)
	}

	var err error
	if funcName == ast.Cast {
		var fc functionClass
		tp := eType2FieldType(testCase.retEvalType)
		switch testCase.retEvalType {
		case types.ETInt:
			fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETDecimal:
			fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETReal:
			fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETDatetime, types.ETTimestamp:
			fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETDuration:
			fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETJson:
			fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETString:
			fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		}
		baseFunc, err = fc.getFunction(ctx, cols)
	} else {
		baseFunc, err = funcs[funcName].getFunction(ctx, cols)
	}
	if err != nil {
		panic(err)
	}
	result = chunk.NewColumn(eType2FieldType(testCase.retEvalType), 1024)
	// Mess up the output to make sure vecEvalXXX to call ResizeXXX/ReserveXXX itself.
	result.AppendNull()
	return baseFunc, fts, input, result
}

// a hack way to calculate length of a chunk.Column.
func getColumnLen(col *chunk.Column, eType types.EvalType) int {
	chk := chunk.New([]*types.FieldType{eType2FieldType(eType)}, 1024, 1024)
	chk.SetCol(0, col)
	return chk.NumRows()
}

// removeTestOptions removes all not needed options like '-test.timeout=' from argument list
func removeTestOptions(args []string) []string {
	argList := args[:0]

	// args contains '-test.timeout=' option for example
	// excluding it to be able to run all tests
	for _, arg := range args {
		if strings.HasPrefix(arg, "builtin") {
			argList = append(argList, arg)
		}
	}
	return argList
}

// testVectorizedBuiltinFunc is used to verify that the vectorized
// expression is evaluated correctly
func testVectorizedBuiltinFunc(c *C, vecExprCases vecExprBenchCases) {
	testFunc := make(map[string]bool)
	argList := removeTestOptions(flag.Args())
	testAll := len(argList) == 0
	for _, arg := range argList {
		testFunc[arg] = true
	}
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			ctx := mock.NewContext()
			baseFunc, fts, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			baseFuncName := fmt.Sprintf("%v", reflect.TypeOf(baseFunc))
			tmp := strings.Split(baseFuncName, ".")
			baseFuncName = tmp[len(tmp)-1]

			if !testAll && testFunc[baseFuncName] != true {
				continue
			}
			// do not forget to implement the vectorized method.
			c.Assert(baseFunc.vectorized(), IsTrue, Commentf("func: %v", baseFuncName))
			commentf := func(row int) CommentInterface {
				return Commentf("func: %v, case %+v, row: %v, rowData: %v", baseFuncName, testCase, row, input.GetRow(row).GetDatumRow(fts))
			}
			it := chunk.NewIterator4Chunk(input)
			i := 0
			var vecWarnCnt uint16
			switch testCase.retEvalType {
			case types.ETInt:
				err := baseFunc.vecEvalInt(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				i64s := output.Int64s()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalInt(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, i64s[i], commentf(i))
					}
					i++
				}
			case types.ETReal:
				err := baseFunc.vecEvalReal(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				f64s := output.Float64s()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalReal(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, f64s[i], commentf(i))
					}
					i++
				}
			case types.ETDecimal:
				err := baseFunc.vecEvalDecimal(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				d64s := output.Decimals()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalDecimal(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(*val, Equals, d64s[i], commentf(i))
					}
					i++
				}
			case types.ETDatetime, types.ETTimestamp:
				err := baseFunc.vecEvalTime(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				t64s := output.Times()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalTime(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, t64s[i], commentf(i))
					}
					i++
				}
			case types.ETDuration:
				err := baseFunc.vecEvalDuration(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				d64s := output.GoDurations()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalDuration(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val.Duration, Equals, d64s[i], commentf(i))
					}
					i++
				}
			case types.ETJson:
				err := baseFunc.vecEvalJSON(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalJSON(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						var cmp int
						cmp = json.CompareBinary(val, output.GetJSON(i))
						c.Assert(cmp, Equals, 0, commentf(i))
					}
					i++
				}
			case types.ETString:
				err := baseFunc.vecEvalString(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getColumnLen(output, testCase.retEvalType), Equals, input.NumRows())
				vecWarnCnt = ctx.GetSessionVars().StmtCtx.WarningCount()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalString(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, output.GetString(i), commentf(i))
					}
					i++
				}
			default:
				c.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
			}

			// check warnings
			totalWarns := ctx.GetSessionVars().StmtCtx.WarningCount()
			c.Assert(2*vecWarnCnt, Equals, totalWarns)
			warns := ctx.GetSessionVars().StmtCtx.GetWarnings()
			for i := 0; i < int(vecWarnCnt); i++ {
				c.Assert(terror.ErrorEqual(warns[i].Err, warns[i+int(vecWarnCnt)].Err), IsTrue)
			}
		}
	}
}

// benchmarkVectorizedBuiltinFunc is used to get the effect of
// using the vectorized expression evaluations
func benchmarkVectorizedBuiltinFunc(b *testing.B, vecExprCases vecExprBenchCases) {
	ctx := mock.NewContext()
	testFunc := make(map[string]bool)
	argList := removeTestOptions(flag.Args())
	testAll := len(argList) == 0
	for _, arg := range argList {
		testFunc[arg] = true
	}
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			baseFunc, _, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			baseFuncName := fmt.Sprintf("%v", reflect.TypeOf(baseFunc))
			tmp := strings.Split(baseFuncName, ".")
			baseFuncName = tmp[len(tmp)-1]

			if !testAll && testFunc[baseFuncName] != true {
				continue
			}

			b.Run(baseFuncName+"-VecBuiltinFunc", func(b *testing.B) {
				b.ResetTimer()
				switch testCase.retEvalType {
				case types.ETInt:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalInt(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETReal:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalReal(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETDecimal:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalDecimal(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETDatetime, types.ETTimestamp:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalTime(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETDuration:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalDuration(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETJson:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalJSON(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETString:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalString(input, output); err != nil {
							b.Fatal(err)
						}
					}
				default:
					b.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
				}
			})
			b.Run(baseFuncName+"-NonVecBuiltinFunc", func(b *testing.B) {
				b.ResetTimer()
				it := chunk.NewIterator4Chunk(input)
				switch testCase.retEvalType {
				case types.ETInt:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalInt(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendInt64(v)
							}
						}
					}
				case types.ETReal:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalReal(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendFloat64(v)
							}
						}
					}
				case types.ETDecimal:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalDecimal(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendMyDecimal(v)
							}
						}
					}
				case types.ETDatetime, types.ETTimestamp:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalTime(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendTime(v)
							}
						}
					}
				case types.ETDuration:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalDuration(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendDuration(v)
							}
						}
					}
				case types.ETJson:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalJSON(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendJSON(v)
							}
						}
					}
				case types.ETString:
					for i := 0; i < b.N; i++ {
						output.Reset()
						for row := it.Begin(); row != it.End(); row = it.Next() {
							v, isNull, err := baseFunc.evalString(row)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendString(v)
							}
						}
					}
				default:
					b.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
				}
			})
		}
	}
}

func genVecEvalBool(numCols int, colTypes, eTypes []types.EvalType) (CNFExprs, *chunk.Chunk) {
	gens := make([]dataGenerator, 0, len(eTypes))
	for _, eType := range eTypes {
		if eType == types.ETString {
			gens = append(gens, &numStrGener{rangeInt64Gener{0, 10}})
		} else {
			gens = append(gens, &defaultGener{nullRation: 0.05, eType: eType})
		}
	}

	ts := make([]types.EvalType, 0, numCols)
	gs := make([]dataGenerator, 0, numCols)
	fts := make([]*types.FieldType, 0, numCols)
	for i := 0; i < numCols; i++ {
		idx := rand.Intn(len(eTypes))
		if colTypes != nil {
			for j := range eTypes {
				if colTypes[i] == eTypes[j] {
					idx = j
					break
				}
			}
		}
		ts = append(ts, eTypes[idx])
		gs = append(gs, gens[idx])
		fts = append(fts, eType2FieldType(eTypes[idx]))
	}

	input := chunk.New(fts, 1024, 1024)
	exprs := make(CNFExprs, 0, numCols)
	for i := 0; i < numCols; i++ {
		fillColumn(ts[i], input, i, vecExprBenchCase{geners: gs})
		exprs = append(exprs, &Column{Index: i, RetType: fts[i]})
	}
	return exprs, input
}

func generateRandomSel() []int {
	rand.Seed(int64(time.Now().UnixNano()))
	var sel []int
	count := 0
	// Use constant 256 to make it faster to generate randomly arranged sel slices
	num := rand.Intn(256) + 1
	existed := make([]bool, 1024)
	for i := 0; i < 1024; i++ {
		existed[i] = false
	}
	for count < num {
		val := rand.Intn(1024)
		if !existed[val] {
			existed[val] = true
			count++
		}
	}
	for i := 0; i < 1024; i++ {
		if existed[i] {
			sel = append(sel, i)
		}
	}
	return sel
}

func (s *testEvaluatorSuite) TestVecEvalBool(c *C) {
	ctx := mock.NewContext()
	eTypes := []types.EvalType{types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	for numCols := 1; numCols <= 10; numCols++ {
		for round := 0; round < 64; round++ {
			exprs, input := genVecEvalBool(numCols, nil, eTypes)
			selected, nulls, err := VecEvalBool(ctx, exprs, input, nil, nil)
			c.Assert(err, IsNil)
			it := chunk.NewIterator4Chunk(input)
			i := 0
			for row := it.Begin(); row != it.End(); row = it.Next() {
				ok, null, err := EvalBool(mock.NewContext(), exprs, row)
				c.Assert(err, IsNil)
				c.Assert(null, Equals, nulls[i])
				c.Assert(ok, Equals, selected[i])
				i++
			}
		}
	}
}

func BenchmarkVecEvalBool(b *testing.B) {
	ctx := mock.NewContext()
	selected := make([]bool, 0, 1024)
	nulls := make([]bool, 0, 1024)
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for numCols := 1; numCols <= 3; numCols++ {
		typeCombination := make([]types.EvalType, numCols)
		var combFunc func(nCols int)
		combFunc = func(nCols int) {
			if nCols == 0 {
				name := ""
				for _, t := range typeCombination {
					for i := range eTypes {
						if t == eTypes[i] {
							name += tNames[t] + "/"
						}
					}
				}
				exprs, input := genVecEvalBool(numCols, typeCombination, eTypes)
				b.Run("Vec-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := VecEvalBool(ctx, exprs, input, selected, nulls)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("Row-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						it := chunk.NewIterator4Chunk(input)
						for row := it.Begin(); row != it.End(); row = it.Next() {
							_, _, err := EvalBool(ctx, exprs, row)
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				})
				return
			}
			for _, eType := range eTypes {
				typeCombination[nCols-1] = eType
				combFunc(nCols - 1)
			}
		}

		combFunc(numCols)
	}
}

func (s *testEvaluatorSuite) TestRowBasedFilterAndVectorizedFilter(c *C) {
	ctx := mock.NewContext()
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	for numCols := 1; numCols <= 10; numCols++ {
		for round := 0; round < 64; round++ {
			exprs, input := genVecEvalBool(numCols, nil, eTypes)
			it := chunk.NewIterator4Chunk(input)
			isNull := make([]bool, it.Len())
			selected, nulls, err := rowBasedFilter(ctx, exprs, it, nil, isNull)
			c.Assert(err, IsNil)
			selected2, nulls2, err2 := vectorizedFilter(ctx, exprs, it, nil, isNull)
			c.Assert(err2, IsNil)
			length := it.Len()
			for i := 0; i < length; i++ {
				c.Assert(nulls2[i], Equals, nulls[i])
				c.Assert(selected2[i], Equals, selected[i])
			}
		}
	}
}

func BenchmarkRowBasedFilterAndVectorizedFilter(b *testing.B) {
	ctx := mock.NewContext()
	selected := make([]bool, 0, 1024)
	nulls := make([]bool, 0, 1024)
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for numCols := 1; numCols <= 2; numCols++ {
		typeCombination := make([]types.EvalType, numCols)
		var combFunc func(nCols int)
		combFunc = func(nCols int) {
			if nCols == 0 {
				name := ""
				for _, t := range typeCombination {
					for i := range eTypes {
						if t == eTypes[i] {
							name += tNames[t] + "/"
						}
					}
				}
				exprs, input := genVecEvalBool(numCols, typeCombination, eTypes)
				it := chunk.NewIterator4Chunk(input)
				b.Run("Vec-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := vectorizedFilter(ctx, exprs, it, selected, nulls)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("Row-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := rowBasedFilter(ctx, exprs, it, selected, nulls)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				return
			}
			for _, eType := range eTypes {
				typeCombination[nCols-1] = eType
				combFunc(nCols - 1)
			}
		}
		combFunc(numCols)
	}

	// Add special case to prove when some calculations are added,
	// the vectorizedFilter for int types will be more faster than rowBasedFilter.
	funcName := ast.Least
	testCase := vecExprBenchCase{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}}
	expr, _, input, _ := genVecExprBenchCase(ctx, funcName, testCase)
	it := chunk.NewIterator4Chunk(input)

	b.Run("Vec-special case", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := vectorizedFilter(ctx, []Expression{expr}, it, selected, nulls)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("Row-special case", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := rowBasedFilter(ctx, []Expression{expr}, it, selected, nulls)
			if err != nil {
				panic(err)
			}
		}
	})
}

func (s *testEvaluatorSuite) TestVectorizedFilterConsiderNull(c *C) {
	ctx := mock.NewContext()
	dafaultEnableVectorizedExpressionVar := ctx.GetSessionVars().EnableVectorizedExpression
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	for numCols := 1; numCols <= 10; numCols++ {
		for round := 0; round < 64; round++ {
			exprs, input := genVecEvalBool(numCols, nil, eTypes)
			it := chunk.NewIterator4Chunk(input)
			isNull := make([]bool, it.Len())
			ctx.GetSessionVars().EnableVectorizedExpression = false
			selected, nulls, err := VectorizedFilterConsiderNull(ctx, exprs, it, nil, isNull)
			c.Assert(err, IsNil)
			ctx.GetSessionVars().EnableVectorizedExpression = true
			selected2, nulls2, err2 := VectorizedFilterConsiderNull(ctx, exprs, it, nil, isNull)
			c.Assert(err2, IsNil)
			length := it.Len()
			for i := 0; i < length; i++ {
				c.Assert(nulls2[i], Equals, nulls[i])
				c.Assert(selected2[i], Equals, selected[i])
			}

			// add test which sel is not nil
			randomSel := generateRandomSel()
			input.SetSel(randomSel)
			it2 := chunk.NewIterator4Chunk(input)
			isNull = isNull[:0]
			ctx.GetSessionVars().EnableVectorizedExpression = false
			selected3, nulls, err := VectorizedFilterConsiderNull(ctx, exprs, it2, nil, isNull)
			c.Assert(err, IsNil)
			ctx.GetSessionVars().EnableVectorizedExpression = true
			selected4, nulls2, err2 := VectorizedFilterConsiderNull(ctx, exprs, it2, nil, isNull)
			c.Assert(err2, IsNil)
			for i := 0; i < length; i++ {
				c.Assert(nulls2[i], Equals, nulls[i])
				c.Assert(selected4[i], Equals, selected3[i])
			}

			unselected := make([]bool, length)
			// unselected[i] == false means that the i-th row is selected
			for i := 0; i < length; i++ {
				unselected[i] = true
			}
			for _, idx := range randomSel {
				unselected[idx] = false
			}
			for i := range selected2 {
				if selected2[i] && unselected[i] {
					selected2[i] = false
				}
			}
			for i := 0; i < length; i++ {
				c.Assert(selected2[i], Equals, selected4[i])
			}
		}
	}
	ctx.GetSessionVars().EnableVectorizedExpression = dafaultEnableVectorizedExpressionVar
}
