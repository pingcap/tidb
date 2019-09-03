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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
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

type vecExprBenchCase struct {
	retEvalType   types.EvalType
	childrenTypes []types.EvalType
}

var vecExprBenchCases = map[string][]vecExprBenchCase{
	ast.Cast: {
		{types.ETInt, []types.EvalType{types.ETInt}},
	},
}

func fillColumn(eType types.EvalType, chk *chunk.Chunk, colIdx int) {
	nullRatio := 0.2
	batchSize := 1024
	switch eType {
	case types.ETInt:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				if rand.Float64() < 0.5 {
					chk.AppendInt64(colIdx, -rand.Int63())
				} else {
					chk.AppendInt64(colIdx, rand.Int63())
				}
			}
		}
	case types.ETReal:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				if rand.Float64() < 0.5 {
					chk.AppendFloat64(colIdx, -rand.Float64())
				} else {
					chk.AppendFloat64(colIdx, rand.Float64())
				}
			}
		}
	case types.ETDecimal:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				d := new(types.MyDecimal)
				f := rand.Float64() * 100000
				if err := d.FromFloat64(f); err != nil {
					panic(err)
				}
				chk.AppendMyDecimal(colIdx, d)
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				gt := types.FromDate(rand.Intn(2200), rand.Intn(10)+1, rand.Intn(20)+1, rand.Intn(12), rand.Intn(60), rand.Intn(60), rand.Intn(1000))
				t := types.Time{Time: gt, Type: convertETType(eType)}
				chk.AppendTime(colIdx, t)
			}
		}
	case types.ETDuration:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				d := types.Duration{Duration: time.Duration(rand.Int())}
				chk.AppendDuration(colIdx, d)
			}
		}
	case types.ETJson:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				j := new(json.BinaryJSON)
				if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"key":%v}`, rand.Int()))); err != nil {
					panic(err)
				}
				chk.AppendJSON(colIdx, *j)
			}
		}
	case types.ETString:
		for i := 0; i < batchSize; i++ {
			if rand.Float64() < nullRatio {
				chk.AppendNull(colIdx)
			} else {
				chk.AppendString(colIdx, fmt.Sprintf("%v", rand.Int()))
			}
		}
	default:
		panic(fmt.Sprintf("EvalType=%v is not supported.", eType))
	}
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
		return types.NewFieldType(mysql.TypeDate)
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

func genVecExprBenchCase(ctx sessionctx.Context, funcName string, testCase vecExprBenchCase) (expr Expression, input *chunk.Chunk, output *chunk.Chunk) {
	fts := make([]*types.FieldType, len(testCase.childrenTypes))
	for i, eType := range testCase.childrenTypes {
		fts[i] = eType2FieldType(eType)
	}
	cols := make([]Expression, len(testCase.childrenTypes))
	input = chunk.New(fts, 1024, 1024)
	for i, eType := range testCase.childrenTypes {
		fillColumn(eType, input, i)
		cols[i] = &Column{Index: i, RetType: fts[i]}
	}

	expr, err := NewFunction(ctx, funcName, eType2FieldType(testCase.retEvalType), cols...)
	if err != nil {
		panic(err)
	}

	output = chunk.New([]*types.FieldType{eType2FieldType(testCase.retEvalType)}, 1024, 1024)
	return expr, input, output
}

func (s *testEvaluatorSuite) TestVectorizedEvalOneVec(c *C) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprBenchCases {
		for _, testCase := range testCases {
			expr, input, output := genVecExprBenchCase(ctx, funcName, testCase)
			output2 := output.CopyConstruct()
			c.Assert(evalOneVec(ctx, expr, input, output, 0), IsNil)
			it := chunk.NewIterator4Chunk(input)
			c.Assert(evalOneColumn(ctx, expr, it, output2, 0), IsNil)

			c1, c2 := output.Column(0), output2.Column(0)
			switch testCase.retEvalType {
			case types.ETInt:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetInt64(i) != c2.GetInt64(i)), IsFalse)
				}
			case types.ETReal:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetFloat64(i) != c2.GetFloat64(i)), IsFalse)
				}
			case types.ETDecimal:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetDecimal(i).Compare(c2.GetDecimal(i)) != 0), IsFalse)
				}
			case types.ETDatetime, types.ETTimestamp:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetTime(i).Compare(c2.GetTime(i)) != 0), IsFalse)
				}
			case types.ETDuration:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetDuration(i, 0) != c2.GetDuration(i, 0)), IsFalse)
				}
			case types.ETJson:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetJSON(i).String() != c2.GetJSON(i).String()), IsFalse)
				}
			case types.ETString:
				for i := 0; i < input.NumRows(); i++ {
					c.Assert(c1.IsNull(i) != c2.IsNull(i) || (!c1.IsNull(i) && c1.GetString(i) != c2.GetString(i)), IsFalse)
				}
			}
		}
	}
}

func BenchmarkVectorizedEvalOneVec(b *testing.B) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprBenchCases {
		for _, testCase := range testCases {
			expr, input, output := genVecExprBenchCase(ctx, funcName, testCase)
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

func genVecBuiltinFuncBenchCase(ctx sessionctx.Context, funcName string, testCase vecExprBenchCase) (baseFunc builtinFunc, input *chunk.Chunk, result *chunk.Column) {
	childrenNumber := len(testCase.childrenTypes)
	fts := make([]*types.FieldType, childrenNumber)
	for i, eType := range testCase.childrenTypes {
		fts[i] = eType2FieldType(eType)
	}
	cols := make([]Expression, childrenNumber)
	input = chunk.New(fts, 1024, 1024)
	for i, eType := range testCase.childrenTypes {
		fillColumn(eType, input, i)
		cols[i] = &Column{Index: i, RetType: fts[i]}
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
	return baseFunc, input, result
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinFunc(c *C) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprBenchCases {
		for _, testCase := range testCases {
			baseFunc, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			it := chunk.NewIterator4Chunk(input)
			i := 0
			switch testCase.retEvalType {
			case types.ETInt:
				err := baseFunc.vecEvalInt(input, output)
				c.Assert(err, IsNil)
				i64s := output.Int64s()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalInt(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, i64s[i])
					}
					i++
				}
			case types.ETReal:
				err := baseFunc.vecEvalReal(input, output)
				c.Assert(err, IsNil)
				f64s := output.Float64s()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalReal(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, f64s[i])
					}
					i++
				}
			case types.ETDecimal:
				err := baseFunc.vecEvalDecimal(input, output)
				c.Assert(err, IsNil)
				d64s := output.Decimals()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalDecimal(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, d64s[i])
					}
					i++
				}
			case types.ETDatetime, types.ETTimestamp:
				err := baseFunc.vecEvalTime(input, output)
				c.Assert(err, IsNil)
				t64s := output.Times()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalTime(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, t64s[i])
					}
					i++
				}
			case types.ETDuration:
				err := baseFunc.vecEvalDuration(input, output)
				c.Assert(err, IsNil)
				d64s := output.GoDurations()
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalDuration(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, d64s[i])
					}
					i++
				}
			case types.ETJson:
				err := baseFunc.vecEvalJSON(input, output)
				c.Assert(err, IsNil)
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalDuration(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, output.GetJSON(i))
					}
					i++
				}
			case types.ETString:
				err := baseFunc.vecEvalString(input, output)
				c.Assert(err, IsNil)
				for row := it.Begin(); row != it.End(); row = it.Next() {
					val, isNull, err := baseFunc.evalDuration(row)
					c.Assert(err, IsNil)
					c.Assert(isNull, Equals, output.IsNull(i))
					if !isNull {
						c.Assert(val, Equals, output.GetString(i))
					}
					i++
				}
			default:
				c.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
			}
		}
	}
}

func BenchmarkVectorizedBuiltinFunc(b *testing.B) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprBenchCases {
		for _, testCase := range testCases {
			baseFunc, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			baseFuncName := fmt.Sprintf("%v", reflect.TypeOf(baseFunc))
			tmp := strings.Split(baseFuncName, ".")
			baseFuncName = tmp[len(tmp)-1]

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
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalInt(row); err != nil {
								b.Fatal(err)
							}
						}
					}
				case types.ETReal:
					for i := 0; i < b.N; i++ {
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalReal(row); err != nil {
								b.Fatal(err)
							}
						}
					}
				case types.ETDecimal:
					for i := 0; i < b.N; i++ {
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalDecimal(row); err != nil {
								b.Fatal(err)
							}
						}
					}
				case types.ETDatetime, types.ETTimestamp:
					for i := 0; i < b.N; i++ {
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalTime(row); err != nil {
								b.Fatal(err)
							}
						}
					}
				case types.ETDuration:
					for i := 0; i < b.N; i++ {
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalDuration(row); err != nil {
								b.Fatal(err)
							}
						}
					}
				case types.ETJson:
					for i := 0; i < b.N; i++ {
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalJSON(row); err != nil {
								b.Fatal(err)
							}
						}
					}
				case types.ETString:
					for i := 0; i < b.N; i++ {
						for row := it.Begin(); row != it.End(); row = it.Next() {
							if _, _, err := baseFunc.evalString(row); err != nil {
								b.Fatal(err)
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
