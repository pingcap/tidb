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

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
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
	builtinFuncName string
	retEvalType     types.EvalType
	childrenTypes   []types.EvalType
}

var vecExprBenchCases = []vecExprBenchCase{
	{ast.Cast, types.ETInt, []types.EvalType{types.ETInt}},
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
	default:
		// TODO: support all EvalTypes later.
		panic(fmt.Sprintf("EvalType=%v is not supported.", eType))
	}
}

func eType2FieldType(eType types.EvalType) *types.FieldType {
	switch eType {
	case types.ETInt:
		return types.NewFieldType(mysql.TypeLonglong)
	}
	// TODO: support all EvalTypes later.
	panic(fmt.Sprintf("EvalType=%v is not supported.", eType))
}

func genVecExprBenchCase(ctx sessionctx.Context, testCase vecExprBenchCase) (Expression, *chunk.Chunk, *chunk.Chunk) {
	fts := make([]*types.FieldType, len(testCase.childrenTypes))
	for i, eType := range testCase.childrenTypes {
		fts[i] = eType2FieldType(eType)
	}
	cols := make([]Expression, len(testCase.childrenTypes))
	input := chunk.New(fts, 1024, 1024)
	for i, eType := range testCase.childrenTypes {
		fillColumn(eType, input, i)
		cols[i] = &Column{Index: i, RetType: fts[i]}
	}

	expr, err := NewFunction(ctx, testCase.builtinFuncName, eType2FieldType(testCase.retEvalType), cols...)
	if err != nil {
		panic(err)
	}

	output := chunk.New([]*types.FieldType{eType2FieldType(testCase.retEvalType)}, 1024, 1024)
	return expr, input, output
}

func TestVectorizedExpression(t *testing.T) {
	ctx := mock.NewContext()
	for _, testCase := range vecExprBenchCases {
		expr, input, output := genVecExprBenchCase(ctx, testCase)
		output2 := output.CopyConstruct()
		if err := evalOneVec(ctx, expr, input, output, 0); err != nil {
			t.Fatal(err)
		}

		it := chunk.NewIterator4Chunk(input)
		if err := evalOneColumn(ctx, expr, it, output2, 0); err != nil {
			t.Fatal(err)
		}

		switch testCase.retEvalType {
		case types.ETInt:
			if !reflect.DeepEqual(output.Column(0).Int64s(), output2.Column(0).Int64s()) {
				t.Fatal(fmt.Sprintf("error testCase %v", testCase))
			}
		default:
			t.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
		}
	}
}

func BenchmarkVectorizedExpression(b *testing.B) {
	ctx := mock.NewContext()
	for _, testCase := range vecExprBenchCases {
		expr, input, output := genVecExprBenchCase(ctx, testCase)
		exprName := expr.String()
		if sf, ok := expr.(*ScalarFunction); ok {
			exprName = fmt.Sprintf("%v", reflect.TypeOf(sf.Function))
			tmp := strings.Split(exprName, ".")
			exprName = tmp[len(tmp)-1]
		}

		b.Run(exprName+"-VecExpr", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := evalOneVec(ctx, expr, input, output, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(exprName+"-NonVecExpr", func(b *testing.B) {
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
