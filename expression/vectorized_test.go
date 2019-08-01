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
	"bytes"
	"fmt"
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

type mockVecBuiltinDoubleInt struct {
	baseBuiltinFunc
}

func (p *mockVecBuiltinDoubleInt) vecEval(input *chunk.Chunk, result *chunk.Column) error {
	col1 := p.args[0]
	if err := col1.VecEval(p.ctx, input, result); err != nil {
		return err
	}
	sel := input.Sel()
	v1 := result.Int64s()
	if sel == nil {
		for i := range v1 {
			// ignore checking null to get a better performance
			v1[i] += v1[i]
		}
	} else {
		for _, i := range sel {
			v1[i] += v1[i]
		}
	}
	return nil
}

type mockRowBuiltinDoubleInt struct {
	baseBuiltinFunc
}

func (p *mockRowBuiltinDoubleInt) evalInt(row chunk.Row) (int64, bool, error) {
	v, isNull, err := p.args[0].EvalInt(p.ctx, row)
	if err != nil {
		return 0, false, err
	}
	return v * 2, isNull, nil
}

type mockVecBuiltinDoubleStr struct {
	baseBuiltinFunc
	buf *chunk.Column
}

func (p *mockVecBuiltinDoubleStr) vecEval(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumEffectiveRows()
	if p.buf == nil {
		p.buf = chunk.NewColumn(p.getRetTp(), n)
	}

	col1 := p.args[0]
	if err := col1.VecEval(p.ctx, input, p.buf); err != nil {
		return err
	}
	sel := input.Sel()
	strBuf := bytes.Buffer{}
	if sel == nil {
		result.Reset()
		for i := 0; i < n; i++ {
			if p.buf.IsNull(i) {
				result.AppendNull()
				continue
			}
			strBuf.Reset()
			strBuf.WriteString(p.buf.GetString(i))
			strBuf.WriteString(p.buf.GetString(i))
			result.AppendString(strBuf.String())
		}
	} else if len(sel) == 0 {
		result.Reset()
	} else {
		pos := result.Length()
		if pos > sel[0] {
			result.Reset()
			pos = 0
		}
		for _, i := range sel {
			for pos < i {
				result.AppendNull()
				pos++
			}
			if p.buf.IsNull(i) {
				result.AppendNull()
			} else {
				strBuf.Reset()
				strBuf.WriteString(p.buf.GetString(i))
				strBuf.WriteString(p.buf.GetString(i))
				result.AppendString(strBuf.String())
			}
			pos++
		}
	}
	return nil
}

type mockRowBuiltinDoubleStr struct {
	baseBuiltinFunc
}

func (p *mockRowBuiltinDoubleInt) evalString(row chunk.Row) (string, bool, error) {
	v, isNull, err := p.args[0].EvalString(p.ctx, row)
	if err != nil {
		return "", false, err
	}
	return v + v, isNull, nil
}

func genMockRowDoubleInt() (*mockRowBuiltinDoubleInt, *chunk.Chunk) {
	col1 := newColumn(1)
	col1.Index = 0
	bf := newBaseBuiltinFuncWithTp(mock.NewContext(), []Expression{col1}, types.ETInt, types.ETInt)
	rowDouble := &mockRowBuiltinDoubleInt{bf}
	rowDouble.baseBuiltinFunc.self = rowDouble
	rowDouble.baseBuiltinFunc.vec = false
	tpll := types.NewFieldType(mysql.TypeLonglong)
	input := chunk.New([]*types.FieldType{tpll}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendInt64(0, int64(i))
	}
	return rowDouble, input
}

func genMockVecDoubleInt() (*mockVecBuiltinDoubleInt, *chunk.Chunk) {
	col1 := newColumn(1)
	col1.Index = 0
	bf := newBaseBuiltinFuncWithTp(mock.NewContext(), []Expression{col1}, types.ETInt, types.ETInt)
	vecDouble := &mockVecBuiltinDoubleInt{bf}
	vecDouble.baseBuiltinFunc.self = vecDouble
	vecDouble.baseBuiltinFunc.vec = true
	tpll := types.NewFieldType(mysql.TypeLonglong)
	input := chunk.New([]*types.FieldType{tpll}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendInt64(0, int64(i))
	}
	return vecDouble, input
}

func genMockRowDoubleStr() (*mockRowBuiltinDoubleStr, *chunk.Chunk) {
	tpstr := types.NewFieldType(mysql.TypeVarString)
	col1 := newColumn(1)
	col1.Index = 0
	col1.RetType = tpstr
	bf := newBaseBuiltinFuncWithTp(mock.NewContext(), []Expression{col1}, types.ETString, types.ETString)
	rowDouble := &mockRowBuiltinDoubleStr{bf}
	rowDouble.baseBuiltinFunc.self = rowDouble
	rowDouble.baseBuiltinFunc.vec = false
	input := chunk.New([]*types.FieldType{tpstr}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendString(0, fmt.Sprintf("%v", i))
	}
	return rowDouble, input
}

func genMockVecDoubleStr() (*mockVecBuiltinDoubleStr, *chunk.Chunk) {
	tpstr := types.NewFieldType(mysql.TypeVarString)
	col1 := newColumn(1)
	col1.Index = 0
	col1.RetType = tpstr
	bf := newBaseBuiltinFuncWithTp(mock.NewContext(), []Expression{col1}, types.ETString, types.ETString)
	vecDouble := &mockVecBuiltinDoubleStr{bf, nil}
	vecDouble.baseBuiltinFunc.self = vecDouble
	vecDouble.baseBuiltinFunc.vec = true
	input := chunk.New([]*types.FieldType{tpstr}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendString(0, fmt.Sprintf("%v", i))
	}
	return vecDouble, input
}

func BenchmarkMockDoubleIntVec(b *testing.B) {
	vecDouble, input := genMockVecDoubleInt()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vecDouble.vecEval(input, result)
	}
}

func BenchmarkMockDoubleIntRow(b *testing.B) {
	rowDouble, input := genMockRowDoubleInt()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result.Reset()
		it := chunk.NewIterator4Chunk(input)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			v, isNull, _ := rowDouble.evalInt(row)
			if isNull {
				result.AppendNull()
			} else {
				result.AppendInt64(v)
			}
		}
	}
}

func BenchmarkMockDoubleIntVec2Row(b *testing.B) {
	vecDouble, input := genMockVecDoubleInt()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result.Reset()
		it := chunk.NewIterator4Chunk(input)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			v, isNull, _ := vecDouble.evalInt(row)
			if isNull {
				result.AppendNull()
			} else {
				result.AppendInt64(v)
			}
		}
	}
}

func BenchmarkMockDoubleIntRow2Vec(b *testing.B) {
	rowDouble, input := genMockRowDoubleInt()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowDouble.vecEval(input, result)
	}
}

func BenchmarkMockDoubleStrVec(b *testing.B) {
	vecDouble, input := genMockVecDoubleStr()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vecDouble.vecEval(input, result)
	}
}

func BenchmarkMockDoubleStrRow(b *testing.B) {
	rowDouble, input := genMockRowDoubleStr()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result.Reset()
		it := chunk.NewIterator4Chunk(input)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			v, isNull, _ := rowDouble.evalString(row)
			if isNull {
				result.AppendNull()
			} else {
				result.AppendString(v)
			}
		}
	}
}

func BenchmarkMockDoubleStrVec2Row(b *testing.B) {
	vecDouble, input := genMockVecDoubleStr()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result.Reset()
		it := chunk.NewIterator4Chunk(input)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			v, isNull, _ := vecDouble.evalString(row)
			if isNull {
				result.AppendNull()
			} else {
				result.AppendString(v)
			}
		}
	}
}

func BenchmarkMockDoubleStrRow2Vec(b *testing.B) {
	rowDouble, input := genMockRowDoubleInt()
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowDouble.vecEval(input, result)
	}
}
