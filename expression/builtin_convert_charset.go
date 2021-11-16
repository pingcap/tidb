// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// InternalFuncToBinary accepts a string and returns another string encoded in a given charset.
const InternalFuncToBinary = "to_binary"

var _ builtinFunc = &builtinInternalToBinarySig{}

type builtinInternalToBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinInternalToBinarySig) Clone() builtinFunc {
	newSig := &builtinInternalToBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInternalToBinarySig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tp := b.args[0].GetType()
	enc := charset.NewEncoding(tp.Charset)
	res, err = enc.EncodeString(val)
	return res, false, err
}

func (b *builtinInternalToBinarySig) vectorized() bool {
	return true
}

func (b *builtinInternalToBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	enc := charset.NewEncoding(b.args[0].GetType().Charset)
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		var str string
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str = buf.GetString(i)
		str, err = enc.EncodeString(str)
		if err != nil {
			return err
		}
		result.AppendString(str)
	}
	return nil
}

// toBinaryMap contains the builtin functions which arguments need to be converted to the correct charset.
var toBinaryMap = map[string]struct{}{
	ast.Hex: {}, ast.Length: {}, ast.OctetLength: {}, ast.ASCII: {},
	ast.ToBase64: {},
}

// BuildToBinaryFunction builds a to_binary ScalarFunction from the Expression.
func BuildToBinaryFunction(ctx sessionctx.Context, expr Expression, tp *types.FieldType) Expression {
	bf, err := newBaseBuiltinFunc(ctx, InternalFuncToBinary, []Expression{expr}, tp.EvalType())
	if err != nil {
		return expr
	}
	chsSig := &builtinInternalToBinarySig{bf}
	return &ScalarFunction{
		FuncName: model.NewCIStr(InternalFuncToBinary),
		RetType:  tp,
		Function: chsSig,
	}
}

// WrapWithToBinary wraps `expr` with to_binary sig.
func WrapWithToBinary(ctx sessionctx.Context, expr Expression, funcName string) Expression {
	retTp := expr.GetType()
	if _, err := charset.GetDefaultCollationLegacy(retTp.Charset); err == nil {
		if _, ok := toBinaryMap[funcName]; ok {
			return BuildToBinaryFunction(ctx, expr, retTp)
		}
	}
	return expr
}
