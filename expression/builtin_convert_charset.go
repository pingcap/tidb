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
	"github.com/pingcap/tidb/util/chunk"
)

var _ builtinFunc = &builtinConvertCharsetSig{}

type builtinConvertCharsetSig struct {
	baseBuiltinFunc
}

func (b *builtinConvertCharsetSig) Clone() builtinFunc {
	newSig := &builtinConvertCharsetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinConvertCharsetSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tp := b.args[0].GetType()
	enc := charset.NewEncoding(tp.Charset)
	res, err = enc.EncodeString(val)
	return res, false, err
}

func (b *builtinConvertCharsetSig) vectorized() bool {
	return true
}

func (b *builtinConvertCharsetSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
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

// charsetConvertMap contains the builtin functions which arguments need to be converted to the correct charset.
var charsetConvertMap = map[string]struct{}{
	ast.Hex: {}, ast.Length: {}, ast.OctetLength: {}, ast.ASCII: {},
	ast.ToBase64: {},
}

// WrapWithConvertCharset wraps `expr` with converting charset sig.
func WrapWithConvertCharset(ctx sessionctx.Context, expr Expression, funcName string) Expression {
	retTp := expr.GetType()
	const convChs = "convert_charset"
	if _, ok := charsetConvertMap[funcName]; ok {
		bf, err := newBaseBuiltinFunc(ctx, convChs, []Expression{expr}, retTp.EvalType())
		if err != nil {
			return expr
		}
		chsSig := &builtinConvertCharsetSig{bf}
		return &ScalarFunction{
			FuncName: model.NewCIStr(convChs),
			RetType:  retTp,
			Function: chsSig,
		}
	}
	return expr
}
