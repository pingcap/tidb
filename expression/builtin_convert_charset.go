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
	"fmt"
	"unicode/utf8"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &tidbToBinaryFunctionClass{}
	_ functionClass = &tidbFromBinaryFunctionClass{}

	_ builtinFunc = &builtinInternalToBinarySig{}
	_ builtinFunc = &builtinInternalFromBinarySig{}
)

var (
	// errCannotConvertString returns when the string can not convert to other charset.
	errCannotConvertString = dbterror.ClassExpression.NewStd(errno.ErrCannotConvertString)
)

// InternalFuncToBinary accepts a string and returns another string encoded in a given charset.
const InternalFuncToBinary = "to_binary"

// InternalFuncFromBinary accepts a string and returns another string decode in a given charset.
const InternalFuncFromBinary = "from_binary"

type tidbToBinaryFunctionClass struct {
	baseFunctionClass
}

func (c *tidbToBinaryFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTp := args[0].GetType().EvalType()
	var sig builtinFunc
	switch argTp {
	case types.ETString:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.tp = args[0].GetType().Clone()
		bf.tp.Tp = mysql.TypeVarString
		bf.tp.Charset, bf.tp.Collate = charset.CharsetBin, charset.CollationBin
		sig = &builtinInternalToBinarySig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ToBinary)
	default:
		return nil, fmt.Errorf("unexpected argTp: %d", argTp)
	}
	return sig, nil
}

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
	var encodedBuf []byte
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		strBytes, err := enc.Encode(encodedBuf, buf.GetBytes(i))
		if err != nil {
			return err
		}
		result.AppendBytes(strBytes)
	}
	return nil
}

type tidbFromBinaryFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *tidbFromBinaryFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTp := args[0].GetType().EvalType()
	var sig builtinFunc
	switch argTp {
	case types.ETString:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.tp = c.tp
		sig = &builtinInternalFromBinarySig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FromBinary)
	default:
		return nil, fmt.Errorf("unexpected argTp: %d", argTp)
	}
	return sig, nil
}

type builtinInternalFromBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinInternalFromBinarySig) Clone() builtinFunc {
	newSig := &builtinInternalFromBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInternalFromBinarySig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return val, isNull, err
	}
	transferString := b.getTransferFunc()
	tBytes, err := transferString([]byte(val))
	return string(tBytes), false, err
}

func (b *builtinInternalFromBinarySig) vectorized() bool {
	return true
}

func (b *builtinInternalFromBinarySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	transferString := b.getTransferFunc()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str, err := transferString(buf.GetBytes(i))
		if err != nil {
			return err
		}
		result.AppendBytes(str)
	}
	return nil
}

func (b *builtinInternalFromBinarySig) getTransferFunc() func([]byte) ([]byte, error) {
	var transferString func([]byte) ([]byte, error)
	if b.tp.Charset == charset.CharsetUTF8MB4 || b.tp.Charset == charset.CharsetUTF8 {
		transferString = func(s []byte) ([]byte, error) {
			if !utf8.Valid(s) {
				return nil, errCannotConvertString.GenWithStackByArgs(fmt.Sprintf("%X", s), charset.CharsetBin, b.tp.Charset)
			}
			return s, nil
		}
	} else {
		enc := charset.NewEncoding(b.tp.Charset)
		var buf []byte
		transferString = func(s []byte) ([]byte, error) {
			str, err := enc.Decode(buf, s)
			if err != nil {
				return nil, errCannotConvertString.GenWithStackByArgs(fmt.Sprintf("%X", s), charset.CharsetBin, b.tp.Charset)
			}
			return str, nil
		}
	}
	return transferString
}

// BuildToBinaryFunction builds to_binary function.
func BuildToBinaryFunction(ctx sessionctx.Context, expr Expression) (res Expression) {
	fc := &tidbToBinaryFunctionClass{baseFunctionClass{InternalFuncToBinary, 1, 1}}
	f, err := fc.getFunction(ctx, []Expression{expr})
	if err != nil {
		return expr
	}
	res = &ScalarFunction{
		FuncName: model.NewCIStr(InternalFuncToBinary),
		RetType:  f.getRetTp(),
		Function: f,
	}
	return FoldConstant(res)
}

// BuildFromBinaryFunction builds from_binary function.
func BuildFromBinaryFunction(ctx sessionctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	fc := &tidbFromBinaryFunctionClass{baseFunctionClass{InternalFuncFromBinary, 1, 1}, tp}
	f, err := fc.getFunction(ctx, []Expression{expr})
	if err != nil {
		return expr
	}
	res = &ScalarFunction{
		FuncName: model.NewCIStr(InternalFuncFromBinary),
		RetType:  tp,
		Function: f,
	}
	return FoldConstant(res)
}

// HandleBinaryLiteral wraps `expr` with to_binary or from_binary sig.
func HandleBinaryLiteral(ctx sessionctx.Context, expr Expression, ec *ExprCollation, funcName string) Expression {
	switch funcName {
	case ast.Concat, ast.ConcatWS, ast.Lower, ast.Lcase, ast.Reverse, ast.Upper, ast.Ucase, ast.Quote, ast.Coalesce,
		ast.Left, ast.Right, ast.Repeat, ast.Trim, ast.LTrim, ast.RTrim, ast.Substr, ast.SubstringIndex, ast.Replace,
		ast.Substring, ast.Mid, ast.Translate, ast.InsertFunc, ast.Lpad, ast.Rpad, ast.Elt, ast.ExportSet, ast.MakeSet,
		ast.FindInSet, ast.Regexp, ast.Field, ast.Locate, ast.Instr, ast.Position, ast.GE, ast.LE, ast.GT, ast.LT, ast.EQ,
		ast.NE, ast.NullEQ, ast.Strcmp, ast.If, ast.Ifnull, ast.Like, ast.In, ast.DateFormat, ast.TimeFormat:
		if ec.Charset == charset.CharsetBin && expr.GetType().Charset != charset.CharsetBin {
			return BuildToBinaryFunction(ctx, expr)
		} else if ec.Charset != charset.CharsetBin && expr.GetType().Charset == charset.CharsetBin {
			ft := expr.GetType().Clone()
			ft.Charset, ft.Collate = ec.Charset, ec.Collation
			return BuildFromBinaryFunction(ctx, expr, ft)
		}
	case ast.Hex, ast.Length, ast.OctetLength, ast.ASCII, ast.ToBase64, ast.AesEncrypt, ast.AesDecrypt, ast.Decode, ast.Encode,
		ast.PasswordFunc, ast.MD5, ast.SHA, ast.SHA1, ast.SHA2, ast.Compress:
		if _, err := charset.GetDefaultCollationLegacy(expr.GetType().Charset); err != nil {
			return BuildToBinaryFunction(ctx, expr)
		}
	}
	return expr
}
