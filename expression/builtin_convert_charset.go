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
	"bytes"
	"fmt"
	"strings"
	"unicode"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/hack"
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
		bf.tp.SetType(mysql.TypeVarString)
		bf.tp.SetCharset(charset.CharsetBin)
		bf.tp.SetCollate(charset.CollationBin)
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
	enc := charset.FindEncoding(tp.GetCharset())
	ret, err := enc.Transform(nil, hack.Slice(val), charset.OpEncode)
	return string(ret), false, err
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
	enc := charset.FindEncoding(b.args[0].GetType().GetCharset())
	result.ReserveString(n)
	encodedBuf := &bytes.Buffer{}
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		val, err := enc.Transform(encodedBuf, buf.GetBytes(i), charset.OpEncode)
		if err != nil {
			return err
		}
		result.AppendBytes(val)
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
	enc := charset.FindEncoding(b.tp.GetCharset())
	valBytes := hack.Slice(val)
	ret, err := enc.Transform(nil, valBytes, charset.OpDecode)
	if err != nil {
		strHex := formatInvalidChars(valBytes)
		err = errCannotConvertString.GenWithStackByArgs(strHex, charset.CharsetBin, b.tp.GetCharset())
	}
	return string(ret), false, err
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
	enc := charset.FindEncoding(b.tp.GetCharset())
	encodedBuf := &bytes.Buffer{}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetBytes(i)
		val, err := enc.Transform(encodedBuf, str, charset.OpDecode)
		if err != nil {
			strHex := formatInvalidChars(str)
			return errCannotConvertString.GenWithStackByArgs(strHex, charset.CharsetBin, b.tp.GetCharset())
		}
		result.AppendBytes(val)
	}
	return nil
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

type funcProp int8

const (
	funcPropNone funcProp = iota
	// The arguments of these functions are wrapped with to_binary().
	// For compatibility reason, legacy charsets arguments are not wrapped.
	// Legacy charsets: utf8mb4, utf8, latin1, ascii, binary.
	funcPropBinAware
	// The arguments of these functions are wrapped with to_binary() or from_binary() according to
	// the evaluated result charset and the argument charset.
	//   For binary argument && string result, wrap it with from_binary().
	//   For string argument && binary result, wrap it with to_binary().
	funcPropAuto
)

// convertActionMap collects from https://dev.mysql.com/doc/refman/8.0/en/string-functions.html.
var convertActionMap = map[funcProp][]string{
	funcPropNone: {
		/* args != strings */
		ast.Bin, ast.CharFunc, ast.DateFormat, ast.Oct, ast.Space,
		/* only 1 string arg, no implicit conversion */
		ast.CharLength, ast.CharacterLength, ast.FromBase64, ast.Lcase, ast.Left, ast.LoadFile,
		ast.Lower, ast.LTrim, ast.Mid, ast.Ord, ast.Quote, ast.Repeat, ast.Reverse, ast.Right,
		ast.RTrim, ast.Soundex, ast.Substr, ast.Substring, ast.Ucase, ast.Unhex, ast.Upper, ast.WeightString,
	},
	funcPropBinAware: {
		/* result is binary-aware */
		ast.ASCII, ast.BitLength, ast.Hex, ast.Length, ast.OctetLength, ast.ToBase64,
		/* encrypt functions */
		ast.AesDecrypt, ast.Decode, ast.Encode, ast.PasswordFunc, ast.MD5, ast.SHA, ast.SHA1,
		ast.SHA2, ast.Compress, ast.AesEncrypt,
	},
	funcPropAuto: {
		/* string functions */ ast.Concat, ast.ConcatWS, ast.ExportSet, ast.Field, ast.FindInSet,
		ast.InsertFunc, ast.Instr, ast.Lpad, ast.Locate, ast.Lpad, ast.MakeSet, ast.Position,
		ast.Replace, ast.Rpad, ast.SubstringIndex, ast.Trim, ast.Elt,
		/* operators */
		ast.GE, ast.LE, ast.GT, ast.LT, ast.EQ, ast.NE, ast.NullEQ, ast.If, ast.Ifnull, ast.In,
		ast.Case, ast.Cast,
		/* string comparing */
		ast.Like, ast.Strcmp,
		/* regex */
		ast.Regexp,
		/* math */
		ast.CRC32,
	},
}

var convertFuncsMap = map[string]funcProp{}

func init() {
	for k, fns := range convertActionMap {
		for _, f := range fns {
			convertFuncsMap[f] = k
		}
	}
}

// HandleBinaryLiteral wraps `expr` with to_binary or from_binary sig.
func HandleBinaryLiteral(ctx sessionctx.Context, expr Expression, ec *ExprCollation, funcName string) Expression {
	argChs, dstChs := expr.GetType().GetCharset(), ec.Charset
	switch convertFuncsMap[funcName] {
	case funcPropNone:
		return expr
	case funcPropBinAware:
		if isLegacyCharset(argChs) {
			return expr
		}
		return BuildToBinaryFunction(ctx, expr)
	case funcPropAuto:
		if argChs != charset.CharsetBin && dstChs == charset.CharsetBin {
			if isLegacyCharset(argChs) {
				return expr
			}
			return BuildToBinaryFunction(ctx, expr)
		} else if argChs == charset.CharsetBin && dstChs != charset.CharsetBin {
			ft := expr.GetType().Clone()
			ft.SetCharset(ec.Charset)
			ft.SetCollate(ec.Collation)
			return BuildFromBinaryFunction(ctx, expr, ft)
		}
	}
	return expr
}

func isLegacyCharset(chs string) bool {
	switch chs {
	case charset.CharsetUTF8, charset.CharsetUTF8MB4, charset.CharsetASCII, charset.CharsetLatin1, charset.CharsetBin:
		return true
	}
	return false
}

func formatInvalidChars(src []byte) string {
	var sb strings.Builder
	const maxBytesToShow = 5
	for i := 0; i < len(src); i++ {
		if i > maxBytesToShow {
			sb.WriteString("...")
			break
		}
		if src[i] > unicode.MaxASCII {
			sb.WriteString(fmt.Sprintf("\\x%X", src[i]))
		} else {
			sb.Write([]byte{src[i]})
		}
	}
	return sb.String()
}
