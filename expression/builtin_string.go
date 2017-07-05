// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
	"golang.org/x/text/transform"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &asciiFunctionClass{}
	_ functionClass = &concatFunctionClass{}
	_ functionClass = &concatWSFunctionClass{}
	_ functionClass = &leftFunctionClass{}
	_ functionClass = &repeatFunctionClass{}
	_ functionClass = &lowerFunctionClass{}
	_ functionClass = &reverseFunctionClass{}
	_ functionClass = &spaceFunctionClass{}
	_ functionClass = &upperFunctionClass{}
	_ functionClass = &strcmpFunctionClass{}
	_ functionClass = &replaceFunctionClass{}
	_ functionClass = &convertFunctionClass{}
	_ functionClass = &substringFunctionClass{}
	_ functionClass = &substringIndexFunctionClass{}
	_ functionClass = &locateFunctionClass{}
	_ functionClass = &hexFunctionClass{}
	_ functionClass = &unhexFunctionClass{}
	_ functionClass = &trimFunctionClass{}
	_ functionClass = &lTrimFunctionClass{}
	_ functionClass = &rTrimFunctionClass{}
	_ functionClass = &rpadFunctionClass{}
	_ functionClass = &bitLengthFunctionClass{}
	_ functionClass = &charFunctionClass{}
	_ functionClass = &charLengthFunctionClass{}
	_ functionClass = &findInSetFunctionClass{}
	_ functionClass = &fieldFunctionClass{}
	_ functionClass = &makeSetFunctionClass{}
	_ functionClass = &octFunctionClass{}
	_ functionClass = &ordFunctionClass{}
	_ functionClass = &quoteFunctionClass{}
	_ functionClass = &binFunctionClass{}
	_ functionClass = &eltFunctionClass{}
	_ functionClass = &exportSetFunctionClass{}
	_ functionClass = &formatFunctionClass{}
	_ functionClass = &fromBase64FunctionClass{}
	_ functionClass = &toBase64FunctionClass{}
	_ functionClass = &insertFuncFunctionClass{}
	_ functionClass = &instrFunctionClass{}
	_ functionClass = &loadFileFunctionClass{}
	_ functionClass = &lpadFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinASCIISig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinConcatWSSig{}
	_ builtinFunc = &builtinLeftSig{}
	_ builtinFunc = &builtinRepeatSig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinReverseSig{}
	_ builtinFunc = &builtinSpaceSig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinStrcmpSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinConvertSig{}
	_ builtinFunc = &builtinSubstringSig{}
	_ builtinFunc = &builtinSubstringIndexSig{}
	_ builtinFunc = &builtinLocateSig{}
	_ builtinFunc = &builtinHexSig{}
	_ builtinFunc = &builtinUnHexSig{}
	_ builtinFunc = &builtinTrimSig{}
	_ builtinFunc = &builtinLTrimSig{}
	_ builtinFunc = &builtinRTrimSig{}
	_ builtinFunc = &builtinRpadSig{}
	_ builtinFunc = &builtinBitLengthSig{}
	_ builtinFunc = &builtinCharSig{}
	_ builtinFunc = &builtinCharLengthSig{}
	_ builtinFunc = &builtinFindInSetSig{}
	_ builtinFunc = &builtinMakeSetSig{}
	_ builtinFunc = &builtinOctSig{}
	_ builtinFunc = &builtinOrdSig{}
	_ builtinFunc = &builtinQuoteSig{}
	_ builtinFunc = &builtinBinSig{}
	_ builtinFunc = &builtinEltSig{}
	_ builtinFunc = &builtinExportSetSig{}
	_ builtinFunc = &builtinFormatSig{}
	_ builtinFunc = &builtinFromBase64Sig{}
	_ builtinFunc = &builtinToBase64Sig{}
	_ builtinFunc = &builtinInsertFuncSig{}
	_ builtinFunc = &builtinInstrSig{}
	_ builtinFunc = &builtinLoadFileSig{}
	_ builtinFunc = &builtinLpadSig{}
)

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flen = 10
	types.SetBinChsClnFlag(tp)
	bf, err := newBaseBuiltinFuncWithTp(args, tp, ctx, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	// ascii function will always return Type BIGINT, Charset Binary, Size 3
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flen = 3
	types.SetBinChsClnFlag(tp)

	bf, err := newBaseBuiltinFuncWithTp(args, tp, ctx, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinASCIISig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinASCIISig struct {
	baseIntBuiltinFunc
}

// eval evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if len(val) == 0 {
		return 0, false, nil
	}
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	retType, argTps := c.inferType(args)
	bf, err := newBaseBuiltinFuncWithTp(args, retType, ctx, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinConcatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

func (c *concatFunctionClass) inferType(args []Expression) (*types.FieldType, []argTp) {
	tps := make([]argTp, len(args))
	existsBinStr, tp := false, mysql.TypeVarString
	for i := 0; i < len(args); i++ {
		tps[i] = tpString
		curArgTp := args[i].GetType()
		tp = types.MergeFieldType(tp, curArgTp.Tp)
		if types.IsBinaryStr(curArgTp) {
			existsBinStr = true
		}
	}
	retType := types.NewFieldType(tp)
	retType.Charset, retType.Collate = charset.CharsetUTF8, charset.CollationUTF8
	if existsBinStr {
		retType.Charset, retType.Collate = charset.CharsetBin, charset.CollationBin
		retType.Flag |= mysql.BinaryFlag
	}
	return retType, tps
}

type builtinConcatSig struct {
	baseStringBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(row, b.ctx.GetSessionVars().StmtCtx)
		if isNull || err != nil {
			return d, isNull, errors.Trace(err)
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinConcatWSSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinConcatWSSig struct {
	baseBuiltinFunc
}

// eval evals a builtinConcatWSSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	var sep string
	s := make([]string, 0, len(args))
	for i, a := range args {
		if a.IsNull() {
			if i == 0 {
				return d, nil
			}
			continue
		}
		ss, err := a.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}

		if i == 0 {
			sep = ss
			continue
		}
		s = append(s, ss)
	}

	d.SetString(strings.Join(s, sep))
	return d, nil
}

type leftFunctionClass struct {
	baseFunctionClass
}

func (c *leftFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLeftSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLeftSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLeftSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	length, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	l := int(length)
	if l < 0 {
		l = 0
	} else if l > len(str) {
		l = len(str)
	}
	d.SetString(str[:l])
	return d, nil
}

type rightFunctionClass struct {
	baseFunctionClass
}

func (c *rightFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRightSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRightSig struct {
	baseBuiltinFunc
}

func (b *builtinRightSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if len(args) != 2 {
		return d, nil
	}
	arg0, arg1 := args[0], args[1]
	if arg0.IsNull() || arg1.IsNull() {
		return d, nil
	}

	str, err := arg0.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	length, err := arg1.ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	if length <= 0 {
		d.SetString("")
		return d, nil
	}
	var result string
	strLen := int64(len(str))
	if strLen >= length {
		result = str[strLen-length:]
	} else {
		result = str
	}
	d.SetString(result)

	return d, nil
}

type repeatFunctionClass struct {
	baseFunctionClass
}

func (c *repeatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	tp := types.NewFieldType(mysql.TypeLongBlob)
	tp.Flen = mysql.MaxBlobWidth

	if isBinary := mysql.HasBinaryFlag(args[0].GetType().Flag); isBinary {
		types.SetBinChsClnFlag(tp)
	} else {
		tp.Charset = charset.CharsetUTF8
		tp.Collate = charset.CollationUTF8
	}

	bf, err := newBaseBuiltinFuncWithTp(args, tp, ctx, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinRepeatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRepeatSig struct {
	baseStringBuiltinFunc
}

// eval evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	num, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if num < 1 {
		return "", false, nil
	}
	if num > math.MaxInt32 {
		num = math.MaxInt32
	}

	if int64(len(str)) > int64(b.tp.Flen)/num {
		return "", true, nil
	}
	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLowerSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLowerSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	x := args[0]
	switch x.Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := x.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetString(strings.ToLower(s))
		return d, nil
	}
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinReverseSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinReverseSig struct {
	baseBuiltinFunc
}

// eval evals a builtinReverseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	x := args[0]
	switch x.Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := x.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetString(stringutil.Reverse(s))
		return d, nil
	}
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinSpaceSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSpaceSig struct {
	baseBuiltinFunc
}

// eval evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	x := args[0]
	if x.IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	if x.Kind() == types.KindString || x.Kind() == types.KindBytes {
		if _, e := types.StrToInt(sc, x.GetString()); e != nil {
			return d, errors.Trace(e)
		}
	}

	v, err := x.ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}

	if v < 0 {
		v = 0
	}

	if v > math.MaxInt32 {
		d.SetNull()
	} else {
		d.SetString(strings.Repeat(" ", int(v)))
	}
	return d, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinUpperSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinUpperSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	x := args[0]
	switch x.Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := x.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetString(strings.ToUpper(s))
		return d, nil
	}
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flen = 2
	types.SetBinChsClnFlag(tp)
	bf, err := newBaseBuiltinFuncWithTp(args, tp, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinStrcmpSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinStrcmpSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row []types.Datum) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	left, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	right, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	res := types.CompareString(left, right)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinReplaceSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinReplaceSig struct {
	baseBuiltinFunc
}

// eval evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	for _, arg := range args {
		if arg.IsNull() {
			return d, nil
		}
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	oldStr, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	newStr, err := args[2].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	if oldStr == "" {
		d.SetString(str)
		return d, nil
	}
	d.SetString(strings.Replace(str, oldStr, newStr, -1))

	return d, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinConvertSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinConvertSig struct {
	baseBuiltinFunc
}

// eval evals a builtinConvertSig.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// Casting nil to any type returns nil
	if args[0].Kind() != types.KindString {
		return d, nil
	}

	str := args[0].GetString()
	Charset := args[1].GetString()

	if strings.ToLower(Charset) == "ascii" {
		d.SetString(str)
		return d, nil
	} else if strings.ToLower(Charset) == "utf8mb4" {
		d.SetString(str)
		return d, nil
	}

	encoding, _ := charset.Lookup(Charset)
	if encoding == nil {
		return d, errors.Errorf("unknown encoding: %s", Charset)
	}

	target, _, err := transform.String(encoding.NewDecoder(), str)
	if err != nil {
		log.Errorf("Convert %s to %s with error: %v", str, Charset, err)
		return d, errors.Trace(err)
	}
	d.SetString(target)
	return d, nil
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinSubstringSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSubstringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstringSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// The meaning of the elements of args.
	// arg[0] -> StrExpr
	// arg[1] -> Pos
	// arg[2] -> Len (Optional)
	if args[0].IsNull() || args[1].IsNull() {
		return
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Errorf("Substring invalid args, need string but get %T", args[0].GetValue())
	}

	if args[1].Kind() != types.KindInt64 {
		return d, errors.Errorf("Substring invalid pos args, need int but get %T", args[1].GetValue())
	}
	pos := args[1].GetInt64()

	length, hasLen := int64(-1), false
	if len(args) == 3 {
		if args[2].IsNull() {
			return
		}
		if args[2].Kind() != types.KindInt64 {
			return d, errors.Errorf("Substring invalid pos args, need int but get %T", args[2].GetValue())
		}
		length, hasLen = args[2].GetInt64(), true
	}
	// The forms without a len argument return a substring from string str starting at position pos.
	// The forms with a len argument return a substring len characters long from string str, starting at position pos.
	// The forms that use FROM are standard SQL syntax. It is also possible to use a negative value for pos.
	// In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning.
	// A negative value may be used for pos in any of the forms of this function.
	if pos < 0 {
		pos = int64(len(str)) + pos
	} else {
		pos--
	}
	if pos > int64(len(str)) || pos < int64(0) {
		pos = int64(len(str))
	}
	if hasLen {
		if end := pos + length; end < pos {
			d.SetString("")
		} else if end > int64(len(str)) {
			d.SetString(str[pos:])
		} else {
			d.SetString(str[pos:end])
		}
	} else {
		d.SetString(str[pos:])
	}
	return d, nil
}

type substringIndexFunctionClass struct {
	baseFunctionClass
}

func (c *substringIndexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinSubstringIndexSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSubstringIndexSig struct {
	baseBuiltinFunc
}

// eval evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// The meaning of the elements of args.
	// args[0] -> StrExpr
	// args[1] -> Delim
	// args[2] -> Count
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Errorf("Substring_Index invalid args, need string but get %T", args[0].GetValue())
	}

	delim, err := args[1].ToString()
	if err != nil {
		return d, errors.Errorf("Substring_Index invalid delim, need string but get %T", args[1].GetValue())
	}
	if len(delim) == 0 {
		d.SetString("")
		return d, nil
	}

	c, err := args[2].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	count := int(c)
	strs := strings.Split(str, delim)
	var (
		start = 0
		end   = len(strs)
	)
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < end {
			start = end - count
		}
	}
	substrs := strs[start:end]
	d.SetString(strings.Join(substrs, delim))
	return d, nil
}

type locateFunctionClass struct {
	baseFunctionClass
}

func (c *locateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLocateSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLocateSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLocateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// The meaning of the elements of args.
	// args[0] -> SubStr
	// args[1] -> Str
	// args[2] -> Pos

	// eval str
	if args[1].IsNull() {
		return d, nil
	}
	str, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	// eval substr
	if args[0].IsNull() {
		return d, nil
	}
	subStr, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	// eval pos
	pos := int64(0)
	if len(args) == 3 {
		p, err := args[2].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		pos = p - 1
	}
	var ret, subStrLen, sentinel int64
	caseSensitive := false
	if args[0].Kind() == types.KindBytes || args[1].Kind() == types.KindBytes {
		caseSensitive = true
		subStrLen = int64(len(subStr))
		sentinel = int64(len(str)) - subStrLen
	} else {
		subStrLen = int64(len([]rune(subStr)))
		sentinel = int64(len([]rune(strings.ToLower(str)))) - subStrLen
	}

	if pos < 0 || pos > sentinel {
		d.SetInt64(0)
		return d, nil
	} else if subStrLen == 0 {
		d.SetInt64(pos + 1)
		return d, nil
	} else if caseSensitive {
		slice := str[pos:]
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			ret = pos + int64(idx) + 1
		}
	} else {
		slice := string([]rune(strings.ToLower(str))[pos:])
		idx := strings.Index(slice, strings.ToLower(subStr))
		if idx != -1 {
			ret = pos + int64(utf8.RuneCountInString(slice[:idx])) + 1
		}
	}
	d.SetInt64(ret)
	return d, nil
}

const spaceChars = "\n\t\r "

type hexFunctionClass struct {
	baseFunctionClass
}

func (c *hexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinHexSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinHexSig struct {
	baseBuiltinFunc
}

// eval evals a builtinHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	case types.KindString, types.KindBytes:
		x, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetString(strings.ToUpper(hex.EncodeToString(hack.Slice(x))))
		return d, nil
	case types.KindInt64, types.KindUint64, types.KindMysqlHex, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
		x, _ := args[0].Cast(b.ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeLonglong))
		h := fmt.Sprintf("%x", uint64(x.GetInt64()))
		d.SetString(strings.ToUpper(h))
		return d, nil
	default:
		return d, errors.Errorf("Hex invalid args, need int or string but get %T", args[0].GetValue())
	}
}

type unhexFunctionClass struct {
	baseFunctionClass
}

func (c *unhexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinUnHexSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinUnHexSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	case types.KindString, types.KindBytes:
		x, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		bytes, err := hex.DecodeString(x)
		if err != nil {
			return d, nil
		}
		d.SetString(string(bytes))
		return d, nil
	case types.KindInt64, types.KindUint64, types.KindMysqlHex, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
		x, _ := args[0].Cast(b.ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeString))
		if x.IsNull() {
			return d, nil
		}
		bytes, err := hex.DecodeString(x.GetString())
		if err != nil {
			return d, nil
		}
		d.SetString(string(bytes))
		return d, nil
	default:
		return d, errors.Errorf("Unhex invalid args, need int or string but get %T", args[0].GetValue())
	}
}

type trimFunctionClass struct {
	baseFunctionClass
}

func (c *trimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinTrimSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinTrimSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTrimSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrimSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// args[0] -> Str
	// args[1] -> RemStr
	// args[2] -> Direction
	// eval str
	if args[0].IsNull() {
		return d, nil
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	remstr := ""
	// eval remstr
	if len(args) > 1 {
		if args[1].Kind() != types.KindNull {
			remstr, err = args[1].ToString()
			if err != nil {
				return d, errors.Trace(err)
			}
		}
	}
	// do trim
	var result string
	var direction ast.TrimDirectionType
	if len(args) > 2 {
		direction = args[2].GetValue().(ast.TrimDirectionType)
	} else {
		direction = ast.TrimBothDefault
	}
	if direction == ast.TrimLeading {
		if len(remstr) > 0 {
			result = trimLeft(str, remstr)
		} else {
			result = strings.TrimLeft(str, spaceChars)
		}
	} else if direction == ast.TrimTrailing {
		if len(remstr) > 0 {
			result = trimRight(str, remstr)
		} else {
			result = strings.TrimRight(str, spaceChars)
		}
	} else if len(remstr) > 0 {
		x := trimLeft(str, remstr)
		result = trimRight(x, remstr)
	} else {
		result = strings.Trim(str, spaceChars)
	}
	d.SetString(result)
	return d, nil
}

type lTrimFunctionClass struct {
	baseFunctionClass
}

func (c *lTrimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLTrimSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinLTrimSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return trimFn(strings.TrimLeft, spaceChars)(args, b.ctx)
}

type rTrimFunctionClass struct {
	baseFunctionClass
}

func (c *rTrimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRTrimSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinRTrimSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return trimFn(strings.TrimRight, spaceChars)(args, b.ctx)
}

// trimFn returns a BuildFunc for ltrim and rtrim.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func trimFn(fn func(string, string) string, cutset string) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		if args[0].IsNull() {
			return d, nil
		}
		str, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetString(fn(str, cutset))
		return d, nil
	}
}

func trimLeft(str, remstr string) string {
	for {
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func trimRight(str, remstr string) string {
	for {
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

type rpadFunctionClass struct {
	baseFunctionClass
}

func (c *rpadFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRpadSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRpadSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRpadSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// RPAD(str,len,padstr)
	// args[0] string, args[1] int, args[2] string
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	length, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	l := int(length)

	padStr, err := args[2].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	if l < 0 || (len(str) < l && padStr == "") {
		d.SetNull()
		return d, nil
	}

	tailLen := l - len(str)
	if tailLen > 0 {
		repeatCount := tailLen/len(padStr) + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	d.SetString(str[:l])

	return d, nil
}

type bitLengthFunctionClass struct {
	baseFunctionClass
}

func (c *bitLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinBitLengthSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinBitLengthSig struct {
	baseBuiltinFunc
}

// eval evals a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetInt64(int64(len(str) * 8))
	return d, nil
}

type charFunctionClass struct {
	baseFunctionClass
}

func (c *charFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCharSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCharSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCharSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char
func (b *builtinCharSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// The kinds of args are int or string, and the last one represents charset.
	var resultStr string
	var intSlice = make([]int64, 0, len(args)-1)

	for _, datum := range args[:len(args)-1] {
		switch datum.Kind() {
		case types.KindNull:
			continue
		case types.KindString:
			i, err := datum.ToInt64(b.ctx.GetSessionVars().StmtCtx)
			if err != nil {
				d.SetString(resultStr)
				return d, nil
			}
			intSlice = append(intSlice, i)
		case types.KindInt64, types.KindUint64, types.KindMysqlHex, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			x, err := datum.Cast(b.ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeLonglong))
			if err != nil {
				return d, errors.Trace(err)
			}
			intSlice = append(intSlice, x.GetInt64())
		default:
			return d, errors.Errorf("Char invalid args, need int or string but get %T", args[0].GetValue())
		}
	}

	resultStr = string(convertInt64ToBytes(intSlice))

	// The last argument represents the charset name after "using".
	// If it is nil, the default charset utf8 is used.
	argCharset := args[len(args)-1]
	if !argCharset.IsNull() {
		char, err := argCharset.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}

		if strings.ToLower(char) == "ascii" || strings.HasPrefix(strings.ToLower(char), "utf8") {
			d.SetString(resultStr)
			return d, nil
		}

		encoding, _ := charset.Lookup(char)
		if encoding == nil {
			return d, errors.Errorf("unknown encoding: %s", char)
		}

		resultStr, _, err = transform.String(encoding.NewDecoder(), resultStr)
		if err != nil {
			log.Errorf("Convert %s to %s with error: %v", resultStr, char, err)
			return d, errors.Trace(err)
		}
	}
	d.SetString(resultStr)
	return d, nil
}

func convertInt64ToBytes(ints []int64) []byte {
	var buf bytes.Buffer
	for i := len(ints) - 1; i >= 0; i-- {
		var count int
		v := ints[i]
		for count < 4 {
			buf.WriteByte(byte(v & 0xff))
			v = v >> 8
			if v == 0 {
				break
			}
			count++
		}
	}
	s := buf.Bytes()
	reverseByteSlice(s)
	return s
}

func reverseByteSlice(slice []byte) {
	var start int
	var end = len(slice) - 1
	for start < end {
		slice[start], slice[end] = slice[end], slice[start]
		start++
		end--
	}
}

type charLengthFunctionClass struct {
	baseFunctionClass
}

func (c *charLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCharLengthSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCharLengthSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCharLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		r := []rune(s)
		d.SetInt64(int64(len(r)))
		return d, nil
	}
}

type findInSetFunctionClass struct {
	baseFunctionClass
}

func (c *findInSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFindInSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFindInSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFindInSetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
// TODO: This function can be optimized by using bit arithmetic when the first argument is
// a constant string and the second is a column of type SET.
func (b *builtinFindInSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// args[0] -> Str
	// args[1] -> StrList
	if args[0].IsNull() || args[1].IsNull() {
		return
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	strlst, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetInt64(0)
	if len(strlst) == 0 {
		return
	}
	for i, s := range strings.Split(strlst, ",") {
		if s == str {
			d.SetInt64(int64(i + 1))
			return
		}
	}
	return
}

type fieldFunctionClass struct {
	baseFunctionClass
}

func (c *fieldFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFieldSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFieldSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFieldSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
// Returns the index (position) of arg0 in the arg1, arg2, arg3, ... list.
// Returns 0 if arg0 is not found.
// If arg0 is NULL, the return value is 0 because NULL fails equality comparison with any value.
// If all arguments are strings, all arguments are compared as strings.
// If all arguments are numbers, they are compared as numbers.
// Otherwise, the arguments are compared as double.
func (b *builtinFieldSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d.SetInt64(0)
	if args[0].IsNull() {
		return
	}
	var (
		pos                  int64
		allString, allNumber bool
		newArgs              []types.Datum
	)
	allString, allNumber = true, true
	for i := 0; i < len(args) && (allString || allNumber); i++ {
		switch args[i].Kind() {
		case types.KindInt64, types.KindUint64, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			allString = false
		case types.KindString, types.KindBytes:
			allNumber = false
		default:
			allString, allNumber = false, false
		}
	}
	newArgs, err = argsToSpecifiedType(args, allString, allNumber, b.ctx)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg0, sc := newArgs[0], b.ctx.GetSessionVars().StmtCtx
	for i, curArg := range newArgs[1:] {
		cmpResult, _ := arg0.CompareDatum(sc, curArg)
		if cmpResult == 0 {
			pos = int64(i + 1)
			break
		}
	}
	d.SetInt64(pos)
	return d, errors.Trace(err)
}

// argsToSpecifiedType converts the type of all arguments in args into string type or double type.
func argsToSpecifiedType(args []types.Datum, allString bool, allNumber bool, ctx context.Context) (newArgs []types.Datum, err error) {
	if allNumber { // If all arguments are numbers, they can be compared directly without type converting.
		return args, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	newArgs = make([]types.Datum, len(args))
	for i, arg := range args {
		if allString {
			str, err := arg.ToString()
			if err != nil {
				return newArgs, errors.Trace(err)
			}
			newArgs[i] = types.NewStringDatum(str)
		} else {
			// If error occurred when convert arg to float64, ignore it and set f as 0.
			f, _ := arg.ToFloat64(sc)
			newArgs[i] = types.NewFloat64Datum(f)
		}
	}
	return
}

type makeSetFunctionClass struct {
	baseFunctionClass
}

func (c *makeSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinMakeSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinMakeSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinMakeSetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetNull()
		return
	}
	var (
		arg0 int64
		sets []string
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, err = args[0].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			continue
		}
		if arg0&(1<<uint(i-1)) > 0 {
			str, err1 := args[i].ToString()
			if err1 != nil {
				return d, errors.Trace(err1)
			}
			sets = append(sets, str)
		}
	}

	d.SetString(strings.Join(sets, ","))
	return d, errors.Trace(err)
}

type octFunctionClass struct {
	baseFunctionClass
}

func (c *octFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinOctSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinOctSig struct {
	baseBuiltinFunc
}

// eval evals a builtinOctSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		negative bool
		overflow bool
	)
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	n, err := arg.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	n = getValidPrefix(strings.TrimSpace(n), 10)
	if len(n) == 0 {
		d.SetString("0")
		return d, nil
	}
	if n[0] == '-' {
		negative = true
		n = n[1:]
	}
	val, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		if numError, ok := err.(*strconv.NumError); ok {
			if numError.Err == strconv.ErrRange {
				overflow = true
			} else {
				return d, errors.Trace(err)
			}
		} else {
			return d, errors.Trace(err)
		}
	}

	if negative && !overflow {
		val = -val
	}
	str := strconv.FormatUint(val, 8)
	d.SetString(str)
	return d, nil
}

type ordFunctionClass struct {
	baseFunctionClass
}

func (c *ordFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinOrdSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinOrdSig struct {
	baseBuiltinFunc
}

// eval evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}

	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}

	str, err := arg.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	if len(str) == 0 {
		d.SetInt64(0)
		return d, nil
	}

	_, size := utf8.DecodeRuneInString(str)
	leftMost := str[:size]

	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		result += int64(leftMost[i]) * factor
		factor *= 256
	}
	d.SetInt64(result)

	return d, nil
}

type quoteFunctionClass struct {
	baseFunctionClass
}

func (c *quoteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinQuoteSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinQuoteSig struct {
	baseBuiltinFunc
}

// eval evals a builtinQuoteSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	var (
		str    string
		buffer bytes.Buffer
	)
	str, err = args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	runes := []rune(str)
	buffer.WriteRune('\'')
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\\', '\'':
			buffer.WriteRune('\\')
			buffer.WriteRune(runes[i])
		case 0:
			buffer.WriteRune('\\')
			buffer.WriteRune('0')
		case '\032':
			buffer.WriteRune('\\')
			buffer.WriteRune('Z')
		default:
			buffer.WriteRune(runes[i])
		}
	}
	buffer.WriteRune('\'')
	d.SetString(buffer.String())
	return d, errors.Trace(err)
}

type binFunctionClass struct {
	baseFunctionClass
}

func (c *binFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinBinSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinBinSig struct {
	baseBuiltinFunc
}

// eval evals a builtinBinSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_bin
func (b *builtinBinSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg := args[0]
	sc := b.ctx.GetSessionVars().StmtCtx
	if arg.IsNull() || (arg.Kind() == types.KindString && arg.GetString() == "") {
		return d, nil
	}

	num, err := arg.ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	bits := fmt.Sprintf("%b", uint64(num))
	d.SetString(bits)
	return d, nil
}

type eltFunctionClass struct {
	baseFunctionClass
}

func (c *eltFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinEltSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinEltSig struct {
	baseBuiltinFunc
}

// eval evals a builtinEltSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_elt
func (b *builtinEltSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	index, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}

	argsLength := int64(len(args))
	if index < 1 || index > (argsLength-1) {
		return d, nil
	}

	result, err := args[index].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(result)

	return d, nil
}

type exportSetFunctionClass struct {
	baseFunctionClass
}

func (c *exportSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinExportSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinExportSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinExportSetSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_export-set
func (b *builtinExportSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		bits         uint64
		on           string
		off          string
		separator    = ","
		numberOfBits = 64
	)
	switch len(args) {
	case 5:
		arg, err := args[4].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		if arg >= 0 && arg < 64 {
			numberOfBits = int(arg)
		}
		fallthrough
	case 4:
		separator, err = args[3].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		fallthrough
	case 3:
		arg, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		bits = uint64(arg)
		on, err = args[1].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		off, err = args[2].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
	}
	var result string
	for i := 0; i < numberOfBits; i++ {
		if bits&1 > 0 {
			result += on
		} else {
			result += off
		}
		bits >>= 1
		if i < numberOfBits-1 {
			result += separator
		}
	}
	d.SetString(result)
	return d, nil
}

type formatFunctionClass struct {
	baseFunctionClass
}

func (c *formatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFormatSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFormatSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFormatSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_format
func (b *builtinFormatSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetNull()
		return
	}
	arg0, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	arg1, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	var arg2 string

	if len(args) == 2 {
		arg2 = "en_US"
	} else if len(args) == 3 {
		arg2, err = args[2].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
	}

	formatString, err := mysql.GetLocaleFormatFunction(arg2)(arg0, arg1)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetString(formatString)
	return d, nil
}

type fromBase64FunctionClass struct {
	baseFunctionClass
}

func (c *fromBase64FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFromBase64Sig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFromBase64Sig struct {
	baseBuiltinFunc
}

// eval evals a builtinFromBase64Sig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	str = strings.Replace(str, "\t", "", -1)
	str = strings.Replace(str, " ", "", -1)
	result, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return d, errors.Trace(err)
	}
	// Set the result to be of type []byte
	d.SetBytes(result)
	return d, nil

}

type toBase64FunctionClass struct {
	baseFunctionClass
}

func (c *toBase64FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinToBase64Sig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinToBase64Sig struct {
	baseBuiltinFunc
}

// eval evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	//encode
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	//A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
	count := len(result)
	if count > 76 {
		resultArr := splitToSubN(result, 76)
		result = strings.Join(resultArr, "\n")
	}
	// Set the result to be of type string
	d.SetString(result)
	return d, nil
}

// splitToSubN splits a string every n runes into a string[]
func splitToSubN(s string, n int) []string {
	subs := make([]string, 0, len(s)/n+1)
	for len(s) > n {
		subs = append(subs, s[:n])
		s = s[n:]
	}
	subs = append(subs, s)
	return subs
}

type insertFuncFunctionClass struct {
	baseFunctionClass
}

func (c *insertFuncFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinInsertFuncSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinInsertFuncSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInsertFuncSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertFuncSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	// Returns NULL if any argument is NULL.
	if args[0].IsNull() || args[1].IsNull() || args[2].IsNull() || args[3].IsNull() {
		return
	}

	str0, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	str := []rune(str0)
	strLen := len(str)

	posInt64, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	pos := int(posInt64)

	lenInt64, err := args[2].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	length := int(lenInt64)

	newstr, err := args[3].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	var s string
	if pos < 1 || pos > strLen {
		s = str0
	} else if length > strLen-pos+1 || length < 0 {
		s = string(str[0:pos-1]) + newstr
	} else {
		s = string(str[0:pos-1]) + newstr + string(str[pos+length-1:])
	}

	d.SetString(s)
	return d, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinInstrSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinInstrSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInstrSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_instr
func (b *builtinInstrSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	// INSTR(str, substr)
	if args[0].IsNull() || args[1].IsNull() {
		return d, nil
	}

	var str, substr string
	if str, err = args[0].ToString(); err != nil {
		return d, errors.Trace(err)
	}
	if substr, err = args[1].ToString(); err != nil {
		return d, errors.Trace(err)
	}

	// INSTR performs case **insensitive** search by default, while at least one argument is binary string
	// we do case sensitive search.
	var caseSensitive bool
	if args[0].Kind() == types.KindBytes || args[1].Kind() == types.KindBytes {
		caseSensitive = true
	}

	var pos, idx int
	if caseSensitive {
		idx = strings.Index(str, substr)
	} else {
		idx = strings.Index(strings.ToLower(str), strings.ToLower(substr))
	}
	if idx == -1 {
		pos = 0
	} else {
		if caseSensitive {
			pos = idx + 1
		} else {
			pos = utf8.RuneCountInString(str[:idx]) + 1
		}
	}
	d.SetInt64(int64(pos))
	return d, nil
}

type loadFileFunctionClass struct {
	baseFunctionClass
}

func (c *loadFileFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLoadFileSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLoadFileSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLoadFileSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_load-file
func (b *builtinLoadFileSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("load_file")
}

type lpadFunctionClass struct {
	baseFunctionClass
}

func (c *lpadFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLpadSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLpadSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLpadSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lpad
func (b *builtinLpadSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// LPAD(str,len,padstr)
	// args[0] string, args[1] int, args[2] string
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	length, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	l := int(length)

	padStr, err := args[2].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	if l < 0 || (len(str) < l && padStr == "") {
		return d, nil
	}

	tailLen := l - len(str)
	if tailLen > 0 {
		repeatCount := tailLen/len(padStr) + 1
		str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
	}
	d.SetString(str[:l])

	return d, nil
}
