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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *ordFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinOrdSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Ord)
	return sig, nil
}

type builtinOrdSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinOrdSig) Clone() builtinFunc {
	newSig := &builtinOrdSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	strBytes := hack.Slice(str)
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	w := len(charset.EncodingUTF8Impl.Peek(strBytes))
	res, err := enc.Transform(nil, strBytes[:w], charset.OpEncode)
	if err != nil {
		// Fallback to the first byte.
		return calcOrd(strBytes[:1]), false, nil
	}
	// Only the first character is considered.
	return calcOrd(res[:len(enc.Peek(res))]), false, nil
}

func calcOrd(leftMost []byte) int64 {
	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		result += int64(leftMost[i]) * factor
		factor *= 256
	}
	return result
}

type quoteFunctionClass struct {
	baseFunctionClass
}

func (c *quoteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	SetBinFlagOrBinStr(args[0].GetType(ctx.GetEvalCtx()), bf.tp)
	flen := args[0].GetType(ctx.GetEvalCtx()).GetFlen()
	newFlen := 2*flen + 2
	if flen == types.UnspecifiedLength {
		newFlen = types.UnspecifiedLength
	}
	bf.tp.SetFlen(newFlen)
	if bf.tp.GetFlen() > mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}
	sig := &builtinQuoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Quote)
	return sig, nil
}

type builtinQuoteSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinQuoteSig) Clone() builtinFunc {
	newSig := &builtinQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals QUOTE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	} else if isNull {
		// If the argument is NULL, the return value is the word "NULL" without enclosing single quotation marks. see ref.
		return "NULL", false, err
	}

	return Quote(str), false, nil
}

// Quote produce a result that can be used as a properly escaped data value in an SQL statement.
func Quote(str string) string {
	buffer := bytes.NewBufferString("")
	buffer.WriteRune('\'')
	for i := 0; i < len(str); {
		r, size := utf8.DecodeRuneInString(str[i:])
		switch r {
		case '\\', '\'':
			buffer.WriteRune('\\')
			buffer.WriteRune(r)
		case 0:
			buffer.WriteRune('\\')
			buffer.WriteRune('0')
		case '\032':
			buffer.WriteRune('\\')
			buffer.WriteRune('Z')
		default:
			buffer.WriteRune(r)
		}
		i += size
	}
	buffer.WriteRune('\'')

	return buffer.String()
}

type binFunctionClass struct {
	baseFunctionClass
}

func (c *binFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinBinSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Bin)
	return sig, nil
}

type builtinBinSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinBinSig) Clone() builtinFunc {
	newSig := &builtinBinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals BIN(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bin
func (b *builtinBinSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%b", uint64(val)), false, nil
}

type eltFunctionClass struct {
	baseFunctionClass
}

func (c *eltFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if argsErr := c.verifyArgs(args); argsErr != nil {
		return nil, argsErr
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETInt)
	for i := 1; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	for _, arg := range args[1:] {
		argType := arg.GetType(ctx.GetEvalCtx())
		if types.IsBinaryStr(argType) {
			types.SetBinChsClnFlag(bf.tp)
		}
		flen := argType.GetFlen()
		if flen == types.UnspecifiedLength || flen > bf.tp.GetFlen() {
			bf.tp.SetFlen(argType.GetFlen())
		}
	}
	sig := &builtinEltSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Elt)
	return sig, nil
}

type builtinEltSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEltSig) Clone() builtinFunc {
	newSig := &builtinEltSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a ELT(N,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_elt
func (b *builtinEltSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	idx, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if idx < 1 || idx >= int64(len(b.args)) {
		return "", true, nil
	}
	arg, isNull, err := b.args[idx].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return arg, false, nil
}

type exportSetFunctionClass struct {
	baseFunctionClass
}

func (c *exportSetFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 5)
	argTps = append(argTps, types.ETInt, types.ETString, types.ETString)
	if len(args) > 3 {
		argTps = append(argTps, types.ETString)
	}
	if len(args) > 4 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	// Calculate the flen as MySQL does.
	l := max(args[2].GetType(ctx.GetEvalCtx()).GetFlen(), args[1].GetType(ctx.GetEvalCtx()).GetFlen())
	sepL := 1
	if len(args) > 3 {
		sepL = args[3].GetType(ctx.GetEvalCtx()).GetFlen()
	}
	bf.tp.SetFlen((l*64 + sepL*63) * 4)
	switch len(args) {
	case 3:
		sig = &builtinExportSet3ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExportSet3Arg)
	case 4:
		sig = &builtinExportSet4ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExportSet4Arg)
	case 5:
		sig = &builtinExportSet5ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExportSet5Arg)
	}
	return sig, nil
}

// exportSet evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func exportSet(bits int64, on, off, separator string, numberOfBits int64) string {
	result := ""
	for i := range numberOfBits {
		if (bits & (1 << i)) > 0 {
			result += on
		} else {
			result += off
		}
		if i < numberOfBits-1 {
			result += separator
		}
	}
	return result
}

type builtinExportSet3ArgSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExportSet3ArgSig) Clone() builtinFunc {
	newSig := &builtinExportSet3ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet3ArgSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	return exportSet(bits, on, off, ",", 64), false, nil
}

type builtinExportSet4ArgSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExportSet4ArgSig) Clone() builtinFunc {
	newSig := &builtinExportSet4ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet4ArgSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	separator, isNull, err := b.args[3].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	return exportSet(bits, on, off, separator, 64), false, nil
}

type builtinExportSet5ArgSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExportSet5ArgSig) Clone() builtinFunc {
	newSig := &builtinExportSet5ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet5ArgSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	separator, isNull, err := b.args[3].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	numberOfBits, isNull, err := b.args[4].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if numberOfBits < 0 || numberOfBits > 64 {
		numberOfBits = 64
	}

	return exportSet(bits, on, off, separator, numberOfBits), false, nil
}

type formatFunctionClass struct {
	baseFunctionClass
}

func (c *formatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 2, 3)
	argTps[1] = types.ETInt
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp == types.ETDecimal || argTp == types.ETInt {
		argTps[0] = types.ETDecimal
	} else {
		argTps[0] = types.ETReal
	}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	charset, colalte := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(colalte)
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	var sig builtinFunc
	if len(args) == 3 {
		sig = &builtinFormatWithLocaleSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FormatWithLocale)
	} else {
		sig = &builtinFormatSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Format)
	}
	return sig, nil
}

// formatMaxDecimals limits the maximum number of decimal digits for result of
// function `format`, this value is same as `FORMAT_MAX_DECIMALS` in MySQL source code.
const formatMaxDecimals int64 = 30

// evalNumDecArgsForFormat evaluates first 2 arguments, i.e, x and d, for function `format`.
func evalNumDecArgsForFormat(ctx EvalContext, f builtinFunc, row chunk.Row) (string, string, bool, error) {
	var xStr string
	arg0, arg1 := f.getArgs()[0], f.getArgs()[1]
	if arg0.GetType(ctx).EvalType() == types.ETDecimal {
		x, isNull, err := arg0.EvalDecimal(ctx, row)
		if isNull || err != nil {
			return "", "", isNull, err
		}
		xStr = x.String()
	} else {
		x, isNull, err := arg0.EvalReal(ctx, row)
		if isNull || err != nil {
			return "", "", isNull, err
		}
		xStr = strconv.FormatFloat(x, 'f', -1, 64)
	}
	d, isNull, err := arg1.EvalInt(ctx, row)
	if isNull || err != nil {
		return "", "", isNull, err
	}
	if d < 0 {
		d = 0
	} else if d > formatMaxDecimals {
		d = formatMaxDecimals
	}
	xStr = roundFormatArgs(xStr, int(d))
	dStr := strconv.FormatInt(d, 10)
	return xStr, dStr, false, nil
}

func roundFormatArgs(xStr string, maxNumDecimals int) string {
	if !strings.Contains(xStr, ".") {
		return xStr
	}

	sign := false
	// xStr cannot have '+' prefix now.
	// It is built in `evalNumDecArgsFormat` after evaluating `Evalxxx` method.
	if strings.HasPrefix(xStr, "-") {
		xStr = strings.Trim(xStr, "-")
		sign = true
	}

	xArr := strings.Split(xStr, ".")
	integerPart := xArr[0]
	decimalPart := xArr[1]

	if len(decimalPart) > maxNumDecimals {
		t := []byte(decimalPart)
		carry := false
		if t[maxNumDecimals] >= '5' {
			carry = true
		}
		for i := maxNumDecimals - 1; i >= 0 && carry; i-- {
			if t[i] == '9' {
				t[i] = '0'
			} else {
				t[i] = t[i] + 1
				carry = false
			}
		}
		decimalPart = string(t)
		t = []byte(integerPart)
		for i := len(integerPart) - 1; i >= 0 && carry; i-- {
			if t[i] == '9' {
				t[i] = '0'
			} else {
				t[i] = t[i] + 1
				carry = false
			}
		}
		if carry {
			integerPart = "1" + string(t)
		} else {
			integerPart = string(t)
		}
	}

	xStr = integerPart + "." + decimalPart
	if sign {
		xStr = "-" + xStr
	}
	return xStr
}

type builtinFormatWithLocaleSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFormatWithLocaleSig) Clone() builtinFunc {
	newSig := &builtinFormatWithLocaleSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D,locale).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatWithLocaleSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	x, d, isNull, err := evalNumDecArgsForFormat(ctx, b, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	locale, isNull, err := b.args[2].EvalString(ctx, row)
	if err != nil {
		return "", false, err
	}
	tc := typeCtx(ctx)
	if isNull {
		tc.AppendWarning(errUnknownLocale.FastGenByArgs("NULL"))
		locale = "en_US"
	}
	formatString, found, err := mysql.FormatByLocale(x, d, locale)
	// If locale was not NULL and not found, warn unknown locale.
	if !isNull && !found {
		tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale))
	}
	return formatString, false, err
}

type builtinFormatSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFormatSig) Clone() builtinFunc {
	newSig := &builtinFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	x, d, isNull, err := evalNumDecArgsForFormat(ctx, b, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	formatString, _, err := mysql.FormatByLocale(x, d, "en_US")
	return formatString, false, err
}

type fromBase64FunctionClass struct {
	baseFunctionClass
}

func (c *fromBase64FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	// The calculation of flen is the same as MySQL.
	if args[0].GetType(ctx.GetEvalCtx()).GetFlen() == types.UnspecifiedLength {
		bf.tp.SetFlen(types.UnspecifiedLength)
	} else {
		bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen() * 3)
		if bf.tp.GetFlen() > mysql.MaxBlobWidth {
			bf.tp.SetFlen(mysql.MaxBlobWidth)
		}
	}

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinFromBase64Sig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_FromBase64)
	return sig, nil
}

// base64NeededDecodedLength return the base64 decoded string length.
func base64NeededDecodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 && n > math.MaxInt64/3 {
		return -1
	}
	if strconv.IntSize == 32 && n > math.MaxInt32/3 {
		return -1
	}
	return n * 3 / 4
}

type builtinFromBase64Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinFromBase64Sig) Clone() builtinFunc {
	newSig := &builtinFromBase64Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	needDecodeLen := base64NeededDecodedLength(len(str))
	if needDecodeLen == -1 {
		return "", true, nil
	}
	if needDecodeLen > int(b.maxAllowedPacket) {
		return "", true, handleAllowedPacketOverflowed(ctx, "from_base64", b.maxAllowedPacket)
	}

	str = strings.ReplaceAll(str, "\t", "")
	str = strings.ReplaceAll(str, " ", "")
	result, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		// When error happens, take `from_base64("asc")` as an example, we should return NULL.
		return "", true, nil
	}
	return string(result), false, nil
}

type toBase64FunctionClass struct {
	baseFunctionClass
}

func (c *toBase64FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)

	if bf.args[0].GetType(ctx.GetEvalCtx()).GetFlen() == types.UnspecifiedLength {
		bf.tp.SetFlen(types.UnspecifiedLength)
	} else {
		bf.tp.SetFlen(base64NeededEncodedLength(bf.args[0].GetType(ctx.GetEvalCtx()).GetFlen()))
	}

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	sig := &builtinToBase64Sig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_ToBase64)
	return sig, nil
}

type builtinToBase64Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinToBase64Sig) Clone() builtinFunc {
	newSig := &builtinToBase64Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// base64NeededEncodedLength return the base64 encoded string length.
func base64NeededEncodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 {
		// len(arg)            -> len(to_base64(arg))
		// 6827690988321067803 -> 9223372036854775804
		// 6827690988321067804 -> -9223372036854775808
		if n > 6827690988321067803 {
			return -1
		}
	} else {
		// len(arg)   -> len(to_base64(arg))
		// 1589695686 -> 2147483645
		// 1589695687 -> -2147483646
		if n > 1589695686 {
			return -1
		}
	}

	length := (n + 2) / 3 * 4
	return length + (length-1)/76
}

// evalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	needEncodeLen := base64NeededEncodedLength(len(str))
	if needEncodeLen == -1 {
		return "", true, nil
	}
	if needEncodeLen > int(b.maxAllowedPacket) {
		return "", true, handleAllowedPacketOverflowed(ctx, "to_base64", b.maxAllowedPacket)
	}
	if b.tp.GetFlen() == -1 || b.tp.GetFlen() > mysql.MaxBlobWidth {
		b.tp.SetFlen(mysql.MaxBlobWidth)
	}

	// encode
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	// A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
	count := len(result)
	if count > 76 {
		resultArr := splitToSubN(result, 76)
		result = strings.Join(resultArr, "\n")
	}

	return result, false, nil
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

type insertFunctionClass struct {
	baseFunctionClass
}

func (c *insertFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	addBinFlag(bf.tp)

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	if types.IsBinaryStr(bf.tp) {
		sig = &builtinInsertSig{bf, maxAllowedPacket}
		sig.setPbCode(tipb.ScalarFuncSig_Insert)
	} else {
		sig = &builtinInsertUTF8Sig{bf, maxAllowedPacket}
		sig.setPbCode(tipb.ScalarFuncSig_InsertUTF8)
	}
	return sig, nil
}

type builtinInsertSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertSig) Clone() builtinFunc {
	newSig := &builtinInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	length, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	newstr, isNull, err := b.args[3].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	strLength := int64(len(str))
	if pos < 1 || pos > strLength {
		return str, false, nil
	}
	if length > strLength-pos+1 || length < 0 {
		length = strLength - pos + 1
	}

	if uint64(strLength-length+int64(len(newstr))) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "insert", b.maxAllowedPacket)
	}

	return str[0:pos-1] + newstr + str[pos+length-1:], false, nil
}

type builtinInsertUTF8Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertUTF8Sig) Clone() builtinFunc {
	newSig := &builtinInsertUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	length, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	newstr, isNull, err := b.args[3].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	runeLength := int64(utf8.RuneCountInString(str))
	if pos < 1 || pos > runeLength {
		return str, false, nil
	}
	if length > runeLength-pos+1 || length < 0 {
		length = runeLength - pos + 1
	}

	headEnd := runeByteIndex(str, int(pos-1))
	tailStart := runeByteIndex(str, int(pos+length-1))
	strHead := str[:headEnd]
	strTail := str[tailStart:]
	if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "insert", b.maxAllowedPacket)
	}
	return strHead + newstr + strTail, false, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(11)
	if bf.collation == charset.CollationBin {
		sig := &builtinInstrSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Instr)
		return sig, nil
	}
	sig := &builtinInstrUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_InstrUTF8)
	return sig, nil
}

type builtinInstrUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInstrUTF8Sig) Clone() builtinFunc {
	newSig := &builtinInstrUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinInstrSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInstrSig) Clone() builtinFunc {
	newSig := &builtinInstrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals INSTR(str,substr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrUTF8Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.args[0].EvalString(ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	substr, IsNull, err := b.args[1].EvalString(ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	if collate.IsCICollation(b.collation) {
		str = strings.ToLower(str)
		substr = strings.ToLower(substr)
	}

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(utf8.RuneCountInString(str[:idx]) + 1), false, nil
}

// evalInt evals INSTR(str,substr), case sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.args[0].EvalString(ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	substr, IsNull, err := b.args[1].EvalString(ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(idx + 1), false, nil
}

type loadFileFunctionClass struct {
	baseFunctionClass
}

func (c *loadFileFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinLoadFileSig{bf}
	return sig, nil
}

type builtinLoadFileSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLoadFileSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return "", true, nil
}

func (b *builtinLoadFileSig) Clone() builtinFunc {
	newSig := &builtinLoadFileSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type weightStringPadding byte

const (
	// weightStringPaddingNone is used for WEIGHT_STRING(expr) if the expr is non-numeric.
	weightStringPaddingNone weightStringPadding = iota
	// weightStringPaddingAsChar is used for WEIGHT_STRING(expr AS CHAR(x)) and the expr is non-numeric.
	weightStringPaddingAsChar
	// weightStringPaddingAsBinary is used for WEIGHT_STRING(expr as BINARY(x)) and the expr is not null.
	weightStringPaddingAsBinary
	// weightStringPaddingNull is used for WEIGHT_STRING(expr [AS (CHAR|BINARY)]) for all other cases, it returns null always.
	weightStringPaddingNull
)

type weightStringFunctionClass struct {
	baseFunctionClass
}

func (c *weightStringFunctionClass) verifyArgs(ctx EvalContext, args []Expression) (weightStringPadding, int, error) {
	padding := weightStringPaddingNone
	l := len(args)
	if l != 1 && l != 3 {
		return weightStringPaddingNone, 0, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	if types.IsTypeNumeric(args[0].GetType(ctx).GetType()) {
		padding = weightStringPaddingNull
	}
	length := 0
	if l == 3 {
		if args[1].GetType(ctx).EvalType() != types.ETString {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[1].StringWithCtx(ctx, errors.RedactLogDisable), c.funcName)
		}
		c1, ok := args[1].(*Constant)
		if !ok {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[1].StringWithCtx(ctx, errors.RedactLogDisable), c.funcName)
		}
		switch x := c1.Value.GetString(); x {
		case "CHAR":
			if padding == weightStringPaddingNone {
				padding = weightStringPaddingAsChar
			}
		case "BINARY":
			padding = weightStringPaddingAsBinary
		default:
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(x, c.funcName)
		}
		if args[2].GetType(ctx).EvalType() != types.ETInt {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[2].StringWithCtx(ctx, errors.RedactLogDisable), c.funcName)
		}
		c2, ok := args[2].(*Constant)
		if !ok {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[1].StringWithCtx(ctx, errors.RedactLogDisable), c.funcName)
		}
		length = int(c2.Value.GetInt64())
		if length == 0 {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[2].StringWithCtx(ctx, errors.RedactLogDisable), c.funcName)
		}
	}
	return padding, length, nil
}

func (c *weightStringFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	padding, length, err := c.verifyArgs(ctx.GetEvalCtx(), args)
	if err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	argTps[0] = types.ETString

	if len(args) == 3 {
		argTps[1] = types.ETString
		argTps[2] = types.ETInt
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	var sig builtinFunc
	if padding == weightStringPaddingNull {
		sig = &builtinWeightStringNullSig{bf}
	} else {
		maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
		sig = &builtinWeightStringSig{bf, padding, length, maxAllowedPacket}
	}
	return sig, nil
}

type builtinWeightStringNullSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinWeightStringNullSig) Clone() builtinFunc {
	newSig := &builtinWeightStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a WEIGHT_STRING(expr [AS CHAR|BINARY]) when the expr is numeric types, it always returns null.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_weight-string
func (b *builtinWeightStringNullSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinWeightStringSig struct {
	baseBuiltinFunc

	padding          weightStringPadding
	length           int
	maxAllowedPacket uint64
}

func (b *builtinWeightStringSig) Clone() builtinFunc {
	newSig := &builtinWeightStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.padding = b.padding
	newSig.length = b.length
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a WEIGHT_STRING(expr [AS (CHAR|BINARY)]) when the expr is non-numeric types.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_weight-string
func (b *builtinWeightStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return "", false, err
	}
	if isNull {
		return "", true, nil
	}

	var ctor collate.Collator
	// TODO: refactor padding codes after padding is implemented by all collators.
	switch b.padding {
	case weightStringPaddingAsChar:
		lenRunes := utf8.RuneCountInString(str)
		if b.length < lenRunes {
			str = str[:runeByteIndex(str, b.length)]
		} else if b.length > lenRunes {
			if uint64(b.length-lenRunes) > b.maxAllowedPacket {
				return "", true, handleAllowedPacketOverflowed(ctx, "weight_string", b.maxAllowedPacket)
			}
			str += strings.Repeat(" ", b.length-lenRunes)
		}
		ctor = collate.GetCollator(b.args[0].GetType(ctx).GetCollate())
	case weightStringPaddingAsBinary:
		lenStr := len(str)
		if b.length < lenStr {
			tpInfo := fmt.Sprintf("BINARY(%d)", b.length)
			tc := typeCtx(ctx)
			tc.AppendWarning(errTruncatedWrongValue.FastGenByArgs(tpInfo, str))
			str = str[:b.length]
		} else if b.length > lenStr {
			if uint64(b.length-lenStr) > b.maxAllowedPacket {
				return "", true, handleAllowedPacketOverflowed(ctx, "cast_as_binary", b.maxAllowedPacket)
			}
			str += strings.Repeat("\x00", b.length-lenStr)
		}
		ctor = collate.GetCollator(charset.CollationBin)
	case weightStringPaddingNone:
		ctor = collate.GetCollator(b.args[0].GetType(ctx).GetCollate())
	default:
		return "", false, ErrIncorrectType.GenWithStackByArgs(ast.WeightString, string(b.padding))
	}
	return string(ctor.Key(str)), false, nil
}

const (
	invalidRune rune   = -1
	invalidByte uint16 = 256
)

type translateFunctionClass struct {
	baseFunctionClass
}

// getFunction sets translate built-in function signature.
// The syntax of translate in Oracle is 'TRANSLATE(expr, from_string, to_string)'.
func (c *translateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(args[0].GetType(ctx.GetEvalCtx())) || types.IsBinaryStr(args[1].GetType(ctx.GetEvalCtx())) || types.IsBinaryStr(args[2].GetType(ctx.GetEvalCtx())) {
		sig := &builtinTranslateBinarySig{bf}
		return sig, nil
	}
	sig := &builtinTranslateUTF8Sig{bf}
	return sig, nil
}

type builtinTranslateBinarySig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTranslateBinarySig) Clone() builtinFunc {
	newSig := &builtinTranslateBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTranslateSig, corresponding to translate(srcStr, fromStr, toStr)
// See https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions196.htm
func (b *builtinTranslateBinarySig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var (
		srcStr, fromStr, toStr     string
		isFromStrNull, isToStrNull bool
		tgt                        []byte
	)
	srcStr, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	fromStr, isFromStrNull, err = b.args[1].EvalString(ctx, row)
	if isFromStrNull || err != nil {
		return d, isFromStrNull, err
	}
	toStr, isToStrNull, err = b.args[2].EvalString(ctx, row)
	if isToStrNull || err != nil {
		return d, isToStrNull, err
	}
	mp := buildTranslateMap4Binary([]byte(fromStr), []byte(toStr))
	for _, charSrc := range []byte(srcStr) {
		if charTo, ok := mp[charSrc]; ok {
			if charTo != invalidByte {
				tgt = append(tgt, byte(charTo))
			}
		} else {
			tgt = append(tgt, charSrc)
		}
	}
	return string(tgt), false, nil
}

type builtinTranslateUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTranslateUTF8Sig) Clone() builtinFunc {
	newSig := &builtinTranslateUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTranslateUTF8Sig, corresponding to translate(srcStr, fromStr, toStr)
// See https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions196.htm
func (b *builtinTranslateUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var (
		srcStr, fromStr, toStr     string
		isFromStrNull, isToStrNull bool
		tgt                        strings.Builder
	)
	srcStr, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	fromStr, isFromStrNull, err = b.args[1].EvalString(ctx, row)
	if isFromStrNull || err != nil {
		return d, isFromStrNull, err
	}
	toStr, isToStrNull, err = b.args[2].EvalString(ctx, row)
	if isToStrNull || err != nil {
		return d, isToStrNull, err
	}
	mp := buildTranslateMap4UTF8([]rune(fromStr), []rune(toStr))
	for _, charSrc := range srcStr {
		if charTo, ok := mp[charSrc]; ok {
			if charTo != invalidRune {
				tgt.WriteRune(charTo)
			}
		} else {
			tgt.WriteRune(charSrc)
		}
	}
	return tgt.String(), false, nil
}

func buildTranslateMap4UTF8(from, to []rune) map[rune]rune {
	mp := make(map[rune]rune)
	lenFrom, lenTo := len(from), len(to)
	minLen := min(lenFrom, lenTo)
	for idx := lenFrom - 1; idx >= lenTo; idx-- {
		mp[from[idx]] = invalidRune
	}
	for idx := minLen - 1; idx >= 0; idx-- {
		mp[from[idx]] = to[idx]
	}
	return mp
}

func buildTranslateMap4Binary(from, to []byte) map[byte]uint16 {
	mp := make(map[byte]uint16)
	lenFrom, lenTo := len(from), len(to)
	minLen := min(lenFrom, lenTo)
	for idx := lenFrom - 1; idx >= lenTo; idx-- {
		mp[from[idx]] = invalidByte
	}
	for idx := minLen - 1; idx >= 0; idx-- {
		mp[from[idx]] = uint16(to[idx])
	}
	return mp
}
