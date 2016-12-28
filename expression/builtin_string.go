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
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
	"golang.org/x/text/transform"
)

type lengthFuncClass struct {
	baseFuncClass
}

type builtinLength struct {
	baseBuiltinFunc
}

func (b *lengthFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLength{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLength) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetInt64(int64(len(s)))
		return d, nil
	}
}

type asciiFuncClass struct {
	baseFuncClass
}

type builtinASCII struct {
	baseBuiltinFunc
}

func (b *asciiFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinASCII{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCII) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		if len(s) == 0 {
			d.SetInt64(0)
			return d, nil
		}
		d.SetInt64(int64(s[0]))
		return d, nil
	}
}

type concatFuncClass struct {
	baseFuncClass
}

type builtinConcat struct {
	baseBuiltinFunc
}

func (b *concatFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinConcat{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcat) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	var s []byte
	for _, a := range args {
		if a.IsNull() {
			return d, nil
		}
		var ss string
		ss, err = a.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		s = append(s, []byte(ss)...)
	}
	d.SetBytesAsString(s)
	return d, nil
}

type concatWSFuncClass struct {
	baseFuncClass
}

type builtinConcatWS struct {
	baseBuiltinFunc
}

func (b *concatWSFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinConcatWS{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWS) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type leftFuncClass struct {
	baseFuncClass
}

type builtinLeft struct {
	baseBuiltinFunc
}

func (b *leftFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLeft{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeft) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type repeatFuncClass struct {
	baseFuncClass
}

type builtinRepeat struct {
	baseBuiltinFunc
}

func (b *repeatFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinRepeat{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeat) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, err
	}
	ch := fmt.Sprintf("%v", str)
	num := 0
	x := args[1]
	switch x.Kind() {
	case types.KindInt64:
		num = int(x.GetInt64())
	case types.KindUint64:
		num = int(x.GetUint64())
	}
	if num < 1 {
		d.SetString("")
		return d, nil
	}
	d.SetString(strings.Repeat(ch, num))
	return d, nil
}

type lowerFuncClass struct {
	baseFuncClass
}

type builtinLower struct {
	baseBuiltinFunc
}

func (b *lowerFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLower{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLower) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type reverseFuncClass struct {
	baseFuncClass
}

type builtinReverse struct {
	baseBuiltinFunc
}

func (b *reverseFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinReverse{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverse) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type spaceFuncClass struct {
	baseFuncClass
}

type builtinSpace struct {
	baseBuiltinFunc
}

func (b *spaceFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinSpace{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpace) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

	v, err := x.ToInt64(sc)
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

type upperFuncClass struct {
	baseFuncClass
}

type builtinUpper struct {
	baseBuiltinFunc
}

func (b *upperFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinUpper{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpper) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type strcmpFuncClass struct {
	baseFuncClass
}

type builtinStrcmp struct {
	baseBuiltinFunc
}

func (b *strcmpFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinStrcmp{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmp) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() || args[1].IsNull() {
		return d, nil
	}
	left, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	right, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	res := types.CompareString(left, right)
	d.SetInt64(int64(res))
	return d, nil
}

type replaceFuncClass struct {
	baseFuncClass
}

type builtinReplace struct {
	baseBuiltinFunc
}

func (b *replaceFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinReplace{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplace) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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
	d.SetString(strings.Replace(str, oldStr, newStr, -1))

	return d, nil
}

type convertFuncClass struct {
	baseFuncClass
}

type builtinConvert struct {
	baseBuiltinFunc
}

func (b *convertFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinConvert{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvert) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type substringFuncClass struct {
	baseFuncClass
}

type builtinSubstring struct {
	baseBuiltinFunc
}

func (b *substringFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinSubstring{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

func (b *builtinSubstring) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	// The meaning of the elements of args.
	// arg[0] -> StrExpr
	// arg[1] -> Pos
	// arg[2] -> Len (Optional)
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

type substringIndexFuncClass struct {
	baseFuncClass
}

type builtinSubstringIndex struct {
	baseBuiltinFunc
}

func (b *substringIndexFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinSubstringIndex{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndex) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type locateFuncClass struct {
	baseFuncClass
}

type builtinLocate struct {
	baseBuiltinFunc
}

func (b *locateFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLocate{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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
		if pos < 0 || pos > int64(len(str)) {
			d.SetInt64(0)
			return d, nil
		}
		if pos > int64(len(str)-len(subStr)) {
			d.SetInt64(0)
			return d, nil
		}
	}
	if len(subStr) == 0 {
		d.SetInt64(pos + 1)
		return d, nil
	}
	i := strings.Index(str[pos:], subStr)
	if i == -1 {
		d.SetInt64(0)
		return d, nil
	}
	d.SetInt64(int64(i) + pos + 1)
	return d, nil
}

const spaceChars = "\n\t\r "

type hexFuncClass struct {
	baseFuncClass
}

type builtinHex struct {
	baseBuiltinFunc
}

func (b *hexFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinHex{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHex) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	case types.KindString:
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

type unHexFuncClass struct {
	baseFuncClass
}

type builtinUnHex struct {
	baseBuiltinFunc
}

func (b *unHexFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinUnHex{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHex) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	case types.KindString:
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

type trimFuncClass struct {
	baseFuncClass
}

type builtinTrim struct {
	baseBuiltinFunc
}

func (b *trimFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinTrim{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type lTrimFuncClass struct {
	baseFuncClass
}

func (b *lTrimFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLRTrim{
		baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx),
		fn:              strings.TrimLeft,
		cutset:          spaceChars,
	}
	f.self = f
	return f, nil
}

type rTrimFuncClass struct {
	baseFuncClass
}

func (b *rTrimFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinLRTrim{
		baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx),
		fn:              strings.TrimRight,
		cutset:          spaceChars,
	}
	f.self = f
	return f, nil
}

type builtinLRTrim struct {
	baseBuiltinFunc

	fn     func(string, string) string
	cutset string
}

// For LTRIM & RTRIM
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinLRTrim) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(b.fn(str, b.cutset))
	return d, nil
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

type rpadFuncClass struct {
	baseFuncClass
}

type builtinRpad struct {
	baseBuiltinFunc
}

func (b *rpadFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinRpad{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpad) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type bitLengthFuncClass struct {
	baseFuncClass
}

type builtinBitLength struct {
	baseBuiltinFunc
}

func (b *bitLengthFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinBitLength{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLength) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type charFuncClass struct {
	baseFuncClass
}

type builtinChar struct {
	baseBuiltinFunc
}

func (b *charFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinChar{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char
func (b *builtinChar) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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

type charLengthFuncClass struct {
	baseFuncClass
}

type builtinCharLength struct {
	baseBuiltinFunc
}

func (b *charLengthFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinCharLength{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLength) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return d, errors.Trace(err)
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
