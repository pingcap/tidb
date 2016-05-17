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

package evaluator

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
	"golang.org/x/text/transform"
)

// https://dev.mysql.com/doc/refman/5.7/en/string-functions.html

func builtinLength(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func builtinASCII(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func builtinConcat(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	var s []byte
	for _, a := range args {
		if a.Kind() == types.KindNull {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func builtinConcatWS(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	var sep string
	s := make([]string, 0, len(args))
	for i, a := range args {
		if a.Kind() == types.KindNull {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func builtinLeft(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	length, err := args[1].ToInt64()
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func builtinRepeat(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func builtinLower(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func builtinReverse(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func builtinUpper(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func builtinStrcmp(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	if args[0].Kind() == types.KindNull || args[1].Kind() == types.KindNull {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func builtinReplace(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	for _, arg := range args {
		if arg.Kind() == types.KindNull {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func builtinConvert(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

func builtinSubstring(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func builtinSubstringIndex(args []types.Datum, _ context.Context) (d types.Datum, err error) {
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

	c, err := args[2].ToInt64()
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func builtinLocate(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// The meaning of the elements of args.
	// args[0] -> SubStr
	// args[1] -> Str
	// args[2] -> Pos
	// eval str
	if args[1].Kind() == types.KindNull {
		return d, nil
	}
	str, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	// eval substr
	if args[0].Kind() == types.KindNull {
		return d, nil
	}
	subStr, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	// eval pos
	pos := int64(0)
	if len(args) == 3 {
		p, err := args[2].ToInt64()
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

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func builtinTrim(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// args[0] -> Str
	// args[1] -> RemStr
	// args[2] -> Direction
	// eval str
	if args[0].Kind() == types.KindNull {
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

// For LTRIM & RTRIM
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func trimFn(fn func(string, string) string, cutset string) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		if args[0].Kind() == types.KindNull {
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
