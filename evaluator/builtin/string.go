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

package builtin

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"golang.org/x/text/transform"
)

// https://dev.mysql.com/doc/refman/5.7/en/string-functions.html

func builtinLength(args []interface{}, _ context.Context) (v interface{}, err error) {
	switch x := args[0].(type) {
	case nil:
		return nil, nil
	default:
		s, err := types.ToString(x)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return int64(len(s)), nil
	}
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func builtinConcat(args []interface{}, _ context.Context) (v interface{}, err error) {
	var s []byte
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
		ss, err := types.ToString(a)
		if err != nil {
			return nil, errors.Trace(err)
		}
		s = append(s, []byte(ss)...)
	}

	return string(s), nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func builtinConcatWS(args []interface{}, _ context.Context) (v interface{}, err error) {
	var sep string
	s := make([]string, 0, len(args))
	for i, a := range args {
		if a == nil {
			if i == 0 {
				return nil, nil
			}
			continue
		}
		ss, err := types.ToString(a)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if i == 0 {
			sep = ss
			continue
		}
		s = append(s, ss)
	}

	return strings.Join(s, sep), nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func builtinLeft(args []interface{}, _ context.Context) (v interface{}, err error) {
	str, err := types.ToString(args[0])
	if err != nil {
		return nil, errors.Errorf("BuiltinLeft invalid args, need string but get %T", args[0])
	}
	// TODO: deal with other types
	length, ok := args[1].(int64)
	if !ok {
		return nil, errors.Errorf("BuiltinLeft invalid args, need int but get %T", args[1])
	}
	l := int(length)
	if l < 0 {
		l = 0
	} else if l > len(str) {
		l = len(str)
	}
	return str[:l], nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func builtinRepeat(args []interface{}, _ context.Context) (v interface{}, err error) {
	ch := fmt.Sprintf("%v", args[0])
	num := 0
	switch x := args[1].(type) {
	case int64:
		num = int(x)
	case uint64:
		num = int(x)
	}
	if num < 1 {
		return "", nil
	}
	return strings.Repeat(ch, num), nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func builtinLower(args []interface{}, _ context.Context) (interface{}, error) {
	switch x := args[0].(type) {
	case nil:
		return nil, nil
	default:
		s, err := types.ToString(x)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return strings.ToLower(s), nil
	}
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func builtinUpper(args []interface{}, _ context.Context) (interface{}, error) {
	switch x := args[0].(type) {
	case nil:
		return nil, nil
	default:
		s, err := types.ToString(x)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return strings.ToUpper(s), nil
	}
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func builtinStrcmp(args []interface{}, _ context.Context) (interface{}, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	left, err := types.ToString(args[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	right, err := types.ToString(args[1])
	if err != nil {
		return nil, errors.Trace(err)
	}
	res := types.CompareString(left, right)
	return res, nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func builtinReplace(args []interface{}, _ context.Context) (interface{}, error) {
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}

	str, err := types.ToString(args[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldStr, err := types.ToString(args[1])
	if err != nil {
		return nil, errors.Trace(err)
	}
	newStr, err := types.ToString(args[2])
	if err != nil {
		return nil, errors.Trace(err)
	}

	return strings.Replace(str, oldStr, newStr, -1), nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func builtinConvert(args []interface{}, _ context.Context) (interface{}, error) {
	value := args[0]
	Charset := args[1].(string)

	// Casting nil to any type returns nil
	if value == nil {
		return nil, nil
	}
	str, ok := value.(string)
	if !ok {
		return nil, nil
	}
	if strings.ToLower(Charset) == "ascii" {
		return value, nil
	} else if strings.ToLower(Charset) == "utf8mb4" {
		return value, nil
	}

	encoding, _ := charset.Lookup(Charset)
	if encoding == nil {
		return nil, errors.Errorf("unknown encoding: %s", Charset)
	}

	target, _, err := transform.String(encoding.NewDecoder(), str)
	if err != nil {
		log.Errorf("Convert %s to %s with error: %v", str, Charset, err)
		return nil, errors.Trace(err)
	}
	return target, nil
}

func builtinSubstring(args []interface{}, _ context.Context) (interface{}, error) {
	// The meaning of the elements of args.
	// arg[0] -> StrExpr
	// arg[1] -> Pos
	// arg[2] -> Len (Optional)
	str, err := types.ToString(args[0])
	if err != nil {
		return nil, errors.Errorf("Substring invalid args, need string but get %T", args[0])
	}

	t := args[1]
	p, ok := t.(int64)
	if !ok {
		return nil, errors.Errorf("Substring invalid pos args, need int but get %T", t)
	}
	pos := int(p)

	length := -1
	if len(args) == 3 {
		t = args[2]
		p, ok = t.(int64)
		if !ok {
			return nil, errors.Errorf("Substring invalid pos args, need int but get %T", t)
		}
		length = int(p)
	}
	// The forms without a len argument return a substring from string str starting at position pos.
	// The forms with a len argument return a substring len characters long from string str, starting at position pos.
	// The forms that use FROM are standard SQL syntax. It is also possible to use a negative value for pos.
	// In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning.
	// A negative value may be used for pos in any of the forms of this function.
	if pos < 0 {
		pos = len(str) + pos
	} else {
		pos--
	}
	if pos > len(str) || pos <= 0 {
		pos = len(str)
	}
	end := len(str)
	if length != -1 {
		end = pos + length
	}
	if end > len(str) {
		end = len(str)
	}
	return str[pos:end], nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func builtinSubstringIndex(args []interface{}, _ context.Context) (interface{}, error) {
	// The meaning of the elements of args.
	//args[0] -> StrExpr
	//args[1] -> Delim
	//args[2] -> Count
	fs := args[0]
	str, err := types.ToString(fs)
	if err != nil {
		return nil, errors.Errorf("Substring_Index invalid args, need string but get %T", fs)
	}

	t := args[1]
	delim, err := types.ToString(t)
	if err != nil {
		return nil, errors.Errorf("Substring_Index invalid delim, need string but get %T", t)
	}
	if len(delim) == 0 {
		return "", nil
	}

	t = args[2]
	c, err := types.ToInt64(t)
	if err != nil {
		return nil, errors.Trace(err)
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
	return strings.Join(substrs, delim), nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func builtinLocate(args []interface{}, _ context.Context) (interface{}, error) {
	// The meaning of the elements of args.
	//args[0] -> SubStr
	//args[1] -> Str
	//args[2] -> Pos
	// eval str
	fs := args[1]
	if fs == nil {
		return nil, nil
	}
	str, err := types.ToString(fs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// eval substr
	fs = args[0]
	if fs == nil {
		return nil, nil
	}
	subStr, err := types.ToString(fs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// eval pos
	pos := int64(0)
	if len(args) == 3 {
		t := args[2]
		p, err := types.ToInt64(t)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pos = p - 1
		if pos < 0 || pos > int64(len(str)) {
			return 0, nil
		}
		if pos > int64(len(str)-len(subStr)) {
			return 0, nil
		}
	}
	if len(subStr) == 0 {
		return pos + 1, nil
	}
	i := strings.Index(str[pos:], subStr)
	if i == -1 {
		return 0, nil
	}
	return int64(i) + pos + 1, nil
}
