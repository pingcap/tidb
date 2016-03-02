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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
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
func builtinConcat(args []interface{}, ctx context.Context) (v interface{}, err error) {
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
func builtinConcatWS(args []interface{}, ctx context.Context) (v interface{}, err error) {
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
func builtinRepeat(args []interface{}, ctx context.Context) (v interface{}, err error) {
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
func builtinLower(args []interface{}, ctx context.Context) (interface{}, error) {
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
func builtinUpper(args []interface{}, ctx context.Context) (interface{}, error) {
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
func builtinStrcmp(args []interface{}, ctx context.Context) (interface{}, error) {
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
func builtinReplace(args []interface{}, ctx context.Context) (interface{}, error) {
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
