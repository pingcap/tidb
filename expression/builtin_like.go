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
	"regexp"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
)

const (
	patMatch = iota + 1
	patOne
	patAny
)

var (
	_ functionClass = &likeFunctionClass{}
	_ functionClass = &regexpFunctionClass{}
)

var (
	_ builtinFunc = &builtinLikeSig{}
	_ builtinFunc = &builtinRegexpSig{}
)

// Handle escapes and wild cards convert pattern characters and pattern types.
func compilePattern(pattern string, escape byte) (patChars, patTypes []byte) {
	var lastAny bool
	patChars = make([]byte, len(pattern))
	patTypes = make([]byte, len(pattern))
	patLen := 0
	for i := 0; i < len(pattern); i++ {
		var tp byte
		var c = pattern[i]
		switch c {
		case escape:
			lastAny = false
			tp = patMatch
			if i < len(pattern)-1 {
				i++
				c = pattern[i]
				if c == escape || c == '_' || c == '%' {
					// valid escape.
				} else {
					// invalid escape, fall back to escape byte
					// mysql will treat escape character as the origin value even
					// the escape sequence is invalid in Go or C.
					// e.g, \m is invalid in Go, but in MySQL we will get "m" for select '\m'.
					// Following case is correct just for escape \, not for others like +.
					// TODO: add more checks for other escapes.
					i--
					c = escape
				}
			}
		case '_':
			lastAny = false
			tp = patOne
		case '%':
			if lastAny {
				continue
			}
			lastAny = true
			tp = patAny
		default:
			lastAny = false
			tp = patMatch
		}
		patChars[patLen] = c
		patTypes[patLen] = tp
		patLen++
	}
	for i := 0; i < patLen-1; i++ {
		if (patTypes[i] == patAny) && (patTypes[i+1] == patOne) {
			patTypes[i] = patOne
			patTypes[i+1] = patAny
		}
	}
	patChars = patChars[:patLen]
	patTypes = patTypes[:patLen]
	return
}

const caseDiff = 'a' - 'A'

func matchByteCI(a, b byte) bool {
	if a == b {
		return true
	}
	if a >= 'a' && a <= 'z' && a-caseDiff == b {
		return true
	}
	return a >= 'A' && a <= 'Z' && a+caseDiff == b
}

func doMatch(str string, patChars, patTypes []byte) bool {
	var sIdx int
	for i := 0; i < len(patChars); i++ {
		switch patTypes[i] {
		case patMatch:
			if sIdx >= len(str) || !matchByteCI(str[sIdx], patChars[i]) {
				return false
			}
			sIdx++
		case patOne:
			sIdx++
			if sIdx > len(str) {
				return false
			}
		case patAny:
			i++
			if i == len(patChars) {
				return true
			}
			for sIdx < len(str) {
				if matchByteCI(patChars[i], str[sIdx]) && doMatch(str[sIdx:], patChars[i:], patTypes[i:]) {
					return true
				}
				sIdx++
			}
			return false
		}
	}
	return sIdx == len(str)
}

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinLikeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinLikeSig struct {
	baseBuiltinFunc
}

func (b *builtinLikeSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinLike(args, b.ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func builtinLike(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	if args[0].IsNull() {
		return
	}

	valStr, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	if args[1].IsNull() {
		return
	}
	patternStr, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	escape := byte(args[2].GetInt64())
	patChars, patTypes := compilePattern(patternStr, escape)
	match := doMatch(valStr, patChars, patTypes)
	d.SetInt64(boolToInt64(match))
	return
}

type regexpFunctionClass struct {
	baseFunctionClass
}

func (c *regexpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinRegexpSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinRegexpSig struct {
	baseBuiltinFunc
}

func (b *builtinRegexpSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinRegexp(args, b.ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func builtinRegexp(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	if args[0].IsNull() || args[1].IsNull() {
		return
	}

	targetStr, err := args[0].ToString()
	if err != nil {
		return d, errors.Errorf("non-string Expression in LIKE: %v (Value of type %T)", args[0], args[0])
	}
	patternStr, err := args[1].ToString()
	if err != nil {
		return d, errors.Errorf("non-string Expression in LIKE: %v (Value of type %T)", args[1], args[1])
	}
	re, err := regexp.Compile(patternStr)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetInt64(boolToInt64(re.MatchString(targetStr)))
	return
}
