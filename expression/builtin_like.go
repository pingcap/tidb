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
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &likeFunctionClass{}
	_ functionClass = &regexpFunctionClass{}
)

var (
	_ builtinFunc = &builtinLikeSig{}
	_ builtinFunc = &builtinRegexpSig{}
)

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinLikeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinLikeSig struct {
	baseBuiltinFunc
}

// https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
func (b *builtinLikeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
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
	patChars, patTypes := stringutil.CompilePattern(patternStr, escape)
	match := stringutil.DoMatch(valStr, patChars, patTypes)
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

// See http://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
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
