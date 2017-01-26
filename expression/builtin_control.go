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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &caseWhenFunctionClass{}
	_ functionClass = &ifFunctionClass{}
	_ functionClass = &ifNullFunctionClass{}
	_ functionClass = &nullIfFunctionClass{}
)

var (
	_ builtinFunc = &builtinCaseWhenSig{}
	_ builtinFunc = &builtinIfSig{}
	_ builtinFunc = &builtinIfNullSig{}
	_ builtinFunc = &builtinNullIfSig{}
)

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCaseWhenSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCaseWhenSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	l := len(args)
	for i := 0; i < l-1; i += 2 {
		if args[i].IsNull() {
			continue
		}
		b, err1 := args[i].ToBool(sc)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		if b == 1 {
			d = args[i+1]
			return
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		d = args[l-1]
	}
	return
}

type ifFunctionClass struct {
	baseFunctionClass
}

func (c *ifFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinIfSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinIfSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (s *builtinIfSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := s.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// if(expr1, expr2, expr3)
	// if expr1 is true, return expr2, otherwise, return expr3
	v1 := args[0]
	v2 := args[1]
	v3 := args[2]

	if v1.IsNull() {
		return v3, nil
	}

	b, err := v1.ToBool(s.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		d := types.Datum{}
		return d, errors.Trace(err)
	}

	// TODO: check return type, must be numeric or string
	if b == 1 {
		return v2, nil
	}

	return v3, nil
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinIfNullSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinIfNullSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_ifnull
func (b *builtinIfNullSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// ifnull(expr1, expr2)
	// if expr1 is not null, return expr1, otherwise, return expr2
	v1 := args[0]
	v2 := args[1]

	if !v1.IsNull() {
		return v1, nil
	}

	return v2, nil
}

type nullIfFunctionClass struct {
	baseFunctionClass
}

func (c *nullIfFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinNullIfSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinNullIfSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_nullif
func (b *builtinNullIfSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// nullif(expr1, expr2)
	// returns null if expr1 = expr2 is true, otherwise returns expr1
	v1 := args[0]
	v2 := args[1]

	if v1.IsNull() || v2.IsNull() {
		return v1, nil
	}

	if n, err1 := v1.CompareDatum(b.ctx.GetSessionVars().StmtCtx, v2); err1 != nil || n == 0 {
		d := types.Datum{}
		return d, errors.Trace(err1)
	}

	return v1, nil
}
