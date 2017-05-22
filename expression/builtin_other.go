// Copyright 2016 PingCAP, Inc.
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
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &castFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getVarFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &valuesFunctionClass{}
	_ functionClass = &bitCountFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinInSig{}
	_ builtinFunc = &builtinRowSig{}
	_ builtinFunc = &builtinCastSig{}
	_ builtinFunc = &builtinSetVarSig{}
	_ builtinFunc = &builtinGetVarSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinValuesSig{}
	_ builtinFunc = &builtinBitCountSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinInSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinInSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInSig.
// See https://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
func (b *builtinInSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	var hasNull bool
	for _, v := range args[1:] {
		if v.IsNull() {
			hasNull = true
			continue
		}

		a, b, err := types.CoerceDatum(sc, args[0], v)
		if err != nil {
			return d, errors.Trace(err)
		}
		ret, err := a.CompareDatum(sc, b)
		if err != nil {
			return d, errors.Trace(err)
		}
		if ret == 0 {
			d.SetInt64(1)
			return d, nil
		}
	}

	if hasNull {
		// If it's no matched but we get null in In, returns null.
		// e.g 1 in (null, 2, 3) returns null.
		return
	}
	d.SetInt64(0)
	return
}

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRowSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRowSig struct {
	baseBuiltinFunc
}

func (b *builtinRowSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d.SetRow(args)
	return
}
