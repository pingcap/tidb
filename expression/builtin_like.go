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
	"sync"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &likeFunctionClass{}
	_ functionClass = &regexpFunctionClass{}
)

var (
	_ builtinFunc = &builtinLikeSig{}
	_ builtinFunc = &builtinRegexpSig{}
	_ builtinFunc = &builtinRegexpUTF8Sig{}
)

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp...)
	bf.tp.Flen = 1
	sig := &builtinLikeSig{bf, nil, false}
	sig.setPbCode(tipb.ScalarFuncSig_LikeSig)
	return sig, nil
}

type builtinLikeSig struct {
	baseBuiltinFunc
	// pattern and isMemorizedPattern is not serialized with builtinLikeSig, treat them as a cache to accelerate
	// the evaluation of builtinLikeSig.
	pattern            collate.WildcardPattern
	isMemorizedPattern bool
}

func (b *builtinLikeSig) Clone() builtinFunc {
	newSig := &builtinLikeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.pattern = b.pattern
	newSig.isMemorizedPattern = b.isMemorizedPattern
	return newSig
}

// evalInt evals a builtinLikeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
func (b *builtinLikeSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	patternStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	escape, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b.pattern == nil {
		b.pattern = b.collator().Pattern()
		if b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx) && b.args[2].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			b.pattern.Compile(patternStr, byte(escape))
			b.isMemorizedPattern = true
		}
	}
	if !b.isMemorizedPattern {
		b.pattern.Compile(patternStr, byte(escape))
	}
	return boolToInt64(b.pattern.DoMatch(valStr)), false, nil
}

type regexpFunctionClass struct {
	baseFunctionClass
}

func (c *regexpFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 1
	var sig builtinFunc
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType()) {
		sig = newBuiltinRegexpSig(bf)
		sig.setPbCode(tipb.ScalarFuncSig_RegexpSig)
	} else {
		sig = newBuiltinRegexpUTF8Sig(bf)
		sig.setPbCode(tipb.ScalarFuncSig_RegexpUTF8Sig)
	}
	return sig, nil
}

type builtinRegexpSharedSig struct {
	baseBuiltinFunc
	compile         func(string) (*regexp.Regexp, error)
	memorizedRegexp *regexp.Regexp
	memorizedErr    error
	once            sync.Once
}

func (b *builtinRegexpSharedSig) clone(from *builtinRegexpSharedSig) {
	b.cloneFrom(&from.baseBuiltinFunc)
	b.compile = from.compile
	if from.memorizedRegexp != nil {
		b.memorizedRegexp = from.memorizedRegexp.Copy()
	}
	b.memorizedErr = from.memorizedErr
}

// evalInt evals `expr REGEXP pat`, or `expr RLIKE pat`.
// See https://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSharedSig) evalInt(row chunk.Row) (int64, bool, error) {
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	re, err := b.compile(pat)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}

type builtinRegexpSig struct {
	builtinRegexpSharedSig
}

func newBuiltinRegexpSig(bf baseBuiltinFunc) *builtinRegexpSig {
	shared := builtinRegexpSharedSig{baseBuiltinFunc: bf}
	shared.compile = regexp.Compile
	return &builtinRegexpSig{builtinRegexpSharedSig: shared}
}

func (b *builtinRegexpSig) Clone() builtinFunc {
	newSig := &builtinRegexpSig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}

type builtinRegexpUTF8Sig struct {
	builtinRegexpSharedSig
}

func newBuiltinRegexpUTF8Sig(bf baseBuiltinFunc) *builtinRegexpUTF8Sig {
	shared := builtinRegexpSharedSig{baseBuiltinFunc: bf}
	shared.compile = func(pat string) (*regexp.Regexp, error) {
		return regexp.Compile("(?i)" + pat)
	}
	return &builtinRegexpUTF8Sig{builtinRegexpSharedSig: shared}
}

func (b *builtinRegexpUTF8Sig) Clone() builtinFunc {
	newSig := &builtinRegexpUTF8Sig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}
