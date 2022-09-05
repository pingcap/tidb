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
	"sync"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &likeFunctionClass{}
)

var (
	_ builtinFunc = &builtinLikeSig{}
)

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinLikeSig{bf, nil, false, sync.Once{}}
	sig.setPbCode(tipb.ScalarFuncSig_LikeSig)
	return sig, nil
}

type builtinLikeSig struct {
	baseBuiltinFunc
	// pattern and isMemorizedPattern is not serialized with builtinLikeSig, treat them as a cache to accelerate
	// the evaluation of builtinLikeSig.
	pattern            collate.WildcardPattern
	isMemorizedPattern bool
	once               sync.Once
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
	memorization := func() {
		if b.pattern == nil {
			b.pattern = b.collator().Pattern()
			if b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx) && b.args[2].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
				b.pattern.Compile(patternStr, byte(escape))
				b.isMemorizedPattern = true
			}
		}
	}
	// Only be executed once to achieve thread-safe
	b.once.Do(memorization)
	if !b.isMemorizedPattern {
		// Must not use b.pattern to avoid data race
		pattern := b.collator().Pattern()
		pattern.Compile(patternStr, byte(escape))
		return boolToInt64(pattern.DoMatch(valStr)), false, nil
	}
	return boolToInt64(b.pattern.DoMatch(valStr)), false, nil
}
