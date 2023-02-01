// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"sync"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &ilikeFunctionClass{}
)

var (
	_ builtinFunc = &builtinIlikeSig{}
)

type ilikeFunctionClass struct {
	baseFunctionClass
}

func (c *ilikeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinIlikeSig{bf, nil, false, sync.Once{}}
	sig.setPbCode(tipb.ScalarFuncSig_LikeSig) // TODO change it
	return sig, nil
}

type builtinIlikeSig struct {
	baseBuiltinFunc
	// pattern and isMemorizedPattern is not serialized with builtinIlikeSig, treat them as a cache to accelerate
	// the evaluation of builtinIlikeSig.
	pattern            collate.WildcardPattern
	isMemorizedPattern bool
	once               sync.Once
}

func (b *builtinIlikeSig) Clone() builtinFunc {
	newSig := &builtinIlikeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.pattern = b.pattern
	newSig.isMemorizedPattern = b.isMemorizedPattern
	return newSig
}

func (b *builtinIlikeSig) getCICollator() collate.Collator {
	return collate.ConvertAndGetCICollation(b.collation)
}

// evalInt evals a builtinIlikeSig.
func (b *builtinIlikeSig) evalInt(row chunk.Row) (int64, bool, error) {
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
			// b.pattern = b.collator().Pattern()
			b.pattern = b.getCICollator().Pattern()
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
		// pattern := b.collator().Pattern()
		pattern := b.getCICollator().Pattern()
		pattern.Compile(patternStr, byte(escape))
		fmt.Println(pattern.DoMatch(valStr))
		fmt.Println(boolToInt64(pattern.DoMatch(valStr)))
		return boolToInt64(pattern.DoMatch(valStr)), false, nil
	}
	fmt.Println(b.pattern.DoMatch(valStr))
	fmt.Println(boolToInt64(b.pattern.DoMatch(valStr)))
	return boolToInt64(b.pattern.DoMatch(valStr)), false, nil
}
