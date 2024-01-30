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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
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

func (c *likeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinLikeSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_LikeSig)
	return sig, nil
}

type builtinLikeSig struct {
	baseBuiltinFunc
	// pattern is not serialized with builtinLikeSig, treat them as a cache to accelerate
	// the evaluation of builtinLikeSig.
	patternCache builtinFuncCache[collate.WildcardPattern]
}

func (b *builtinLikeSig) Clone() builtinFunc {
	newSig := &builtinLikeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLikeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
func (b *builtinLikeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	patternStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	escape, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	var pattern collate.WildcardPattern
	if b.args[1].ConstLevel() >= ConstOnlyInContext && b.args[2].ConstLevel() >= ConstOnlyInContext {
		pattern, err = b.patternCache.getOrInitCache(ctx, func() (collate.WildcardPattern, error) {
			ret := b.collator().Pattern()
			ret.Compile(patternStr, byte(escape))
			return ret, nil
		})
		intest.AssertNoError(err)
		if err != nil {
			return 0, true, err
		}
	} else {
		pattern = b.collator().Pattern()
		pattern.Compile(patternStr, byte(escape))
	}
	return boolToInt64(pattern.DoMatch(valStr)), false, nil
}
