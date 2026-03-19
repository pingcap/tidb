// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &ftsMatchWordFunctionClass{}
)

var (
	_ builtinFunc = &builtinFtsMatchWordSig{}
)

type ftsMatchWordFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMatchWordSig struct {
	baseBuiltinFunc
}

func (b *builtinFtsMatchWordSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchWordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *ftsMatchWordFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argAgainst := args[0]
	argAgainstConstant, ok := argAgainst.(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	if argAgainstConstant.Value.Kind() != types.KindString {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	argsMatch := args[1:]
	for _, arg := range argsMatch {
		_, ok := arg.(*Column)
		if !ok {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("not matching a column")
		}
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString, types.ETString)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	sig := &builtinFtsMatchWordSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchWord)
	return sig, nil
}

func (b *builtinFtsMatchWordSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Reject executing match against in TiDB side.
	return 0, false, errors.Errorf("cannot use 'FTS_MATCH_WORD()' outside of fulltext index")
}
