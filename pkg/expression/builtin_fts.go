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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &ftsMatchWordFunctionClass{}
	_ functionClass = &ftsMatchPrefixFunctionClass{}
	_ functionClass = &ftsMysqlMatchAgainstFunctionClass{}
)

var (
	_ builtinFunc = &builtinFtsMatchWordSig{}
	_ builtinFunc = &builtinFtsMatchPrefixSig{}
	_ builtinFunc = &builtinFtsMysqlMatchAgainstSig{}
)

type ftsMatchWordFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMatchWordSig struct {
	baseBuiltinFunc
}

type ftsMysqlMatchAgainstFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMysqlMatchAgainstSig struct {
	baseBuiltinFunc
	modifier ast.FulltextSearchModifier
}

func (b *builtinFtsMatchWordSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchWordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFtsMysqlMatchAgainstSig) Clone() builtinFunc {
	newSig := &builtinFtsMysqlMatchAgainstSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.modifier = b.modifier
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

func (b *builtinFtsMysqlMatchAgainstSig) SetModifier(modifier ast.FulltextSearchModifier) {
	b.modifier = modifier
}

// SetFTSMysqlMatchAgainstModifier sets the modifier for the internal `MATCH ... AGAINST` builtin signature.
// It is expected to be called by planner right after building the scalar function.
func SetFTSMysqlMatchAgainstModifier(sf *ScalarFunction, modifier ast.FulltextSearchModifier) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.SetModifier(modifier)
	return nil
}

func (c *ftsMysqlMatchAgainstFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	argTps = append(argTps, types.ETString)
	for range argsMatch {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	sig := &builtinFtsMysqlMatchAgainstSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchExpression)
	return sig, nil
}

func (b *builtinFtsMysqlMatchAgainstSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Reject executing match against in TiDB side.
	return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' outside of fulltext index")
}

type ftsMatchPrefixFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMatchPrefixSig struct {
	baseBuiltinFunc
}

func (b *builtinFtsMatchPrefixSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchPrefixSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *ftsMatchPrefixFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	sig := &builtinFtsMatchPrefixSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchPrefix)
	return sig, nil
}

func (b *builtinFtsMatchPrefixSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Reject executing match against in TiDB side.
	return 0, false, errors.Errorf("cannot use 'FTS_MATCH_PREFIX()' outside of fulltext index")
}
