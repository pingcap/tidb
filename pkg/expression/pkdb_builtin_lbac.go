// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/privilege/lbac"
	lbacmodel "github.com/pingcap/tidb/pkg/privilege/lbac/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type seclabelFunctionClass struct {
	baseFunctionClass
}

func (c *seclabelFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetCharset(charset.CharsetBin)
	bf.tp.SetCollate(charset.CollationBin)
	sig := &builtinSeclabelSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_Unspecified)
	return sig, nil
}

type builtinSeclabelSig struct {
	baseBuiltinFunc
	expropt.PrivilegeCheckerPropReader
}

func (b *builtinSeclabelSig) Clone() builtinFunc {
	newSig := &builtinSeclabelSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinSeclabelSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSeclabelSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	policyName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	label, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !variable.EnableLBAC.Load() {
		return label, false, nil
	}
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return "", true, err
	}
	cache := privChecker.GetSecurityLabelCache()
	if cache == nil {
		return "", true, lbac.ErrLBACCacheUnavailable
	}
	validator, err := lbac.NewSecurityLabelValidator(cache)
	if err != nil {
		return "", true, err
	}
	encoded, err := validator.EncodeLabelValue(policyName, label)
	if err != nil {
		return "", true, err
	}
	return string(encoded), false, nil
}

type seclabelToCharFunctionClass struct {
	baseFunctionClass
}

func (c *seclabelToCharFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSeclabelToCharSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_Unspecified)
	return sig, nil
}

type builtinSeclabelToCharSig struct {
	baseBuiltinFunc
	expropt.PrivilegeCheckerPropReader
}

func (b *builtinSeclabelToCharSig) Clone() builtinFunc {
	newSig := &builtinSeclabelToCharSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinSeclabelToCharSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSeclabelToCharSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	policyName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	label, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !variable.EnableLBAC.Load() {
		return label, false, nil
	}
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return "", true, err
	}
	cache := privChecker.GetSecurityLabelCache()
	if cache == nil {
		return "", true, lbac.ErrLBACCacheUnavailable
	}
	validator, err := lbac.NewSecurityLabelValidator(cache)
	if err != nil {
		return "", true, err
	}
	decoded, err := validator.DecodeLabelValue(policyName, []byte(label))
	if err != nil {
		return "", true, err
	}
	return decoded, false, nil
}

type lbacDominatesFunctionClass struct {
	baseFunctionClass
}

func (c *lbacDominatesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinLBACDominatesSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Unspecified)
	return sig, nil
}

type builtinLBACDominatesSig struct{ baseBuiltinFunc }

func (b *builtinLBACDominatesSig) Clone() builtinFunc {
	newSig := &builtinLBACDominatesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLBACDominatesSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	left, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	right, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	ok, err := lbacmodel.LabelBytesDominates([]byte(left), []byte(right))
	if err != nil {
		return 0, false, err
	}
	if ok {
		return 1, false, nil
	}
	return 0, false, nil
}
