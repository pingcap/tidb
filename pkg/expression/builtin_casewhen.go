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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	thenArgs := make([]Expression, 0, (l+1)/2)
	for i := 1; i < l; i += 2 {
		thenArgs = append(thenArgs, args[i])
	}
	if l%2 == 1 {
		thenArgs = append(thenArgs, args[l-1])
	}
	fieldTp, err := InferType4ControlFuncs(ctx, c.funcName, thenArgs...)
	if err != nil {
		return nil, err
	}
	// Here we turn off NotNullFlag. Because if all when-clauses are false,
	// the result of case-when expr is NULL.
	tempFlag := fieldTp.GetFlag()
	types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
	fieldTp.SetFlag(tempFlag)
	tp := fieldTp.EvalType()

	argTps := make([]*types.FieldType, 0, l)
	for i := 0; i < l-1; i += 2 {
		if args[i], err = wrapWithIsTrue(ctx, true, args[i], false); err != nil {
			return nil, err
		}
		argTps = append(argTps, args[i].GetType(ctx.GetEvalCtx()), fieldTp.Clone())
	}
	if l%2 == 1 {
		argTps = append(argTps, fieldTp.Clone())
	}
	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, tp, argTps...)
	if err != nil {
		return nil, err
	}
	fieldTp.SetCharset(bf.tp.GetCharset())
	fieldTp.SetCollate(bf.tp.GetCollate())
	bf.tp = fieldTp

	switch tp {
	case types.ETInt:
		bf.tp.SetDecimal(0)
		sig = &builtinCaseWhenIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenInt)
	case types.ETReal:
		sig = &builtinCaseWhenRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenReal)
	case types.ETDecimal:
		sig = &builtinCaseWhenDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDecimal)
	case types.ETString:
		bf.tp.SetDecimal(types.UnspecifiedLength)
		sig = &builtinCaseWhenStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCaseWhenTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenTime)
	case types.ETDuration:
		sig = &builtinCaseWhenDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDuration)
	case types.ETJson:
		sig = &builtinCaseWhenJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenJson)
	case types.ETVectorFloat32:
		sig = &builtinCaseWhenVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_CaseWhenVectorFloat32)
	default:
		return nil, errors.Errorf("%s is not supported for CASE WHEN", tp)
	}
	return sig, nil
}

type builtinCaseWhenIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenIntSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenIntSig) evalInt(ctx EvalContext, row chunk.Row) (ret int64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalInt(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalInt(ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenRealSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenRealSig) evalReal(ctx EvalContext, row chunk.Row) (ret float64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalReal(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenDecimalSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return nil, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDecimal(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenStringSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenStringSig) evalString(ctx EvalContext, row chunk.Row) (ret string, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return "", isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalString(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenTimeSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenTimeSig) evalTime(ctx EvalContext, row chunk.Row) (ret types.Time, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return ret, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalTime(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenDurationSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (ret types.Duration, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return ret, true, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDuration(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenJSONSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinCaseWhenJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (ret types.BinaryJSON, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return
		}
		if isNull || condition == 0 {
			continue
		}
		return args[i+1].EvalJSON(ctx, row)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		return args[l-1].EvalJSON(ctx, row)
	}
	return ret, true, nil
}

type builtinCaseWhenVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCaseWhenVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinCaseWhenVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalVectorFloat32 evals a builtinCaseWhenVectorFloat32Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (ret types.VectorFloat32, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return
		}
		if isNull || condition == 0 {
			continue
		}
		return args[i+1].EvalVectorFloat32(ctx, row)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		return args[l-1].EvalVectorFloat32(ctx, row)
	}
	return ret, true, nil
}

