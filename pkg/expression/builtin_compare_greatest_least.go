// Copyright 2017 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)


// ResolveType4Between resolves eval type for between expression.
func ResolveType4Between(ctx EvalContext, args [3]Expression) types.EvalType {
	cmpTp := args[0].GetType(ctx).EvalType()
	for i := 1; i < 3; i++ {
		cmpTp = getBaseCmpType(cmpTp, args[i].GetType(ctx).EvalType(), nil, nil)
	}

	hasTemporal := false
	if cmpTp == types.ETString {
		if args[0].GetType(ctx).GetType() == mysql.TypeDuration {
			cmpTp = types.ETDuration
		} else {
			for _, arg := range args {
				if types.IsTypeTemporal(arg.GetType(ctx).GetType()) {
					hasTemporal = true
					break
				}
			}
			if hasTemporal {
				cmpTp = types.ETDatetime
			}
		}
	}
	if (args[0].GetType(ctx).EvalType() == types.ETInt || IsBinaryLiteral(args[0])) &&
		(args[1].GetType(ctx).EvalType() == types.ETInt || IsBinaryLiteral(args[1])) &&
		(args[2].GetType(ctx).EvalType() == types.ETInt || IsBinaryLiteral(args[2])) {
		return types.ETInt
	}

	return cmpTp
}

// GLCmpStringMode represents Greatest/Least integral string comparison mode
type GLCmpStringMode uint8

const (
	// GLCmpStringDirectly Greatest and Least function compares string directly
	GLCmpStringDirectly GLCmpStringMode = iota
	// GLCmpStringAsDate Greatest/Least function compares string as 'yyyy-mm-dd' format
	GLCmpStringAsDate
	// GLCmpStringAsDatetime Greatest/Least function compares string as 'yyyy-mm-dd hh:mm:ss' format
	GLCmpStringAsDatetime
)

// GLRetTimeType represents Greatest/Least return time type
type GLRetTimeType uint8

const (
	// GLRetNoneTemporal Greatest/Least function returns non temporal time
	GLRetNoneTemporal GLRetTimeType = iota
	// GLRetDate Greatest/Least function returns date type, 'yyyy-mm-dd'
	GLRetDate
	// GLRetDatetime Greatest/Least function returns datetime type, 'yyyy-mm-dd hh:mm:ss'
	GLRetDatetime
)

// resolveType4Extremum gets compare type for GREATEST and LEAST and BETWEEN (mainly for datetime).
func resolveType4Extremum(ctx EvalContext, args []Expression) (_ *types.FieldType, fieldTimeType GLRetTimeType, cmpStringMode GLCmpStringMode) {
	aggType := aggregateType(ctx, args)
	var temporalItem *types.FieldType
	if aggType.EvalType().IsStringKind() {
		for i := range args {
			item := args[i].GetType(ctx)
			// Find the temporal value in the arguments but prefer DateTime value.
			if types.IsTypeTemporal(item.GetType()) {
				if temporalItem == nil || item.GetType() == mysql.TypeDatetime {
					temporalItem = item
				}
			}
		}

		if !types.IsTypeTemporal(aggType.GetType()) && temporalItem != nil && types.IsTemporalWithDate(temporalItem.GetType()) {
			if temporalItem.GetType() == mysql.TypeDate {
				cmpStringMode = GLCmpStringAsDate
			} else {
				cmpStringMode = GLCmpStringAsDatetime
			}
		}
		// TODO: String charset, collation checking are needed.
	}
	var timeType = GLRetNoneTemporal
	if aggType.GetType() == mysql.TypeDate {
		timeType = GLRetDate
	} else if aggType.GetType() == mysql.TypeDatetime || aggType.GetType() == mysql.TypeTimestamp {
		timeType = GLRetDatetime
	}
	return aggType, timeType, cmpStringMode
}

// unsupportedJSONComparison reports warnings while there is a JSON type in least/greatest function's arguments
func unsupportedJSONComparison(ctx BuildContext, args []Expression) {
	for _, arg := range args {
		tp := arg.GetType(ctx.GetEvalCtx()).GetType()
		if tp == mysql.TypeJSON {
			ctx.GetEvalCtx().AppendWarning(errUnsupportedJSONComparison)
			break
		}
	}
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(ctx.GetEvalCtx(), args)
	resTp := resFieldType.EvalType()
	argTp := resTp
	if cmpStringMode != GLCmpStringDirectly {
		// Args are temporal and string mixed, we cast all args as string and parse it to temporal manually to compare.
		argTp = types.ETString
	} else if resTp == types.ETJson {
		unsupportedJSONComparison(ctx, args)
		argTp = types.ETString
		resTp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = argTp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, resTp, argTps...)
	if err != nil {
		return nil, err
	}
	switch argTp {
	case types.ETInt:
		bf.tp.AddFlag(resFieldType.GetFlag())
		sig = &builtinGreatestIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestInt)
	case types.ETReal:
		sig = &builtinGreatestRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestReal)
	case types.ETDecimal:
		sig = &builtinGreatestDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestDecimal)
	case types.ETString:
		if cmpStringMode == GLCmpStringAsDate {
			sig = &builtinGreatestCmpStringAsTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestCmpStringAsDate)
		} else if cmpStringMode == GLCmpStringAsDatetime {
			sig = &builtinGreatestCmpStringAsTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestCmpStringAsTime)
		} else {
			sig = &builtinGreatestStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestString)
		}
	case types.ETDuration:
		sig = &builtinGreatestDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_GreatestDuration)
	case types.ETDatetime, types.ETTimestamp:
		if fieldTimeType == GLRetDate {
			sig = &builtinGreatestTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestDate)
		} else {
			sig = &builtinGreatestTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_GreatestTime)
		}
	case types.ETVectorFloat32:
		sig = &builtinGreatestVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_GreatestVectorFloat32)
	default:
		return nil, errors.Errorf("unsupported type %s during evaluation", argTp)
	}

	flen, decimal := fixFlenAndDecimalForGreatestAndLeast(ctx.GetEvalCtx(), args)
	sig.getRetTp().SetFlenUnderLimit(flen)
	sig.getRetTp().SetDecimalUnderLimit(decimal)

	return sig, nil
}

func fixFlenAndDecimalForGreatestAndLeast(ctx EvalContext, args []Expression) (flen, decimal int) {
	for _, arg := range args {
		argFlen, argDecimal := arg.GetType(ctx).GetFlen(), arg.GetType(ctx).GetDecimal()
		if argFlen > flen {
			flen = argFlen
		}
		if argDecimal > decimal {
			decimal = argDecimal
		}
	}
	return flen, decimal
}

type builtinGreatestIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestIntSig) Clone() builtinFunc {
	newSig := &builtinGreatestIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(ctx EvalContext, row chunk.Row) (maxv int64, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if v > maxv {
			maxv = v
		}
	}
	return
}

type builtinGreatestRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestRealSig) Clone() builtinFunc {
	newSig := &builtinGreatestRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(ctx EvalContext, row chunk.Row) (maxv float64, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if v > maxv {
			maxv = v
		}
	}
	return
}

type builtinGreatestDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestDecimalSig) Clone() builtinFunc {
	newSig := &builtinGreatestDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (maxv *types.MyDecimal, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if v.Compare(maxv) > 0 {
			maxv = v
		}
	}
	return
}

type builtinGreatestStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestStringSig) Clone() builtinFunc {
	newSig := &builtinGreatestStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) evalString(ctx EvalContext, row chunk.Row) (maxv string, isNull bool, err error) {
	maxv, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return maxv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return maxv, isNull, err
		}
		if types.CompareString(v, maxv, b.collation) > 0 {
			maxv = v
		}
	}
	return
}

type builtinGreatestCmpStringAsTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinGreatestCmpStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinGreatestCmpStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

// evalString evals a builtinGreatestCmpStringAsTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestCmpStringAsTimeSig) evalString(ctx EvalContext, row chunk.Row) (strRes string, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		v, err = doTimeConversionForGL(b.cmpAsDate, ctx, v)
		if err != nil {
			return v, true, err
		}
		// In MySQL, if the compare result is zero, than we will try to use the string comparison result
		if i == 0 || strings.Compare(v, strRes) > 0 {
			strRes = v
		}
	}
	return strRes, false, nil
}

func doTimeConversionForGL(cmpAsDate bool, ctx EvalContext, strVal string) (string, error) {
	var t types.Time
	var err error
	tc := typeCtx(ctx)
	if cmpAsDate {
		t, err = types.ParseDate(tc, strVal)
		if err == nil {
			t, err = t.Convert(tc, mysql.TypeDate)
		}
	} else {
		t, err = types.ParseDatetime(tc, strVal)
		if err == nil {
			t, err = t.Convert(tc, mysql.TypeDatetime)
		}
	}
	if err != nil {
		if err = handleInvalidTimeError(ctx, err); err != nil {
			return "", err
		}
	} else {
		strVal = t.String()
	}
	return strVal, nil
}

type builtinGreatestTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinGreatestTimeSig) Clone() builtinFunc {
	newSig := &builtinGreatestTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinGreatestTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalTime(ctx, row)
		if isNull || err != nil {
			return types.ZeroTime, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	// Convert ETType Time value to MySQL actual type, distinguish date and datetime
	tc := typeCtx(ctx)
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	if res, err = res.Convert(tc, resTimeTp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	return res, false, nil
}

type builtinGreatestDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestDurationSig) Clone() builtinFunc {
	newSig := &builtinGreatestDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGreatestDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalDuration(ctx, row)
		if isNull || err != nil {
			return types.Duration{}, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	return res, false, nil
}

type builtinGreatestVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGreatestVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinGreatestVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGreatestVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalVectorFloat32(ctx, row)
		if isNull || err != nil {
			return types.VectorFloat32{}, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	return res, false, nil
}

type leastFunctionClass struct {
	baseFunctionClass
}

func (c *leastFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(ctx.GetEvalCtx(), args)
	resTp := resFieldType.EvalType()
	argTp := resTp
	if cmpStringMode != GLCmpStringDirectly {
		// Args are temporal and string mixed, we cast all args as string and parse it to temporal manually to compare.
		argTp = types.ETString
	} else if resTp == types.ETJson {
		unsupportedJSONComparison(ctx, args)
		argTp = types.ETString
		resTp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = argTp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, resTp, argTps...)
	if err != nil {
		return nil, err
	}
	switch argTp {
	case types.ETInt:
		bf.tp.AddFlag(resFieldType.GetFlag())
		sig = &builtinLeastIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastInt)
	case types.ETReal:
		sig = &builtinLeastRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastReal)
	case types.ETDecimal:
		sig = &builtinLeastDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastDecimal)
	case types.ETString:
		if cmpStringMode == GLCmpStringAsDate {
			sig = &builtinLeastCmpStringAsTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_LeastCmpStringAsDate)
		} else if cmpStringMode == GLCmpStringAsDatetime {
			sig = &builtinLeastCmpStringAsTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_LeastCmpStringAsTime)
		} else {
			sig = &builtinLeastStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LeastString)
		}
	case types.ETDuration:
		sig = &builtinLeastDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LeastDuration)
	case types.ETDatetime, types.ETTimestamp:
		if fieldTimeType == GLRetDate {
			sig = &builtinLeastTimeSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_LeastDate)
		} else {
			sig = &builtinLeastTimeSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_LeastTime)
		}
	case types.ETVectorFloat32:
		sig = &builtinLeastVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_LeastVectorFloat32)
	default:
		return nil, errors.Errorf("unsupported type %s during evaluation", argTp)
	}
	flen, decimal := fixFlenAndDecimalForGreatestAndLeast(ctx.GetEvalCtx(), args)
	sig.getRetTp().SetFlenUnderLimit(flen)
	sig.getRetTp().SetDecimalUnderLimit(decimal)
	return sig, nil
}

type builtinLeastIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastIntSig) Clone() builtinFunc {
	newSig := &builtinLeastIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_least
func (b *builtinLeastIntSig) evalInt(ctx EvalContext, row chunk.Row) (minv int64, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if v < minv {
			minv = v
		}
	}
	return
}

type builtinLeastRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastRealSig) Clone() builtinFunc {
	newSig := &builtinLeastRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(ctx EvalContext, row chunk.Row) (minv float64, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if v < minv {
			minv = v
		}
	}
	return
}

type builtinLeastDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastDecimalSig) Clone() builtinFunc {
	newSig := &builtinLeastDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (minv *types.MyDecimal, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if v.Compare(minv) < 0 {
			minv = v
		}
	}
	return
}

type builtinLeastStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastStringSig) Clone() builtinFunc {
	newSig := &builtinLeastStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) evalString(ctx EvalContext, row chunk.Row) (minv string, isNull bool, err error) {
	minv, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return minv, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return minv, isNull, err
		}
		if types.CompareString(v, minv, b.collation) < 0 {
			minv = v
		}
	}
	return
}

type builtinLeastCmpStringAsTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinLeastCmpStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinLeastCmpStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

// evalString evals a builtinLeastCmpStringAsTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastCmpStringAsTimeSig) evalString(ctx EvalContext, row chunk.Row) (strRes string, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		v, err = doTimeConversionForGL(b.cmpAsDate, ctx, v)
		if err != nil {
			return v, true, err
		}
		if i == 0 || strings.Compare(v, strRes) < 0 {
			strRes = v
		}
	}

	return strRes, false, nil
}

type builtinLeastTimeSig struct {
	baseBuiltinFunc
	cmpAsDate bool
}

func (b *builtinLeastTimeSig) Clone() builtinFunc {
	newSig := &builtinLeastTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinLeastTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalTime(ctx, row)
		if isNull || err != nil {
			return types.ZeroTime, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	// Convert ETType Time value to MySQL actual type, distinguish date and datetime
	tc := typeCtx(ctx)
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	if res, err = res.Convert(tc, resTimeTp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	return res, false, nil
}

func getAccurateTimeTypeForGLRet(cmpAsDate bool) byte {
	var resTimeTp byte
	if cmpAsDate {
		resTimeTp = mysql.TypeDate
	} else {
		resTimeTp = mysql.TypeDatetime
	}
	return resTimeTp
}

type builtinLeastDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastDurationSig) Clone() builtinFunc {
	newSig := &builtinLeastDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeastDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalDuration(ctx, row)
		if isNull || err != nil {
			return types.Duration{}, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	return res, false, nil
}

type builtinLeastVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeastVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinLeastVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeastVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	for i := range b.args {
		v, isNull, err := b.args[i].EvalVectorFloat32(ctx, row)
		if isNull || err != nil {
			return types.VectorFloat32{}, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	return res, false, nil
}

type intervalFunctionClass struct {
	baseFunctionClass
}

func (c *intervalFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	allInt := true
	hasNullable := false
	// if we have nullable columns in the argument list, we won't do a binary search, instead we will linearly scan the arguments.
	// this behavior is in line with MySQL's, see MySQL's source code here:
	// https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/sql/item_cmpfunc.cc#L2713-L2788
	// https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/sql/item_cmpfunc.cc#L2632-L2686
	for i := range args {
		tp := args[i].GetType(ctx.GetEvalCtx())
		if tp.EvalType() != types.ETInt {
			allInt = false
		}
		if !mysql.HasNotNullFlag(tp.GetFlag()) {
			hasNullable = true
		}
	}

	argTps, argTp := make([]types.EvalType, 0, len(args)), types.ETReal
	if allInt {
		argTp = types.ETInt
	}
	for range args {
		argTps = append(argTps, argTp)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	if allInt {
		sig = &builtinIntervalIntSig{bf, hasNullable}
		sig.setPbCode(tipb.ScalarFuncSig_IntervalInt)
	} else {
		sig = &builtinIntervalRealSig{bf, hasNullable}
		sig.setPbCode(tipb.ScalarFuncSig_IntervalReal)
	}
	return sig, nil
}

type builtinIntervalIntSig struct {
	baseBuiltinFunc
	hasNullable bool
}

func (b *builtinIntervalIntSig) Clone() builtinFunc {
	newSig := &builtinIntervalIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntervalIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}
	isUint1 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	var idx int
	if b.hasNullable {
		idx, err = b.linearSearch(ctx, arg0, isUint1, b.args[1:], row)
	} else {
		idx, err = b.binSearch(ctx, arg0, isUint1, b.args[1:], row)
	}
	return int64(idx), err != nil, err
}

// linearSearch linearly scans the argument least to find the position of the first value that is larger than the given target.
func (b *builtinIntervalIntSig) linearSearch(ctx EvalContext, target int64, isUint1 bool, args []Expression, row chunk.Row) (i int, err error) {
	i = 0
	for ; i < len(args); i++ {
		isUint2 := mysql.HasUnsignedFlag(args[i].GetType(ctx).GetFlag())
		arg, isNull, err := args[i].EvalInt(ctx, row)
		if err != nil {
			return 0, err
		}
		var less bool
		if !isNull {
			switch {
			case !isUint1 && !isUint2:
				less = target < arg
			case isUint1 && isUint2:
				less = uint64(target) < uint64(arg)
			case !isUint1 && isUint2:
				less = target < 0 || uint64(target) < uint64(arg)
			case isUint1 && !isUint2:
				less = arg > 0 && uint64(target) < uint64(arg)
			}
		}
		if less {
			break
		}
	}
	return i, nil
}

// binSearch is a binary search method.
// All arguments are treated as integers.
// It is required that arg[0] < args[1] < args[2] < ... < args[n] for this function to work correctly.
// This is because a binary search is used (very fast).
func (b *builtinIntervalIntSig) binSearch(ctx EvalContext, target int64, isUint1 bool, args []Expression, row chunk.Row) (_ int, err error) {
	i, j, cmp := 0, len(args), false
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalInt(ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			v = target
		}
		isUint2 := mysql.HasUnsignedFlag(args[mid].GetType(ctx).GetFlag())
		switch {
		case !isUint1 && !isUint2:
			cmp = target < v
		case isUint1 && isUint2:
			cmp = uint64(target) < uint64(v)
		case !isUint1 && isUint2:
			cmp = target < 0 || uint64(target) < uint64(v)
		case isUint1 && !isUint2:
			cmp = v > 0 && uint64(target) < uint64(v)
		}
		if !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, err
}

type builtinIntervalRealSig struct {
	baseBuiltinFunc
	hasNullable bool
}

func (b *builtinIntervalRealSig) Clone() builtinFunc {
	newSig := &builtinIntervalRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.hasNullable = b.hasNullable
	return newSig
}

// evalInt evals a builtinIntervalRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalRealSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}

	var idx int
	if b.hasNullable {
		idx, err = b.linearSearch(ctx, arg0, b.args[1:], row)
	} else {
		idx, err = b.binSearch(ctx, arg0, b.args[1:], row)
	}
	return int64(idx), err != nil, err
}

func (b *builtinIntervalRealSig) linearSearch(ctx EvalContext, target float64, args []Expression, row chunk.Row) (i int, err error) {
	i = 0
	for ; i < len(args); i++ {
		arg, isNull, err := args[i].EvalReal(ctx, row)
		if err != nil {
			return 0, err
		}
		if !isNull && target < arg {
			break
		}
	}
	return i, nil
}

func (b *builtinIntervalRealSig) binSearch(ctx EvalContext, target float64, args []Expression, row chunk.Row) (_ int, err error) {
	i, j := 0, len(args)
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalReal(ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			i = mid + 1
		} else if cmp := target < v; !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, err
}
