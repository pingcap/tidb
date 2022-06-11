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
	"math"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &coalesceFunctionClass{}
	_ functionClass = &greatestFunctionClass{}
	_ functionClass = &leastFunctionClass{}
	_ functionClass = &intervalFunctionClass{}
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinCoalesceIntSig{}
	_ builtinFunc = &builtinCoalesceRealSig{}
	_ builtinFunc = &builtinCoalesceDecimalSig{}
	_ builtinFunc = &builtinCoalesceStringSig{}
	_ builtinFunc = &builtinCoalesceTimeSig{}
	_ builtinFunc = &builtinCoalesceDurationSig{}

	_ builtinFunc = &builtinGreatestIntSig{}
	_ builtinFunc = &builtinGreatestRealSig{}
	_ builtinFunc = &builtinGreatestDecimalSig{}
	_ builtinFunc = &builtinGreatestStringSig{}
	_ builtinFunc = &builtinGreatestDurationSig{}
	_ builtinFunc = &builtinGreatestTimeSig{}
	_ builtinFunc = &builtinGreatestCmpStringAsTimeSig{}
	_ builtinFunc = &builtinLeastIntSig{}
	_ builtinFunc = &builtinLeastRealSig{}
	_ builtinFunc = &builtinLeastDecimalSig{}
	_ builtinFunc = &builtinLeastStringSig{}
	_ builtinFunc = &builtinLeastTimeSig{}
	_ builtinFunc = &builtinLeastDurationSig{}
	_ builtinFunc = &builtinLeastCmpStringAsTimeSig{}
	_ builtinFunc = &builtinIntervalIntSig{}
	_ builtinFunc = &builtinIntervalRealSig{}

	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTRealSig{}
	_ builtinFunc = &builtinLTDecimalSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLTDurationSig{}
	_ builtinFunc = &builtinLTTimeSig{}

	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLERealSig{}
	_ builtinFunc = &builtinLEDecimalSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinLEDurationSig{}
	_ builtinFunc = &builtinLETimeSig{}

	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTRealSig{}
	_ builtinFunc = &builtinGTDecimalSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGTTimeSig{}
	_ builtinFunc = &builtinGTDurationSig{}

	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGERealSig{}
	_ builtinFunc = &builtinGEDecimalSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinGETimeSig{}
	_ builtinFunc = &builtinGEDurationSig{}

	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNERealSig{}
	_ builtinFunc = &builtinNEDecimalSig{}
	_ builtinFunc = &builtinNEStringSig{}
	_ builtinFunc = &builtinNETimeSig{}
	_ builtinFunc = &builtinNEDurationSig{}

	_ builtinFunc = &builtinNullEQIntSig{}
	_ builtinFunc = &builtinNullEQRealSig{}
	_ builtinFunc = &builtinNullEQDecimalSig{}
	_ builtinFunc = &builtinNullEQStringSig{}
	_ builtinFunc = &builtinNullEQTimeSig{}
	_ builtinFunc = &builtinNullEQDurationSig{}
)

// coalesceFunctionClass returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	fieldTps := make([]*types.FieldType, 0, len(args))
	for _, arg := range args {
		fieldTps = append(fieldTps, arg.GetType())
	}

	// Use the aggregated field type as retType.
	resultFieldType := types.AggFieldType(fieldTps)
	var tempType uint
	resultEvalType := types.AggregateEvalType(fieldTps, &tempType)
	resultFieldType.SetFlag(tempType)
	retEvalTp := resultFieldType.EvalType()

	fieldEvalTps := make([]types.EvalType, 0, len(args))
	for range args {
		fieldEvalTps = append(fieldEvalTps, retEvalTp)
	}

	fsp, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retEvalTp, fieldEvalTps...)
	if err != nil {
		return nil, err
	}

	bf.tp.AddFlag(resultFieldType.GetFlag())
	resultFieldType.SetFlen(0)
	resultFieldType.SetDecimal(types.UnspecifiedLength)

	// Set retType to BINARY(0) if all arguments are of type NULL.
	if resultFieldType.GetType() == mysql.TypeNull {
		types.SetBinChsClnFlag(bf.tp)
	} else {
		maxIntLen := 0
		maxFlen := 0

		// Find the max length of field in `maxFlen`,
		// and max integer-part length in `maxIntLen`.
		for _, argTp := range fieldTps {
			if argTp.GetDecimal() > resultFieldType.GetDecimal() {
				resultFieldType.SetDecimal(argTp.GetDecimal())
			}
			argIntLen := argTp.GetFlen()
			if argTp.GetDecimal() > 0 {
				argIntLen -= argTp.GetDecimal() + 1
			}

			// Reduce the sign bit if it is a signed integer/decimal
			if !mysql.HasUnsignedFlag(argTp.GetFlag()) {
				argIntLen--
			}
			if argIntLen > maxIntLen {
				maxIntLen = argIntLen
			}
			if argTp.GetFlen() > maxFlen || argTp.GetFlen() == types.UnspecifiedLength {
				maxFlen = argTp.GetFlen()
			}
		}
		// For integer, field length = maxIntLen + (1/0 for sign bit)
		// For decimal, field length = maxIntLen + maxDecimal + (1/0 for sign bit)
		if resultEvalType == types.ETInt || resultEvalType == types.ETDecimal {
			resultFieldType.SetFlen(maxIntLen + resultFieldType.GetDecimal())
			if resultFieldType.GetDecimal() > 0 {
				resultFieldType.SetFlen(resultFieldType.GetFlen() + 1)
			}
			if !mysql.HasUnsignedFlag(resultFieldType.GetFlag()) {
				resultFieldType.SetFlen(resultFieldType.GetFlen() + 1)
			}
			bf.tp = resultFieldType
		} else {
			bf.tp.SetFlen(maxFlen)
		}
		// Set the field length to maxFlen for other types.
		if bf.tp.GetFlen() > mysql.MaxDecimalWidth {
			bf.tp.SetFlen(mysql.MaxDecimalWidth)
		}
	}

	switch retEvalTp {
	case types.ETInt:
		sig = &builtinCoalesceIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceInt)
	case types.ETReal:
		sig = &builtinCoalesceRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceReal)
	case types.ETDecimal:
		sig = &builtinCoalesceDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDecimal)
	case types.ETString:
		sig = &builtinCoalesceStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCoalesceTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceTime)
	case types.ETDuration:
		bf.tp.SetDecimal(fsp)
		sig = &builtinCoalesceDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDuration)
	case types.ETJson:
		sig = &builtinCoalesceJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceJson)
	}

	return sig, nil
}

// builtinCoalesceIntSig is builtin function coalesce signature which return type int
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceIntSig) Clone() builtinFunc {
	newSig := &builtinCoalesceIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalInt(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceRealSig is builtin function coalesce signature which return type real
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceRealSig) Clone() builtinFunc {
	newSig := &builtinCoalesceRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalReal(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDecimalSig is builtin function coalesce signature which return type decimal
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDecimalSig) Clone() builtinFunc {
	newSig := &builtinCoalesceDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDecimal(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceStringSig is builtin function coalesce signature which return type string
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceStringSig) Clone() builtinFunc {
	newSig := &builtinCoalesceStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalString(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceTimeSig is builtin function coalesce signature which return type time
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceTimeSig) Clone() builtinFunc {
	newSig := &builtinCoalesceTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalTime(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDurationSig is builtin function coalesce signature which return type duration
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDurationSig) Clone() builtinFunc {
	newSig := &builtinCoalesceDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDuration(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceJSONSig is builtin function coalesce signature which return type json.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceJSONSig) Clone() builtinFunc {
	newSig := &builtinCoalesceJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalJSON(b.ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

func aggregateType(args []Expression) *types.FieldType {
	fieldTypes := make([]*types.FieldType, len(args))
	for i := range fieldTypes {
		fieldTypes[i] = args[i].GetType()
	}
	return types.AggFieldType(fieldTypes)
}

// ResolveType4Between resolves eval type for between expression.
func ResolveType4Between(args [3]Expression) types.EvalType {
	cmpTp := args[0].GetType().EvalType()
	for i := 1; i < 3; i++ {
		cmpTp = getBaseCmpType(cmpTp, args[i].GetType().EvalType(), nil, nil)
	}

	hasTemporal := false
	if cmpTp == types.ETString {
		if args[0].GetType().GetType() == mysql.TypeDuration {
			cmpTp = types.ETDuration
		} else {
			for _, arg := range args {
				if types.IsTypeTemporal(arg.GetType().GetType()) {
					hasTemporal = true
					break
				}
			}
			if hasTemporal {
				cmpTp = types.ETDatetime
			}
		}
	}
	if (args[0].GetType().EvalType() == types.ETInt || IsBinaryLiteral(args[0])) &&
		(args[1].GetType().EvalType() == types.ETInt || IsBinaryLiteral(args[1])) &&
		(args[2].GetType().EvalType() == types.ETInt || IsBinaryLiteral(args[2])) {
		return types.ETInt
	}

	return cmpTp
}

// GLCmpStringMode represents Greatest/Least interal string comparison mode
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
func resolveType4Extremum(args []Expression) (_ *types.FieldType, fieldTimeType GLRetTimeType, cmpStringMode GLCmpStringMode) {
	aggType := aggregateType(args)
	var temporalItem *types.FieldType
	if aggType.EvalType().IsStringKind() {
		for i := range args {
			item := args[i].GetType()
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
func unsupportedJSONComparison(ctx sessionctx.Context, args []Expression) {
	for _, arg := range args {
		tp := arg.GetType().GetType()
		if tp == mysql.TypeJSON {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedJSONComparison)
			break
		}
	}
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(args)
	resTp := resFieldType.EvalType()
	argTp := resTp
	if cmpStringMode != GLCmpStringDirectly {
		// Args are temporal and string mixed, we cast all args as string and parse it to temporal mannualy to compare.
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
	}

	flen, decimal := fixFlenAndDecimalForGreatestAndLeast(args)
	sig.getRetTp().SetFlen(flen)
	sig.getRetTp().SetDecimal(decimal)

	return sig, nil
}

func fixFlenAndDecimalForGreatestAndLeast(args []Expression) (flen, decimal int) {
	for _, arg := range args {
		argFlen, argDecimal := arg.GetType().GetFlen(), arg.GetType().GetDecimal()
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
}

func (b *builtinGreatestIntSig) Clone() builtinFunc {
	newSig := &builtinGreatestIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(row chunk.Row) (max int64, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestRealSig) Clone() builtinFunc {
	newSig := &builtinGreatestRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(row chunk.Row) (max float64, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestDecimalSig) Clone() builtinFunc {
	newSig := &builtinGreatestDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(row chunk.Row) (max *types.MyDecimal, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v.Compare(max) > 0 {
			max = v
		}
	}
	return
}

type builtinGreatestStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestStringSig) Clone() builtinFunc {
	newSig := &builtinGreatestStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) evalString(row chunk.Row) (max string, isNull bool, err error) {
	max, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if types.CompareString(v, max, b.collation) > 0 {
			max = v
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
func (b *builtinGreatestCmpStringAsTimeSig) evalString(row chunk.Row) (strRes string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args); i++ {
		v, isNull, err := b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		v, err = doTimeConversionForGL(b.cmpAsDate, b.ctx, sc, v)
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

func doTimeConversionForGL(cmpAsDate bool, ctx sessionctx.Context, sc *stmtctx.StatementContext, strVal string) (string, error) {
	var t types.Time
	var err error
	if cmpAsDate {
		t, err = types.ParseDate(sc, strVal)
		if err == nil {
			t, err = t.Convert(sc, mysql.TypeDate)
		}
	} else {
		t, err = types.ParseDatetime(sc, strVal)
		if err == nil {
			t, err = t.Convert(sc, mysql.TypeDatetime)
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

func (b *builtinGreatestTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	for i := 0; i < len(b.args); i++ {
		v, isNull, err := b.args[i].EvalTime(b.ctx, row)
		if isNull || err != nil {
			return types.ZeroTime, true, err
		}
		if i == 0 || v.Compare(res) > 0 {
			res = v
		}
	}
	// Convert ETType Time value to MySQL actual type, distinguish date and datetime
	sc := b.ctx.GetSessionVars().StmtCtx
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	if res, err = res.Convert(sc, resTimeTp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	return res, false, nil
}

type builtinGreatestDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestDurationSig) Clone() builtinFunc {
	newSig := &builtinGreatestDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGreatestDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	for i := 0; i < len(b.args); i++ {
		v, isNull, err := b.args[i].EvalDuration(b.ctx, row)
		if isNull || err != nil {
			return types.Duration{}, true, err
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

func (c *leastFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(args)
	resTp := resFieldType.EvalType()
	argTp := resTp
	if cmpStringMode != GLCmpStringDirectly {
		// Args are temporal and string mixed, we cast all args as string and parse it to temporal mannualy to compare.
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
	}
	flen, decimal := fixFlenAndDecimalForGreatestAndLeast(args)
	sig.getRetTp().SetFlen(flen)
	sig.getRetTp().SetDecimal(decimal)
	return sig, nil
}

type builtinLeastIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastIntSig) Clone() builtinFunc {
	newSig := &builtinLeastIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_least
func (b *builtinLeastIntSig) evalInt(row chunk.Row) (min int64, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastRealSig) Clone() builtinFunc {
	newSig := &builtinLeastRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(row chunk.Row) (min float64, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastDecimalSig) Clone() builtinFunc {
	newSig := &builtinLeastDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(row chunk.Row) (min *types.MyDecimal, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v.Compare(min) < 0 {
			min = v
		}
	}
	return
}

type builtinLeastStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastStringSig) Clone() builtinFunc {
	newSig := &builtinLeastStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) evalString(row chunk.Row) (min string, isNull bool, err error) {
	min, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.args); i++ {
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if types.CompareString(v, min, b.collation) < 0 {
			min = v
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
func (b *builtinLeastCmpStringAsTimeSig) evalString(row chunk.Row) (strRes string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args); i++ {
		v, isNull, err := b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		v, err = doTimeConversionForGL(b.cmpAsDate, b.ctx, sc, v)
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

func (b *builtinLeastTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	for i := 0; i < len(b.args); i++ {
		v, isNull, err := b.args[i].EvalTime(b.ctx, row)
		if isNull || err != nil {
			return types.ZeroTime, true, err
		}
		if i == 0 || v.Compare(res) < 0 {
			res = v
		}
	}
	// Convert ETType Time value to MySQL actual type, distinguish date and datetime
	sc := b.ctx.GetSessionVars().StmtCtx
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	if res, err = res.Convert(sc, resTimeTp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
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
}

func (b *builtinLeastDurationSig) Clone() builtinFunc {
	newSig := &builtinLeastDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeastDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	for i := 0; i < len(b.args); i++ {
		v, isNull, err := b.args[i].EvalDuration(b.ctx, row)
		if isNull || err != nil {
			return types.Duration{}, true, err
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

func (c *intervalFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
		tp := args[i].GetType()
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
func (b *builtinIntervalIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}
	isUint1 := mysql.HasUnsignedFlag(b.args[0].GetType().GetFlag())
	var idx int
	if b.hasNullable {
		idx, err = b.linearSearch(arg0, isUint1, b.args[1:], row)
	} else {
		idx, err = b.binSearch(arg0, isUint1, b.args[1:], row)
	}
	return int64(idx), err != nil, err
}

// linearSearch linearly scans the argument least to find the position of the first value that is larger than the given target.
func (b *builtinIntervalIntSig) linearSearch(target int64, isUint1 bool, args []Expression, row chunk.Row) (i int, err error) {
	i = 0
	for ; i < len(args); i++ {
		isUint2 := mysql.HasUnsignedFlag(args[i].GetType().GetFlag())
		arg, isNull, err := args[i].EvalInt(b.ctx, row)
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
func (b *builtinIntervalIntSig) binSearch(target int64, isUint1 bool, args []Expression, row chunk.Row) (_ int, err error) {
	i, j, cmp := 0, len(args), false
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalInt(b.ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			v = target
		}
		isUint2 := mysql.HasUnsignedFlag(args[mid].GetType().GetFlag())
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
func (b *builtinIntervalRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}
	var idx int
	if b.hasNullable {
		idx, err = b.linearSearch(arg0, b.args[1:], row)
	} else {
		idx, err = b.binSearch(arg0, b.args[1:], row)
	}
	return int64(idx), err != nil, err
}

func (b *builtinIntervalRealSig) linearSearch(target float64, args []Expression, row chunk.Row) (i int, err error) {
	i = 0
	for ; i < len(args); i++ {
		arg, isNull, err := args[i].EvalReal(b.ctx, row)
		if err != nil {
			return 0, err
		}
		if !isNull && target < arg {
			break
		}
	}
	return i, nil
}

func (b *builtinIntervalRealSig) binSearch(target float64, args []Expression, row chunk.Row) (_ int, err error) {
	i, j := 0, len(args)
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalReal(b.ctx, row)
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

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *compareFunctionClass) getDisplayName() string {
	var nameBuilder strings.Builder
	c.op.Format(&nameBuilder)
	return nameBuilder.String()
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft != nil && rft != nil && (lft.GetType() == mysql.TypeUnspecified || rft.GetType() == mysql.TypeUnspecified) {
		if lft.GetType() == rft.GetType() {
			return types.ETString
		}
		if lft.GetType() == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || (lft != nil && lft.Hybrid())) && (rhs == types.ETInt || (rft != nil && rft.Hybrid())) {
		return types.ETInt
	} else if (lhs == types.ETDecimal && rhs == types.ETString) || (lhs == types.ETString && rhs == types.ETDecimal) {
		return types.ETReal
	} else if ((lhs == types.ETInt || (lft != nil && lft.Hybrid())) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || (rft != nil && rft.Hybrid())) || rhs == types.ETDecimal) {
		return types.ETDecimal
	} else if lft != nil && rft != nil && (types.IsTemporalWithDate(lft.GetType()) && rft.GetType() == mysql.TypeYear ||
		lft.GetType() == mysql.TypeYear && types.IsTemporalWithDate(rft.GetType())) {
		return types.ETDatetime
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if (lhsEvalType.IsStringKind() && rhsFieldType.GetType() == mysql.TypeJSON) ||
		(lhsFieldType.GetType() == mysql.TypeJSON && rhsEvalType.IsStringKind()) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.GetType()) || types.IsTypeTime(rhsFieldType.GetType())) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.GetType() == rhsFieldType.GetType() {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.GetType() == mysql.TypeDuration && rhsFieldType.GetType() == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		} else if isTemporalColumn(lhs) && isRHSConst ||
			isTemporalColumn(rhs) && isLHSConst {
			/*
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isLHSColumn := lhs.(*Column)
			if !isLHSColumn {
				col = rhs.(*Column)
			}
			if col.GetType().GetType() == mysql.TypeDuration {
				cmpType = types.ETDuration
			}
		}
	}
	return cmpType
}

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(ctx sessionctx.Context, lhs, rhs Expression) CompareFunc {
	switch GetAccurateCmpType(lhs, rhs) {
	case types.ETInt:
		return CompareInt
	case types.ETReal:
		return CompareReal
	case types.ETDecimal:
		return CompareDecimal
	case types.ETString:
		coll, _ := CheckAndDeriveCollationFromExprs(ctx, "", types.ETInt, lhs, rhs)
		return genCompareString(coll.Collation)
	case types.ETDuration:
		return CompareDuration
	case types.ETDatetime, types.ETTimestamp:
		return CompareTime
	case types.ETJson:
		return CompareJSON
	}
	return nil
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.GetType()) && ft.GetType() != mysql.TypeDuration {
		return false
	}
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
// 					If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
// 					If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func tryToConvertConstantInt(ctx sessionctx.Context, targetFieldType *types.FieldType, con *Constant) (_ *Constant, isExceptional bool) {
	if con.GetType().EvalType() == types.ETInt {
		return con, false
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	dt, err = dt.ConvertTo(sc, targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:        dt,
				RetType:      targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, true
		}
		return con, false
	}
	return &Constant{
		Value:        dt,
		RetType:      targetFieldType,
		DeferredExpr: con.DeferredExpr,
		ParamMarker:  con.ParamMarker,
	}, false
}

// RefineComparedConstant changes a non-integer constant argument to its ceiling or floor result by the given op.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
// 					If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
// 					If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func RefineComparedConstant(ctx sessionctx.Context, targetFieldType types.FieldType, con *Constant, op opcode.Op) (_ *Constant, isExceptional bool) {
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	if targetFieldType.GetType() == mysql.TypeBit {
		targetFieldType = *types.NewFieldType(mysql.TypeLonglong)
	}
	var intDatum types.Datum
	intDatum, err = dt.ConvertTo(sc, &targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:        intDatum,
				RetType:      &targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, true
		}
		return con, false
	}
	c, err := intDatum.Compare(sc, &con.Value, collate.GetBinaryCollator())
	if err != nil {
		return con, false
	}
	if c == 0 {
		return &Constant{
			Value:        intDatum,
			RetType:      &targetFieldType,
			DeferredExpr: con.DeferredExpr,
			ParamMarker:  con.ParamMarker,
		}, false
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		switch con.GetType().EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			return con, true
		case types.ETString:
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			// **note**
			// 1. We compare `doubleDatum` with the `integral part of doubleDatum` rather then intDatum to handle the
			//    case when `targetFieldType.GetType()` is `TypeYear`.
			// 2. When `targetFieldType.GetType()` is `TypeYear`, we can not compare `doubleDatum` with `intDatum` directly,
			//    because we'll convert values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges
			//    2000 to 2069 and 1970 to 1999.
			// 3. Suppose the value of `con` is 2, when `targetFieldType.GetType()` is `TypeYear`, the value of `doubleDatum`
			//    will be 2.0 and the value of `intDatum` will be 2002 in this case.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(sc, types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				return con, false
			}
			if doubleDatum.GetFloat64() != math.Trunc(doubleDatum.GetFloat64()) {
				return con, true
			}
			return &Constant{
				Value:        intDatum,
				RetType:      &targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, false
		}
	}
	return con, false
}

// refineArgs will rewrite the arguments if the compare expression is `int column <cmp> non-int constant` or
// `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`. It also handles comparing year type
// with int constant if the int constant falls into a sensible year representation.
// This refine operation depends on the values of these args, but these values can change when using plan-cache.
// So we have to skip this operation or mark the plan as over-optimized when using plan-cache.
func (c *compareFunctionClass) refineArgs(ctx sessionctx.Context, args []Expression) []Expression {
	arg0Type, arg1Type := args[0].GetType(), args[1].GetType()
	arg0IsInt := arg0Type.EvalType() == types.ETInt
	arg1IsInt := arg1Type.EvalType() == types.ETInt
	arg0IsString := arg0Type.EvalType() == types.ETString
	arg1IsString := arg1Type.EvalType() == types.ETString
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	isExceptional, finalArg0, finalArg1 := false, args[0], args[1]
	isPositiveInfinite, isNegativeInfinite := false, false
	if MaybeOverOptimized4PlanCache(ctx, args) {
		// To keep the result be compatible with MySQL, refine `int non-constant <cmp> str constant`
		// here and skip this refine operation in all other cases for safety.
		if (arg0IsInt && !arg0IsCon && arg1IsString && arg1IsCon) || (arg1IsInt && !arg1IsCon && arg0IsString && arg0IsCon) {
			ctx.GetSessionVars().StmtCtx.SkipPlanCache = true
			RemoveMutableConst(ctx, args)
		} else {
			return args
		}
	} else if ctx.GetSessionVars().StmtCtx.SkipPlanCache {
		// We should remove the mutable constant for correctness, because its value may be changed.
		RemoveMutableConst(ctx, args)
	}
	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		arg1, isExceptional = RefineComparedConstant(ctx, *arg0Type, arg1, c.op)
		// Why check not null flag
		// eg: int_col > const_val(which is less than min_int32)
		// If int_col got null, compare result cannot be true
		if !isExceptional || (isExceptional && mysql.HasNotNullFlag(arg0Type.GetFlag())) {
			finalArg1 = arg1
		}
		// TODO if the plan doesn't care about whether the result of the function is null or false, we don't need
		// to check the NotNullFlag, then more optimizations can be enabled.
		isExceptional = isExceptional && mysql.HasNotNullFlag(arg0Type.GetFlag())
		if isExceptional && arg1.GetType().EvalType() == types.ETInt {
			// Judge it is inf or -inf
			// For int:
			//			inf:  01111111 & 1 == 1
			//		   -inf:  10000000 & 1 == 0
			// For uint:
			//			inf:  11111111 & 1 == 1
			//		   -inf:  00000000 & 1 == 0
			if arg1.Value.GetInt64()&1 == 1 {
				isPositiveInfinite = true
			} else {
				isNegativeInfinite = true
			}
		}
	}
	// non-int constant [cmp] int non-constant
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		arg0, isExceptional = RefineComparedConstant(ctx, *arg1Type, arg0, symmetricOp[c.op])
		if !isExceptional || (isExceptional && mysql.HasNotNullFlag(arg1Type.GetFlag())) {
			finalArg0 = arg0
		}
		// TODO if the plan doesn't care about whether the result of the function is null or false, we don't need
		// to check the NotNullFlag, then more optimizations can be enabled.
		isExceptional = isExceptional && mysql.HasNotNullFlag(arg1Type.GetFlag())
		if isExceptional && arg0.GetType().EvalType() == types.ETInt {
			if arg0.Value.GetInt64()&1 == 1 {
				isNegativeInfinite = true
			} else {
				isPositiveInfinite = true
			}
		}
	}
	// int constant [cmp] year type
	if arg0IsCon && arg0IsInt && arg1Type.GetType() == mysql.TypeYear && !arg0.Value.IsNull() {
		adjusted, failed := types.AdjustYear(arg0.Value.GetInt64(), false)
		if failed == nil {
			arg0.Value.SetInt64(adjusted)
			finalArg0 = arg0
		}
	}
	// year type [cmp] int constant
	if arg1IsCon && arg1IsInt && arg0Type.GetType() == mysql.TypeYear && !arg1.Value.IsNull() {
		adjusted, failed := types.AdjustYear(arg1.Value.GetInt64(), false)
		if failed == nil {
			arg1.Value.SetInt64(adjusted)
			finalArg1 = arg1
		}
	}
	if isExceptional && (c.op == opcode.EQ || c.op == opcode.NullEQ) {
		// This will always be false.
		return []Expression{NewZero(), NewOne()}
	}
	if isPositiveInfinite {
		// If the op is opcode.LT, opcode.LE
		// This will always be true.
		// If the op is opcode.GT, opcode.GE
		// This will always be false.
		return []Expression{NewZero(), NewOne()}
	}
	if isNegativeInfinite {
		// If the op is opcode.GT, opcode.GE
		// This will always be true.
		// If the op is opcode.LT, opcode.LE
		// This will always be false.
		return []Expression{NewOne(), NewZero()}
	}

	return c.refineArgsByUnsignedFlag(ctx, []Expression{finalArg0, finalArg1})
}

func (c *compareFunctionClass) refineArgsByUnsignedFlag(ctx sessionctx.Context, args []Expression) []Expression {
	// Only handle int cases, cause MySQL declares that `UNSIGNED` is deprecated for FLOAT, DOUBLE and DECIMAL types,
	// and support for it would be removed in a future version.
	if args[0].GetType().EvalType() != types.ETInt || args[1].GetType().EvalType() != types.ETInt {
		return args
	}
	colArgs := make([]*Column, 2)
	constArgs := make([]*Constant, 2)
	for i, arg := range args {
		switch x := arg.(type) {
		case *Constant:
			constArgs[i] = x
		case *Column:
			colArgs[i] = x
		case *CorrelatedColumn:
			colArgs[i] = &x.Column
		}
	}
	for i := 0; i < 2; i++ {
		if con, col := constArgs[1-i], colArgs[i]; con != nil && col != nil {
			v, isNull, err := con.EvalInt(ctx, chunk.Row{})
			if err != nil || isNull || v > 0 {
				return args
			}
			if mysql.HasUnsignedFlag(con.RetType.GetFlag()) && !mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
				op := c.op
				if i == 1 {
					op = symmetricOp[c.op]
				}
				if op == opcode.EQ || op == opcode.NullEQ {
					if _, err := types.ConvertUintToInt(uint64(v), types.IntergerSignedUpperBound(col.RetType.GetType()), col.RetType.GetType()); err != nil {
						args[i], args[1-i] = NewOne(), NewZero()
						return args
					}
				}
			}
			if mysql.HasUnsignedFlag(col.RetType.GetFlag()) && mysql.HasNotNullFlag(col.RetType.GetFlag()) && !mysql.HasUnsignedFlag(con.RetType.GetFlag()) {
				op := c.op
				if i == 1 {
					op = symmetricOp[c.op]
				}
				if v == 0 && (op == opcode.LE || op == opcode.GT || op == opcode.NullEQ || op == opcode.EQ || op == opcode.NE) {
					return args
				}
				// `unsigned_col < 0` equals to `1 < 0`,
				// `unsigned_col > -1` equals to `1 > 0`,
				// `unsigned_col <= -1` equals to `1 <= 0`,
				// `unsigned_col >= 0` equals to `1 >= 0`,
				// `unsigned_col == -1` equals to `1 == 0`,
				// `unsigned_col != -1` equals to `1 != 0`,
				// `unsigned_col <=> -1` equals to `1 <=> 0`,
				// so we can replace the column argument with `1`, and the other constant argument with `0`.
				args[i], args[1-i] = NewOne(), NewZero()
				return args
			}
		}
	}
	return args
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx sessionctx.Context, rawArgs []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(rawArgs); err != nil {
		return nil, err
	}
	args := c.refineArgs(ctx, rawArgs)
	cmpType := GetAccurateCmpType(args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx sessionctx.Context, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, tp, tp)
	if err != nil {
		return nil, err
	}
	if tp == types.ETJson {
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			DisableParseJSONFlag4Expr(args[i])
		}
	}
	bf.tp.SetFlen(1)
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEInt)
		case opcode.NullEQ:
			sig = &builtinNullEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQInt)
		}
	case types.ETReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTReal)
		case opcode.LE:
			sig = &builtinLERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEReal)
		case opcode.GT:
			sig = &builtinGTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTReal)
		case opcode.GE:
			sig = &builtinGERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEReal)
		case opcode.EQ:
			sig = &builtinEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQReal)
		case opcode.NE:
			sig = &builtinNERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEReal)
		case opcode.NullEQ:
			sig = &builtinNullEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQReal)
		}
	case types.ETDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDecimal)
		case opcode.LE:
			sig = &builtinLEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDecimal)
		case opcode.GT:
			sig = &builtinGTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDecimal)
		case opcode.GE:
			sig = &builtinGEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDecimal)
		case opcode.EQ:
			sig = &builtinEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDecimal)
		case opcode.NE:
			sig = &builtinNEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDecimal)
		case opcode.NullEQ:
			sig = &builtinNullEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDecimal)
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEString)
		case opcode.NullEQ:
			sig = &builtinNullEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQString)
		}
	case types.ETDuration:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDuration)
		case opcode.LE:
			sig = &builtinLEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDuration)
		case opcode.GT:
			sig = &builtinGTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDuration)
		case opcode.GE:
			sig = &builtinGEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDuration)
		case opcode.EQ:
			sig = &builtinEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDuration)
		case opcode.NE:
			sig = &builtinNEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDuration)
		case opcode.NullEQ:
			sig = &builtinNullEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDuration)
		}
	case types.ETDatetime, types.ETTimestamp:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTTime)
		case opcode.LE:
			sig = &builtinLETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LETime)
		case opcode.GT:
			sig = &builtinGTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTTime)
		case opcode.GE:
			sig = &builtinGETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GETime)
		case opcode.EQ:
			sig = &builtinEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQTime)
		case opcode.NE:
			sig = &builtinNETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NETime)
		case opcode.NullEQ:
			sig = &builtinNullEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQTime)
		}
	case types.ETJson:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTJson)
		case opcode.LE:
			sig = &builtinLEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEJson)
		case opcode.GT:
			sig = &builtinGTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTJson)
		case opcode.GE:
			sig = &builtinGEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEJson)
		case opcode.EQ:
			sig = &builtinEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQJson)
		case opcode.NE:
			sig = &builtinNEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEJson)
		case opcode.NullEQ:
			sig = &builtinNullEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQJson)
		}
	}
	return
}

type builtinLTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLTIntSig) Clone() builtinFunc {
	newSig := &builtinLTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTIntSig) evalIntWithCtx(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

func (b *builtinLTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLTRealSig) Clone() builtinFunc {
	newSig := &builtinLTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDecimalSig) Clone() builtinFunc {
	newSig := &builtinLTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLTStringSig) Clone() builtinFunc {
	newSig := &builtinLTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinLTDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDurationSig) Clone() builtinFunc {
	newSig := &builtinLTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLTTimeSig) Clone() builtinFunc {
	newSig := &builtinLTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinLTJSONSig) Clone() builtinFunc {
	newSig := &builtinLTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLEIntSig) Clone() builtinFunc {
	newSig := &builtinLEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLERealSig struct {
	baseBuiltinFunc
}

func (b *builtinLERealSig) Clone() builtinFunc {
	newSig := &builtinLERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDecimalSig) Clone() builtinFunc {
	newSig := &builtinLEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLEStringSig) Clone() builtinFunc {
	newSig := &builtinLEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinLEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDurationSig) Clone() builtinFunc {
	newSig := &builtinLEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLETimeSig) Clone() builtinFunc {
	newSig := &builtinLETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinLEJSONSig) Clone() builtinFunc {
	newSig := &builtinLEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGTIntSig) Clone() builtinFunc {
	newSig := &builtinGTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGTRealSig) Clone() builtinFunc {
	newSig := &builtinGTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDecimalSig) Clone() builtinFunc {
	newSig := &builtinGTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGTStringSig) Clone() builtinFunc {
	newSig := &builtinGTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinGTDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDurationSig) Clone() builtinFunc {
	newSig := &builtinGTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGTTimeSig) Clone() builtinFunc {
	newSig := &builtinGTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinGTJSONSig) Clone() builtinFunc {
	newSig := &builtinGTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGEIntSig) Clone() builtinFunc {
	newSig := &builtinGEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGERealSig struct {
	baseBuiltinFunc
}

func (b *builtinGERealSig) Clone() builtinFunc {
	newSig := &builtinGERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDecimalSig) Clone() builtinFunc {
	newSig := &builtinGEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGEStringSig) Clone() builtinFunc {
	newSig := &builtinGEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinGEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDurationSig) Clone() builtinFunc {
	newSig := &builtinGEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGETimeSig) Clone() builtinFunc {
	newSig := &builtinGETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinGEJSONSig) Clone() builtinFunc {
	newSig := &builtinGEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinEQIntSig) Clone() builtinFunc {
	newSig := &builtinEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinEQRealSig) Clone() builtinFunc {
	newSig := &builtinEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinEQStringSig) Clone() builtinFunc {
	newSig := &builtinEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinEQDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDurationSig) Clone() builtinFunc {
	newSig := &builtinEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinEQTimeSig) Clone() builtinFunc {
	newSig := &builtinEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinEQJSONSig) Clone() builtinFunc {
	newSig := &builtinEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNEIntSig) Clone() builtinFunc {
	newSig := &builtinNEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNERealSig struct {
	baseBuiltinFunc
}

func (b *builtinNERealSig) Clone() builtinFunc {
	newSig := &builtinNERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDecimalSig) Clone() builtinFunc {
	newSig := &builtinNEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNEStringSig) Clone() builtinFunc {
	newSig := &builtinNEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinNEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDurationSig) Clone() builtinFunc {
	newSig := &builtinNEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNETimeSig) Clone() builtinFunc {
	newSig := &builtinNETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNEJSONSig) Clone() builtinFunc {
	newSig := &builtinNEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNullEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQIntSig) Clone() builtinFunc {
	newSig := &builtinNullEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, isNull0, err
	}
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		return 0, isNull1, err
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(b.args[0].GetType().GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType().GetFlag())
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case isUnsigned0 && isUnsigned1 && types.CompareUint64(uint64(arg0), uint64(arg1)) == 0:
		res = 1
	case !isUnsigned0 && !isUnsigned1 && types.CompareInt64(arg0, arg1) == 0:
		res = 1
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQRealSig) Clone() builtinFunc {
	newSig := &builtinNullEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalReal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareFloat64(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinNullEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalDecimal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQStringSig) Clone() builtinFunc {
	newSig := &builtinNullEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalString(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalString(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareString(arg0, arg1, b.collation) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQDurationSig) Clone() builtinFunc {
	newSig := &builtinNullEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalDuration(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalDuration(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQTimeSig) Clone() builtinFunc {
	newSig := &builtinNullEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalTime(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalTime(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQJSONSig) Clone() builtinFunc {
	newSig := &builtinNullEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalJSON(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalJSON(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	default:
		cmpRes := json.CompareBinary(arg0, arg1)
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	if lhsIsNull && rhsIsNull {
		return 0
	}
	if lhsIsNull {
		return -1
	}
	return 1
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// CompareInt compares two integers.
func CompareInt(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	// compare null values.
	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}

	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType().GetFlag()), mysql.HasUnsignedFlag(rhsArg.GetType().GetFlag())
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	return int64(res), false, nil
}

func genCompareString(collation string) func(sctx sessionctx.Context, lhsArg Expression, rhsArg Expression, lhsRow chunk.Row, rhsRow chunk.Row) (int64, bool, error) {
	return func(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
		return CompareStringWithCollationInfo(sctx, lhsArg, rhsArg, lhsRow, rhsRow, collation)
	}
}

// CompareStringWithCollationInfo compares two strings with the specified collation information.
func CompareStringWithCollationInfo(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row, collation string) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareString(arg0, arg1, collation)), false, nil
}

// CompareReal compares two float-point values.
func CompareReal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareTime compares two datetime or timestamps.
func CompareTime(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalTime(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalTime(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareDuration compares two durations.
func CompareDuration(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDuration(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDuration(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareJSON compares two JSONs.
func CompareJSON(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalJSON(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalJSON(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(json.CompareBinary(arg0, arg1)), false, nil
}
