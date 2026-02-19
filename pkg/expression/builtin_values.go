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
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
	tp     *types.FieldType
}

func (c *valuesFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	switch c.tp.EvalType() {
	case types.ETInt:
		sig = &builtinValuesIntSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETReal:
		sig = &builtinValuesRealSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETDecimal:
		sig = &builtinValuesDecimalSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETString:
		sig = &builtinValuesStringSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinValuesTimeSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETDuration:
		sig = &builtinValuesDurationSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETJson:
		sig = &builtinValuesJSONSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETVectorFloat32:
		sig = &builtinValuesVectorFloat32Sig{baseBuiltinFunc: bf, offset: c.offset}
	default:
		return nil, errors.Errorf("%s is not supported for VALUES()", c.tp.EvalType())
	}
	return sig, nil
}

type builtinValuesIntSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesIntSig) Clone() builtinFunc {
	newSig := &builtinValuesIntSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesIntSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(ctx EvalContext, _ chunk.Row) (int64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return 0, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		// For BinaryLiteral, see issue #15310
		val := row.GetRaw(b.offset)
		if len(val) > 8 {
			return 0, true, errors.New("Session current insert values is too long")
		}
		if len(val) < 8 {
			var binary types.BinaryLiteral = val
			v, err := binary.ToInt(typeCtx(ctx))
			if err != nil {
				return 0, true, errors.Trace(err)
			}
			return int64(v), false, nil
		}
		return row.GetInt64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesRealSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesRealSig) Clone() builtinFunc {
	newSig := &builtinValuesRealSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesRealSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(ctx EvalContext, _ chunk.Row) (float64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return 0, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		if b.getRetTp().GetType() == mysql.TypeFloat {
			return float64(row.GetFloat32(b.offset)), false, nil
		}
		return row.GetFloat64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDecimalSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesDecimalSig) Clone() builtinFunc {
	newSig := &builtinValuesDecimalSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesDecimalSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(ctx EvalContext, _ chunk.Row) (*types.MyDecimal, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return nil, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return &types.MyDecimal{}, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return nil, true, nil
		}
		return row.GetMyDecimal(b.offset), false, nil
	}
	return nil, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesStringSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesStringSig) Clone() builtinFunc {
	newSig := &builtinValuesStringSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesStringSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalString evals a builtinValuesStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) evalString(ctx EvalContext, _ chunk.Row) (string, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return "", true, nil
	}
	if b.offset >= row.Len() {
		return "", true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
	}

	if row.IsNull(b.offset) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if retType := b.getRetTp(); retType.Hybrid() {
		val := row.GetDatum(b.offset, retType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	return row.GetString(b.offset), false, nil
}

type builtinValuesTimeSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesTimeSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinValuesTimeSig) Clone() builtinFunc {
	newSig := &builtinValuesTimeSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinValuesTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(ctx EvalContext, _ chunk.Row) (types.Time, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return types.ZeroTime, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.ZeroTime, true, nil
		}
		return row.GetTime(b.offset), false, nil
	}
	return types.ZeroTime, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDurationSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesDurationSig) Clone() builtinFunc {
	newSig := &builtinValuesDurationSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesDurationSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalDuration evals a builtinValuesDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(ctx EvalContext, _ chunk.Row) (types.Duration, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return types.Duration{}, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.Duration{}, true, nil
		}
		duration := row.GetDuration(b.offset, b.getRetTp().GetDecimal())
		return duration, false, nil
	}
	return types.Duration{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesJSONSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesJSONSig) Clone() builtinFunc {
	newSig := &builtinValuesJSONSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesJSONSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(ctx EvalContext, _ chunk.Row) (types.BinaryJSON, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.BinaryJSON{}, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return types.BinaryJSON{}, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.BinaryJSON{}, true, nil
		}
		return row.GetJSON(b.offset), false, nil
	}
	return types.BinaryJSON{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesVectorFloat32Sig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinValuesVectorFloat32Sig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesVectorFloat32Sig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalVectorFloat32 evals a builtinValuesVectorFloat32Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, _ chunk.Row) (types.VectorFloat32, bool, error) {
	sessionvar, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	row := sessionvar.CurrInsertValues
	if row.IsEmpty() {
		return types.ZeroVectorFloat32, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.ZeroVectorFloat32, true, nil
		}
		return row.GetVectorFloat32(b.offset), false, nil
	}
	return types.ZeroVectorFloat32, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}
