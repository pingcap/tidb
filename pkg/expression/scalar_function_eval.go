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
	"bytes"
	"slices"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(ctx EvalContext, row chunk.Row) (d types.Datum, err error) {
	var (
		res    any
		isNull bool
	)
	intest.AssertNotNil(ctx)
	switch tp, evalType := sf.GetType(ctx), sf.GetType(ctx).EvalType(); evalType {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = sf.EvalInt(ctx, row)
		if mysql.HasUnsignedFlag(tp.GetFlag()) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = sf.EvalReal(ctx, row)
	case types.ETDecimal:
		res, isNull, err = sf.EvalDecimal(ctx, row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = sf.EvalTime(ctx, row)
	case types.ETDuration:
		res, isNull, err = sf.EvalDuration(ctx, row)
	case types.ETJson:
		res, isNull, err = sf.EvalJSON(ctx, row)
	case types.ETVectorFloat32:
		res, isNull, err = sf.EvalVectorFloat32(ctx, row)
	case types.ETString:
		var str string
		str, isNull, err = sf.EvalString(ctx, row)
		if !isNull && err == nil && tp.GetType() == mysql.TypeEnum {
			res, err = types.ParseEnum(tp.GetElems(), str, tp.GetCollate())
			tc := typeCtx(ctx)
			err = tc.HandleTruncate(err)
		} else {
			res = str
		}
	}

	if isNull || err != nil {
		d.SetNull()
		return d, err
	}
	d.SetValue(res, sf.RetType)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalInt(ctx, row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalReal(ctx, row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalDecimal(ctx, row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalString(ctx, row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalTime(ctx, row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalDuration(ctx, row)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	intest.Assert(ctx != nil)
	if intest.EnableAssert {
		ctx = wrapEvalAssert(ctx, sf.Function)
	}
	return sf.Function.evalJSON(ctx, row)
}

// EvalVectorFloat32 implements Expression interface.
func (sf *ScalarFunction) EvalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	return sf.Function.evalVectorFloat32(ctx, row)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode() []byte {
	if len(sf.hashcode) > 0 {
		if intest.InTest {
			assertCheckHashCode(sf)
		}
		return sf.hashcode
	}
	ReHashCode(sf)
	return sf.hashcode
}

// CanonicalHashCode implements Expression interface.
func (sf *ScalarFunction) CanonicalHashCode() []byte {
	if len(sf.canonicalhashcode) > 0 {
		return sf.canonicalhashcode
	}
	simpleCanonicalizedHashCode(sf)
	return sf.canonicalhashcode
}

// ExpressionsSemanticEqual is used to judge whether two expression tree is semantic equivalent.
func ExpressionsSemanticEqual(expr1, expr2 Expression) bool {
	return bytes.Equal(expr1.CanonicalHashCode(), expr2.CanonicalHashCode())
}

// simpleCanonicalizedHashCode is used to judge whether two expression is semantically equal.
func simpleCanonicalizedHashCode(sf *ScalarFunction) {
	if sf.canonicalhashcode != nil {
		sf.canonicalhashcode = sf.canonicalhashcode[:0]
	}
	sf.canonicalhashcode = append(sf.canonicalhashcode, scalarFunctionFlag)

	argsHashCode := make([][]byte, 0, len(sf.GetArgs()))
	for _, arg := range sf.GetArgs() {
		argsHashCode = append(argsHashCode, arg.CanonicalHashCode())
	}
	switch sf.FuncName.L {
	case ast.Plus, ast.Mul, ast.EQ, ast.In, ast.LogicOr, ast.LogicAnd:
		// encode original function name.
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(sf.FuncName.L))
		// reorder parameters hashcode, eg: a+b and b+a should has the same hashcode here.
		slices.SortFunc(argsHashCode, func(i, j []byte) int {
			return bytes.Compare(i, j)
		})
		for _, argCode := range argsHashCode {
			sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
		}

	case ast.GE, ast.LE: // directed binary OP: a >= b and b <= a should have the same hashcode.
		// encode GE function name.
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GE))
		// encode GE function name and switch the args order.
		if sf.FuncName.L == ast.GE {
			for _, argCode := range argsHashCode {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
			}
		} else {
			for i := len(argsHashCode) - 1; i >= 0; i-- {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argsHashCode[i]...)
			}
		}
	case ast.GT, ast.LT:
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GT))
		if sf.FuncName.L == ast.GT {
			for _, argCode := range argsHashCode {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
			}
		} else {
			for i := len(argsHashCode) - 1; i >= 0; i-- {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argsHashCode[i]...)
			}
		}
	case ast.UnaryNot:
		child, ok := sf.GetArgs()[0].(*ScalarFunction)
		if !ok {
			// encode original function name.
			sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(sf.FuncName.L))
			// use the origin arg hash code.
			for _, argCode := range argsHashCode {
				sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
			}
		} else {
			childArgsHashCode := make([][]byte, 0, len(child.GetArgs()))
			for _, arg := range child.GetArgs() {
				childArgsHashCode = append(childArgsHashCode, arg.CanonicalHashCode())
			}
			switch child.FuncName.L {
			case ast.GT: // not GT  ==> LE  ==> use GE and switch args
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GE))
				for i := len(childArgsHashCode) - 1; i >= 0; i-- {
					sf.canonicalhashcode = append(sf.canonicalhashcode, childArgsHashCode[i]...)
				}
			case ast.LT: // not LT  ==> GE
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GE))
				for _, argCode := range childArgsHashCode {
					sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
				}
			case ast.GE: // not GE  ==> LT  ==> use GT and switch args
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GT))
				for i := len(childArgsHashCode) - 1; i >= 0; i-- {
					sf.canonicalhashcode = append(sf.canonicalhashcode, childArgsHashCode[i]...)
				}
			case ast.LE: // not LE  ==> GT
				sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(ast.GT))
				for _, argCode := range childArgsHashCode {
					sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
				}
			}
		}
	default:
		// encode original function name.
		sf.canonicalhashcode = codec.EncodeCompactBytes(sf.canonicalhashcode, hack.Slice(sf.FuncName.L))
		for _, argCode := range argsHashCode {
			sf.canonicalhashcode = append(sf.canonicalhashcode, argCode...)
		}
		// Cast is a special case. The RetType should also be considered as an argument.
		// Please see `newFunctionImpl()` for detail.
		if sf.FuncName.L == ast.Cast {
			evalTp := sf.RetType.EvalType()
			sf.canonicalhashcode = append(sf.canonicalhashcode, byte(evalTp))
		}
	}
}

// Hash64 implements HashEquals.<0th> interface.
func (sf *ScalarFunction) Hash64(h base.Hasher) {
	h.HashByte(scalarFunctionFlag)
	h.HashString(sf.FuncName.L)
	if sf.RetType == nil {
		h.HashByte(base.NilFlag)
	} else {
		h.HashByte(base.NotNilFlag)
		sf.RetType.Hash64(h)
	}
	// hash the arg length to avoid hash collision.
	h.HashInt(len(sf.GetArgs()))
	for _, arg := range sf.GetArgs() {
		arg.Hash64(h)
	}
}

// Equals implements HashEquals.<1th> interface.
func (sf *ScalarFunction) Equals(other any) bool {
	sf2, ok := other.(*ScalarFunction)
	if !ok {
		return false
	}
	if sf == nil {
		return sf2 == nil
	}
	if sf2 == nil {
		return false
	}
	ok = sf.FuncName.L == sf2.FuncName.L
	ok = ok && (sf.RetType == nil && sf2.RetType == nil || sf.RetType != nil && sf2.RetType != nil && sf.RetType.Equals(sf2.RetType))
	if len(sf.GetArgs()) != len(sf2.GetArgs()) {
		return false
	}
	for i, arg := range sf.GetArgs() {
		ok = ok && arg.Equals(sf2.GetArgs()[i])
		if !ok {
			return false
		}
	}
	return ok
}

// ReHashCode is used after we change the argument in place.
func ReHashCode(sf *ScalarFunction) {
	sf.hashcode = sf.hashcode[:0]
	sf.hashcode = slices.Grow(sf.hashcode, 1+len(sf.FuncName.L)+len(sf.GetArgs())*8+1)
	sf.hashcode = append(sf.hashcode, scalarFunctionFlag)
	sf.hashcode = codec.EncodeCompactBytes(sf.hashcode, hack.Slice(sf.FuncName.L))
	for _, arg := range sf.GetArgs() {
		sf.hashcode = append(sf.hashcode, arg.HashCode()...)
	}
	// Cast is a special case. The RetType should also be considered as an argument.
	// Please see `newFunctionImpl()` for detail.
	if sf.FuncName.L == ast.Cast {
		evalTp := sf.RetType.EvalType()
		sf.hashcode = append(sf.hashcode, byte(evalTp))
	}
	if sf.FuncName.L == ast.Grouping {
		sf.hashcode = codec.EncodeInt(sf.hashcode, int64(sf.Function.(*BuiltinGroupingImplSig).GetGroupingMode()))
		marks := sf.Function.(*BuiltinGroupingImplSig).GetMetaGroupingMarks()
		sf.hashcode = codec.EncodeInt(sf.hashcode, int64(len(marks)))
		for _, mark := range marks {
			sf.hashcode = codec.EncodeInt(sf.hashcode, int64(len(mark)))
			// we need to sort map keys to ensure the hashcode is deterministic.
			keys := make([]uint64, 0, len(mark))
			for k := range mark {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				sf.hashcode = codec.EncodeInt(sf.hashcode, int64(k))
			}
		}
	}
}
