// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &vecDimsFunctionClass{}
	_ functionClass = &vecL1DistanceFunctionClass{}
	_ functionClass = &vecL2DistanceFunctionClass{}
	_ functionClass = &vecNegativeInnerProductFunctionClass{}
	_ functionClass = &vecCosineDistanceFunctionClass{}
	_ functionClass = &vecL2NormFunctionClass{}
	_ functionClass = &vecFromTextFunctionClass{}
	_ functionClass = &vecAsTextFunctionClass{}
)

var (
	_ builtinFunc = &builtinVecDimsSig{}
	_ builtinFunc = &builtinVecL1DistanceSig{}
	_ builtinFunc = &builtinVecL2DistanceSig{}
	_ builtinFunc = &builtinVecNegativeInnerProductSig{}
	_ builtinFunc = &builtinVecCosineDistanceSig{}
	_ builtinFunc = &builtinVecL2NormSig{}
	_ builtinFunc = &builtinVecFromTextSig{}
	_ builtinFunc = &builtinVecAsTextSig{}
)

type vecDimsFunctionClass struct {
	baseFunctionClass
}

type builtinVecDimsSig struct {
	baseBuiltinFunc
}

func (b *builtinVecDimsSig) Clone() builtinFunc {
	newSig := &builtinVecDimsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecDimsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecDimsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecDimsSig)
	return sig, nil
}

func (b *builtinVecDimsSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	v, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return int64(v.Len()), false, nil
}

type vecL1DistanceFunctionClass struct {
	baseFunctionClass
}

type builtinVecL1DistanceSig struct {
	baseBuiltinFunc
}

func (b *builtinVecL1DistanceSig) Clone() builtinFunc {
	newSig := &builtinVecL1DistanceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecL1DistanceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecL1DistanceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecL1DistanceSig)
	return sig, nil
}

func (b *builtinVecL1DistanceSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	v1, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	v2, isNull, err := b.args[1].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	d, err := v1.L1Distance(v2)
	if err != nil {
		return res, false, err
	}

	if math.IsNaN(d) {
		return 0, true, nil
	}
	return d, false, nil
}

type vecL2DistanceFunctionClass struct {
	baseFunctionClass
}

type builtinVecL2DistanceSig struct {
	baseBuiltinFunc
}

func (b *builtinVecL2DistanceSig) Clone() builtinFunc {
	newSig := &builtinVecL2DistanceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecL2DistanceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecL2DistanceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecL2DistanceSig)
	return sig, nil
}

func (b *builtinVecL2DistanceSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	v1, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	v2, isNull, err := b.args[1].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	d, err := v1.L2Distance(v2)
	if err != nil {
		return res, false, err
	}

	if math.IsNaN(d) {
		return 0, true, nil
	}
	return d, false, nil
}

type vecNegativeInnerProductFunctionClass struct {
	baseFunctionClass
}

type builtinVecNegativeInnerProductSig struct {
	baseBuiltinFunc
}

func (b *builtinVecNegativeInnerProductSig) Clone() builtinFunc {
	newSig := &builtinVecNegativeInnerProductSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecNegativeInnerProductFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecNegativeInnerProductSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecNegativeInnerProductSig)
	return sig, nil
}

func (b *builtinVecNegativeInnerProductSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	v1, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	v2, isNull, err := b.args[1].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	d, err := v1.NegativeInnerProduct(v2)
	if err != nil {
		return res, false, err
	}

	if math.IsNaN(d) {
		return 0, true, nil
	}
	return d, false, nil
}

type vecCosineDistanceFunctionClass struct {
	baseFunctionClass
}

type builtinVecCosineDistanceSig struct {
	baseBuiltinFunc
}

func (b *builtinVecCosineDistanceSig) Clone() builtinFunc {
	newSig := &builtinVecCosineDistanceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecCosineDistanceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecCosineDistanceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecCosineDistanceSig)
	return sig, nil
}

func (b *builtinVecCosineDistanceSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	v1, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	v2, isNull, err := b.args[1].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	d, err := v1.CosineDistance(v2)
	if err != nil {
		return res, false, err
	}

	if math.IsNaN(d) {
		return 0, true, nil
	}
	return d, false, nil
}

type vecL2NormFunctionClass struct {
	baseFunctionClass
}

type builtinVecL2NormSig struct {
	baseBuiltinFunc
}

func (b *builtinVecL2NormSig) Clone() builtinFunc {
	newSig := &builtinVecL2NormSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecL2NormFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecL2NormSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecL2NormSig)
	return sig, nil
}

func (b *builtinVecL2NormSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	v, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	d := v.L2Norm()
	if math.IsNaN(d) {
		return 0, true, nil
	}
	return d, false, nil
}

type vecFromTextFunctionClass struct {
	baseFunctionClass
}

type builtinVecFromTextSig struct {
	baseBuiltinFunc
}

func (b *builtinVecFromTextSig) Clone() builtinFunc {
	newSig := &builtinVecFromTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecFromTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETVectorFloat32, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecFromTextSig{bf}
	// sig.setPbCode(tipb.ScalarFuncSig_VecFromTextSig)
	return sig, nil
}

func (b *builtinVecFromTextSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	v, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	vec, err := types.ParseVectorFloat32(v)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	if err = vec.CheckDimsFitColumn(b.tp.GetFlen()); err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}

	return vec, false, nil
}

type vecAsTextFunctionClass struct {
	baseFunctionClass
}

type builtinVecAsTextSig struct {
	baseBuiltinFunc
}

func (b *builtinVecAsTextSig) Clone() builtinFunc {
	newSig := &builtinVecAsTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *vecAsTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETVectorFloat32)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinVecAsTextSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_VecAsTextSig)
	return sig, nil
}

func (b *builtinVecAsTextSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	v, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return v.String(), false, nil
}
