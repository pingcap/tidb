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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
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
