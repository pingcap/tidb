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
	"fmt"
	"strconv"
	"strings"

	"github.com/antchfx/xmlquery"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// arrayElementFunctionClass's logic is basically the same as that of jsonExtractFunctionClass.
type arrayElementFunctionClass struct {
	baseFunctionClass
}

type builtinArrayElementSig struct {
	baseBuiltinFunc
}

func (b *builtinArrayElementSig) Clone() builtinFunc {
	newSig := &builtinArrayElementSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *arrayElementFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *arrayElementFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinArrayElementSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinArrayElementSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return
	}
	if res.TypeCode != types.JSONTypeCodeArray {
		return res, true, nil
	}
	idxStr := strings.Builder{}
	idxStr.WriteString("$")
	for _, arg := range b.args[1:] {
		v, isNull, err := arg.EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		idx, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return res, true, fmt.Errorf("invalid array index: %v", v)
		}
		if idx < 0 {
			return res, true, nil
		}
		idxStr.WriteByte('[')
		idxStr.WriteString(v)
		idxStr.WriteByte(']')
	}
	pathExpr, err := types.ParseJSONPathExpr(idxStr.String())
	if err != nil {
		return res, true, err
	}
	pathExprs := []types.JSONPathExpression{pathExpr}
	var found bool
	if res, found = res.Extract(pathExprs); !found {
		return res, true, nil
	}
	return res, false, nil
}

// extractValueFunctionClass's logic is basically the same as that of jsonExtractFunctionClass.
type extractValueFunctionClass struct {
	baseFunctionClass
}

type builtinExtractValueSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractValueSig) Clone() builtinFunc {
	newSig := &builtinExtractValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *extractValueFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinExtractValueSig{bf}
	return sig, nil
}

func (b *builtinExtractValueSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	var xmlValue, path string
	xmlValue, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return
	}
	path, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return
	}
	doc, err := xmlquery.Parse(strings.NewReader(xmlValue))
	if err != nil {
		ctx.AppendWarning(types.ErrWrongValue2.FastGenByArgs("XML value", err.Error()))
		return "", true, nil
	}
	result, err := evalExtractValue(doc, path)
	if err != nil {
		return "", false, err
	}
	if result.isScalar {
		return result.scalar, false, nil
	}
	return renderExtractValueMatches(doc, result.matches), false, nil
}
