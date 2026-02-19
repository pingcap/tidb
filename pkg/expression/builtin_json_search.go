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
	goJSON "encoding/json"
	"strings"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
)

type jsonContainsPathFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsPathSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONContainsPathSig) Clone() builtinFunc {
	newSig := &builtinJSONContainsPathSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsPathFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonContainsPathFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson, types.ETString}
	for i := 3; i <= len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONContainsPathSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsPathSig)
	return sig, nil
}

func (b *builtinJSONContainsPathSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType = strings.ToLower(containType)
	if containType != types.JSONContainsPathAll && containType != types.JSONContainsPathOne {
		return res, true, types.ErrJSONBadOneOrAllArg.GenWithStackByArgs("json_contains_path")
	}
	var pathExpr types.JSONPathExpression
	contains := int64(1)
	for i := 2; i < len(b.args); i++ {
		path, isNull, err := b.args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		if pathExpr, err = types.ParseJSONPathExpr(path); err != nil {
			return res, true, err
		}
		_, exists := obj.Extract([]types.JSONPathExpression{pathExpr})
		switch {
		case exists && containType == types.JSONContainsPathOne:
			return 1, false, nil
		case !exists && containType == types.JSONContainsPathOne:
			contains = 0
		case !exists && containType == types.JSONContainsPathAll:
			return 0, false, nil
		}
	}
	return contains, false, nil
}

func jsonModify(ctx EvalContext, args []Expression, row chunk.Row, mt types.JSONModifyType) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]types.JSONPathExpression, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i += 2 {
		// TODO: We can cache pathExprs if args are constants.
		var s string
		s, isNull, err = args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr types.JSONPathExpression
		pathExpr, err = types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	values := make([]types.BinaryJSON, 0, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		var value types.BinaryJSON
		value, isNull, err = args[i].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			value = types.CreateBinaryJSON(nil)
		}
		values = append(values, value)
	}
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonMemberOfFunctionClass struct {
	baseFunctionClass
}

type builtinJSONMemberOfSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMemberOfSig) Clone() builtinFunc {
	newSig := &builtinJSONMemberOfSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonMemberOfFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, "member of", true, args, 1)
}

func (c *jsonMemberOfFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson, types.ETJson}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[0])
	sig := &builtinJSONMemberOfSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMemberOfSig)
	return sig, nil
}

func (b *builtinJSONMemberOfSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	target, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	obj, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if obj.TypeCode != types.JSONTypeCodeArray {
		return boolToInt64(types.CompareBinaryJSON(obj, target) == 0), false, nil
	}

	elemCount := obj.GetElemCount()
	for i := range elemCount {
		if types.CompareBinaryJSON(obj.ArrayGetElem(i), target) == 0 {
			return 1, false, nil
		}
	}

	return 0, false, nil
}

type jsonContainsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONContainsSig) Clone() builtinFunc {
	newSig := &builtinJSONContainsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1)
}

func (c *jsonContainsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONContainsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsSig)
	return sig, nil
}

func (b *builtinJSONContainsSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	var pathExpr types.JSONPathExpression
	if len(b.args) == 3 {
		path, isNull, err := b.args[2].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err = types.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}
		var exists bool
		obj, exists = obj.Extract([]types.JSONPathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
	}

	if types.ContainsBinaryJSON(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonOverlapsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONOverlapsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONOverlapsSig) Clone() builtinFunc {
	newSig := &builtinJSONOverlapsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonOverlapsFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1)
}

func (c *jsonOverlapsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONOverlapsSig{bf}
	return sig, nil
}

func (b *builtinJSONOverlapsSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if types.OverlapsBinaryJSON(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonValidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	var sig builtinFunc
	argType := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argType {
	case types.ETJson:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidJsonSig)
	case types.ETString:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidStringSig)
	default:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argType)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidOthersSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidOthersSig)
	}
	return sig, nil
}

type builtinJSONValidJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidJSONSig) Clone() builtinFunc {
	newSig := &builtinJSONValidJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	_, isNull, err = b.args[0].EvalJSON(ctx, row)
	return 1, isNull, err
}

type builtinJSONValidStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidStringSig) Clone() builtinFunc {
	newSig := &builtinJSONValidStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidStringSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}

	data := hack.Slice(val)
	if goJSON.Valid(data) {
		res = 1
	}
	return res, false, nil
}

type builtinJSONValidOthersSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidOthersSig) Clone() builtinFunc {
	newSig := &builtinJSONValidOthersSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidOthersSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidOthersSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	datum, err := b.args[0].Eval(ctx, row)
	return 0, datum.IsNull(), err
}

type jsonArrayAppendFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayAppendSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *jsonArrayAppendFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if len(args) < 3 || (len(args)&1 != 1) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	return nil
}

func (c *jsonArrayAppendFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArrayAppendSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayAppendSig)
	return sig, nil
}

func (b *builtinJSONArrayAppendSig) Clone() builtinFunc {
	newSig := &builtinJSONArrayAppendSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayAppendSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, sNull, err := b.args[i].EvalString(ctx, row)
		if sNull || err != nil {
			return res, true, err
		}
		value, vNull, err := b.args[i+1].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if vNull {
			value = types.CreateBinaryJSON(nil)
		}
		res, isNull, err = b.appendJSONArray(res, s, value)
		if isNull || err != nil {
			return res, isNull, err
		}
	}
	return res, false, nil
}

func (b *builtinJSONArrayAppendSig) appendJSONArray(res types.BinaryJSON, p string, v types.BinaryJSON) (types.BinaryJSON, bool, error) {
	// We should do the following checks to get correct values in res.Extract
	pathExpr, err := types.ParseJSONPathExpr(p)
	if err != nil {
		return res, true, err
	}
	if pathExpr.CouldMatchMultipleValues() {
		return res, true, types.ErrInvalidJSONPathMultipleSelection
	}

	obj, exists := res.Extract([]types.JSONPathExpression{pathExpr})
	if !exists {
		// If path not exists, just do nothing and no errors.
		return res, false, nil
	}

	if obj.TypeCode != types.JSONTypeCodeArray {
		// res.Extract will return a json object instead of an array if there is an object at path pathExpr.
		// JSON_ARRAY_APPEND({"a": "b"}, "$", {"b": "c"}) => [{"a": "b"}, {"b", "c"}]
		// We should wrap them to a single array first.
		obj, err = types.CreateBinaryJSONWithCheck([]any{obj})
		if err != nil {
			return res, true, err
		}
	}

	// wrap the new value `v` into an array explicitly, in case that the `v` is an array itself.
	// For example, `JSON_ARRAY_APPEND('[1]', '$', JSON_ARRAY(2, 3))` should return `[1, [2, 3]]`
	v = types.CreateBinaryJSON([]any{v})

	obj = types.MergeBinaryJSON([]types.BinaryJSON{obj, v})
	res, err = res.Modify([]types.JSONPathExpression{pathExpr}, []types.BinaryJSON{obj}, types.JSONModifySet)
	return res, false, err
}

type jsonArrayInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayInsertSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *jsonArrayInsertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArrayInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayInsertSig)
	return sig, nil
}

func (b *builtinJSONArrayInsertSig) Clone() builtinFunc {
	newSig := &builtinJSONArrayInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayInsertSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.args[i].EvalString(ctx, row)
		if err != nil || isNull {
			return res, true, err
		}

		pathExpr, err := types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}

		value, isnull, err := b.args[i+1].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = types.CreateBinaryJSON(nil)
		}

		res, err = res.ArrayInsert(pathExpr, value)
		if err != nil {
			return res, true, err
		}
	}
	return res, false, nil
}

type jsonMergePatchFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePatchFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

func (c *jsonMergePatchFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergePatchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePatchSig)
	return sig, nil
}

type builtinJSONMergePatchSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMergePatchSig) Clone() builtinFunc {
	newSig := &builtinJSONMergePatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONMergePatchSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	values := make([]*types.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		var value types.BinaryJSON
		value, isNull, err = arg.EvalJSON(ctx, row)
		if err != nil {
			return
		}
		if isNull {
			values = append(values, nil)
		} else {
			values = append(values, &value)
		}
	}
	tmpRes, err := types.MergePatchBinaryJSON(values)
	if err != nil {
		return
	}
	if tmpRes != nil {
		res = *tmpRes
	} else {
		isNull = true
	}
	return res, isNull, nil
}

type jsonMergePreserveFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePreserveFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

func (c *jsonMergePreserveFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePreserveSig)
	return sig, nil
}

