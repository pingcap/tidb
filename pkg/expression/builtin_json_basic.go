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
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type jsonTypeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONTypeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONTypeSig) Clone() builtinFunc {
	newSig := &builtinJSONTypeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonTypeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETJson)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(51) // flen of JSON_TYPE is length of UNSIGNED INTEGER.
	bf.tp.AddFlag(mysql.BinaryFlag)
	sig := &builtinJSONTypeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonTypeSig)
	return sig, nil
}

func (c *jsonTypeFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

// verifyJSONArgsType verifies that all args specified in `jsonArgsIndex` are JSON or non-binary string or NULL.
// the `useJSONErr` specifies to use `ErrIncorrectType` or `ErrInvalidTypeForJSON`. If it's true, the error will be `ErrInvalidTypeForJSON`
func verifyJSONArgsType(ctx EvalContext, funcName string, useJSONErr bool, args []Expression, jsonArgsIndex ...int) error {
	if jsonArgsIndex == nil {
		// if no index is specified, verify all args
		jsonArgsIndex = make([]int, len(args))
		for i := range args {
			jsonArgsIndex[i] = i
		}
	}
	for _, argIndex := range jsonArgsIndex {
		arg := args[argIndex]

		typ := arg.GetType(ctx)
		if typ.GetType() == mysql.TypeNull {
			continue
		}

		evalType := typ.EvalType()
		switch evalType {
		case types.ETString:
			cs := typ.GetCharset()
			if cs == charset.CharsetBin {
				return types.ErrInvalidJSONCharset.GenWithStackByArgs(cs)
			}
			continue
		case types.ETJson:
			continue
		default:
			if useJSONErr {
				return ErrInvalidTypeForJSON.GenWithStackByArgs(argIndex+1, funcName)
			}
			return ErrIncorrectType.GenWithStackByArgs(strconv.Itoa(argIndex+1), funcName)
		}
	}
	return nil
}

func (b *builtinJSONTypeSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	var j types.BinaryJSON
	j, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return j.Type(), false, nil
}

type jsonSumCRC32FunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *jsonSumCRC32FunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}

	if args[0].GetType(ctx).EvalType() != types.ETJson {
		return ErrInvalidTypeForJSON.GenWithStackByArgs(1, "JSON_SUM_CRC32")
	}

	return nil
}

func (c *jsonSumCRC32FunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	arrayType := c.tp.ArrayType()
	switch arrayType.GetType() {
	case mysql.TypeYear, mysql.TypeJSON, mysql.TypeFloat, mysql.TypeNewDecimal:
		return nil, ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("calculating json_sum_crc32 on array of %s", arrayType.String()))
	}
	if arrayType.EvalType() == types.ETString && arrayType.GetCharset() != charset.CharsetUTF8MB4 && arrayType.GetCharset() != charset.CharsetBin {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("unsupported charset")
	}
	if arrayType.EvalType() == types.ETString && arrayType.GetFlen() == types.UnspecifiedLength {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("calculating json_sum_crc32 on array of char/binary BLOBs with unspecified length")
	}

	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	sig = &builtinJSONSumCRC32Sig{bf}
	return sig, nil
}

type builtinJSONSumCRC32Sig struct {
	baseBuiltinFunc
}

func (b *builtinJSONSumCRC32Sig) Clone() builtinFunc {
	newSig := &builtinJSONSumCRC32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONSumCRC32Sig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if val.TypeCode != types.JSONTypeCodeArray {
		return 0, false, ErrInvalidTypeForJSON.GenWithStackByArgs(1, "JSON_SUM_CRC32")
	}

	ft := b.tp.ArrayType()
	f := convertJSON2Tp(ft.EvalType())
	if f == nil {
		return 0, false, ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("calculating sum of %s", ft.String()))
	}

	var sum int64
	for i := range val.GetElemCount() {
		item, err := f(fakeSctx, val.ArrayGetElem(i), ft)
		if err != nil {
			if ErrInvalidJSONForFuncIndex.Equal(err) {
				err = errors.Errorf("Invalid JSON value for CAST to type %s", ft.CompactStr())
			}
			return 0, false, err
		}
		sum += int64(crc32.ChecksumIEEE(fmt.Appendf(nil, "%v", item)))
	}

	return sum, false, err
}

// BuildJSONSumCrc32FunctionWithCheck builds a JSON_SUM_CRC32 ScalarFunction from the Expression and return error if any.
// The logic is almost the same as build CAST function, except that the return type is fixed to bigint.
func BuildJSONSumCrc32FunctionWithCheck(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression, err error) {
	argType := expr.GetType(ctx.GetEvalCtx())
	// If source argument's nullable, then target type should be nullable
	if !mysql.HasNotNullFlag(argType.GetFlag()) {
		tp.DelFlag(mysql.NotNullFlag)
	}
	expr = TryPushCastIntoControlFunctionForHybridType(ctx, expr, tp)

	if tp.EvalType() != types.ETJson || !tp.IsArray() {
		return nil, errors.Errorf("json_sum_crc32 can only built on JSON array, got type %s", tp.EvalType())
	}

	fc := &jsonSumCRC32FunctionClass{baseFunctionClass{ast.JSONSumCrc32, 1, 1}, tp}
	f, err := fc.getFunction(ctx, []Expression{expr})
	return &ScalarFunction{
		FuncName: ast.NewCIStr(ast.JSONSumCrc32),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Function: f,
	}, err
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

type builtinJSONExtractSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONExtractSig) Clone() builtinFunc {
	newSig := &builtinJSONExtractSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonExtractFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonExtractFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig := &builtinJSONExtractSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinJSONExtractSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return
	}
	pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err := types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	var found bool
	if res, found = res.Extract(pathExprs); !found {
		return res, true, nil
	}
	return res, false, nil
}

type jsonUnquoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONUnquoteSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONUnquoteSig) Clone() builtinFunc {
	newSig := &builtinJSONUnquoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonUnquoteFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, false, args, 0)
}

func (c *jsonUnquoteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	bf.tp.AddFlag(mysql.BinaryFlag)
	DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[0])
	sig := &builtinJSONUnquoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonUnquoteSig)
	return sig, nil
}

func (b *builtinJSONUnquoteSig) evalString(ctx EvalContext, row chunk.Row) (str string, isNull bool, err error) {
	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if len(str) >= 2 && str[0] == '"' && str[len(str)-1] == '"' && !goJSON.Valid([]byte(str)) {
		return "", false, types.ErrInvalidJSONText.GenWithStackByArgs("The document root must not be followed by other values.")
	}
	str, err = types.UnquoteString(str)
	if err != nil {
		return "", false, err
	}
	return str, false, nil
}

type jsonSetFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSetSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONSetSig) Clone() builtinFunc {
	newSig := &builtinJSONSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSetFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig := &builtinJSONSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSetSig)
	return sig, nil
}

func (b *builtinJSONSetSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(ctx, b.args, row, types.JSONModifySet)
	return res, isNull, err
}

type jsonInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONInsertSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONInsertSig) Clone() builtinFunc {
	newSig := &builtinJSONInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonInsertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig := &builtinJSONInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonInsertSig)
	return sig, nil
}

func (b *builtinJSONInsertSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(ctx, b.args, row, types.JSONModifyInsert)
	return res, isNull, err
}

type jsonReplaceFunctionClass struct {
	baseFunctionClass
}

type builtinJSONReplaceSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONReplaceSig) Clone() builtinFunc {
	newSig := &builtinJSONReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonReplaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig := &builtinJSONReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonReplaceSig)
	return sig, nil
}

func (b *builtinJSONReplaceSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(ctx, b.args, row, types.JSONModifyReplace)
	return res, isNull, err
}

type jsonRemoveFunctionClass struct {
	baseFunctionClass
}

type builtinJSONRemoveSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONRemoveSig) Clone() builtinFunc {
	newSig := &builtinJSONRemoveSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonRemoveFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
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
	sig := &builtinJSONRemoveSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonRemoveSig)
	return sig, nil
}

func (b *builtinJSONRemoveSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(ctx, row)
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
	res, err = res.Remove(pathExprs)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonMergeFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergeFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

type builtinJSONMergeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMergeSig) Clone() builtinFunc {
	newSig := &builtinJSONMergeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonMergeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergeSig)
	return sig, nil
}

func (b *builtinJSONMergeSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	values := make([]types.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		var value types.BinaryJSON
		value, isNull, err = arg.EvalJSON(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		values = append(values, value)
	}
	res = types.MergeBinaryJSON(values)
	// function "JSON_MERGE" is deprecated since MySQL 5.7.22. Synonym for function "JSON_MERGE_PRESERVE".
	// See https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		tc := typeCtx(ctx)
		tc.AppendWarning(errDeprecatedSyntaxNoReplacement.FastGenByArgs("JSON_MERGE", ""))
	}
	return res, false, nil
}

type jsonObjectFunctionClass struct {
	baseFunctionClass
}

type builtinJSONObjectSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONObjectSig) Clone() builtinFunc {
	newSig := &builtinJSONObjectSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonObjectFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 0 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i += 2 {
		if args[i].GetType(ctx.GetEvalCtx()).EvalType() == types.ETString && args[i].GetType(ctx.GetEvalCtx()).GetCharset() == charset.CharsetBin {
			return nil, types.ErrInvalidJSONCharset.GenWithStackByArgs(args[i].GetType(ctx.GetEvalCtx()).GetCharset())
		}
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONObjectSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonObjectSig)
	return sig, nil
}

func (b *builtinJSONObjectSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	if len(b.args)&1 == 1 {
		err = ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return res, true, err
	}
	jsons := make(map[string]any, len(b.args)>>1)
	var key string
	var value types.BinaryJSON
	for i, arg := range b.args {
		if i&1 == 0 {
			key, isNull, err = arg.EvalString(ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				return res, true, types.ErrJSONDocumentNULLKey
			}
		} else {
			value, isNull, err = arg.EvalJSON(ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				value = types.CreateBinaryJSON(nil)
			}
			jsons[key] = value
		}
	}
	bj, err := types.CreateBinaryJSONWithCheck(jsons)
	if err != nil {
		return res, true, err
	}
	return bj, false, nil
}

type jsonArrayFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArraySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONArraySig) Clone() builtinFunc {
	newSig := &builtinJSONArraySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonArrayFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
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
	for i := range args {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArraySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArraySig)
	return sig, nil
}

func (b *builtinJSONArraySig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	jsons := make([]any, 0, len(b.args))
	for _, arg := range b.args {
		j, isNull, err := arg.EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			j = types.CreateBinaryJSON(nil)
		}
		jsons = append(jsons, j)
	}
	bj, err := types.CreateBinaryJSONWithCheck(jsons)
	if err != nil {
		return res, true, err
	}
	return bj, false, nil
}

