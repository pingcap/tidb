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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	json2 "encoding/json"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &jsonTypeFunctionClass{}
	_ functionClass = &jsonExtractFunctionClass{}
	_ functionClass = &jsonUnquoteFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSetFunctionClass{}
	_ functionClass = &jsonInsertFunctionClass{}
	_ functionClass = &jsonReplaceFunctionClass{}
	_ functionClass = &jsonRemoveFunctionClass{}
	_ functionClass = &jsonMergeFunctionClass{}
	_ functionClass = &jsonObjectFunctionClass{}
	_ functionClass = &jsonArrayFunctionClass{}
	_ functionClass = &jsonContainsFunctionClass{}
	_ functionClass = &jsonContainsPathFunctionClass{}
	_ functionClass = &jsonValidFunctionClass{}
	_ functionClass = &jsonArrayAppendFunctionClass{}
	_ functionClass = &jsonArrayInsertFunctionClass{}
	_ functionClass = &jsonMergePatchFunctionClass{}
	_ functionClass = &jsonMergePreserveFunctionClass{}
	_ functionClass = &jsonPrettyFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSearchFunctionClass{}
	_ functionClass = &jsonStorageSizeFunctionClass{}
	_ functionClass = &jsonDepthFunctionClass{}
	_ functionClass = &jsonKeysFunctionClass{}
	_ functionClass = &jsonLengthFunctionClass{}

	_ builtinFunc = &builtinJSONTypeSig{}
	_ builtinFunc = &builtinJSONQuoteSig{}
	_ builtinFunc = &builtinJSONUnquoteSig{}
	_ builtinFunc = &builtinJSONArraySig{}
	_ builtinFunc = &builtinJSONArrayAppendSig{}
	_ builtinFunc = &builtinJSONArrayInsertSig{}
	_ builtinFunc = &builtinJSONObjectSig{}
	_ builtinFunc = &builtinJSONExtractSig{}
	_ builtinFunc = &builtinJSONSetSig{}
	_ builtinFunc = &builtinJSONInsertSig{}
	_ builtinFunc = &builtinJSONReplaceSig{}
	_ builtinFunc = &builtinJSONRemoveSig{}
	_ builtinFunc = &builtinJSONMergeSig{}
	_ builtinFunc = &builtinJSONContainsSig{}
	_ builtinFunc = &builtinJSONStorageSizeSig{}
	_ builtinFunc = &builtinJSONDepthSig{}
	_ builtinFunc = &builtinJSONSearchSig{}
	_ builtinFunc = &builtinJSONKeysSig{}
	_ builtinFunc = &builtinJSONKeys2ArgsSig{}
	_ builtinFunc = &builtinJSONLengthSig{}
	_ builtinFunc = &builtinJSONValidJSONSig{}
	_ builtinFunc = &builtinJSONValidStringSig{}
	_ builtinFunc = &builtinJSONValidOthersSig{}
)

type jsonTypeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONTypeSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONTypeSig) Clone() builtinFunc {
	newSig := &builtinJSONTypeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonTypeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	bf.tp.Charset, bf.tp.Collate = ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.Flen = 51 // Flen of JSON_TYPE is length of UNSIGNED INTEGER.
	sig := &builtinJSONTypeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonTypeSig)
	return sig, nil
}

func (b *builtinJSONTypeSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	var j json.BinaryJSON
	j, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return j.Type(), false, nil
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

type builtinJSONExtractSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONExtractSig) Clone() builtinFunc {
	newSig := &builtinJSONExtractSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonExtractFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONExtractSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinJSONExtractSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return
	}
	pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(b.ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err := json.ParseJSONPathExpr(s)
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
}

func (b *builtinJSONUnquoteSig) Clone() builtinFunc {
	newSig := &builtinJSONUnquoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonUnquoteFunctionClass) verifyArgs(args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	if evalType := args[0].GetType().EvalType(); evalType != types.ETString && evalType != types.ETJson {
		return ErrIncorrectType.GenWithStackByArgs("1", "json_unquote")
	}
	return nil
}

func (c *jsonUnquoteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = mysql.MaxFieldVarCharLength
	DisableParseJSONFlag4Expr(args[0])
	sig := &builtinJSONUnquoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonUnquoteSig)
	return sig, nil
}

func (b *builtinJSONUnquoteSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	str, err = json.UnquoteString(str)
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
}

func (b *builtinJSONSetSig) Clone() builtinFunc {
	newSig := &builtinJSONSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSetSig)
	return sig, nil
}

func (b *builtinJSONSetSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(b.ctx, b.args, row, json.ModifySet)
	return res, isNull, err
}

type jsonInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONInsertSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONInsertSig) Clone() builtinFunc {
	newSig := &builtinJSONInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonInsertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonInsertSig)
	return sig, nil
}

func (b *builtinJSONInsertSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(b.ctx, b.args, row, json.ModifyInsert)
	return res, isNull, err
}

type jsonReplaceFunctionClass struct {
	baseFunctionClass
}

type builtinJSONReplaceSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONReplaceSig) Clone() builtinFunc {
	newSig := &builtinJSONReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonReplaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonReplaceSig)
	return sig, nil
}

func (b *builtinJSONReplaceSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(b.ctx, b.args, row, json.ModifyReplace)
	return res, isNull, err
}

type jsonRemoveFunctionClass struct {
	baseFunctionClass
}

type builtinJSONRemoveSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONRemoveSig) Clone() builtinFunc {
	newSig := &builtinJSONRemoveSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonRemoveFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONRemoveSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonRemoveSig)
	return sig, nil
}

func (b *builtinJSONRemoveSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(b.ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
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

type builtinJSONMergeSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONMergeSig) Clone() builtinFunc {
	newSig := &builtinJSONMergeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonMergeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergeSig)
	return sig, nil
}

func (b *builtinJSONMergeSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	values := make([]json.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		var value json.BinaryJSON
		value, isNull, err = arg.EvalJSON(b.ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		values = append(values, value)
	}
	res = json.MergeBinary(values)
	// function "JSON_MERGE" is deprecated since MySQL 5.7.22. Synonym for function "JSON_MERGE_PRESERVE".
	// See https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("JSON_MERGE"))
	}
	return res, false, nil
}

type jsonObjectFunctionClass struct {
	baseFunctionClass
}

type builtinJSONObjectSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONObjectSig) Clone() builtinFunc {
	newSig := &builtinJSONObjectSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonObjectFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 0 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 1; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONObjectSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonObjectSig)
	return sig, nil
}

func (b *builtinJSONObjectSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	if len(b.args)&1 == 1 {
		err = ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return res, true, err
	}
	jsons := make(map[string]interface{}, len(b.args)>>1)
	var key string
	var value json.BinaryJSON
	for i, arg := range b.args {
		if i&1 == 0 {
			key, isNull, err = arg.EvalString(b.ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				err = errors.New("JSON documents may not contain NULL member names")
				return res, true, err
			}
		} else {
			value, isNull, err = arg.EvalJSON(b.ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				value = json.CreateBinary(nil)
			}
			jsons[key] = value
		}
	}
	return json.CreateBinary(jsons), false, nil
}

type jsonArrayFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArraySig struct {
	baseBuiltinFunc
}

func (b *builtinJSONArraySig) Clone() builtinFunc {
	newSig := &builtinJSONArraySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonArrayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := range args {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONArraySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArraySig)
	return sig, nil
}

func (b *builtinJSONArraySig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	jsons := make([]interface{}, 0, len(b.args))
	for _, arg := range b.args {
		j, isNull, err := arg.EvalJSON(b.ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			j = json.CreateBinary(nil)
		}
		jsons = append(jsons, j)
	}
	return json.CreateBinary(jsons), false, nil
}

type jsonContainsPathFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsPathSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONContainsPathSig) Clone() builtinFunc {
	newSig := &builtinJSONContainsPathSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsPathFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson, types.ETString}
	for i := 3; i <= len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONContainsPathSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsPathSig)
	return sig, nil
}

func (b *builtinJSONContainsPathSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType = strings.ToLower(containType)
	if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
		return res, true, json.ErrInvalidJSONContainsPathType
	}
	var pathExpr json.PathExpression
	contains := int64(1)
	for i := 2; i < len(b.args); i++ {
		path, isNull, err := b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		if pathExpr, err = json.ParseJSONPathExpr(path); err != nil {
			return res, true, err
		}
		_, exists := obj.Extract([]json.PathExpression{pathExpr})
		switch {
		case exists && containType == json.ContainsPathOne:
			return 1, false, nil
		case !exists && containType == json.ContainsPathOne:
			contains = 0
		case !exists && containType == json.ContainsPathAll:
			return 0, false, nil
		}
	}
	return contains, false, nil
}

func jsonModify(ctx sessionctx.Context, args []Expression, row chunk.Row, mt json.ModifyType) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]json.PathExpression, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i += 2 {
		// TODO: We can cache pathExprs if args are constants.
		var s string
		s, isNull, err = args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	values := make([]json.BinaryJSON, 0, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		var value json.BinaryJSON
		value, isNull, err = args[i].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			value = json.CreateBinary(nil)
		}
		values = append(values, value)
	}
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonContainsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONContainsSig) Clone() builtinFunc {
	newSig := &builtinJSONContainsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsFunctionClass) verifyArgs(args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	if evalType := args[0].GetType().EvalType(); evalType != types.ETJson && evalType != types.ETString {
		return json.ErrInvalidJSONData.GenWithStackByArgs(1, "json_contains")
	}
	if evalType := args[1].GetType().EvalType(); evalType != types.ETJson && evalType != types.ETString {
		return json.ErrInvalidJSONData.GenWithStackByArgs(2, "json_contains")
	}
	return nil
}

func (c *jsonContainsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONContainsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsSig)
	return sig, nil
}

func (b *builtinJSONContainsSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.args[1].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	var pathExpr json.PathExpression
	if len(b.args) == 3 {
		path, isNull, err := b.args[2].EvalString(b.ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err = json.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard
		}
		var exists bool
		obj, exists = obj.Extract([]json.PathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
	}

	if json.ContainsBinary(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonValidFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	var sig builtinFunc
	argType := args[0].GetType().EvalType()
	switch argType {
	case types.ETJson:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETJson)
		sig = &builtinJSONValidJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidJsonSig)
	case types.ETString:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
		sig = &builtinJSONValidStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidStringSig)
	default:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argType)
		sig = &builtinJSONValidOthersSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidOthersSig)
	}
	return sig, nil
}

type builtinJSONValidJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONValidJSONSig) Clone() builtinFunc {
	newSig := &builtinJSONValidJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidJSONSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	_, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	return 1, isNull, err
}

type builtinJSONValidStringSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONValidStringSig) Clone() builtinFunc {
	newSig := &builtinJSONValidStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidStringSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}

	data := hack.Slice(val)
	if json2.Valid(data) {
		res = 1
	} else {
		res = 0
	}
	return res, false, nil
}

type builtinJSONValidOthersSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONValidOthersSig) Clone() builtinFunc {
	newSig := &builtinJSONValidOthersSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidOthersSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidOthersSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	return 0, false, nil
}

type jsonArrayAppendFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayAppendSig struct {
	baseBuiltinFunc
}

func (c *jsonArrayAppendFunctionClass) verifyArgs(args []Expression) error {
	if len(args) < 3 || (len(args)&1 != 1) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	return nil
}

func (c *jsonArrayAppendFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
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

func (b *builtinJSONArrayAppendSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			return res, true, err
		}

		// We should do the following checks to get correct values in res.Extract
		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, json.ErrInvalidJSONPath.GenWithStackByArgs(s)
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard.GenWithStackByArgs(s)
		}

		obj, exists := res.Extract([]json.PathExpression{pathExpr})
		if !exists {
			// If path not exists, just do nothing and no errors.
			continue
		}

		if obj.TypeCode != json.TypeCodeArray {
			// res.Extract will return a json object instead of an array if there is an object at path pathExpr.
			// JSON_ARRAY_APPEND({"a": "b"}, "$", {"b": "c"}) => [{"a": "b"}, {"b", "c"}]
			// We should wrap them to a single array first.
			obj = json.CreateBinary([]interface{}{obj})
		}

		value, isnull, err := b.args[i+1].EvalJSON(b.ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = json.CreateBinary(nil)
		}

		obj = json.MergeBinary([]json.BinaryJSON{obj, value})
		res, err = res.Modify([]json.PathExpression{pathExpr}, []json.BinaryJSON{obj}, json.ModifySet)
		if err != nil {
			// We checked pathExpr in the same way as res.Modify do.
			// So err should always be nil, the function should never return here.
			return res, true, err
		}
	}
	return res, false, nil
}

type jsonArrayInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayInsertSig struct {
	baseBuiltinFunc
}

func (c *jsonArrayInsertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
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

func (b *builtinJSONArrayInsertSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.args[i].EvalString(b.ctx, row)
		if err != nil || isNull {
			return res, true, err
		}

		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, json.ErrInvalidJSONPath.GenWithStackByArgs(s)
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard.GenWithStackByArgs(s)
		}

		value, isnull, err := b.args[i+1].EvalJSON(b.ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = json.CreateBinary(nil)
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

func (c *jsonMergePatchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_MERGE_PATCH")
}

type jsonMergePreserveFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePreserveFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePreserveSig)
	return sig, nil
}

type jsonPrettyFunctionClass struct {
	baseFunctionClass
}

func (c *jsonPrettyFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_PRETTY")
}

type jsonQuoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONQuoteSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONQuoteSig) Clone() builtinFunc {
	newSig := &builtinJSONQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonQuoteFunctionClass) verifyArgs(args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	if evalType := args[0].GetType().EvalType(); evalType != types.ETString {
		return ErrIncorrectType.GenWithStackByArgs("1", "json_quote")
	}
	return nil
}

func (c *jsonQuoteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	DisableParseJSONFlag4Expr(args[0])
	sig := &builtinJSONQuoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonQuoteSig)
	return sig, nil
}

func (b *builtinJSONQuoteSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return strconv.Quote(str), false, nil
}

type jsonSearchFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSearchSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONSearchSig) Clone() builtinFunc {
	newSig := &builtinJSONSearchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSearchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// json_doc, one_or_all, search_str[, escape_char[, path] ...])
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONSearchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSearchSig)
	return sig, nil
}

func (b *builtinJSONSearchSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	// json_doc
	var obj json.BinaryJSON
	obj, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// one_or_all
	var containType string
	containType, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
		return res, true, errors.AddStack(json.ErrInvalidJSONContainsPathType)
	}

	// search_str & escape_char
	var searchStr string
	searchStr, isNull, err = b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	escape := byte('\\')
	if len(b.args) >= 4 {
		var escapeStr string
		escapeStr, isNull, err = b.args[3].EvalString(b.ctx, row)
		if err != nil {
			return res, isNull, err
		}
		if isNull || len(escapeStr) == 0 {
			escape = byte('\\')
		} else if len(escapeStr) == 1 {
			escape = escapeStr[0]
		} else {
			return res, true, errIncorrectArgs.GenWithStackByArgs("ESCAPE")
		}
	}
	patChars, patTypes := stringutil.CompilePattern(searchStr, escape)

	// result
	result := make([]interface{}, 0)

	// walk json_doc
	walkFn := func(fullpath json.PathExpression, bj json.BinaryJSON) (stop bool, err error) {
		if bj.TypeCode == json.TypeCodeString && stringutil.DoMatch(string(bj.GetString()), patChars, patTypes) {
			result = append(result, fullpath.String())
			if containType == json.ContainsPathOne {
				return true, nil
			}
		}
		return false, nil
	}
	if len(b.args) >= 5 { // path...
		pathExprs := make([]json.PathExpression, 0, len(b.args)-4)
		for i := 4; i < len(b.args); i++ {
			var s string
			s, isNull, err = b.args[i].EvalString(b.ctx, row)
			if isNull || err != nil {
				return res, isNull, err
			}
			var pathExpr json.PathExpression
			pathExpr, err = json.ParseJSONPathExpr(s)
			if err != nil {
				return res, true, err
			}
			pathExprs = append(pathExprs, pathExpr)
		}
		err = obj.Walk(walkFn, pathExprs...)
		if err != nil {
			return res, true, err
		}
	} else {
		err = obj.Walk(walkFn)
		if err != nil {
			return res, true, err
		}
	}

	// return
	switch len(result) {
	case 0:
		return res, true, nil
	case 1:
		return json.CreateBinary(result[0]), false, nil
	default:
		return json.CreateBinary(result), false, nil
	}
}

type jsonStorageSizeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONStorageSizeSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONStorageSizeSig) Clone() builtinFunc {
	newSig := &builtinJSONStorageSizeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonStorageSizeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETJson)
	sig := &builtinJSONStorageSizeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonStorageSizeSig)
	return sig, nil
}

func (b *builtinJSONStorageSizeSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	buf, err := obj.MarshalJSON()
	if err != nil {
		return res, isNull, err
	}

	return int64(len(buf)), false, nil
}

type jsonDepthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONDepthSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONDepthSig) Clone() builtinFunc {
	newSig := &builtinJSONDepthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonDepthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETJson)
	sig := &builtinJSONDepthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonDepthSig)
	return sig, nil
}

func (b *builtinJSONDepthSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return int64(obj.GetElemDepth()), false, nil
}

type jsonKeysFunctionClass struct {
	baseFunctionClass
}

func (c *jsonKeysFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson}
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	var sig builtinFunc
	switch len(args) {
	case 1:
		sig = &builtinJSONKeysSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeysSig)
	case 2:
		sig = &builtinJSONKeys2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeys2ArgsSig)
	}
	return sig, nil
}

type builtinJSONKeysSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONKeysSig) Clone() builtinFunc {
	newSig := &builtinJSONKeysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeysSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if res.TypeCode != json.TypeCodeObject {
		return res, true, nil
	}
	return res.GetKeys(), false, nil
}

type builtinJSONKeys2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONKeys2ArgsSig) Clone() builtinFunc {
	newSig := &builtinJSONKeys2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeys2ArgsSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	path, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	pathExpr, err := json.ParseJSONPathExpr(path)
	if err != nil {
		return res, true, err
	}
	if pathExpr.ContainsAnyAsterisk() {
		return res, true, json.ErrInvalidJSONPathWildcard
	}

	res, exists := res.Extract([]json.PathExpression{pathExpr})
	if !exists {
		return res, true, nil
	}
	if res.TypeCode != json.TypeCodeObject {
		return res, true, nil
	}

	return res.GetKeys(), false, nil
}

type jsonLengthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONLengthSig) Clone() builtinFunc {
	newSig := &builtinJSONLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonLengthSig)
	return sig, nil
}

func (b *builtinJSONLengthSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
		return 1, false, nil
	}

	if len(b.args) == 2 {
		path, isNull, err := b.args[1].EvalString(b.ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}

		pathExpr, err := json.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard
		}

		var exists bool
		obj, exists = obj.Extract([]json.PathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
		if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
			return 1, false, nil
		}
	}
	return int64(obj.GetElemCount()), false, nil
}
