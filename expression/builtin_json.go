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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	// castJSONDirectly is used for really cast to JSON, which means for
	// string we should parse it into JSON but not use that as primitive.
	castJSONDirectly int = 0
	// castJSONPostWrapped is used for post-wrapped cast to JSON, which
	// means for string we should use that as primitive but not parse it.
	castJSONPostWrapped int = -1
)

// jsonFunctionNameToPB is for pushdown json functions to storage engine.
var jsonFunctionNameToPB = map[string]tipb.ExprType{
	ast.JSONType:     tipb.ExprType_JsonType,
	ast.JSONExtract:  tipb.ExprType_JsonExtract,
	ast.JSONUnquote:  tipb.ExprType_JsonUnquote,
	ast.JSONValid:    tipb.ExprType_JsonValid,
	ast.JSONObject:   tipb.ExprType_JsonObject,
	ast.JSONArray:    tipb.ExprType_JsonArray,
	ast.JSONMerge:    tipb.ExprType_JsonMerge,
	ast.JSONSet:      tipb.ExprType_JsonSet,
	ast.JSONInsert:   tipb.ExprType_JsonInsert,
	ast.JSONReplace:  tipb.ExprType_JsonReplace,
	ast.JSONRemove:   tipb.ExprType_JsonRemove,
	ast.JSONContains: tipb.ExprType_JsonContains,
}

var (
	_ functionClass = &jsonTypeFunctionClass{}
	_ functionClass = &jsonExtractFunctionClass{}
	_ functionClass = &jsonUnquoteFunctionClass{}
	_ functionClass = &jsonSetFunctionClass{}
	_ functionClass = &jsonInsertFunctionClass{}
	_ functionClass = &jsonReplaceFunctionClass{}
	_ functionClass = &jsonRemoveFunctionClass{}
	_ functionClass = &jsonMergeFunctionClass{}
	_ functionClass = &jsonObjectFunctionClass{}
	_ functionClass = &jsonArrayFunctionClass{}

	// Type of JSON value.
	_ builtinFunc = &builtinJSONTypeSig{}
	// Unquote JSON value.
	_ builtinFunc = &builtinJSONUnquoteSig{}
	// Create JSON array.
	_ builtinFunc = &builtinJSONArraySig{}
	// Create JSON object.
	_ builtinFunc = &builtinJSONObjectSig{}
	// Return data from JSON document.
	_ builtinFunc = &builtinJSONExtractSig{}
	// Insert data into JSON document.
	_ builtinFunc = &builtinJSONSetSig{}
	// Insert data into JSON document.
	_ builtinFunc = &builtinJSONInsertSig{}
	// Replace values in JSON document.
	_ builtinFunc = &builtinJSONReplaceSig{}
	// Remove data from JSON document.
	_ builtinFunc = &builtinJSONRemoveSig{}
	// Merge JSON documents, preserving duplicate keys.
	_ builtinFunc = &builtinJSONMergeSig{}
)

type jsonTypeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONTypeSig struct {
	baseStringBuiltinFunc
}

func (c *jsonTypeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpJSON)
	bf.tp.Flen = 51 // max length of UNSIGNED INTEGER.
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONTypeSig{baseStringBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonTypeSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONTypeSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	var j json.JSON
	j, isNull, err = b.args[0].EvalJSON(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	return j.Type(), false, nil
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

type builtinJSONExtractSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonExtractFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	argTps = append(argTps, tpJSON)
	for range args[1:] {
		argTps = append(argTps, tpString)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONExtractSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONExtractSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(row, sc)
		if isNull || err != nil {
			return res, isNull, errors.Trace(err)
		}
		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, errors.Trace(err)
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
	baseStringBuiltinFunc
}

func (c *jsonUnquoteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpJSON)
	sig := &builtinJSONUnquoteSig{baseStringBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonUnquoteSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONUnquoteSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	var j json.JSON
	j, isNull, err = b.args[0].EvalJSON(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	res, err = j.Unquote()
	return res, false, errors.Trace(err)
}

type jsonSetFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSetSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenByArgs(c.funcName)
	}
	argTps := make([]evalTp, 0, len(args))
	argTps = append(argTps, tpJSON)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, tpString, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONSetSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSetSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONSetSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	return jsonModify(b.args, row, json.ModifySet, sc)
}

type jsonInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONInsertSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonInsertFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenByArgs(c.funcName)
	}
	argTps := make([]evalTp, 0, len(args))
	argTps = append(argTps, tpJSON)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, tpString, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONInsertSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonInsertSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONInsertSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	return jsonModify(b.args, row, json.ModifyInsert, sc)
}

type jsonReplaceFunctionClass struct {
	baseFunctionClass
}

type builtinJSONReplaceSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonReplaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenByArgs(c.funcName)
	}
	argTps := make([]evalTp, 0, len(args))
	argTps = append(argTps, tpJSON)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, tpString, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONReplaceSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonReplaceSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONReplaceSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	return jsonModify(b.args, row, json.ModifyReplace, sc)
}

type jsonRemoveFunctionClass struct {
	baseFunctionClass
}

type builtinJSONRemoveSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonRemoveFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	argTps = append(argTps, tpJSON)
	for range args[1:] {
		argTps = append(argTps, tpString)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONRemoveSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonRemoveSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONRemoveSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	res, isNull, err = b.args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		var s string
		s, isNull, err = arg.EvalString(row, sc)
		if isNull || err != nil {
			return res, isNull, errors.Trace(err)
		}
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, errors.Trace(err)
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	res, err = res.Remove(pathExprs)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	return res, false, nil
}

type jsonMergeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONMergeSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonMergeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	for range args {
		argTps = append(argTps, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	for i := range args {
		args[i].GetType().Decimal = castJSONDirectly
	}
	sig := &builtinJSONMergeSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergeSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONMergeSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	values := make([]json.JSON, 0, len(b.args))
	for _, arg := range b.args {
		var value json.JSON
		value, isNull, err = arg.EvalJSON(row, sc)
		if isNull || err != nil {
			return res, isNull, errors.Trace(err)
		}
		values = append(values, value)
	}
	res = values[0].Merge(values[1:])
	return res, false, nil
}

type jsonObjectFunctionClass struct {
	baseFunctionClass
}

type builtinJSONObjectSig struct {
	baseJSONBuiltinFunc
}

func (c *jsonObjectFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	if len(args)&1 != 0 {
		return nil, ErrIncorrectParameterCount.GenByArgs(c.funcName)
	}
	argTps := make([]evalTp, 0, len(args))
	for i := 0; i < len(args)-1; i += 2 {
		argTps = append(argTps, tpString, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	sig := &builtinJSONObjectSig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonObjectSig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONObjectSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	if len(b.args)&1 == 1 {
		err = ErrIncorrectParameterCount.GenByArgs(ast.JSONObject)
		return res, true, errors.Trace(err)
	}
	sc := b.getCtx().GetSessionVars().StmtCtx
	jsons := make(map[string]json.JSON, len(b.args)>>1)
	var key string
	var value json.JSON
	for i, arg := range b.args {
		if i&1 == 0 {
			key, isNull, err = arg.EvalString(row, sc)
			if isNull {
				err = errors.New("JSON documents may not contain NULL member names")
				return res, true, errors.Trace(err)
			}
		} else {
			value, isNull, err = arg.EvalJSON(row, sc)
			if isNull {
				value = json.CreateJSON(nil)
			}
			jsons[key] = value
		}
	}
	return json.CreateJSON(jsons), false, nil
}

type jsonArrayFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArraySig struct {
	baseJSONBuiltinFunc
}

func (c *jsonArrayFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	for range args {
		argTps = append(argTps, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, argTps...)
	sig := &builtinJSONArraySig{baseJSONBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArraySig)
	return sig.setSelf(sig), nil
}

func (b *builtinJSONArraySig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	jsons := make([]json.JSON, 0, len(b.args))
	for _, arg := range b.args {
		j, isNull, err := arg.EvalJSON(row, b.getCtx().GetSessionVars().StmtCtx)
		if err != nil {
			return res, true, errors.Trace(err)
		}
		if isNull {
			j = json.CreateJSON(nil)
		}
		jsons = append(jsons, j)
	}
	return json.CreateJSON(jsons), false, nil
}

// jsonModify is similar with `jsonModify`, but for new framework.
func jsonModify(args []Expression, row []types.Datum, mt json.ModifyType, sc *variable.StatementContext) (res json.JSON, isNull bool, err error) {
	res, isNull, err = args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	pathExprs := make([]json.PathExpression, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i += 2 {
		// TODO: We can cache pathExprs if args are constants.
		var s string
		s, isNull, err = args[i].EvalString(row, sc)
		if isNull || err != nil {
			return res, isNull, errors.Trace(err)
		}
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, errors.Trace(err)
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	values := make([]json.JSON, 0, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		var value json.JSON
		value, isNull, err = args[i].EvalJSON(row, sc)
		if err != nil {
			return res, true, errors.Trace(err)
		}
		if isNull {
			value = json.CreateJSON(nil)
		}
		values = append(values, value)
	}
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	return res, false, nil
}
