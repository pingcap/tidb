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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	// castJSONDirectly is used for really cast to JSON.
	castJSONDirectly int = 0
	// castJSONPostWrapped is used for post-wrapped cast to JSON.
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

	_ builtinFunc = &builtinJSONTypeSig{}
	_ builtinFunc = &builtinJSONUnquoteSig{}
	_ builtinFunc = &builtinJSONArraySig{}
	_ builtinFunc = &builtinJSONObjectSig{}
	_ builtinFunc = &builtinJSONExtractSig{}
	_ builtinFunc = &builtinJSONSetSig{}
	_ builtinFunc = &builtinJSONInsertSig{}
	_ builtinFunc = &builtinJSONReplaceSig{}
	_ builtinFunc = &builtinJSONRemoveSig{}
	_ builtinFunc = &builtinJSONMergeSig{}
)

// argsAnyNull returns true if args contains any null.
func argsAnyNull(args []types.Datum) bool {
	for _, arg := range args {
		if arg.Kind() == types.KindNull {
			return true
		}
	}
	return false
}

// datum2JSON gets or converts to JSON from datum.
func datum2JSON(d types.Datum, sc *variable.StatementContext) (j json.JSON, err error) {
	tp := types.NewFieldType(mysql.TypeJSON)
	if d, err = d.ConvertTo(sc, tp); err == nil {
		j = d.GetMysqlJSON()
	}
	return j, errors.Trace(err)
}

// parsePathExprs parses strings in datums into json.PathExpression.
func parsePathExprs(datums []types.Datum) ([]json.PathExpression, error) {
	pathExprs := make([]json.PathExpression, 0, len(datums))
	for _, datum := range datums {
		pathExpr, err := json.ParseJSONPathExpr(datum.GetString())
		if err != nil {
			return nil, errors.Trace(err)
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	return pathExprs, nil
}

// createJSONFromDatums creates JSONs from Datums.
func createJSONFromDatums(datums []types.Datum) (jsons []json.JSON, err error) {
	jsons = make([]json.JSON, 0, len(datums))
	for _, datum := range datums {
		j, err := datum.ToMysqlJSON()
		if err != nil {
			return jsons, errors.Trace(err)
		}
		jsons = append(jsons, j)
	}
	return jsons, nil
}

// jsonModify is the portal for modify JSON with path expressions and values.
// If the first argument is null, returns null;
// If any path expressions in arguments are null, return null;
func jsonModify(args []types.Datum, mt json.ModifyType, sc *variable.StatementContext) (d types.Datum, err error) {
	// alloc 1 extra element, for len(args) is an even number.
	pes := make([]types.Datum, 0, (len(args)-1)/2+1)
	vs := make([]types.Datum, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i++ {
		if i&1 == 1 {
			pes = append(pes, args[i])
		} else {
			vs = append(vs, args[i])
		}
	}
	if args[0].Kind() == types.KindNull || argsAnyNull(pes) {
		return d, nil
	}

	j, err := datum2JSON(args[0], sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	pathExprs, err := parsePathExprs(pes)
	if err != nil {
		return d, errors.Trace(err)
	}
	values, err := createJSONFromDatums(vs)
	if err != nil {
		return d, errors.Trace(err)
	}
	j, err = j.Modify(pathExprs, values, mt)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlJSON(j)
	return d, nil
}

// jsonModify1 is similar with `jsonModify`, but for new framework.
func jsonModify1(args []Expression, row []types.Datum, mt json.ModifyType, sc *variable.StatementContext) (res json.JSON, isNull bool, err error) {
	res, isNull, err = args[0].EvalJSON(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}
	pathExprs := make([]json.PathExpression, 0, (len(args)-1)/2+1)
	values := make([]json.JSON, 0, (len(args)-1)/2+1)
	for i, arg := range args[1:] {
		if i&1 == 0 {
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
		} else {
			var value json.JSON
			value, isNull, err = arg.EvalJSON(row, sc)
			if err != nil {
				return res, true, errors.Trace(err)
			}
			if isNull {
				value = json.CreateJSON(nil)
			}
			values = append(values, value)
		}
	}
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	return res, false, nil
}

// JSONType is for json_type builtin function.
func JSONType(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	if argsAnyNull(args) {
		return d, nil
	}
	djson, err := datum2JSON(args[0], sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(djson.Type())
	return
}

// JSONExtract is for json_extract builtin function.
func JSONExtract(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	if argsAnyNull(args) {
		return d, nil
	}
	djson, err := datum2JSON(args[0], sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	pathExprs, err := parsePathExprs(args[1:])
	if err != nil {
		return d, errors.Trace(err)
	}
	if djson1, found := djson.Extract(pathExprs); found {
		d.SetMysqlJSON(djson1)
	}
	return
}

// JSONUnquote is for json_unquote builtin function.
func JSONUnquote(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	if argsAnyNull(args) {
		return d, nil
	}
	djson, err := args[0].ToMysqlJSON()
	if err != nil {
		return d, errors.Trace(err)
	}
	unquoted, err := djson.Unquote()
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(unquoted)
	return
}

// JSONSet is for json_set builtin function.
func JSONSet(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	return jsonModify(args, json.ModifySet, sc)
}

// JSONInsert is for json_insert builtin function.
func JSONInsert(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	return jsonModify(args, json.ModifyInsert, sc)
}

// JSONReplace is for json_replace builtin function.
func JSONReplace(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	return jsonModify(args, json.ModifyReplace, sc)
}

// JSONRemove is for json_remove builtin function.
func JSONRemove(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	if argsAnyNull(args) {
		return d, nil
	}
	j, err := datum2JSON(args[0], sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	pathExprs, err := parsePathExprs(args[1:])
	if err != nil {
		return d, errors.Trace(err)
	}
	j, err = j.Remove(pathExprs)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlJSON(j)
	return
}

// JSONMerge is for json_merge builtin function.
func JSONMerge(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	if argsAnyNull(args) {
		return d, nil
	}
	jsons := make([]json.JSON, 0, len(args))
	for _, arg := range args {
		j, err := datum2JSON(arg, sc)
		if err != nil {
			return d, errors.Trace(err)
		}
		jsons = append(jsons, j)
	}
	d.SetMysqlJSON(jsons[0].Merge(jsons[1:]))
	return
}

// JSONObject creates a json from an ordered key-value slice. It retrieves 2 arguments at least.
func JSONObject(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	if len(args)&1 == 1 {
		err = ErrIncorrectParameterCount.GenByArgs(ast.JSONObject)
		return
	}
	var jsonMap = make(map[string]json.JSON, len(args)>>1)
	var keyTp = types.NewFieldType(mysql.TypeVarchar)
	for i := 0; i < len(args); i += 2 {
		if args[i].Kind() == types.KindNull {
			err = errors.New("JSON documents may not contain NULL member names")
			return
		}
		key, err := args[i].ConvertTo(sc, keyTp)
		if err != nil {
			return d, errors.Trace(err)
		}
		value, err := args[i+1].ToMysqlJSON()
		if err != nil {
			return d, errors.Trace(err)
		}
		jsonMap[key.GetString()] = value
	}
	j := json.CreateJSON(jsonMap)
	d.SetMysqlJSON(j)
	return
}

// JSONArray creates a json from a slice.
func JSONArray(args []types.Datum, sc *variable.StatementContext) (d types.Datum, err error) {
	jsons, err := createJSONFromDatums(args)
	if err != nil {
		return d, errors.Trace(err)
	}
	j := json.CreateJSON(jsons)
	d.SetMysqlJSON(j)
	return
}

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
	bf.tp.Charset, bf.tp.Collate = mysql.DefaultCharset, mysql.DefaultCollationName
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONTypeSig{baseStringBuiltinFunc{bf}}
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
	tps := make([]evalTp, 0, len(args))
	tps = append(tps, tpJSON)
	for range args[1:] {
		tps = append(tps, tpString)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONExtractSig{baseJSONBuiltinFunc{bf}}
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
	bf.tp.Charset, bf.tp.Collate = mysql.DefaultCharset, mysql.DefaultCollationName
	sig := &builtinJSONUnquoteSig{baseStringBuiltinFunc{bf}}
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
	tps := make([]evalTp, 0, len(args))
	tps = append(tps, tpJSON)
	for i := range args[1:] {
		if i&1 == 0 {
			tps = append(tps, tpString)
		} else {
			tps = append(tps, tpJSON)
		}
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONSetSig{baseJSONBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

func (b *builtinJSONSetSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	return jsonModify1(b.args, row, json.ModifySet, sc)
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
	tps := make([]evalTp, 0, len(args))
	tps = append(tps, tpJSON)
	for i := range args[1:] {
		if i&1 == 0 {
			tps = append(tps, tpString)
		} else {
			tps = append(tps, tpJSON)
		}
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONInsertSig{baseJSONBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

func (b *builtinJSONInsertSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	return jsonModify1(b.args, row, json.ModifyInsert, sc)
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
	tps := make([]evalTp, 0, len(args))
	tps = append(tps, tpJSON)
	for i := range args[1:] {
		if i&1 == 0 {
			tps = append(tps, tpString)
		} else {
			tps = append(tps, tpJSON)
		}
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONReplaceSig{baseJSONBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

func (b *builtinJSONReplaceSig) evalJSON(row []types.Datum) (res json.JSON, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	return jsonModify1(b.args, row, json.ModifyReplace, sc)
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
	tps := make([]evalTp, 0, len(args))
	tps = append(tps, tpJSON)
	for range args[1:] {
		tps = append(tps, tpString)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	args[0].GetType().Decimal = castJSONDirectly
	sig := &builtinJSONRemoveSig{baseJSONBuiltinFunc{bf}}
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
		pathExpr, err := json.ParseJSONPathExpr(s)
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
	tps := make([]evalTp, 0, len(args))
	for range args {
		tps = append(tps, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	for i := range args {
		args[i].GetType().Decimal = castJSONDirectly
	}
	sig := &builtinJSONMergeSig{baseJSONBuiltinFunc{bf}}
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
	tps := make([]evalTp, 0, len(args))
	for i := range args {
		if i&1 == 0 {
			tps = append(tps, tpString)
		} else {
			tps = append(tps, tpJSON)
		}
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	sig := &builtinJSONObjectSig{baseJSONBuiltinFunc{bf}}
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
	tps := make([]evalTp, 0, len(args))
	for range args {
		tps = append(tps, tpJSON)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpJSON, tps...)
	sig := &builtinJSONArraySig{baseJSONBuiltinFunc{bf}}
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
