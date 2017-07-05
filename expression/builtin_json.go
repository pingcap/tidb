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
	djson, err := datum2JSON(args[0], sc)
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
		err = errIncorrectParameterCount.GenByArgs(ast.JSONObject)
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
	baseBuiltinFunc
}

func (c *jsonTypeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONTypeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONTypeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONType(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

type builtinJSONExtractSig struct {
	baseBuiltinFunc
}

func (c *jsonExtractFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONExtractSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONExtractSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONExtract(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonUnquoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONUnquoteSig struct {
	baseBuiltinFunc
}

func (c *jsonUnquoteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONUnquoteSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONUnquoteSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONUnquote(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonSetFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSetSig struct {
	baseBuiltinFunc
}

func (c *jsonSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONSetSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONSet(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONInsertSig struct {
	baseBuiltinFunc
}

func (c *jsonInsertFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONInsertSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONInsertSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONInsert(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonReplaceFunctionClass struct {
	baseFunctionClass
}

type builtinJSONReplaceSig struct {
	baseBuiltinFunc
}

func (c *jsonReplaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONReplaceSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONReplaceSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONReplace(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonRemoveFunctionClass struct {
	baseFunctionClass
}

type builtinJSONRemoveSig struct {
	baseBuiltinFunc
}

func (c *jsonRemoveFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONRemoveSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONRemoveSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONRemove(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonMergeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONMergeSig struct {
	baseBuiltinFunc
}

func (c *jsonMergeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONMergeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONMergeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONMerge(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonObjectFunctionClass struct {
	baseFunctionClass
}

type builtinJSONObjectSig struct {
	baseBuiltinFunc
}

func (c *jsonObjectFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONObjectSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONObjectSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONObject(args, b.ctx.GetSessionVars().StmtCtx)
}

type jsonArrayFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArraySig struct {
	baseBuiltinFunc
}

func (c *jsonArrayFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONArraySig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

func (b *builtinJSONArraySig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	return JSONArray(args, b.ctx.GetSessionVars().StmtCtx)
}
