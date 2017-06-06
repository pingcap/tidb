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
	_ functionClass = &jsonMergeFunctionClass{}
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
func createJSONFromDatums(datums []types.Datum) ([]json.JSON, error) {
	jsons := make([]json.JSON, 0, len(datums))
	for _, datum := range datums {
		// TODO: Here semantic of creating JSON from datum is the same with types.compareMysqlJSON.
		// But it's different from "CAST semantic" because for string, cast will parse it into
		// JSON but here and compareMysqlJSON needs a JSON with the string as primitives.
		// We should rewrite this as a function for here and compareMysqlJSON both can use it.
		var j json.JSON
		switch datum.Kind() {
		case types.KindNull:
			j = json.CreateJSON(nil)
		case types.KindMysqlJSON:
			j = datum.GetMysqlJSON()
		case types.KindInt64, types.KindUint64:
			j = json.CreateJSON(datum.GetInt64())
		case types.KindFloat32, types.KindFloat64:
			j = json.CreateJSON(datum.GetFloat64())
		case types.KindMysqlDecimal:
			f64, err := datum.GetMysqlDecimal().ToFloat64()
			if err != nil {
				return jsons, errors.Trace(err)
			}
			j = json.CreateJSON(f64)
		case types.KindString, types.KindBytes:
			j = json.CreateJSON(datum.GetString())
		default:
			s, err := datum.ToString()
			if err != nil {
				return jsons, errors.Trace(err)
			}
			j = json.CreateJSON(s)
		}
		jsons = append(jsons, j)
	}
	return jsons, nil
}

// jsonModify is the portal for modify JSON with path expressions and values.
func jsonModify(b baseBuiltinFunc, row []types.Datum, mt json.ModifyType) (j json.JSON, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return j, errors.Trace(err)
	}
	if argsAnyNull(args) {
		return j, nil
	}
	j, err = datum2JSON(args[0], b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return j, errors.Trace(err)
	}

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
	pathExprs, err := parsePathExprs(pes)
	if err != nil {
		return j, errors.Trace(err)
	}
	values, err := createJSONFromDatums(vs)
	if err != nil {
		return j, errors.Trace(err)
	}

	j, err = j.Modify(pathExprs, values, mt)
	if err != nil {
		return j, errors.Trace(err)
	}
	return j, nil
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
	if argsAnyNull(args) {
		return d, nil
	}
	djson, err := datum2JSON(args[0], b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(djson.Type())
	return
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
	if argsAnyNull(args) {
		return d, nil
	}
	djson, err := datum2JSON(args[0], b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	pathExprs, err := parsePathExprs(args[1:])
	if err != nil {
		return d, errors.Trace(err)
	}
	if djson, found := djson.Extract(pathExprs); found {
		d.SetMysqlJSON(djson)
	}
	return
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
	if argsAnyNull(args) {
		return d, nil
	}
	djson, err := datum2JSON(args[0], b.ctx.GetSessionVars().StmtCtx)
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
	j, err := jsonModify(b.baseBuiltinFunc, row, json.ModifySet)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlJSON(j)
	return
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
	j, err := jsonModify(b.baseBuiltinFunc, row, json.ModifyInsert)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlJSON(j)
	return
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
	j, err := jsonModify(b.baseBuiltinFunc, row, json.ModifyReplace)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlJSON(j)
	return
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
	if argsAnyNull(args) {
		return d, nil
	}
	jsons := make([]json.JSON, 0, len(args))
	for _, arg := range args {
		j, err := datum2JSON(arg, b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		jsons = append(jsons, j)
	}
	d.SetMysqlJSON(jsons[0].Merge(jsons[1:]))
	return
}
