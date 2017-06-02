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
)

// datum2JSON gets or converts internal JSON from datum.
func datum2JSON(d types.Datum, sc *variable.StatementContext) (j json.JSON, err error) {
	tp := types.NewFieldType(mysql.TypeJSON)
	if d, err = d.ConvertTo(sc, tp); err == nil {
		j = d.GetMysqlJSON()
	}
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
	var args []types.Datum
	if args, err = b.evalArgs(row); err == nil {
		if args[0].Kind() != types.KindNull {
			var djson json.JSON
			var sc = b.ctx.GetSessionVars().StmtCtx
			if djson, err = datum2JSON(args[0], sc); err == nil {
				d.SetString(djson.Type())
			}
		}
	}
	return d, errors.Trace(err)
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

	var djson json.JSON
	var pathExprs = make([]json.PathExpression, 0, len(args)-1)
	for i, arg := range args {
		if arg.Kind() == types.KindNull {
			d.SetNull()
			return
		}
		if i != 0 {
			pathExpr, err := json.ParseJSONPathExpr(arg.GetString())
			if err != nil {
				return d, errors.Trace(err)
			}
			pathExprs = append(pathExprs, pathExpr)
		} else {
			sc := b.ctx.GetSessionVars().StmtCtx
			if djson, err = datum2JSON(arg, sc); err != nil {
				return d, errors.Trace(err)
			}
		}
	}
	if djson, found := djson.Extract(pathExprs); found {
		d.SetMysqlJSON(djson)
	}
	return d, errors.Trace(err)
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
	var args []types.Datum
	if args, err = b.evalArgs(row); err == nil {
		if args[0].Kind() != types.KindNull {
			var djson json.JSON
			var sc = b.ctx.GetSessionVars().StmtCtx
			if djson, err = datum2JSON(args[0], sc); err == nil {
				var unquoted string
				if unquoted, err = djson.Unquote(); err == nil {
					d.SetString(unquoted)
				}
			}
		}
	}
	return d, errors.Trace(err)
}
