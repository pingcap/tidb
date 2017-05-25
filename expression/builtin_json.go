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
	"github.com/pingcap/tidb/util/types"
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
)

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
	sc := b.ctx.GetSessionVars().StmtCtx

	if args[0].Kind() != types.KindNull {
		var djson types.Datum
		djson, err = args[0].ConvertTo(sc, types.NewFieldType(mysql.TypeJSON))
		if err == nil {
			d.SetString(djson.GetMysqlJSON().Type())
		}
	}
	return d, errors.Trace(err)
}
