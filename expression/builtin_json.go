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
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
	"github.com/pingcap/tipb/go-tipb"
)

func argDatumToJson(jsond types.Datum, errfmt string, position int) (jsonObj json.Json, err error) {
	switch kind := jsond.Kind(); kind {
	case types.KindString, types.KindBytes:
		jsonObj = json.CreateJson(nil)
		jsonObj.ParseFromString(jsond.GetString())
	case types.KindMysqlJson:
		jsonObj = jsond.GetMysqlJson()
	case types.KindNull:
		jsonObj = json.CreateJson(nil)
		return
	default:
		err = json.ErrInvalidJsonData.Gen(errfmt, position)
		return
	}
	return
}

func argDatumToString(pathd types.Datum, errfmt string, position int) (path string, err error) {
	switch kind := pathd.Kind(); kind {
	case types.KindString, types.KindBytes:
		path = pathd.GetString()
	default:
		err = json.ErrInvalidJsonPath.Gen(errfmt, position)
		return
	}
	return
}

// JsonExtract do really json_extract(jsond, pathd) work.
func JsonExtract(jsond, pathd types.Datum) (d types.Datum, err error) {
	check_err := func() {
		if err != nil {
			return
		}
	}

	var jsonObj json.Json
	var path string

	jsonObj, err = argDatumToJson(jsond, "invalid json data in argument %d", 0)
	check_err()

	path, err = argDatumToString(pathd, "invalid json path in argument %d", 1)
	check_err()

	jsonObj, err = jsonObj.Extract(path)
	check_err()

	d.SetValue(jsonObj)
	return
}

var (
	_ functionClass = &jsonExtractFunctionClass{}
)

var jsonFunctionNameToPB = map[string]tipb.ExprType{
	ast.JsonType:     tipb.ExprType_JsonType,
	ast.JsonExtract:  tipb.ExprType_JsonExtract,
	ast.JsonValid:    tipb.ExprType_JsonValid,
	ast.JsonObject:   tipb.ExprType_JsonObject,
	ast.JsonArray:    tipb.ExprType_JsonArray,
	ast.JsonMerge:    tipb.ExprType_JsonMerge,
	ast.JsonSet:      tipb.ExprType_JsonSet,
	ast.JsonInsert:   tipb.ExprType_JsonInsert,
	ast.JsonReplace:  tipb.ExprType_JsonReplace,
	ast.JsonRemove:   tipb.ExprType_JsonRemove,
	ast.JsonContains: tipb.ExprType_JsonContains,
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

func (c *jsonExtractFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJsonExtractSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinJsonExtractSig struct {
	baseBuiltinFunc
}

func (b *builtinJsonExtractSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return JsonExtract(args[0], args[1])
}
