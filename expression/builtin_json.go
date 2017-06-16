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
	"github.com/pingcap/tidb/expression/xeval"
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
	_ functionClass = &jsonExtractFunctionClass{}
	_ functionClass = &jsonUnquoteFunctionClass{}
	_ functionClass = &jsonSetFunctionClass{}
	_ functionClass = &jsonInsertFunctionClass{}
	_ functionClass = &jsonReplaceFunctionClass{}
	_ functionClass = &jsonMergeFunctionClass{}
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
	return xeval.JSONType(args, b.ctx.GetSessionVars().StmtCtx)
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
	return xeval.JSONExtract(args, b.ctx.GetSessionVars().StmtCtx)
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
	return xeval.JSONUnquote(args, b.ctx.GetSessionVars().StmtCtx)
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
	return xeval.JSONSet(args, b.ctx.GetSessionVars().StmtCtx)
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
	return xeval.JSONInsert(args, b.ctx.GetSessionVars().StmtCtx)
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
	return xeval.JSONReplace(args, b.ctx.GetSessionVars().StmtCtx)
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
	return xeval.JSONMerge(args, b.ctx.GetSessionVars().StmtCtx)
}
