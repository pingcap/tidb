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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &jsonExtractFunctionClass{}
)

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

	pathExpr, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	var j types.Json
	switch args[0].Kind() {
	case types.KindString, types.KindBytes:
		j = types.CreateJson(nil)
		j.ParseFromString(args[0].GetString())
	case types.KindMysqlJson:
		j = args[0].GetMysqlJson()
	default:
		return types.Datum{}, errors.Trace(errors.New("invalid argument 0"))
	}

	retval, err := j.Extract(pathExpr)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetValue(retval)
	return d, err
}
