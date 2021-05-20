// Copyright 2018 PingCAP, Inc.
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

package aggregation

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
)

type typeInferFunc func(desc *baseFuncDesc, ctx sessionctx.Context)
type removeNotNullFunc func(hasGroupBy, allAggsFirstRow bool) bool

type extensionAggFuncInfo struct {
	typeInfer     typeInferFunc
	removeNotNull removeNotNullFunc
}

var (
	extensionAggFuncs = make(map[string]extensionAggFuncInfo)
)

func extensionAggFuncTypeInfer(desc *baseFuncDesc, ctx sessionctx.Context) error {
	extAggFunc, have := extensionAggFuncs[desc.Name]
	if !have {
		return errors.Errorf("unsupported agg function: %s", desc.Name)
	}
	extAggFunc.typeInfer(desc, ctx)
	return nil
}

func extensionAggFuncUpdateNotNullFlag4RetType(desc *AggFuncDesc, hasGroupBy, allAggsFirstRow bool) (bool, error) {
	extAggFunc, have := extensionAggFuncs[desc.Name]
	if !have {
		return false, errors.Errorf("unsupported agg function: %s", desc.Name)
	}
	ret := extAggFunc.removeNotNull(hasGroupBy, allAggsFirstRow)
	return ret, nil
}
