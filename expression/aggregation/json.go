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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

type jsonArrayAggFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (sf *jsonArrayAggFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) (err error) {
	a := sf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}

	evalCtx.Value, err = ConcatBinaryJSONArray(evalCtx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetResult implements Aggregation interface.
func (sf *jsonArrayAggFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	d.SetValue(evalCtx.Value)
	return
}

// GetPartialResult implements Aggregation interface.
func (sf *jsonArrayAggFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{evalCtx.Value}
}
