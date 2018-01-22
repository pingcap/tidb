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

package aggregation

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type bitAndFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (bf *bitAndFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	a := bf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if evalCtx.Value.IsNull() {
		evalCtx.Value.SetUint64(math.MaxUint64)
	}
	if !value.IsNull() {
		evalCtx.Value.SetUint64(evalCtx.Value.GetUint64() & value.GetUint64())
	}
	return nil
}

// GetResult implements Aggregation interface.
func (bf *bitAndFunction) GetResult(evalCtx *AggEvaluateContext) types.Datum {
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (bf *bitAndFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{bf.GetResult(evalCtx)}
}
