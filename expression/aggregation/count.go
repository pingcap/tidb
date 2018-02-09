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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type countFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (cf *countFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	var datumBuf []types.Datum
	if cf.HasDistinct {
		datumBuf = make([]types.Datum, 0, len(cf.Args))
	}
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		if cf.Mode == FinalMode {
			evalCtx.Count += value.GetInt64()
		}
		if cf.HasDistinct {
			datumBuf = append(datumBuf, value)
		}
	}
	if cf.HasDistinct {
		d, err := evalCtx.DistinctChecker.Check(datumBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if cf.Mode == CompleteMode {
		evalCtx.Count++
	}
	return nil
}

// GetResult implements Aggregation interface.
func (cf *countFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *countFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{cf.GetResult(evalCtx)}
}
