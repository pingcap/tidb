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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type countFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (cf *countFunction) Update(evalCtx *AggEvaluateContext, _ *stmtctx.StatementContext, row chunk.Row) error {
	var datumBuf []types.Datum
	if cf.HasDistinct {
		datumBuf = make([]types.Datum, 0, len(cf.Args))
	}
	for _, a := range cf.Args {
		value, err := a.Eval(evalCtx.Ctx, row)
		if err != nil {
			return err
		}
		if value.IsNull() {
			return nil
		}
		if cf.Mode == FinalMode || cf.Mode == Partial2Mode {
			evalCtx.Count += value.GetInt64()
		}
		if cf.HasDistinct {
			datumBuf = append(datumBuf, value)
		}
	}
	if cf.HasDistinct {
		d, err := evalCtx.DistinctChecker.Check(datumBuf)
		if err != nil {
			return err
		}
		if !d {
			return nil
		}
	}
	if cf.Mode == CompleteMode || cf.Mode == Partial1Mode {
		evalCtx.Count++
	}
	return nil
}

func (cf *countFunction) ResetContext(ctx expression.EvalContext, evalCtx *AggEvaluateContext) {
	if cf.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(ctx)
	}
	evalCtx.Ctx = ctx
	evalCtx.Count = 0
}

// GetResult implements Aggregation interface.
func (*countFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *countFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{cf.GetResult(evalCtx)}
}
