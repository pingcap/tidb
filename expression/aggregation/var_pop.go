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

type baseVarianceFunction struct {
	aggFunction
}

func (af *baseVarianceFunction) updateValue(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	value, err := af.Args[2].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	variance, err := value.ToFloat64(sc)
	if err != nil {
		return errors.Trace(err)
	}

	value, err = af.Args[1].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	sum, err := value.ToFloat64(sc)
	if err != nil {
		return errors.Trace(err)
	}

	count, err := af.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}

	if evalCtx.Value.IsNull() || evalCtx.Count == 0 {
		evalCtx.Value.SetFloat64(sum)
		evalCtx.Variance.SetFloat64(variance)
	} else if count.GetInt64() != 0 {
		variance = types.CalculateMerge(evalCtx.Count, count.GetInt64(), evalCtx.Value.GetFloat64(), sum, evalCtx.Variance.GetFloat64(), variance)
		evalCtx.Value.SetFloat64(evalCtx.Value.GetFloat64() + sum)
		evalCtx.Variance.SetFloat64(variance)
	}
	evalCtx.Count += count.GetInt64()
	return nil
}

func (af *baseVarianceFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	evalCtx.Value.SetNull()
	evalCtx.Variance.SetNull()
	evalCtx.Count = 0
}

// Update implements Aggregation interface.
func (af *baseVarianceFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	switch af.Mode {
	case Partial1Mode, CompleteMode:
		var err error
		value, err := af.Args[0].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.IsNull() {
			return nil
		}
		v, err := value.ToFloat64(sc)
		if err != nil {
			return errors.Trace(err)
		}

		if evalCtx.Value.IsNull() || evalCtx.Count == 0 {
			evalCtx.Value.SetFloat64(v)
			evalCtx.Variance.SetFloat64(0)
		} else {
			variance := types.CalculateMerge(evalCtx.Count, 1, evalCtx.Value.GetFloat64(), v, evalCtx.Variance.GetFloat64(), 0)
			evalCtx.Value.SetFloat64(evalCtx.Value.GetFloat64() + v)
			evalCtx.Variance.SetFloat64(variance)
		}
		evalCtx.Count++
	case Partial2Mode, FinalMode:
		err := af.updateValue(sc, evalCtx, row)
		return errors.Trace(err)
	case DedupMode:
		panic("DedupMode is not supported now.")
	}
	return nil
}

// GetPartialResult implements Aggregation interface.
func (af *baseVarianceFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{types.NewIntDatum(evalCtx.Count), evalCtx.Value, evalCtx.Variance}
}

type varPopFunction struct {
	baseVarianceFunction
}

// GetResult implements Aggregation interface.
func (af *varPopFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	if evalCtx.Count == 0 {
		d.SetNull()
		return
	}

	d.SetFloat64(evalCtx.Variance.GetFloat64() / float64(evalCtx.Count))
	return
}
