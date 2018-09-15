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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

type varPopFunction struct {
	aggFunction
}

func (af *varPopFunction) updateValue(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	value, err := af.Args[2].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	squareSum, err := value.ToFloat64(sc)
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

	if evalCtx.Value.IsNull() {
		evalCtx.Value.SetFloat64(sum)
		evalCtx.Extra.SetFloat64(squareSum)
	} else {
		evalCtx.Value.SetFloat64(evalCtx.Value.GetFloat64() + sum)
		evalCtx.Extra.SetFloat64(evalCtx.Extra.GetFloat64() + squareSum)
	}
	count, err := af.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	evalCtx.Count += count.GetInt64()
	return nil
}

func (af *varPopFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	evalCtx.Value.SetNull()
	evalCtx.Extra.SetNull()
	evalCtx.Count = 0
}

// Update implements Aggregation interface.
func (af *varPopFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	var err error
	switch af.Mode {
	case Partial1Mode, CompleteMode:
		a := af.Args[0]
		value, err := a.Eval(row)
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

		if evalCtx.Value.IsNull() {
			evalCtx.Value.SetFloat64(v)
			evalCtx.Extra.SetFloat64(v * v)
		} else {
			evalCtx.Value.SetFloat64(evalCtx.Value.GetFloat64() + v)
			evalCtx.Extra.SetFloat64(evalCtx.Extra.GetFloat64() + v*v)
		}
		evalCtx.Count++
		return nil
	case Partial2Mode, FinalMode:
		err = af.updateValue(sc, evalCtx, row)
	case DedupMode:
		panic("DedupMode is not supported now.")
	}
	return errors.Trace(err)
}

// GetResult implements Aggregation interface.
func (af *varPopFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	if evalCtx.Count == 0 {
		d.SetNull()
		return
	}
	squareSum := evalCtx.Extra.GetFloat64()
	sum := evalCtx.Value.GetFloat64()
	d.SetFloat64((squareSum - sum*sum/float64(evalCtx.Count)) / float64(evalCtx.Count))
	return
}

// GetPartialResult implements Aggregation interface.
func (af *varPopFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{types.NewIntDatum(evalCtx.Count), evalCtx.Value, evalCtx.Extra}
}
