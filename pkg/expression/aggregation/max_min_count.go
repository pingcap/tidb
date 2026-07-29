// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/util/collate"
)

type maxMinCountFunction struct {
	aggFunction
	isMax bool
	ctor  collate.Collator
}

func (mmf *maxMinCountFunction) ResetContext(ctx expression.EvalContext, evalCtx *AggEvaluateContext) {
	mmf.aggFunction.ResetContext(ctx, evalCtx)
	evalCtx.Count = 0
}

// GetResult implements Aggregation interface.
func (*maxMinCountFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (mmf *maxMinCountFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{mmf.GetResult(evalCtx), evalCtx.Value}
}

// Update implements Aggregation interface.
func (mmf *maxMinCountFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	var (
		value types.Datum
		count int64
		err   error
	)
	if len(mmf.Args) > 1 {
		// In two-phase execution, final/partial2 phase receives two columns:
		// count and extrema value from partial phase.
		var isNull bool
		count, isNull, err = mmf.Args[0].EvalInt(evalCtx.Ctx, row)
		if err != nil {
			return err
		}
		if isNull || count == 0 {
			return nil
		}
		value, err = mmf.Args[1].Eval(evalCtx.Ctx, row)
		if err != nil {
			return err
		}
		if value.IsNull() {
			return nil
		}
	} else {
		a := mmf.Args[0]
		value, err = a.Eval(evalCtx.Ctx, row)
		if err != nil {
			return err
		}
		if value.IsNull() {
			return nil
		}
		count = 1
	}

	if evalCtx.Value.IsNull() {
		value.Copy(&evalCtx.Value)
		evalCtx.Count = count
		return nil
	}

	cmp, err := evalCtx.Value.Compare(sc.TypeCtx(), &value, mmf.ctor)
	if err != nil {
		return err
	}
	if (mmf.isMax && cmp == -1) || (!mmf.isMax && cmp == 1) {
		value.Copy(&evalCtx.Value)
		evalCtx.Count = count
		return nil
	}
	if cmp == 0 {
		evalCtx.Count += count
	}
	return nil
}
