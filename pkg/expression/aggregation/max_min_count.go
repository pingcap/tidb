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

func (mmcf *maxMinCountFunction) ResetContext(ctx expression.EvalContext, evalCtx *AggEvaluateContext) {
	evalCtx.Ctx = ctx
	evalCtx.Value.SetNull()
	evalCtx.Count = 0
}

// GetResult implements Aggregation interface.
func (*maxMinCountFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (mmcf *maxMinCountFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{mmcf.GetResult(evalCtx)}
}

// Update implements Aggregation interface.
func (mmcf *maxMinCountFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	value, err := mmcf.Args[0].Eval(evalCtx.Ctx, row)
	if err != nil {
		return err
	}
	if value.IsNull() {
		return nil
	}

	if evalCtx.Value.IsNull() {
		value.Copy(&evalCtx.Value)
		evalCtx.Count = 1
		return nil
	}

	cmp, err := evalCtx.Value.Compare(sc.TypeCtx(), &value, mmcf.ctor)
	if err != nil {
		return err
	}
	if (mmcf.isMax && cmp == -1) || (!mmcf.isMax && cmp == 1) {
		value.Copy(&evalCtx.Value)
		evalCtx.Count = 1
		return nil
	}
	if cmp == 0 {
		evalCtx.Count++
	}
	return nil
}
