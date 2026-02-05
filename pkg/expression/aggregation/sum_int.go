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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type sumIntFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (sf *sumIntFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	value, isNull, err := sf.Args[0].EvalInt(evalCtx.Ctx, row)
	if err != nil || isNull {
		return err
	}
	argTp := sf.Args[0].GetType(evalCtx.Ctx)
	if mysql.HasUnsignedFlag(argTp.GetFlag()) {
		uintVal := uint64(value)
		if sf.HasDistinct {
			d, err := evalCtx.DistinctChecker.Check([]types.Datum{types.NewUintDatum(uintVal)})
			if err != nil {
				return err
			}
			if !d {
				return nil
			}
		}
		if evalCtx.Value.IsNull() {
			evalCtx.Value.SetUint64(uintVal)
		} else {
			sum, err := types.AddUint64(evalCtx.Value.GetUint64(), uintVal)
			if err != nil {
				return err
			}
			evalCtx.Value.SetUint64(sum)
		}
		evalCtx.Count++
		return nil
	}
	if sf.HasDistinct {
		d, err := evalCtx.DistinctChecker.Check([]types.Datum{types.NewIntDatum(value)})
		if err != nil {
			return err
		}
		if !d {
			return nil
		}
	}
	if evalCtx.Value.IsNull() {
		evalCtx.Value.SetInt64(value)
	} else {
		sum, err := types.AddInt64(evalCtx.Value.GetInt64(), value)
		if err != nil {
			return err
		}
		evalCtx.Value.SetInt64(sum)
	}
	evalCtx.Count++
	return nil
}

// GetResult implements Aggregation interface.
func (*sumIntFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (sf *sumIntFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{sf.GetResult(evalCtx)}
}
