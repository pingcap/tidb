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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type countFunction struct {
	aggFunction
}

// Clone implements Aggregation interface.
func (cf *countFunction) Clone() Aggregation {
	nf := *cf
	for i, arg := range cf.Args {
		nf.Args[i] = arg.Clone()
	}
	return &nf
}

// CalculateDefaultValue implements Aggregation interface.
func (cf *countFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (d types.Datum, valid bool) {
	for _, arg := range cf.Args {
		result := expression.EvaluateExprWithNull(ctx, schema, arg)
		if con, ok := result.(*expression.Constant); ok {
			if con.Value.IsNull() {
				return types.NewDatum(0), true
			}
		} else {
			return d, false
		}
	}
	return types.NewDatum(1), true
}

// GetType implements Aggregation interface.
func (cf *countFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeLonglong)
	ft.Flen = 21
	types.SetBinChsClnFlag(ft)
	return ft
}

// Update implements Aggregation interface.
func (cf *countFunction) Update(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	var datumBuf []types.Datum
	if cf.Distinct {
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
		if cf.mode == FinalMode {
			ctx.Count += value.GetInt64()
		}
		if cf.Distinct {
			datumBuf = append(datumBuf, value)
		}
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(datumBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if cf.mode == CompleteMode {
		ctx.Count++
	}
	return nil
}

// GetResult implements Aggregation interface.
func (cf *countFunction) GetResult(ctx *AggEvaluateContext) (d types.Datum) {
	d.SetInt64(ctx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *countFunction) GetPartialResult(ctx *AggEvaluateContext) []types.Datum {
	return []types.Datum{cf.GetResult(ctx)}
}
