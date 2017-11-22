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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type firstRowFunction struct {
	aggFunction
}

// Clone implements Aggregation interface.
func (ff *firstRowFunction) Clone() Aggregation {
	nf := *ff
	for i, arg := range ff.Args {
		nf.Args[i] = arg.Clone()
	}
	return &nf
}

// GetType implements Aggregation interface.
func (ff *firstRowFunction) GetType() *types.FieldType {
	return ff.Args[0].GetType()
}

// Update implements Aggregation interface.
func (ff *firstRowFunction) Update(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	if ctx.GotFirstRow {
		return nil
	}
	if len(ff.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	value, err := ff.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Value = value
	ctx.GotFirstRow = true
	return nil
}

// GetResult implements Aggregation interface.
func (ff *firstRowFunction) GetResult(ctx *AggEvaluateContext) types.Datum {
	return ctx.Value
}

// GetPartialResult implements Aggregation interface.
func (ff *firstRowFunction) GetPartialResult(ctx *AggEvaluateContext) []types.Datum {
	return []types.Datum{ff.GetResult(ctx)}
}

// CalculateDefaultValue implements Aggregation interface.
func (ff *firstRowFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := ff.Args[0]
	result := expression.EvaluateExprWithNull(ctx, schema, arg)
	if con, ok := result.(*expression.Constant); ok {
		return con.Value, true
	}
	return d, false
}
