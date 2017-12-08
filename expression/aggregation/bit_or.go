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

type bitOrFunction struct {
	aggFunction
}

// Clone implements Aggregation interface.
func (bf *bitOrFunction) Clone() Aggregation {
	nf := *bf
	for i, arg := range bf.Args {
		nf.Args[i] = arg.Clone()
	}
	return &nf
}

// CalculateDefaultValue implements Aggregation interface.
func (bf *bitOrFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := bf.Args[0]
	result := expression.EvaluateExprWithNull(ctx, schema, arg)
	if con, ok := result.(*expression.Constant); ok {
		if con.Value.IsNull() {
			return types.NewDatum(0), true
		}
		return con.Value, true
	}
	return types.NewDatum(0), true
}

// GetType implements Aggregation interface.
func (bf *bitOrFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeLonglong)
	ft.Flen = 21
	types.SetBinChsClnFlag(ft)
	ft.Flag |= mysql.UnsignedFlag | mysql.NotNullFlag
	return ft
}

// Update implements Aggregation interface.
func (bf *bitOrFunction) Update(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	a := bf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if ctx.Value.IsNull() {
		ctx.Value.SetUint64(0)
	}
	if !value.IsNull() {
		ctx.Value.SetUint64(ctx.Value.GetUint64() | value.GetUint64())
	}
	return nil
}

// GetResult implements Aggregation interface.
func (bf *bitOrFunction) GetResult(ctx *AggEvaluateContext) types.Datum {
	return ctx.Value
}

// GetPartialResult implements Aggregation interface.
func (bf *bitOrFunction) GetPartialResult(ctx *AggEvaluateContext) []types.Datum {
	return []types.Datum{bf.GetResult(ctx)}
}
