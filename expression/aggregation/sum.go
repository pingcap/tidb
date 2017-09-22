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
	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

type sumFunction struct {
	aggFunction
}

// Clone implements Aggregation interface.
func (sf *sumFunction) Clone() Aggregation {
	nf := *sf
	for i, arg := range sf.Args {
		nf.Args[i] = arg.Clone()
	}
	return &nf
}

// Update implements Aggregation interface.
func (sf *sumFunction) Update(ctx *AggEvaluateContext, sc *variable.StatementContext, row []types.Datum) error {
	return sf.updateSum(ctx, sc, row)
}

// GetResult implements Aggregation interface.
func (sf *sumFunction) GetResult(ctx *AggEvaluateContext) (d types.Datum) {
	return ctx.Value
}

// GetPartialResult implements Aggregation interface.
func (sf *sumFunction) GetPartialResult(ctx *AggEvaluateContext) []types.Datum {
	return []types.Datum{sf.GetResult(ctx)}
}

// CalculateDefaultValue implements Aggregation interface.
func (sf *sumFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := sf.Args[0]
	result, err := expression.EvaluateExprWithNull(ctx, schema, arg)
	if err != nil {
		log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", sf, err.Error())
		return d, false
	}
	if con, ok := result.(*expression.Constant); ok {
		d, err = calculateSum(ctx.GetSessionVars().StmtCtx, d, con.Value)
		if err != nil {
			log.Warnf("CalculateSum failed in function %s, err msg is %s", sf, err.Error())
		}
		return d, err == nil
	}
	return d, false
}

// GetType implements Aggregation interface.
func (sf *sumFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	types.SetBinChsClnFlag(ft)
	ft.Flen = mysql.MaxRealWidth
	ft.Decimal = sf.Args[0].GetType().Decimal
	return ft
}
