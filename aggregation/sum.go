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

// Clone implements AggregationFunction interface.
func (sf *sumFunction) Clone() AggregationFunction {
	nf := *sf
	for i, arg := range sf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// Update implements AggregationFunction interface.
func (sf *sumFunction) Update(row []types.Datum, groupKey []byte, sc *variable.StatementContext) error {
	return sf.updateSum(row, groupKey, sc)
}

// StreamUpdate implements AggregationFunction interface.
func (sf *sumFunction) StreamUpdate(row []types.Datum, sc *variable.StatementContext) error {
	return sf.streamUpdateSum(row, sc)
}

// GetGroupResult implements AggregationFunction interface.
func (sf *sumFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	return sf.getContext(groupKey).Value
}

// GetPartialResult implements AggregationFunction interface.
func (sf *sumFunction) GetPartialResult(groupKey []byte) []types.Datum {
	return []types.Datum{sf.GetGroupResult(groupKey)}
}

// GetStreamResult implements AggregationFunction interface.
func (sf *sumFunction) GetStreamResult() (d types.Datum) {
	if sf.streamCtx == nil {
		return
	}
	d = sf.streamCtx.Value
	sf.streamCtx = nil
	return
}

// CalculateDefaultValue implements AggregationFunction interface.
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

// GetType implements AggregationFunction interface.
func (sf *sumFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	types.SetBinChsClnFlag(ft)
	ft.Flen = mysql.MaxRealWidth
	ft.Decimal = sf.Args[0].GetType().Decimal
	return ft
}
