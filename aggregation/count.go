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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

type countFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (cf *countFunction) Clone() AggregationFunction {
	nf := *cf
	for i, arg := range cf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// CalculateDefaultValue implements AggregationFunction interface.
func (cf *countFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (d types.Datum, valid bool) {
	for _, arg := range cf.Args {
		result, err := expression.EvaluateExprWithNull(ctx, schema, arg)
		if err != nil {
			log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", cf, err.Error())
			return d, false
		}
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

// GetType implements AggregationFunction interface.
func (cf *countFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeLonglong)
	ft.Flen = 21
	types.SetBinChsClnFlag(ft)
	return ft
}

// Update implements AggregationFunction interface.
func (cf *countFunction) Update(row []types.Datum, groupKey []byte, sc *variable.StatementContext) error {
	ctx := cf.getContext(groupKey)
	if cf.Distinct {
		cf.datumBuf = cf.datumBuf[:0]
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
			cf.datumBuf = append(cf.datumBuf, value)
		}
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(cf.datumBuf)
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

// StreamUpdate implements AggregationFunction interface.
func (cf *countFunction) StreamUpdate(row []types.Datum, sc *variable.StatementContext) error {
	ctx := cf.getStreamedContext()
	if cf.Distinct {
		cf.datumBuf = cf.datumBuf[:0]
	}
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		if cf.Distinct {
			cf.datumBuf = append(cf.datumBuf, value)
		}
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(cf.datumBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	ctx.Count++
	return nil
}

// GetGroupResult implements AggregationFunction interface.
func (cf *countFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetInt64(cf.getContext(groupKey).Count)
	return d
}

// GetPartialResult implements AggregationFunction interface.
func (cf *countFunction) GetPartialResult(groupKey []byte) []types.Datum {
	return []types.Datum{cf.GetGroupResult(groupKey)}
}

// GetStreamResult implements AggregationFunction interface.
func (cf *countFunction) GetStreamResult() (d types.Datum) {
	if cf.streamCtx == nil {
		return types.NewDatum(0)
	}
	d.SetInt64(cf.streamCtx.Count)
	cf.streamCtx = nil
	return
}
