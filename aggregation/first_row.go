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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

type firstRowFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (ff *firstRowFunction) Clone() AggregationFunction {
	nf := *ff
	for i, arg := range ff.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// GetType implements AggregationFunction interface.
func (ff *firstRowFunction) GetType() *types.FieldType {
	return ff.Args[0].GetType()
}

// Update implements AggregationFunction interface.
func (ff *firstRowFunction) Update(row []types.Datum, groupKey []byte, sc *variable.StatementContext) error {
	ctx := ff.getContext(groupKey)
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

// StreamUpdate implements AggregationFunction interface.
func (ff *firstRowFunction) StreamUpdate(row []types.Datum, sc *variable.StatementContext) error {
	ctx := ff.getStreamedContext()
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

// GetGroupResult implements AggregationFunction interface.
func (ff *firstRowFunction) GetGroupResult(groupKey []byte) types.Datum {
	return ff.getContext(groupKey).Value
}

// GetPartialResult implements AggregationFunction interface.
func (ff *firstRowFunction) GetPartialResult(groupKey []byte) []types.Datum {
	return []types.Datum{ff.GetGroupResult(groupKey)}
}

// GetStreamResult implements AggregationFunction interface.
func (ff *firstRowFunction) GetStreamResult() (d types.Datum) {
	if ff.streamCtx == nil {
		return
	}
	d = ff.streamCtx.Value
	ff.streamCtx = nil
	return
}

// CalculateDefaultValue implements AggregationFunction interface.
func (ff *firstRowFunction) CalculateDefaultValue(schema *expression.Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := ff.Args[0]
	result, err := expression.EvaluateExprWithNull(ctx, schema, arg)
	if err != nil {
		log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", ff, err.Error())
		return d, false
	}
	if con, ok := result.(*expression.Constant); ok {
		return con.Value, true
	}
	return d, false
}
