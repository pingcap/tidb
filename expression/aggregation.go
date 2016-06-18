// Copyright 2016 PingCAP, Inc.
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

package expression

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/distinct"
	"github.com/pingcap/tidb/util/types"
)

// AggregationFunction stands for aggregate functions.
type AggregationFunction interface {
	// Update during executing.
	Update(row []types.Datum, groupKey []byte, ctx context.Context) error

	// GetGroupResult will be called when all data have been processed.
	GetGroupResult(groupKey []byte) types.Datum

	// GetArgs stands for getting all arguments.
	GetArgs() []Expression

	// SetArgs set argument by index.
	SetArgs(idx int, expr Expression)

	// Clear collects the mapper's memory.
	Clear()
}

// NewAggFunction creates a new AggregationFunction.
func NewAggFunction(funcType string, funcArgs []Expression, distinct bool) AggregationFunction {
	switch strings.ToLower(funcType) {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: newAggFunc(funcArgs, distinct)}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: newAggFunc(funcArgs, distinct)}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: newAggFunc(funcArgs, distinct)}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggFunction: newAggFunc(funcArgs, distinct)}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: newAggFunc(funcArgs, distinct), isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: newAggFunc(funcArgs, distinct), isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggFunction: newAggFunc(funcArgs, distinct)}
	}
	return nil
}

type aggCtxMapper map[string]*ast.AggEvaluateContext

type aggFunction struct {
	Args         []Expression
	Distinct     bool
	resultMapper aggCtxMapper
}

func newAggFunc(args []Expression, dist bool) aggFunction {
	return aggFunction{
		Args:         args,
		resultMapper: make(aggCtxMapper, 0),
		Distinct:     dist}
}

func (af *aggFunction) Clear() {
	af.resultMapper = make(aggCtxMapper, 0)
}

// GetArgs implements AggregationFunction interface.
func (af *aggFunction) GetArgs() []Expression {
	return af.Args
}

// SetArgs implements AggregationFunction interface.
func (af *aggFunction) SetArgs(idx int, expr Expression) {
	af.Args[idx] = expr
}

func (af *aggFunction) getContext(groupKey []byte) *ast.AggEvaluateContext {
	ctx, ok := af.resultMapper[string(groupKey)]
	if !ok {
		ctx = &ast.AggEvaluateContext{}
		if af.Distinct {
			ctx.DistinctChecker = distinct.CreateDistinctChecker()
		}
		af.resultMapper[string(groupKey)] = ctx
	}
	return ctx
}

func (af *aggFunction) updateSum(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := af.getContext(groupKey)
	a := af.Args[0]
	value, err := a.Eval(row, ectx)
	if err != nil {
		return errors.Trace(err)
	}
	if value.GetValue() == nil {
		return nil
	}
	if af.Distinct {
		d, err1 := ctx.DistinctChecker.Check([]interface{}{value.GetValue()})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	ctx.Value, err = types.CalculateSum(ctx.Value, value.GetValue())
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count++
	return nil
}

type sumFunction struct {
	aggFunction
}

// Update implements AggregationFunction interface.
func (sf *sumFunction) Update(row []types.Datum, groupKey []byte, ctx context.Context) error {
	return sf.updateSum(row, groupKey, ctx)
}

// GetGroupResult implements AggregationFunction interface.
func (sf *sumFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetValue(sf.getContext(groupKey).Value)
	return
}

type countFunction struct {
	aggFunction
}

// Update implements AggregationFunction interface.
func (cf *countFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := cf.getContext(groupKey)
	var vals []interface{}
	if cf.Distinct {
		vals = make([]interface{}, 0, len(cf.Args))
	}
	for _, a := range cf.Args {
		value, err := a.Eval(row, ectx)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		if cf.Distinct {
			vals = append(vals, value.GetValue())
		}
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
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

type avgFunction struct {
	aggFunction
}

// Update implements AggregationFunction interface.
func (af *avgFunction) Update(row []types.Datum, groupKey []byte, ctx context.Context) error {
	return af.updateSum(row, groupKey, ctx)
}

// GetGroupResult implements AggregationFunction interface.
func (af *avgFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	ctx := af.getContext(groupKey)
	switch x := ctx.Value.(type) {
	case float64:
		t := x / float64(ctx.Count)
		ctx.Value = t
		d.SetFloat64(t)
	case mysql.Decimal:
		t := x.Div(mysql.NewDecimalFromUint(uint64(ctx.Count), 0))
		ctx.Value = t
		d.SetMysqlDecimal(t)
	}
	return
}

type concatFunction struct {
	aggFunction
}

// Update implements AggregationFunction interface.
func (cf *concatFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := cf.getContext(groupKey)
	vals := make([]interface{}, 0, len(cf.Args))
	for _, a := range cf.Args {
		value, err := a.Eval(row, ectx)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		vals = append(vals, value.GetValue())
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if ctx.Buffer == nil {
		ctx.Buffer = &bytes.Buffer{}
	} else {
		// now use comma separator
		ctx.Buffer.WriteString(",")
	}
	for _, val := range vals {
		ctx.Buffer.WriteString(fmt.Sprintf("%v", val))
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// GetGroupResult implements AggregationFunction interface.
func (cf *concatFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	ctx := cf.getContext(groupKey)
	if ctx.Buffer != nil {
		d.SetString(ctx.Buffer.String())
	} else {
		d.SetNull()
	}
	return d
}

type maxMinFunction struct {
	aggFunction
	isMax bool
}

// GetGroupResult implements AggregationFunction interface.
func (mmf *maxMinFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetValue(mmf.getContext(groupKey).Value)
	return
}

// Update implements AggregationFunction interface.
func (mmf *maxMinFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := mmf.getContext(groupKey)
	if len(mmf.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncMaxMin")
	}
	a := mmf.Args[0]
	value, err := a.Eval(row, ectx)
	if err != nil {
		return errors.Trace(err)
	}
	if !ctx.Evaluated {
		ctx.Value = value.GetValue()
	}
	if value.GetValue() == nil {
		return nil
	}
	var c int
	c, err = types.Compare(ctx.Value, value.GetValue())
	if err != nil {
		return errors.Trace(err)
	}
	if (mmf.isMax && c == -1) || (!mmf.isMax && c == 1) {
		ctx.Value = value.GetValue()
	}
	ctx.Evaluated = true
	return nil
}

type firstRowFunction struct {
	aggFunction
}

// Update implements AggregationFunction interface.
func (ff *firstRowFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := ff.getContext(groupKey)
	if ctx.Evaluated {
		return nil
	}
	if len(ff.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncMaxMin")
	}
	value, err := ff.Args[0].Eval(row, ectx)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Value = value.GetValue()
	ctx.Evaluated = true
	return nil
}

// GetGroupResult implements AggregationFunction interface.
func (ff *firstRowFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetValue(ff.getContext(groupKey).Value)
	return
}
