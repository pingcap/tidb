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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/distinct"
	"github.com/pingcap/tidb/util/types"
	"strings"
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
		return &sumFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}}
	case ast.AggFuncCount:
		return &countFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}}
	case ast.AggFuncAvg:
		return &avgFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}}
	case ast.AggFuncMax:
		return &maxMinFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}, isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCtxMapper, 0), Distinct: distinct}}
	}
	return nil
}

type aggrCtxMapper map[string]*ast.AggEvaluateContext

type aggrFunction struct {
	Args         []Expression
	Distinct     bool
	resultMapper aggrCtxMapper
}

func (af *aggrFunction) Clear() {
	af.resultMapper = nil
}

// GetArgs implements AggregationFunction interface.
func (af *aggrFunction) GetArgs() []Expression {
	return af.Args
}

// SetArgs implements AggregationFunction interface.
func (af *aggrFunction) SetArgs(idx int, expr Expression) {
	af.Args[idx] = expr
}

func (af *aggrFunction) getContext(groupKey []byte) *ast.AggEvaluateContext {
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

func (af *aggrFunction) updateSum(row []types.Datum, groupKey []byte, ectx context.Context) error {
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
		d, err := ctx.DistinctChecker.Check([]interface{}{value.GetValue()})
		if err != nil {
			return errors.Trace(err)
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
	aggrFunction
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
	aggrFunction
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
	aggrFunction
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
	aggrFunction
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
	aggrFunction
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
	aggrFunction
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
