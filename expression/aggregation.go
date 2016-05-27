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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/distinct"
	"github.com/pingcap/tidb/util/types"
)

type AggregationFunction interface {
	Update(row []types.Datum, groupKey []byte) error
	GetGroupResult(groupKey []byte) types.Datum
}

func NewAggrFunction(funcType string, funcArgs []Expression) AggregationFunction {
	switch funcType {
	case ast.AggFuncSum:
		return &sumFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}}
	case ast.AggFuncCount:
		return &countFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}}
	case ast.AggFuncAvg:
		return &avgFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}}
	case ast.AggFuncMax:
		return &maxMinFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}, isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggrFunction: aggrFunction{Args: funcArgs, resultMapper: make(aggrCxtMapper, 0)}}
	}
	return nil
}

type aggrCxtMapper map[string]*ast.AggEvaluateContext

type aggrFunction struct {
	Args         []Expression
	Distinct     bool
	resultMapper aggrCxtMapper
}

func (af *aggrFunction) GetContext(groupKey []byte) *ast.AggEvaluateContext {
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

func (af *aggrFunction) updateSum(row []types.Datum, groupKey []byte) error {
	ctx := af.GetContext(groupKey)
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.GetValue() == nil {
		return nil
	}
	if af.Distinct {
		d, err := ctx.DistinctChecker.Check([]interface{}{value})
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

func (sf *sumFunction) Update(row []types.Datum, groupKey []byte) error {
	return sf.updateSum(row, groupKey)
}

func (sf *sumFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetValue(sf.GetContext(groupKey).Value)
	return
}

type countFunction struct {
	aggrFunction
}

func (cf *countFunction) Update(row []types.Datum, groupKey []byte) error {
	ctx := cf.GetContext(groupKey)
	var vals []interface{}
	if cf.Distinct {
		vals = make([]interface{}, 0, len(cf.Args))
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

func (sf *countFunction) GetGroupResult(groupKey []byte) types.Datum {
	d := types.Datum{}
	d.SetInt64(sf.GetContext(groupKey).Count)
	return d
}

type avgFunction struct {
	aggrFunction
}

func (af *avgFunction) Update(row []types.Datum, groupKey []byte) error {
	return af.updateSum(row, groupKey)
}

func (af *avgFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	ctx := af.GetContext(groupKey)
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

func (cf *concatFunction) Update(row []types.Datum, groupKey []byte) error {
	ctx := cf.GetContext(groupKey)
	vals := make([]interface{}, 0, len(cf.Args))
	for _, a := range cf.Args {
		value, err := a.Eval(row)
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

func (cf *concatFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	ctx := cf.GetContext(groupKey)
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

func (mf *maxMinFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetValue(mf.GetContext(groupKey).Value)
	return
}

func (af *maxMinFunction) Update(row []types.Datum, groupKey []byte) error {
	ctx := af.GetContext(groupKey)
	if len(af.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncMaxMin")
	}
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.GetValue() == nil {
		return nil
	}
	var c int
	c, err = types.Compare(ctx.Value, value.GetValue())
	if err != nil {
		return errors.Trace(err)
	}
	if (af.isMax && c == -1) || (!af.isMax && c == 1) {
		ctx.Value = value.GetValue()
	}
	return nil
}

type firstRowFunction struct {
	aggrFunction
}

func (af *firstRowFunction) Update(row []types.Datum, groupKey []byte) error {
	ctx := af.GetContext(groupKey)
	if ctx.Evaluated {
		return nil
	}
	if len(af.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncMaxMin")
	}
	value, err := af.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Value = value.GetValue()
	ctx.Evaluated = true
	return nil
}

func (ff *firstRowFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetValue(ff.GetContext(groupKey).Value)
	return
}
