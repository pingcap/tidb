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

package mocktikv

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql/xeval"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var singleGroup = []byte("SingleGroup")

func getGroupKey(eval *xeval.Evaluator, exprs []*tipb.Expr) ([]byte, error) {
	if len(exprs) == 0 {
		return singleGroup, nil
	}
	vals := make([]types.Datum, 0, len(exprs))
	for _, expr := range exprs {
		v, err := eval.Eval(expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	bs, err := codec.EncodeValue(nil, vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return bs, nil
}

func itemsToExprs(items []*tipb.ByItem) []*tipb.Expr {
	exprs := make([]*tipb.Expr, 0, len(items))
	for _, item := range items {
		exprs = append(exprs, item.Expr)
	}
	return exprs
}

// aggregate updates aggregate functions with rows.
func aggregate(ctx *selectContext, handle int64, row map[int64][]byte) error {
	// Put row data into evaluate for later evaluation.
	err := setColumnValueToEval(ctx.eval, handle, row, ctx.aggColumns)
	if err != nil {
		return errors.Trace(err)
	}
	// Get group key.
	gk, err := getGroupKey(ctx.eval, itemsToExprs(ctx.sel.GroupBy))
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := ctx.groups[string(gk)]; !ok {
		ctx.groups[string(gk)] = struct{}{}
		ctx.groupKeys = append(ctx.groupKeys, gk)
	}
	// Update aggregate funcs.
	for _, agg := range ctx.aggregates {
		agg.currentGroup = gk
		args := make([]types.Datum, 0, len(agg.expr.Children))
		// Evaluate arguments.
		for _, x := range agg.expr.Children {
			cv, err := ctx.eval.Eval(x)
			if err != nil {
				return errors.Trace(err)
			}
			args = append(args, cv)
		}
		agg.update(ctx.eval, args)
	}
	return nil
}

// Partial result for a single aggregate group.
type aggItem struct {
	// Number of rows, this could be used in cout/avg
	count uint64
	// This could be used to store sum/max/min
	value types.Datum
	// TODO: support group_concat
	buffer *bytes.Buffer // Buffer is used for group_concat.
	// It will check if the agg has met the first row key.
	gotFirstRow bool
}

// This is similar to ast.AggregateFuncExpr but use tipb.Expr.
type aggregateFuncExpr struct {
	expr         *tipb.Expr
	currentGroup []byte
	// contextPerGroupMap is used to store aggregate evaluation context.
	// Each entry for a group.
	contextPerGroupMap map[string](*aggItem)
}

// Clear clears aggregate computing context.
func (n *aggregateFuncExpr) clear() {
	n.currentGroup = []byte{}
	n.contextPerGroupMap = nil
}

// Update is used for update aggregate context.
func (n *aggregateFuncExpr) update(eval *xeval.Evaluator, args []types.Datum) error {
	switch n.expr.GetTp() {
	case tipb.ExprType_Count:
		return n.updateCount(eval, args)
	case tipb.ExprType_First:
		return n.updateFirst(eval, args)
	case tipb.ExprType_Sum, tipb.ExprType_Avg:
		return n.updateSum(eval, args)
	case tipb.ExprType_Max:
		return n.updateMaxMin(eval, args, true)
	case tipb.ExprType_Min:
		return n.updateMaxMin(eval, args, false)
	}
	return errors.Errorf("Unknown AggExpr: %v", n.expr.GetTp())
}

func (n *aggregateFuncExpr) toDatums(eval *xeval.Evaluator) (ds []types.Datum, err error) {
	switch n.expr.GetTp() {
	case tipb.ExprType_Count:
		ds = n.getCountDatum()
	case tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min:
		ds = n.getValueDatum()
	case tipb.ExprType_Sum:
		d, err := getSumValue(eval, n.getAggItem())
		if err != nil {
			return nil, errors.Trace(err)
		}
		ds = []types.Datum{d}
	case tipb.ExprType_Avg:
		item := n.getAggItem()
		sum, err := getSumValue(eval, item)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cnt := types.NewUintDatum(item.count)
		ds = []types.Datum{cnt, sum}
	}
	return
}

func getSumValue(eval *xeval.Evaluator, item *aggItem) (types.Datum, error) {
	v := item.value
	var d types.Datum
	if !v.IsNull() {
		// For sum result, we should convert it to decimal.
		de, err1 := v.ToDecimal(eval.StatementCtx)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		d = types.NewDecimalDatum(de)
	}
	return d, nil
}

// Convert count to datum list.
func (n *aggregateFuncExpr) getCountDatum() []types.Datum {
	item := n.getAggItem()
	return []types.Datum{types.NewUintDatum(item.count)}
}

// Convert value to datum list.
func (n *aggregateFuncExpr) getValueDatum() []types.Datum {
	item := n.getAggItem()
	return []types.Datum{item.value}
}

var singleGroupKey = []byte("SingleGroup")

// getAggItem gets aggregate evaluation context for the current group.
// If it is nil, add a new context into contextPerGroupMap.
func (n *aggregateFuncExpr) getAggItem() *aggItem {
	if n.currentGroup == nil {
		n.currentGroup = singleGroupKey
	}
	if n.contextPerGroupMap == nil {
		n.contextPerGroupMap = make(map[string](*aggItem))
	}
	if _, ok := n.contextPerGroupMap[string(n.currentGroup)]; !ok {
		n.contextPerGroupMap[string(n.currentGroup)] = &aggItem{}
	}
	return n.contextPerGroupMap[string(n.currentGroup)]
}

func (n *aggregateFuncExpr) updateCount(eval *xeval.Evaluator, args []types.Datum) error {
	for _, a := range args {
		if a.IsNull() {
			return nil
		}
	}
	aggItem := n.getAggItem()
	aggItem.count++
	return nil
}

func (n *aggregateFuncExpr) updateFirst(eval *xeval.Evaluator, args []types.Datum) error {
	aggItem := n.getAggItem()
	if aggItem.gotFirstRow {
		return nil
	}
	if len(args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	aggItem.value = args[0]
	aggItem.gotFirstRow = true
	return nil
}

func (n *aggregateFuncExpr) updateSum(eval *xeval.Evaluator, args []types.Datum) error {
	if len(args) != 1 {
		// This should not happen. The length of argument list is already checked in the early stage.
		// This is just in case of error.
		return errors.Errorf("Wrong number of argument for sum, need 1 but get %d", len(args))
	}
	arg := args[0]
	if arg.IsNull() {
		return nil
	}
	aggItem := n.getAggItem()
	if aggItem.value.IsNull() {
		aggItem.value = arg
		aggItem.count = 1
		return nil
	}
	var err error
	aggItem.value, err = xeval.ComputeArithmetic(eval.StatementCtx, tipb.ExprType_Plus, arg, aggItem.value)
	if err != nil {
		return errors.Trace(err)
	}
	aggItem.count++
	return nil
}

func (n *aggregateFuncExpr) updateMaxMin(eval *xeval.Evaluator, args []types.Datum, max bool) error {
	if len(args) != 1 {
		// This should not happen. The length of argument list is already checked in the early stage.
		// This is just in case of error.
		name := "max"
		if !max {
			name = "min"
		}
		return errors.Errorf("Wrong number of argument for %s, need 1 but get %d", name, len(args))
	}
	arg := args[0]
	if arg.IsNull() {
		return nil
	}
	aggItem := n.getAggItem()
	if aggItem.value.IsNull() {
		aggItem.value = arg
		return nil
	}
	c, err := aggItem.value.CompareDatum(eval.StatementCtx, arg)
	if err != nil {
		return errors.Trace(err)
	}
	if max {
		if c == -1 {
			aggItem.value = arg
		}
	} else if c == 1 {
		aggItem.value = arg
	}
	return nil
}
