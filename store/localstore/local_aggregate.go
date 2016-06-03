package localstore

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var singleGroup = []byte("SingleGroup")

func (rs *localRegion) getGroupKey(ctx *selectContext) ([]byte, error) {
	items := ctx.sel.GetGroupBy()
	if len(items) == 0 {
		return singleGroup, nil
	}
	vals := make([]types.Datum, 0, len(items))
	for _, item := range items {
		v, err := ctx.eval.Eval(item.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	bs, err := codec.EncodeValue([]byte{}, vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return bs, nil
}

// Update aggregate functions with rows.
func (rs *localRegion) aggregate(ctx *selectContext, row [][]byte) error {
	cols := ctx.sel.TableInfo.Columns
	for i, col := range cols {
		_, datum, err := codec.DecodeOne(row[i])
		if err != nil {
			return errors.Trace(err)
		}
		ctx.eval.Row[col.GetColumnId()] = datum
	}
	// Get group key.
	gk, err := rs.getGroupKey(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := ctx.groups[string(gk)]; !ok {
		ctx.groups[string(gk)] = true
		ctx.groupKeys = append(ctx.groupKeys, gk)
	}
	// Update aggregate funcs.
	for _, agg := range ctx.aggregates {
		agg.currentGroup = gk
		args := make([]types.Datum, len(agg.expr.Children))
		// Evaluate args
		for _, x := range agg.expr.Children {
			cv, err := ctx.eval.Eval(x)
			if err != nil {
				return errors.Trace(err)
			}
			args = append(args, cv)
		}
		agg.update(ctx, args)
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
func (n *aggregateFuncExpr) update(ctx *selectContext, args []types.Datum) error {
	switch n.expr.GetTp() {
	case tipb.ExprType_Count:
		return n.updateCount(ctx, args)
	}
	return errors.Errorf("Unknown AggExpr: %v", n.expr.GetTp())
}

func (n *aggregateFuncExpr) toDatums() []types.Datum {
	switch n.expr.GetTp() {
	case tipb.ExprType_Count:
		return n.getCountValue()
	}
	// TODO: return error
	return nil
}

// Convert count and value to datum list
func (n *aggregateFuncExpr) getCountValue() []types.Datum {
	item := n.getAggItem()
	if item == nil {
		// TODO: should this happen?
		return nil
	}
	ds := make([]types.Datum, 2)
	ds[0] = types.NewUintDatum(item.count)
	ds[1] = item.value
	return ds
}

var singleGroupKey = []byte("SingleGroup")

// getContext gets aggregate evaluation context for the current group.
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

func (n *aggregateFuncExpr) updateCount(ctx *selectContext, args []types.Datum) error {
	aggItem := n.getAggItem()
	aggItem.count++
	return nil
}
