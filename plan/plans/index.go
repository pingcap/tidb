// Copyright 2015 PingCAP, Inc.
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

package plans

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ plan.Plan = (*indexPlan)(nil)

type bound int

const (
	minNotNullVal bound = 0
	maxVal        bound = 1
)

// String implements fmt.Stringer interface.
func (b bound) String() string {
	if b == minNotNullVal {
		return "-inf"
	} else if b == maxVal {
		return "+inf"
	}
	return ""
}

// index span is the range of value to be scanned.
type indexSpan struct {
	// seekVal is different from lowVal, it is casted from lowVal and
	// must be less than or equal to lowVal, used to seek the index.
	seekVal     interface{}
	lowVal      interface{}
	lowExclude  bool
	highVal     interface{}
	highExclude bool
}

// cut off the range less than val and return the new span.
// the new span may be nil if val is larger than span's high value.
func (span *indexSpan) cutOffLow(val interface{}, exclude bool) *indexSpan {
	out := &indexSpan{}
	*out = *span
	cmp := indexCompare(span.lowVal, val)
	if cmp == 0 {
		if !out.lowExclude {
			out.lowExclude = exclude
		}
		return out
	} else if cmp < 0 {
		out.lowVal = val
		out.lowExclude = exclude

		cmp2 := indexCompare(out.lowVal, out.highVal)
		if cmp2 < 0 {
			return out
		} else if cmp2 > 0 {
			return nil
		} else {
			if out.lowExclude || out.highExclude {
				return nil
			}
			return out
		}
	} else {
		return out
	}
}

// cut out the range greater than val and return the new span.
// the new span may be nil if val is less than span's low value.
func (span *indexSpan) cutOffHigh(val interface{}, exclude bool) *indexSpan {
	out := &indexSpan{}
	*out = *span
	cmp := indexCompare(span.highVal, val)
	if cmp == 0 {
		if !out.highExclude {
			out.highExclude = exclude
		}
		return out
	} else if cmp > 0 {
		out.highVal = val
		out.highExclude = exclude

		cmp2 := indexCompare(out.lowVal, out.highVal)
		if cmp2 < 0 {
			return out
		} else if cmp2 > 0 {
			return nil
		} else {
			if out.lowExclude || out.highExclude {
				return nil
			}
			return out
		}
	} else {
		return out
	}
}

type indexPlan struct {
	src        table.Table
	col        *column.Col
	unique     bool
	idxName    string
	idx        kv.Index
	spans      []*indexSpan // multiple spans are ordered by their values and without overlapping.
	cursor     int
	skipLowCmp bool
	iter       kv.IndexIterator
}

// comparison function that takes minNotNullVal and maxVal into account.
func indexCompare(a interface{}, b interface{}) int {
	if a == nil && b == nil {
		return 0
	} else if b == nil {
		return 1
	} else if b == nil {
		return -1
	}

	// a and b both not nil
	if a == minNotNullVal && b == minNotNullVal {
		return 0
	} else if b == minNotNullVal {
		return 1
	} else if a == minNotNullVal {
		return -1
	}

	// a and b both not min value
	if a == maxVal && b == maxVal {
		return 0
	} else if a == maxVal {
		return 1
	} else if b == maxVal {
		return -1
	}

	n, err := types.Compare(a, b)
	if err != nil {
		// Old compare panics if err, so here we do the same thing now.
		// TODO: return err instead of panic.
		panic(fmt.Sprintf("should never happend %v", err))
	}
	return n
}

// Explain implements plan.Plan Explain interface.
func (r *indexPlan) Explain(w format.Formatter) {
	w.Format("┌Iterate rows of table %q using index %q where %s in ", r.src.TableName(), r.idxName, r.col.Name.L)
	for _, span := range r.spans {
		open := "["
		close := "]"
		if span.lowExclude {
			open = "("
		}
		if span.highExclude {
			close = ")"
		}
		w.Format("%s%v,%v%s ", open, span.lowVal, span.highVal, close)
	}
	w.Format("\n└Output field names %v\n", field.RFQNames(r.GetFields()))
}

// GetFields implements plan.Plan GetFields interface.
func (r *indexPlan) GetFields() []*field.ResultField {
	return field.ColsToResultFields(r.src.Cols(), r.src.TableName().O)
}

// Filter implements plan.Plan Filter interface.
// Filter merges BinaryOperations and determines the lower and upper bound.
func (r *indexPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	var spans []*indexSpan
	switch x := expr.(type) {
	case *expression.BinaryOperation:
		ok, name, val, err := x.IsIdentCompareVal()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			break
		}
		_, tname, cname := field.SplitQualifiedName(name)
		if tname != "" && r.src.TableName().L != tname {
			break
		}
		if r.col.ColumnInfo.Name.L != cname {
			break
		}
		seekVal, err := types.Convert(val, &r.col.FieldType)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		spans = filterSpans(r.spans, toSpans(x.Op, val, seekVal))
	case *expression.Ident:
		if r.col.Name.L != x.L {
			break
		}
		spans = filterSpans(r.spans, toSpans(opcode.GE, minNotNullVal, nil))
	case *expression.UnaryOperation:
		if x.Op != '!' {
			break
		}

		cname, ok := x.V.(*expression.Ident)
		if !ok {
			break
		}
		if r.col.Name.L != cname.L {
			break
		}
		spans = filterSpans(r.spans, toSpans(opcode.EQ, nil, nil))
	}

	if spans == nil {
		return r, false, nil
	}

	return &indexPlan{
		src:     r.src,
		col:     r.col,
		unique:  r.unique,
		idxName: r.idxName,
		idx:     r.idx,
		spans:   spans,
	}, true, nil
}

// return the intersection range between origin and filter.
func filterSpans(origin []*indexSpan, filter []*indexSpan) []*indexSpan {
	newSpans := make([]*indexSpan, 0, len(filter))
	for _, fSpan := range filter {
		for _, oSpan := range origin {
			newSpan := oSpan.cutOffLow(fSpan.lowVal, fSpan.lowExclude)
			if newSpan == nil {
				continue
			}
			newSpan = newSpan.cutOffHigh(fSpan.highVal, fSpan.highExclude)
			if newSpan == nil {
				continue
			}
			newSpans = append(newSpans, newSpan)
		}
	}
	return newSpans
}

// generate a slice of span from operator and value.
func toSpans(op opcode.Op, val, seekVal interface{}) []*indexSpan {
	var spans []*indexSpan
	switch op {
	case opcode.EQ:
		spans = append(spans, &indexSpan{
			seekVal:     seekVal,
			lowVal:      val,
			lowExclude:  false,
			highVal:     val,
			highExclude: false,
		})
	case opcode.LT:
		spans = append(spans, &indexSpan{
			lowVal:      minNotNullVal,
			highVal:     val,
			highExclude: true,
		})
	case opcode.LE:
		spans = append(spans, &indexSpan{
			lowVal:      minNotNullVal,
			highVal:     val,
			highExclude: false,
		})
	case opcode.GE:
		spans = append(spans, &indexSpan{
			seekVal:    seekVal,
			lowVal:     val,
			lowExclude: false,
			highVal:    maxVal,
		})
	case opcode.GT:
		spans = append(spans, &indexSpan{
			seekVal:    seekVal,
			lowVal:     val,
			lowExclude: true,
			highVal:    maxVal,
		})
	case opcode.NE:
		spans = append(spans, &indexSpan{
			lowVal:      minNotNullVal,
			highVal:     val,
			highExclude: true,
		})
		spans = append(spans, &indexSpan{
			seekVal:    seekVal,
			lowVal:     val,
			lowExclude: true,
			highVal:    maxVal,
		})
	default:
		panic("should never happen")
	}
	return spans
}

// Next implements plan.Plan Next interface.
func (r *indexPlan) Next(ctx context.Context) (*plan.Row, error) {
	for {
		if r.cursor == len(r.spans) {
			return nil, nil
		}
		span := r.spans[r.cursor]
		if r.isPointLookup(span) {
			// Do point lookup on index will prevent prefetch cost.
			row, err := r.pointLookup(ctx, span.seekVal)
			if err != nil {
				return nil, errors.Trace(err)
			}
			r.cursor++
			if row != nil {
				return row, nil
			}
			continue
		}
		if r.iter == nil {
			seekVal := span.seekVal
			if span.lowVal == minNotNullVal {
				seekVal = []byte{}
			}
			var txn kv.Transaction
			txn, err := ctx.GetTxn(false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			r.iter, _, err = r.idx.Seek(txn, []interface{}{seekVal})
			if err != nil {
				return nil, types.EOFAsNil(err)
			}
		}
		idxKey, h, err := r.iter.Next()
		if err != nil {
			return nil, types.EOFAsNil(err)
		}
		val := idxKey[0]
		if !r.skipLowCmp {
			cmp := indexCompare(val, span.lowVal)
			if cmp < 0 || (cmp == 0 && span.lowExclude) {
				continue
			}
			r.skipLowCmp = true
		}
		cmp := indexCompare(val, span.highVal)
		if cmp > 0 || (cmp == 0 && span.highExclude) {
			// This span has finished iteration.
			// Move to the next span.
			r.iter.Close()
			r.iter = nil
			r.cursor++
			r.skipLowCmp = false
			continue
		}
		var row *plan.Row

		txn, err := ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Because we're in a range query, we can prefetch next few lines
		// in one RPC call fill the cache, for reducing RPC calls.
		txn.SetOption(kv.RangePrefetchOnCacheMiss, nil)
		row, err = r.lookupRow(ctx, h)
		txn.DelOption(kv.RangePrefetchOnCacheMiss)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return row, nil
	}
}

func (r *indexPlan) isPointLookup(span *indexSpan) bool {
	equalOp := span.lowVal == span.highVal && !span.lowExclude && !span.highExclude
	if !equalOp || !r.unique || span.lowVal == nil {
		return false
	}
	n, err := types.Compare(span.seekVal, span.lowVal)
	if err != nil {
		return false
	}
	return n == 0
}

func (r *indexPlan) lookupRow(ctx context.Context, h int64) (*plan.Row, error) {
	row := &plan.Row{}
	var err error
	row.Data, err = r.src.Row(ctx, h)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rowKey := &plan.RowKeyEntry{
		Tbl: r.src,
		Key: string(r.src.RecordKey(h, nil)),
	}
	row.RowKeys = append(row.RowKeys, rowKey)
	return row, nil
}

// pointLookup do not seek index but call Exists method to get a handle, which is cheaper.
func (r *indexPlan) pointLookup(ctx context.Context, val interface{}) (*plan.Row, error) {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var exist bool
	var h int64
	// We expect a kv.ErrKeyExists Error because we pass -1 as the handle which is not equal to the existed handle.
	exist, h, err = r.idx.Exist(txn, []interface{}{val}, -1)
	if !exist {
		return nil, errors.Trace(err)
	}
	if terror.ErrorNotEqual(kv.ErrKeyExists, err) {
		return nil, errors.Trace(err)
	}
	var row *plan.Row
	row, err = r.lookupRow(ctx, h)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

// Close implements plan.Plan Close interface.
func (r *indexPlan) Close() error {
	if r.iter != nil {
		r.iter.Close()
		r.iter = nil
	}
	r.cursor = 0
	r.skipLowCmp = false
	return nil
}
