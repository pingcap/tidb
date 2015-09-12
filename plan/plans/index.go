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

	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
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
	src     table.Table
	colName string
	idxName string
	idx     kv.Index
	spans   []*indexSpan // multiple spans are ordered by their values and without overlapping.
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

func (r *indexPlan) doSpan(ctx context.Context, txn kv.Transaction, span *indexSpan, f plan.RowIterFunc) error {
	seekVal := span.lowVal
	if span.lowVal == minNotNullVal {
		seekVal = []byte{}
	}
	it, _, err := r.idx.Seek(txn, []interface{}{seekVal})
	if err != nil {
		return types.EOFAsNil(err)
	}
	defer it.Close()
	var skipLowCompare bool
	for {
		k, h, err := it.Next()
		if err != nil {
			return types.EOFAsNil(err)
		}
		val := k[0]
		if !skipLowCompare {
			if span.lowExclude && indexCompare(span.lowVal, val) == 0 {
				continue
			}
			skipLowCompare = true
		}
		cmp := indexCompare(span.highVal, val)
		if cmp < 0 || (cmp == 0 && span.highExclude) {
			return nil
		}
		data, err := r.src.Row(ctx, h)
		if err != nil {
			return err
		}

		if more, err := f(h, data); err != nil || !more {
			return err
		}
	}
}

// Do implements plan.Plan Do interface.
// It scans a span from the lower bound to upper bound.
func (r *indexPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	for _, span := range r.spans {
		err := r.doSpan(ctx, txn, span, f)
		if err != nil {
			return err
		}
	}
	return nil
}

// Explain implements plan.Plan Explain interface.
func (r *indexPlan) Explain(w format.Formatter) {
	w.Format("┌Iterate rows of table %q using index %q where %s in ", r.src.TableName(), r.idxName, r.colName)
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
// Filter merges BinaryOperations, and determines the lower and upper bound.
func (r *indexPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	switch x := expr.(type) {
	case *expressions.BinaryOperation:
		ok, cname, val, err := x.IsIdentRelOpVal()
		if err != nil {
			return nil, false, err
		}

		if !ok || r.colName != cname {
			break
		}

		col := column.FindCol(r.src.Cols(), cname)
		if col == nil {
			break
		}

		if val, err = types.Convert(val, &col.FieldType); err != nil {
			return nil, false, err
		}
		r.spans = filterSpans(r.spans, toSpans(x.Op, val))
		return r, true, nil
	case *expressions.Ident:
		if r.colName != x.L {
			break
		}
		r.spans = filterSpans(r.spans, toSpans(opcode.GE, minNotNullVal))
		return r, true, nil
	case *expressions.UnaryOperation:
		if x.Op != '!' {
			break
		}

		operand, ok := x.V.(*expressions.Ident)
		if !ok {
			break
		}

		cname := operand.L
		if r.colName != cname {
			break
		}
		r.spans = filterSpans(r.spans, toSpans(opcode.EQ, nil))
		return r, true, nil
	}

	return r, false, nil
}

// return the intersection range between origin and filter.
func filterSpans(origin []*indexSpan, filter []*indexSpan) []*indexSpan {
	var newSpans []*indexSpan
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
func toSpans(op opcode.Op, val interface{}) []*indexSpan {
	var spans []*indexSpan
	switch op {
	case opcode.EQ:
		spans = append(spans, &indexSpan{
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
			lowVal:     val,
			lowExclude: false,
			highVal:    maxVal,
		})
	case opcode.GT:
		spans = append(spans, &indexSpan{
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
			lowVal:     val,
			lowExclude: true,
			highVal:    maxVal,
		})
	default:
		panic("should never happen")
	}
	return spans
}
