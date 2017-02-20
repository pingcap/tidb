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

package mocktikv

import (
	"container/heap"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

type sortRow struct {
	key  []types.Datum
	meta tipb.RowMeta
	data []byte
}

// topnSorter implements sort.Interface. When all rows have been processed, the topnSorter will sort the whole data in heap.
type topnSorter struct {
	orderByItems []*tipb.ByItem
	rows         []*sortRow
	err          error
	sc           *variable.StatementContext
}

func (t *topnSorter) Len() int {
	return len(t.rows)
}

func (t *topnSorter) Swap(i, j int) {
	t.rows[i], t.rows[j] = t.rows[j], t.rows[i]
}

func (t *topnSorter) Less(i, j int) bool {
	for index, by := range t.orderByItems {
		v1 := t.rows[i].key[index]
		v2 := t.rows[j].key[index]

		ret, err := v1.CompareDatum(t.sc, v2)
		if err != nil {
			t.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}

	return false
}

// topnHeap holds the top n elements using heap structure. It implements heap.Interface.
// When we insert a row, topnHeap will check if the row can become one of the top n element or not.
type topnHeap struct {
	topnSorter

	// totalCount is equal to the limit count, which means the max size of heap.
	totalCount int
	// heapSize means the current size of this heap.
	heapSize int
}

func (t *topnHeap) Len() int {
	return t.heapSize
}

func (t *topnHeap) Push(x interface{}) {
	t.rows = append(t.rows, x.(*sortRow))
	t.heapSize++
}

func (t *topnHeap) Pop() interface{} {
	return nil
}

func (t *topnHeap) Less(i, j int) bool {
	for index, by := range t.orderByItems {
		v1 := t.rows[i].key[index]
		v2 := t.rows[j].key[index]

		ret, err := v1.CompareDatum(t.sc, v2)
		if err != nil {
			t.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret > 0 {
			return true
		} else if ret < 0 {
			return false
		}
	}

	return false
}

// tryToAddRow tries to add a row to heap.
// When this row is not less than any rows in heap, it will never become the top n element.
// Then this function returns false.
func (t *topnHeap) tryToAddRow(row *sortRow) bool {
	success := false
	if t.heapSize == t.totalCount {
		t.rows = append(t.rows, row)
		// When this row is less than the top element, it will replace it and adjust the heap structure.
		if t.Less(0, t.heapSize) {
			t.Swap(0, t.heapSize)
			heap.Fix(t, 0)
			success = true
		}
		t.rows = t.rows[:t.heapSize]
	} else {
		heap.Push(t, row)
		success = true
	}
	return success
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (h *rpcHandler) evalTopN(ctx *selectContext, handle int64, values map[int64][]byte, columns []*tipb.ColumnInfo) error {
	err := h.setColumnValueToCtx(ctx, handle, values, ctx.topnColumns)
	if err != nil {
		return errors.Trace(err)
	}
	newRow := &sortRow{
		meta: tipb.RowMeta{Handle: handle},
	}
	for _, item := range ctx.topnHeap.orderByItems {
		result, err := ctx.eval.Eval(item.Expr)
		if err != nil {
			return errors.Trace(err)
		}
		newRow.key = append(newRow.key, result)
	}
	if ctx.topnHeap.tryToAddRow(newRow) {
		for _, col := range columns {
			val := values[col.GetColumnId()]
			newRow.data = append(newRow.data, val...)
			newRow.meta.Length += int64(len(val))
		}
	}
	return errors.Trace(ctx.topnHeap.err)
}
