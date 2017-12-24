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

package ranger

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// Range is the interface of the three type of range.
type Range interface {
	fmt.Stringer
	Convert2IntRange() IntColumnRange
	Convert2ColumnRange() *ColumnRange
	Convert2IndexRange() *IndexRange
}

// IntColumnRange represents a range for a integer column, both low and high are inclusive.
type IntColumnRange struct {
	LowVal  int64
	HighVal int64
}

// IsPoint returns if the table range is a point.
func (tr *IntColumnRange) IsPoint() bool {
	return tr.HighVal == tr.LowVal
}

func (tr IntColumnRange) String() string {
	var l, r string
	if tr.LowVal == math.MinInt64 {
		l = "(-inf"
	} else {
		l = "[" + strconv.FormatInt(tr.LowVal, 10)
	}
	if tr.HighVal == math.MaxInt64 {
		r = "+inf)"
	} else {
		r = strconv.FormatInt(tr.HighVal, 10) + "]"
	}
	return l + "," + r
}

// Convert2IntRange implements the Convert2IntRange interface.
func (tr IntColumnRange) Convert2IntRange() IntColumnRange {
	return tr
}

// Convert2ColumnRange implements the Convert2ColumnRange interface.
func (tr IntColumnRange) Convert2ColumnRange() *ColumnRange {
	panic("you shouldn't call this method.")
}

// Convert2IndexRange implements the Convert2IndexRange interface.
func (tr IntColumnRange) Convert2IndexRange() *IndexRange {
	panic("you shouldn't call this method.")
}

// ColumnRange represents a range for a column.
type ColumnRange struct {
	Low      types.Datum
	High     types.Datum
	LowExcl  bool
	HighExcl bool
}

func (cr *ColumnRange) String() string {
	var l, r string
	if cr.LowExcl {
		l = "("
	} else {
		l = "["
	}
	if cr.HighExcl {
		r = ")"
	} else {
		r = "]"
	}
	return l + formatDatum(cr.Low) + "," + formatDatum(cr.High) + r
}

// Convert2IntRange implements the Convert2IntRange interface.
func (cr *ColumnRange) Convert2IntRange() IntColumnRange {
	panic("you shouldn't call this method.")
}

// Convert2ColumnRange implements the Convert2ColumnRange interface.
func (cr *ColumnRange) Convert2ColumnRange() *ColumnRange {
	return cr
}

// Convert2IndexRange implements the Convert2IndexRange interface.
func (cr *ColumnRange) Convert2IndexRange() *IndexRange {
	panic("you shouldn't call this method.")
}

// IndexRange represents a range for an index.
type IndexRange struct {
	LowVal  []types.Datum
	HighVal []types.Datum

	LowExclude  bool // Low value is exclusive.
	HighExclude bool // High value is exclusive.
}

// Clone clones a IndexRange.
func (ir *IndexRange) Clone() *IndexRange {
	newRange := &IndexRange{
		LowVal:      make([]types.Datum, 0, len(ir.LowVal)),
		HighVal:     make([]types.Datum, 0, len(ir.HighVal)),
		LowExclude:  ir.LowExclude,
		HighExclude: ir.HighExclude,
	}
	for i, length := 0, len(ir.LowVal); i < length; i++ {
		newRange.LowVal = append(newRange.LowVal, ir.LowVal[i])
	}
	for i, length := 0, len(ir.HighVal); i < length; i++ {
		newRange.HighVal = append(newRange.HighVal, ir.HighVal[i])
	}
	return newRange
}

// IsPoint returns if the index range is a point.
func (ir *IndexRange) IsPoint(sc *stmtctx.StatementContext) bool {
	if len(ir.LowVal) != len(ir.HighVal) {
		return false
	}
	for i := range ir.LowVal {
		a := ir.LowVal[i]
		b := ir.HighVal[i]
		if a.Kind() == types.KindMinNotNull || b.Kind() == types.KindMaxValue {
			return false
		}
		cmp, err := a.CompareDatum(sc, &b)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}
	}
	return !ir.LowExclude && !ir.HighExclude
}

// Convert2IndexRange implements the Convert2IndexRange interface.
func (ir *IndexRange) String() string {
	lowStrs := make([]string, 0, len(ir.LowVal))
	for _, d := range ir.LowVal {
		lowStrs = append(lowStrs, formatDatum(d))
	}
	highStrs := make([]string, 0, len(ir.LowVal))
	for _, d := range ir.HighVal {
		highStrs = append(highStrs, formatDatum(d))
	}
	l, r := "[", "]"
	if ir.LowExclude {
		l = "("
	}
	if ir.HighExclude {
		r = ")"
	}
	return l + strings.Join(lowStrs, " ") + "," + strings.Join(highStrs, " ") + r
}

// Convert2IntRange implements the Convert2IntRange interface.
func (ir *IndexRange) Convert2IntRange() IntColumnRange {
	panic("you shouldn't call this method.")
}

// Convert2ColumnRange implements the Convert2ColumnRange interface.
func (ir *IndexRange) Convert2ColumnRange() *ColumnRange {
	panic("you shouldn't call this method.")
}

// Convert2IndexRange implements the Convert2IndexRange interface.
func (ir *IndexRange) Convert2IndexRange() *IndexRange {
	return ir
}

// PrefixEqualLen tells you how long the prefix of the range is a point.
// e.g. If this range is (1 2 3, 1 2 +inf), then the return value is 2.
func (ir *IndexRange) PrefixEqualLen(sc *stmtctx.StatementContext) (int, error) {
	// Here, len(ir.LowVal) always equal to len(ir.HighVal)
	for i := 0; i < len(ir.LowVal); i++ {
		cmp, err := ir.LowVal[i].CompareDatum(sc, &ir.HighVal[i])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp != 0 {
			return i, nil
		}
	}
	return len(ir.LowVal), nil
}

func formatDatum(d types.Datum) string {
	if d.Kind() == types.KindMinNotNull {
		return "-inf"
	}
	if d.Kind() == types.KindMaxValue {
		return "+inf"
	}
	return fmt.Sprintf("%v", d.GetValue())
}
