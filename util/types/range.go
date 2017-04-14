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

package types

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/sessionctx/variable"
)

// IntColumnRange represents a range for a integer column, both low and high are inclusive.
type IntColumnRange struct {
	LowVal  int64
	HighVal int64
}

// IsPoint returns if the table range is a point.
func (tr *IntColumnRange) IsPoint() bool {
	return tr.HighVal == tr.LowVal
}

// ColumnRange represents a range for a column.
type ColumnRange struct {
	Low      Datum
	High     Datum
	lowExcl  bool
	highExcl bool
}

// IndexRange represents a range for an index.
type IndexRange struct {
	LowVal  []Datum
	HighVal []Datum

	LowExclude  bool // Low value is exclusive.
	HighExclude bool // High value is exclusive.
}

// IsPoint returns if the index range is a point.
func (ir *IndexRange) IsPoint(sc *variable.StatementContext) bool {
	if len(ir.LowVal) != len(ir.HighVal) {
		return false
	}
	for i := range ir.LowVal {
		a := ir.LowVal[i]
		b := ir.HighVal[i]
		if a.Kind() == KindMinNotNull || b.Kind() == KindMaxValue {
			return false
		}
		cmp, err := a.CompareDatum(sc, b)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}
	}
	return !ir.LowExclude && !ir.HighExclude
}

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

// Align appends low value and high value up to the number of columns with max value, min not null value or null value.
func (ir *IndexRange) Align(numColumns int) {
	for i := len(ir.LowVal); i < numColumns; i++ {
		if ir.LowExclude {
			ir.LowVal = append(ir.LowVal, MaxValueDatum())
		} else {
			ir.LowVal = append(ir.LowVal, Datum{})
		}
	}
	for i := len(ir.HighVal); i < numColumns; i++ {
		if ir.HighExclude {
			ir.HighVal = append(ir.HighVal, Datum{})
		} else {
			ir.HighVal = append(ir.HighVal, MaxValueDatum())
		}
	}
}

func formatDatum(d Datum) string {
	if d.Kind() == KindMinNotNull {
		return "-inf"
	}
	if d.Kind() == KindMaxValue {
		return "+inf"
	}
	return fmt.Sprintf("%v", d.GetValue())
}
