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

package plan

import (
	"math"
)

// Pre-defined cost factors.
const (
	FullRangeCount   = 10000
	HalfRangeCount   = 4000
	MiddleRangeCount = 100
	RowCost          = 1.0
	IndexCost        = 2.0
	SortCost         = 2.0
	FilterRate       = 0.5
)

// CostEstimator estimates the cost of a plan.
type costEstimator struct {
}

// Enter implements Visitor Enter interface.
func (c *costEstimator) Enter(p Plan) (Plan, bool) {
	return p, false
}

// Leave implements Visitor Leave interface.
func (c *costEstimator) Leave(p Plan) (Plan, bool) {
	switch v := p.(type) {
	case *IndexScan:
		c.indexScan(v)
	case *TableScan:
		c.tableScan(v)
	case *SelectFields:
		if v.Src() != nil {
			v.startupCost = v.Src().StartupCost()
			v.rowCount = v.Src().RowCount()
			v.totalCost = v.Src().TotalCost()
		}
	case *SelectLock:
		v.startupCost = v.Src().StartupCost()
		v.rowCount = v.Src().RowCount()
		v.totalCost = v.Src().TotalCost()
	case *Filter:
		v.startupCost = v.Src().StartupCost()
		v.rowCount = v.Src().RowCount() * FilterRate
		v.totalCost = v.Src().TotalCost()
	case *Sort:
		if v.Bypass {
			// Bypassed sort doesn't add extra cost.
			v.startupCost = v.Src().StartupCost()
			v.rowCount = v.Src().RowCount()
			v.totalCost = v.Src().TotalCost()
		} else {
			// Sort plan must retrieves all the rows before returns the first row.
			v.startupCost = v.Src().TotalCost() + v.Src().RowCount()*SortCost
			if v.limit == 0 {
				v.rowCount = v.Src().RowCount()
			} else {
				v.rowCount = math.Min(v.Src().RowCount(), v.limit)
			}
			v.totalCost = v.startupCost + v.rowCount*RowCost
		}
	case *Limit:
		v.rowCount = v.Src().RowCount()
		v.startupCost = v.Src().StartupCost()
		v.totalCost = v.Src().TotalCost()
	}
	return p, true
}

func (c *costEstimator) tableScan(v *TableScan) {
	var rowCount float64
	if len(v.Ranges) == 1 && v.Ranges[0].LowVal == math.MinInt64 && v.Ranges[0].HighVal == math.MaxInt64 {
		// full range use default row count.
		rowCount = FullRangeCount
	} else {
		for _, v := range v.Ranges {
			// for condition like 'a = 0'.
			if v.LowVal == v.HighVal {
				rowCount++
				continue
			}
			// For condition like 'a < 0'.
			if v.LowVal == math.MinInt64 {
				rowCount += HalfRangeCount
			}
			// For condition like 'a > 0'.
			if v.HighVal == math.MaxInt64 {
				rowCount += HalfRangeCount
			}
			// For condition like 'a > 0 and a < 1'.
			rowCount += 100
		}
	}
	v.startupCost = 0
	if v.limit == 0 {
		// limit is zero means no limit.
		v.rowCount = rowCount
	} else {
		v.rowCount = math.Min(rowCount, v.limit)
	}
	v.totalCost = v.rowCount * RowCost
}

func (c *costEstimator) indexScan(v *IndexScan) {
	var rowCount float64
	if len(v.Ranges) == 1 && v.Ranges[0].LowVal[0] == nil && v.Ranges[0].HighVal[0] == MaxVal {
		// full range use default row count.
		rowCount = FullRangeCount
	} else {
		for _, v := range v.Ranges {
			// for condition like 'a = 0'.
			if v.IsPoint() {
				rowCount++
				continue
			}
			// For condition like 'a < 0'.
			if v.LowVal[0] == nil || v.LowVal[0] == MinNotNullVal {
				rowCount += HalfRangeCount
			}
			// For condition like 'a > 0'.
			if v.HighVal[0] == MaxVal {
				rowCount += HalfRangeCount
			}
			// For condition like 'a > 0 and a < 1'.
			rowCount += 100
		}
		// If the index has too many ranges, the row count may exceed the default row count.
		// Make sure the cost is lower than full range.
		if rowCount >= FullRangeCount {
			rowCount = FullRangeCount - 1
		}
	}
	v.startupCost = 0
	if v.limit == 0 {
		// limit is zero means no limit.
		v.rowCount = rowCount
	} else {
		v.rowCount = math.Min(rowCount, v.limit)
	}
	v.totalCost = v.rowCount * RowCost
}

// EstimateCost estimates the cost of the plan.
func EstimateCost(p Plan) float64 {
	var estimator costEstimator
	p.Accept(&estimator)
	return p.TotalCost()
}
