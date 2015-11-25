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
	DefaultRowCount = 10000
	RowCost         = 1.0
	IndexCost       = 2.0
	SortCost        = 2.0
	FilterRate      = 0.5
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
		v.startupCost = 0
		v.rowCount = math.Min(DefaultRowCount, v.limit)
		v.totalCost = v.rowCount * RowCost
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
		v.startupCost = v.Src().TotalCost() + v.Src().RowCount()*SortCost
		v.rowCount = math.Min(v.Src().RowCount(), v.limit)
		v.totalCost = v.startupCost + v.rowCount*RowCost
	case *Limit:
		v.rowCount = v.Src().RowCount()
		v.startupCost = v.Src().StartupCost()
		v.totalCost = v.Src().TotalCost()
	}
	return p, true
}

func (c *costEstimator) indexScan(v *IndexScan) {
	var rowCount float64
	for _, v := range v.Ranges {
		if v.IsPoint() {
			rowCount++
			continue
		}
		if v.LowVal[0] == nil || v.LowVal[0] == MinNotNullVal {
			rowCount += 4000
		}
		if v.HighVal[0] == MaxVal {
			rowCount += 4000
		}
		rowCount += 1000
	}
	if rowCount > DefaultRowCount {
		rowCount = DefaultRowCount
	}
	v.startupCost = 0
	v.rowCount = math.Min(rowCount, v.limit)
	v.totalCost = v.rowCount * RowCost
}

// EstimateCost estimates the cost of the plan.
func EstimateCost(p Plan) float64 {
	var estimator costEstimator
	p.Accept(&estimator)
	return p.TotalCost()
}
