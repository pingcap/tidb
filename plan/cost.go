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
	IndexCost        = 1.1
	SortCost         = 2.0
	FilterRate       = 0.5
)

// CostEstimator estimates the cost of a plan.

// Enter implements Visitor Enter interface.
func estimate(p Plan) {
	var child Plan
	for _, c := range p.GetChildren() {
		estimate(c)
		child = c
	}
	switch v := p.(type) {
	case *IndexScan:
		indexScan(v)
	case *Limit:
		v.rowCount = child.RowCount()
		v.startupCost = child.StartupCost()
		v.totalCost = child.TotalCost()
	case *SelectFields:
		if child != nil {
			v.startupCost = child.StartupCost()
			v.rowCount = child.RowCount()
			v.totalCost = child.TotalCost()
		}
	case *SelectLock:
		v.startupCost = child.StartupCost()
		v.rowCount = child.RowCount()
		v.totalCost = child.TotalCost()
	case *Sort:
		// Sort plan must retrieve all the rows before returns the first row.
		v.startupCost = child.TotalCost() + child.RowCount()*SortCost
		if v.limit == 0 {
			v.rowCount = child.RowCount()
		} else {
			v.rowCount = math.Min(child.RowCount(), v.limit)
		}
		v.totalCost = v.startupCost + v.rowCount*RowCost
	case *TableScan:
		tableScan(v)
	}
}

func tableScan(v *TableScan) {
	var rowCount float64 = FullRangeCount
	for _, con := range v.AccessConditions {
		rowCount *= guesstimateFilterRate(con)
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

func indexScan(v *IndexScan) {
	var rowCount float64 = FullRangeCount
	for _, con := range v.AccessConditions {
		rowCount *= guesstimateFilterRate(con)
	}
	v.startupCost = 0
	if v.limit == 0 {
		// limit is zero means no limit.
		v.rowCount = rowCount
	} else {
		v.rowCount = math.Min(rowCount, v.limit)
	}
	v.totalCost = v.rowCount * IndexCost
}

// EstimateCost estimates the cost of the plan.
func EstimateCost(p Plan) float64 {
	estimate(p)
	return p.TotalCost()
}
