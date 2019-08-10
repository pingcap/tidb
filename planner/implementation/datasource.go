// Copyright 2019 PingCAP, Inc.
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

package implementation

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
)

// TableDualImpl implementation of PhysicalTableDual.
type TableDualImpl struct {
	baseImpl
}

// NewTableDualImpl creates a new table dual Implementation.
func NewTableDualImpl(dual *plannercore.PhysicalTableDual) *TableDualImpl {
	return &TableDualImpl{baseImpl{plan: dual}}
}

// CalcCost calculates the cost of the table dual Implementation.
func (impl *TableDualImpl) CalcCost(outCount float64, childCosts []float64, children ...*memo.Group) float64 {
	return 0
}
