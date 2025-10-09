// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package findbesttask

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
)

// Handle handles the findBestTask for different logical plan.
func Handle(e base.LogicalPlan, prop *property.PhysicalProperty) (bestTask base.Task, err error) {
	// since different logical operator may have different findBestTask before like:
	// 	utilfuncp.FindBestTask4BaseLogicalPlan = findBestTask
	//	utilfuncp.FindBestTask4LogicalCTE = findBestTask4LogicalCTE
	//	utilfuncp.FindBestTask4LogicalShow = findBestTask4LogicalShow
	//	utilfuncp.FindBestTask4LogicalCTETable = findBestTask4LogicalCTETable
	//	utilfuncp.FindBestTask4LogicalMemTable = findBestTask4LogicalMemTable
	//	utilfuncp.FindBestTask4LogicalDataSource = findBestTask4LogicalDataSource
	//	utilfuncp.FindBestTask4LogicalShowDDLJobs = findBestTask4LogicalShowDDLJobs
	// once we call GE's findBestTask from group expression level, we should judge from here, and get the
	// wrapped logical plan and then call their specific function pointer to handle logic inside. At the
	// same time, we will pass ge (also implement LogicalPlan interface) as the first parameter for iterate
	// ge's children in memo scenario.
	// And since base.LogicalPlan is a common parent pointer of GE and LogicalPlan, we can use same portal.
	switch e.GetWrappedLogicalPlan().(type) {
	case *logicalop.LogicalCTE:
		return utilfuncp.FindBestTask4LogicalCTE(e, prop)
	case *logicalop.LogicalShow:
		return utilfuncp.FindBestTask4LogicalShow(e, prop)
	case *logicalop.LogicalCTETable:
		return utilfuncp.FindBestTask4LogicalCTETable(e, prop)
	case *logicalop.LogicalMemTable:
		return utilfuncp.FindBestTask4LogicalMemTable(e, prop)
	case *logicalop.LogicalTableDual:
		return physicalop.FindBestTask4LogicalTableDual(e, prop)
	case *logicalop.DataSource:
		return utilfuncp.FindBestTask4LogicalDataSource(e, prop)
	case *logicalop.LogicalShowDDLJobs:
		return utilfuncp.FindBestTask4LogicalShowDDLJobs(e, prop)
	default:
		return utilfuncp.FindBestTask4BaseLogicalPlan(e, prop)
	}
}
