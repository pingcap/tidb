// Copyright 2024 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

var (
	_ base.LogicalPlan = &logicalop.LogicalJoin{}
	_ base.LogicalPlan = &logicalop.LogicalAggregation{}
	_ base.LogicalPlan = &logicalop.LogicalProjection{}
	_ base.LogicalPlan = &logicalop.LogicalSelection{}
	_ base.LogicalPlan = &logicalop.LogicalApply{}
	_ base.LogicalPlan = &logicalop.LogicalMaxOneRow{}
	_ base.LogicalPlan = &logicalop.LogicalTableDual{}
	_ base.LogicalPlan = &DataSource{}
	_ base.LogicalPlan = &TiKVSingleGather{}
	_ base.LogicalPlan = &LogicalTableScan{}
	_ base.LogicalPlan = &LogicalIndexScan{}
	_ base.LogicalPlan = &LogicalUnionAll{}
	_ base.LogicalPlan = &logicalop.LogicalSort{}
	_ base.LogicalPlan = &logicalop.LogicalLock{}
	_ base.LogicalPlan = &logicalop.LogicalLimit{}
	_ base.LogicalPlan = &logicalop.LogicalWindow{}
	_ base.LogicalPlan = &LogicalExpand{}
	_ base.LogicalPlan = &logicalop.LogicalUnionScan{}
	_ base.LogicalPlan = &logicalop.LogicalMemTable{}
	_ base.LogicalPlan = &logicalop.LogicalShow{}
	_ base.LogicalPlan = &logicalop.LogicalShowDDLJobs{}
	_ base.LogicalPlan = &LogicalCTE{}
	_ base.LogicalPlan = &logicalop.LogicalCTETable{}
	_ base.LogicalPlan = &logicalop.LogicalSequence{}
)
