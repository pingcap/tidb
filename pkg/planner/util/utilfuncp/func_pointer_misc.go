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

package utilfuncp

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// this file is used for passing function pointer at init(){} to avoid some import cycles.

// HasMaxOneRowUtil is used in baseLogicalPlan implementation of LogicalPlan interface, while
// the original HasMaxOneRowUtil has some dependency of original core pkg: like Datasource which
// hasn't been moved out of core pkg, so associative func pointer is introduced.
// todo: (1) arenatlx, remove this func pointer when concrete Logical Operators moved out of core.
var HasMaxOneRowUtil func(p base.LogicalPlan, childMaxOneRow []bool) bool

// AppendCandidate4PhysicalOptimizeOp is used in all logicalOp's findBestTask to trace the physical
// optimizing steps. Since we try to move baseLogicalPlan out of core, then other concrete logical
// operators, this appendCandidate4PhysicalOptimizeOp will make logicalOp/pkg back import core/pkg;
// if we move appendCandidate4PhysicalOptimizeOp together with baseLogicalPlan to logicalOp/pkg, it
// will heavily depend on concrete other logical operators inside, which are still defined in core/pkg
// too.
// todo: (2) arenatlx, remove this func pointer when concrete Logical Operators moved out of core.
var AppendCandidate4PhysicalOptimizeOp func(pop *optimizetrace.PhysicalOptimizeOp, lp base.LogicalPlan,
	pp base.PhysicalPlan, prop *property.PhysicalProperty)

// GetTaskPlanCost returns the cost of this task.
// The new cost interface will be used if EnableNewCostInterface is true.
// The second returned value indicates whether this task is valid.
// todo: (3) arenatlx, remove this func pointer when Task pkg is moved out of core, and
// getTaskPlanCost can be some member function usage of its family.
var GetTaskPlanCost func(t base.Task, pop *optimizetrace.PhysicalOptimizeOp) (float64, bool, error)

// AddSelection will add a selection if necessary.
// This function is util function pointer that initialized by core functionality.
// todo: (3) arenatlx, remove this func pointer when inside referred LogicalSelection is moved out of core.
var AddSelection func(p base.LogicalPlan, child base.LogicalPlan, conditions []expression.Expression,
	chIdx int, opt *optimizetrace.LogicalOptimizeOp)
