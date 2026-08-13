// Copyright 2023 PingCAP, Inc.
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

package planner

import (
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
)

// Planner represents a distribute plan planner.
type Planner struct{}

// NewPlanner creates a new planer instance.
func NewPlanner() *Planner {
	return &Planner{}
}

// Run runs the distribute plan.
func (*Planner) Run(planCtx PlanCtx, plan LogicalPlan, taskMgr *storage.TaskManager) (int64, error) {
	taskMeta, err := plan.ToTaskMeta()
	if err != nil {
		return 0, err
	}

	targetScope := handle.GetTargetScope()

	return taskMgr.CreateTaskWithSession(
		planCtx.Ctx,
		planCtx.SessionCtx,
		planCtx.TaskKey,
		planCtx.TaskType,
		planCtx.Keyspace,
		planCtx.ThreadCnt,
		targetScope,
		planCtx.MaxNodeCnt,
		plan.GetTaskExtraParams(),
		taskMeta,
	)
}
