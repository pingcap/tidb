// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ExplainForConnectionExec returns the minimal session payload used by the
// caller side to render EXPLAIN FOR CONNECTION.
type ExplainForConnectionExec struct {
	exec.BaseExecutor

	connectionID uint64
	done         bool
}

// Open implements the Executor interface.
func (e *ExplainForConnectionExec) Open(context.Context) error {
	e.done = false
	return nil
}

// Next implements the Executor interface.
func (e *ExplainForConnectionExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	sm := e.Ctx().GetSessionManager()
	if sm == nil {
		return nil
	}

	pi, ok := sm.GetProcessInfo(e.connectionID)
	if !ok {
		return nil
	}
	briefBinaryPlan := pi.BriefBinaryPlan
	if plan, ok := pi.Plan.(base.Plan); ok && plan != nil && pi.RuntimeStatsColl != nil {
		if regenerated := plannercore.GetBriefBinaryPlan(plan, pi.RuntimeStatsColl); regenerated != "" {
			briefBinaryPlan = regenerated
		}
	}

	req.AppendString(0, pi.User)
	req.AppendString(1, pi.Host)
	req.AppendString(2, briefBinaryPlan)
	return nil
}

func (b *executorBuilder) buildExplainForConnection(v *plannercore.ExplainForConnection) exec.Executor {
	return &ExplainForConnectionExec{
		BaseExecutor: exec.NewBaseExecutor(b.sctx, v.Schema(), v.ID()),
		connectionID: v.ConnectionID,
	}
}
