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
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
)

func (b *executorBuilder) buildRefreshMaterializedView(v *plannercore.RefreshMaterializedView) exec.Executor {
	return &RefreshMaterializedViewExec{
		BaseExecutor: exec.NewBaseExecutor(b.sctx, v.Schema(), v.ID()),
		stmt:         v.Statement,
	}
}

func (b *executorBuilder) buildDryRunRefreshMaterializedView(v *plannercore.DryRunRefreshMaterializedView) exec.Executor {
	return &RefreshMaterializedViewDryRunExec{
		BaseExecutor: exec.NewBaseExecutor(b.sctx, v.Schema(), v.ID()),
		stmt:         v.Statement,
	}
}

func (b *executorBuilder) buildProfileRefreshMaterializedView(v *plannercore.ProfileRefreshMaterializedView) exec.Executor {
	return &RefreshMaterializedViewProfileExec{
		BaseExecutor: exec.NewBaseExecutor(b.sctx, v.Schema(), v.ID()),
		stmt:         v.Statement,
	}
}
