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

package export

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
)

// initialMaxNodeCount is a placeholder submit-time node count; OnPrepare's
// setResources overwrites task.MaxNodeCount/RequiredSlots from the real,
// chunk-derived data size once the task starts running (NextGen only — see
// exportScheduler.OnPrepare).
const initialMaxNodeCount = 1

// SubmitTask submits an export DXF task. requiredSlots is a submit-time
// estimate; see initialMaxNodeCount for why the final resource sizing
// happens later, in OnPrepare. Prepare mode must be required, not the
// default: OnPrepare is where generateChunks/writePreparedPlan run and
// PreparedPlanPath gets set, and the framework skips OnPrepare entirely
// unless the task explicitly opts in.
func SubmitTask(ctx context.Context, store kv.Storage, taskKey string, requiredSlots int, meta *TaskMeta) (*proto.Task, error) {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	extraParams := proto.ExtraParams{PrepareMode: proto.PrepareModeRequired}
	return handle.SubmitTaskWithExtraParams(ctx, taskKey, proto.Export, store.GetKeyspace(),
		requiredSlots, handle.GetTargetScope(), initialMaxNodeCount, extraParams, metaBytes)
}
