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

// maxNodeCount limits the export task to a single executor node for now;
// multi-node execution and auto-scaling sizing come later.
const maxNodeCount = 1

// SubmitTask submits an export DXF task.
func SubmitTask(ctx context.Context, store kv.Storage, taskKey string, concurrency int, meta *TaskMeta) (*proto.Task, error) {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return handle.SubmitTask(ctx, taskKey, proto.Export, store.GetKeyspace(),
		concurrency, handle.GetTargetScope(), maxNodeCount, metaBytes)
}
