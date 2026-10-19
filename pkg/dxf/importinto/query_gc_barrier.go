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

package importinto

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/util/gcutil"
)

// QueryGCBarrierTTL covers several task keeper rounds and a DXF owner switch.
const QueryGCBarrierTTL = 5 * time.Minute

// QueryGCBarrierKeeper keeps the ReadTS of an IMPORT INTO ... FROM SELECT task
// from GC until its query step finishes, including while the task waits for
// resources. The barrier is not deleted; it expires within QueryGCBarrierTTL
// once the keeper stops renewing it.
type QueryGCBarrierKeeper struct{}

// KeepTask implements scheduler.TaskKeeper.
func (QueryGCBarrierKeeper) KeepTask(
	ctx context.Context, task *proto.Task, acquireRuntime func() (sqlsvrapi.Runtime, func(), error),
) error {
	readTS, need, err := taskNeedKeepGC(task)
	if err != nil || !need {
		return err
	}
	taskRuntime, release, err := acquireRuntime()
	if err != nil {
		return err
	}
	defer release()
	return gcutil.SetGCBarrier(ctx, taskRuntime.Store(), QueryGCBarrierID(readTS), readTS, QueryGCBarrierTTL)
}

// taskNeedKeepGC returns whether task still reads at its ReadTS, and the
// ReadTS to keep from GC if so.
func taskNeedKeepGC(task *proto.Task) (uint64, bool, error) {
	// Later steps no longer read at ReadTS.
	if task.Step != proto.StepInit && task.Step != proto.ImportStepQuery {
		return 0, false, nil
	}
	var taskMeta TaskMeta
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return 0, false, errors.Trace(err)
	}
	// ReadTS is unset until the query snapshot is decided.
	query := taskMeta.Plan.Query
	if query == nil || query.ReadTS == 0 {
		return 0, false, nil
	}
	return query.ReadTS, true, nil
}

// QueryGCBarrierID returns the GC barrier ID that protects readTS.
func QueryGCBarrierID(readTS uint64) string {
	return fmt.Sprintf("import-into-query-%d", readTS)
}

func init() {
	scheduler.RegisterTaskKeeper(proto.ImportInto, QueryGCBarrierKeeper{})
}
