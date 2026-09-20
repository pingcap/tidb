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

package dxfutil

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// sessionProvider provides a session used to access the SQL server runtime.
type sessionProvider interface {
	WithNewSession(func(se sessionctx.Context) error) error
}

// AcquireTaskRuntime returns a runtime view for the task keyspace and a release function.
// The sessionProvider must supply sessions whose store keyspace is the current node's keyspace;
// this is used to detect whether the task belongs to a different keyspace.
// Callers must call the release function when the returned runtime is no longer used.
func AcquireTaskRuntime(
	sessionProvider sessionProvider,
	taskKS string,
	holderID string,
) (sqlsvrapi.Runtime, func(), error) {
	var taskRuntime sqlsvrapi.Runtime
	if err := sessionProvider.WithNewSession(func(se sessionctx.Context) error {
		currentKS := se.GetStore().GetKeyspace()
		sqlServer := se.GetSQLServer()
		if taskKS != currentKS {
			var err2 error
			taskRuntime, err2 = sqlServer.AcquireKSRuntime(taskKS, holderID)
			return err2
		}
		taskRuntime = sqlServer.GetRuntime()
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return taskRuntime, func() {
		releaseTaskRuntime(taskRuntime)
	}, nil
}

func releaseTaskRuntime(runtime sqlsvrapi.Runtime) {
	if hdl, ok := runtime.(sqlsvrapi.KSRuntimeHandle); ok {
		hdl.Release()
	}
}

// CheckTaskRuntime checks if the runtime is valid for the task with the target keyspace.
func CheckTaskRuntime(runtime sqlsvrapi.Runtime, taskKS string) error {
	storeKS := runtime.Store().GetKeyspace()
	if storeKS != taskKS {
		// shouldn't happen normally, but since keyspace mismatch might cause
		// correctness error, we check it at runtime too.
		return errors.Trace(fmt.Errorf("store keyspace mismatch with task: %s vs %s",
			storeKS, taskKS))
	}
	taskMgr := storage.NewTaskManager(runtime.SysSessionPool())
	if err := taskMgr.WithNewSession(func(se sessionctx.Context) error {
		sessKs := se.GetStore().GetKeyspace()
		if storeKS != sessKs {
			// shouldn't happen normally. we do it for the same reason as above.
			return errors.Trace(fmt.Errorf("invalid task runtime with mismatched keyspace: %s vs %s",
				storeKS, sessKs))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GenHolderID generates a holder ID for the given DXF component and task ID.
func GenHolderID(component string, taskID int64) string {
	return fmt.Sprintf("DXF/%s/%d", component, taskID)
}
