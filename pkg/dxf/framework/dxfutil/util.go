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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
)

const (
	// TaskCancelMessage marks a task reverted because of user cancellation.
	TaskCancelMessage = "cancelled by user"
	taskDataError     = "data-error"
	taskCancelled     = "cancelled"
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

func withNewSession(pool util.SessionPool, fn func(se sessionctx.Context) error) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePerThousand(); err != nil {
		return err
	}
	v, err := pool.Get()
	if err != nil {
		return err
	}
	// When using global sort, the subtask meta might be quite large because it
	// includes the filenames of all generated KV and statistics files.
	se := v.(sessionctx.Context)
	limitBak := se.GetSessionVars().TxnEntrySizeLimit
	defer func() {
		se.GetSessionVars().TxnEntrySizeLimit = limitBak
		pool.Put(v)
	}()
	se.GetSessionVars().TxnEntrySizeLimit = vardef.TxnEntrySizeLimit.Load()
	return fn(se)
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
	if err := withNewSession(runtime.SysSessionPool(), func(se sessionctx.Context) error {
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

// IsCancelledErr checks whether an error marks a user-cancelled task.
func IsCancelledErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), TaskCancelMessage)
}

// IsDataError checks whether an error is caused by invalid or conflicting input data.
func IsDataError(taskErr error) bool {
	if taskErr == nil {
		return false
	}
	errMsg := taskErr.Error()
	// Keep these checks string-based to avoid depending on Lightning error definitions
	// from the DXF framework. We can replace this when those error definitions are
	// split out of the Lightning package. DXF error serialization keeps only the
	// outer error code, so nested data errors must be identified by their canonical
	// messages.
	//
	// import-into examples:
	// [Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk:
	// encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0
	// at offset 0: Value conversion failed for column 'event_id'. Expected type:
	// bigint, received value: ?. Reason: [types:1292]Truncated incorrect DOUBLE value: '?'.
	//
	// add-index examples:
	// [kv:1062]Duplicate entry '1' for key 't.idx'
	isImportDataErr := strings.Contains(errMsg, "ErrEncodeKV") &&
		(strings.Contains(errMsg, "Value conversion failed for column") ||
			(strings.Contains(errMsg, "Check constraint '") && strings.Contains(errMsg, "' is violated")) ||
			strings.Contains(errMsg, "Table has no partition for value"))
	isImportConflictErr := (strings.Contains(errMsg, "[executor:8167]") && strings.Contains(errMsg, "Duplicate key conflict found")) ||
		(strings.Contains(errMsg, "ErrFoundDataConflictRecords") && strings.Contains(errMsg, "found data conflict records")) ||
		(strings.Contains(errMsg, "ErrFoundIndexConflictRecords") && strings.Contains(errMsg, "found index conflict records"))
	isUKDupEntryErr := strings.Contains(errMsg, "[kv:1062]") && strings.Contains(errMsg, "Duplicate entry")
	return isImportDataErr || isImportConflictErr || isUKDupEntryErr
}

// ClassifyTaskError returns a safe, coarse classification for a terminal task error.
func ClassifyTaskError(state proto.TaskState, taskErr error) string {
	if taskErr == nil {
		return ""
	}
	switch state {
	case proto.TaskStateFailed:
		return proto.TaskStateFailed.String()
	case proto.TaskStateReverted:
		if IsCancelledErr(taskErr) {
			return taskCancelled
		}
		if IsDataError(taskErr) {
			return taskDataError
		}
		return proto.TaskStateFailed.String()
	default:
		return ""
	}
}
