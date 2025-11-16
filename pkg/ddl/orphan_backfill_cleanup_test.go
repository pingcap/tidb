// Copyright 2025 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ddl "github.com/pingcap/tidb/pkg/ddl"
	distproto "github.com/pingcap/tidb/pkg/disttask/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestOrphanBackfillReconcileCancelsOrphan(t *testing.T) {
	// Speed up cleanup loop and disable age threshold via failpoints BEFORE DDL starts.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/orphanBackfillCheckInterval", "return(1)"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/orphanBackfillCheckInterval")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/orphanBackfillAgeSeconds", "return(0)"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/orphanBackfillAgeSeconds")
	// In unit tests, bypass owner check to avoid races with owner election.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/orphanBackfillIgnoreOwner", "return(true)"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/orphanBackfillIgnoreOwner")

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// Bootstrap a domain with DDL initialized.
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	// Speed up test by shrinking leases/timeouts.
	vardef.SetSchemaLease(100 * time.Millisecond)

	// Ensure dist task loop is initialized (BootstrapSession calls InitDistTaskLoop internally).
	// Create a backfill task whose parent job id doesn't exist.
	taskMgr, err := diststorage.GetTaskManager()
	require.NoError(t, err)

	ctx := context.Background()
	const orphanJobID int64 = 999999999 // unlikely to exist in tests
	taskKey := "ddl/backfill/999999999"

	taskID, err := taskMgr.CreateTask(ctx, taskKey, distproto.Backfill, "", 1, "", 0, distproto.ExtraParams{}, distproto.EmptyMeta)
	require.NoError(t, err)
	require.Greater(t, taskID, int64(0))

	// Make it look old enough to be considered by reconciler.
	_, err = taskMgr.ExecuteSQLWithNewSession(ctx,
		"update mysql.tidb_global_task set create_time = DATE_SUB(NOW(), INTERVAL 48 HOUR) where id = %?", taskID)
	require.NoError(t, err)

	// Sanity: check the task exists and is pending.
	task, err := taskMgr.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, distproto.TaskStatePending, task.State)
	require.Equal(t, taskKey, task.Key)

	// Also trigger one immediate reconcile pass (test-only helper) to avoid race with ticker.
	require.NoError(t, ddl.ForTest_OrphanBackfillReconcileOnceFromInterface(dom.DDL()))

	// Poll for a short period until the background cleanup loop cancels the task.
	deadline := time.Now().Add(10 * time.Second)
	for {
		task, err = taskMgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		if task.State == distproto.TaskStateCancelling || task.State == distproto.TaskStateReverting || task.State == distproto.TaskStateReverted {
			break
		}
		if time.Now().After(deadline) {
			require.Fail(t, fmt.Sprintf("timeout waiting for orphan cleanup, current state=%s", task.State))
		}
		time.Sleep(200 * time.Millisecond)
	}

	_ = orphanJobID // documentation/clarity
}


