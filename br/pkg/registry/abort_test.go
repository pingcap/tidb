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

package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestAbortKeepsTaskWithLiveHeartbeat(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/br/pkg/registry/is-task-stale-ticker-duration", "return(1)")

	cluster, err := mock.NewCluster()
	require.NoError(t, err)
	defer cluster.Stop()

	ctx := context.Background()
	r, err := registry.NewRestoreRegistry(ctx, gluetidb.New(), cluster.Domain)
	require.NoError(t, err)
	defer r.Close()

	info := registry.RegistrationInfo{
		FilterStrings:     []string{"db.*"},
		StartTS:           1,
		RestoredTS:        2,
		UpstreamClusterID: 3,
		Cmd:               "point restore",
	}
	taskID, _, err := r.ResumeOrCreateRegistration(ctx, info, true)
	require.NoError(t, err)
	require.NotZero(t, taskID)

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = r.UpdateHeartbeat(heartbeatCtx, taskID)
			}
		}
	}()

	deletedTaskID, err := r.FindAndDeleteMatchingTask(ctx, info, true)
	stopHeartbeat()
	<-heartbeatDone
	require.NoError(t, err)
	require.Zero(t, deletedTaskID)

	// once the heartbeat stops, the task is stale and can be aborted.
	deletedTaskID, err = r.FindAndDeleteMatchingTask(ctx, info, true)
	require.NoError(t, err)
	require.Equal(t, taskID, deletedTaskID)
}
