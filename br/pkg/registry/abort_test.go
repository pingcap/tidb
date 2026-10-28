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

func newRegistryWithRunningTask(t *testing.T) (*registry.Registry, registry.RegistrationInfo, uint64) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/br/pkg/registry/is-task-stale-ticker-duration", "return(1)")

	cluster, err := mock.NewCluster()
	require.NoError(t, err)
	t.Cleanup(cluster.Stop)

	ctx := context.Background()
	r, err := registry.NewRestoreRegistry(ctx, gluetidb.New(), cluster.Domain)
	require.NoError(t, err)
	t.Cleanup(r.Close)

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
	return r, info, taskID
}

func registeredTaskCount(t *testing.T, r *registry.Registry, taskID uint64) int {
	registrations, err := r.GetRegistrationsByMaxID(context.Background(), taskID+1)
	require.NoError(t, err)
	return len(registrations)
}

func TestAbortKeepsTaskWithLiveHeartbeat(t *testing.T) {
	r, info, taskID := newRegistryWithRunningTask(t)
	ctx := context.Background()

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

func TestAbortDeletesTaskWhoseHeartbeatStopsDuringTheAbort(t *testing.T) {
	r, info, taskID := newRegistryWithRunningTask(t)
	ctx := context.Background()

	lastHeartbeat := time.AfterFunc(1500*time.Millisecond, func() { _ = r.UpdateHeartbeat(ctx, taskID) })
	defer lastHeartbeat.Stop()

	deletedTaskID, err := r.FindAndDeleteMatchingTask(ctx, info, true)
	require.NoError(t, err)
	require.Equal(t, taskID, deletedTaskID)
	require.Zero(t, registeredTaskCount(t, r, taskID))
}

func TestAbortKeepsTaskWhenCancelledWhileWaitingForItsHeartbeat(t *testing.T) {
	r, info, taskID := newRegistryWithRunningTask(t)
	ctx := context.Background()

	lastHeartbeat := time.AfterFunc(1500*time.Millisecond, func() { _ = r.UpdateHeartbeat(ctx, taskID) })
	defer lastHeartbeat.Stop()
	abortCtx, cancel := context.WithTimeout(ctx, 4500*time.Millisecond)
	defer cancel()

	deletedTaskID, err := r.FindAndDeleteMatchingTask(abortCtx, info, true)
	require.NoError(t, err)
	require.Zero(t, deletedTaskID)
	require.Equal(t, 1, registeredTaskCount(t, r, taskID))
}
