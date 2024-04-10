// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestTaskRegister(t *testing.T) {
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	ctx := context.Background()
	client := testEtcdCluster.RandClient()

	register := NewTaskRegisterWithTTL(client, 10*time.Second, RegisterRestore, "test")
	err := register.RegisterTask(ctx)
	require.NoError(t, err)

	list, err := GetImportTasksFrom(ctx, client)
	require.NoError(t, err)

	for _, task := range list.Tasks {
		t.Log(task.MessageToUser())
		require.Equal(t, "/tidb/brie/import/restore/test", task.Key)
	}

	require.NoError(t, register.Close(ctx))
}

func TestTaskRegisterOnce(t *testing.T) {
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	// should not close the client manually, the test will fail, since Terminate will close it too.
	client := testEtcdCluster.RandClient()

	ctx := context.Background()
	register := NewTaskRegisterWithTTL(client, 10*time.Second, RegisterImportInto, "test")
	defer register.Close(ctx)
	err := register.RegisterTaskOnce(ctx)
	require.NoError(t, err)

	// sleep 3 seconds to make sure the lease TTL is smaller.
	time.Sleep(3 * time.Second)
	list, err := GetImportTasksFrom(ctx, client)
	require.NoError(t, err)
	require.Len(t, list.Tasks, 1)
	currTask := list.Tasks[0]
	t.Log(currTask.MessageToUser())
	require.Equal(t, "/tidb/brie/import/import-into/test", currTask.Key)

	// then register again, this time will only refresh the lease, and left TTL will be larger.
	err = register.RegisterTaskOnce(ctx)
	require.NoError(t, err)
	list, err = GetImportTasksFrom(ctx, client)
	require.NoError(t, err)
	require.Len(t, list.Tasks, 1)
	thisTask := list.Tasks[0]
	require.Equal(t, currTask.Key, thisTask.Key)
	require.Equal(t, currTask.LeaseID, thisTask.LeaseID)
	require.Greater(t, thisTask.TTL, currTask.TTL)
}

func TestTaskRegisterFailedGrant(t *testing.T) {
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, GRPCKeepAliveInterval: time.Second, GRPCKeepAliveTimeout: 10 * time.Second})
	defer testEtcdCluster.Terminate(t)

	ctx := context.Background()
	client := testEtcdCluster.RandClient()

	register := NewTaskRegisterWithTTL(client, 3*time.Second, RegisterRestore, "test")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-failed-to-grant", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-always-grant", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-keepalive-stop", "return(true)"))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-failed-to-grant")
		failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-always-grant")
		failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-keepalive-stop")
	}()
	err := register.RegisterTask(ctx)
	require.NoError(t, err)

	time.Sleep(RegisterRetryInternal)
	list, err := GetImportTasksFrom(ctx, client)
	require.NoError(t, err)
	require.Equal(t, 0, len(list.Tasks), list)

	failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-keepalive-stop")
	failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-failed-to-grant")
	time.Sleep(RegisterRetryInternal)
	list, err = GetImportTasksFrom(ctx, client)
	require.NoError(t, err)
	for _, task := range list.Tasks {
		t.Log(task.MessageToUser())
		require.Equal(t, "/tidb/brie/import/restore/test", task.Key)
	}
	require.True(t, len(list.Tasks) > 0)
	err = register.Close(ctx)
	// for flaky test, the lease would expire
	if err != nil && !strings.Contains(err.Error(), "requested lease not found") {
		require.NoError(t, err)
	}
}

func TestTaskRegisterFailedReput(t *testing.T) {
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, GRPCKeepAliveInterval: time.Second, GRPCKeepAliveTimeout: 10 * time.Second})
	defer testEtcdCluster.Terminate(t)

	ctx := context.Background()
	client := testEtcdCluster.RandClient()

	register := NewTaskRegisterWithTTL(client, 3*time.Second, RegisterRestore, "test")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-failed-to-reput", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-always-grant", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-keepalive-stop", "return(true)"))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-failed-to-reput")
		failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-always-grant")
		failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-keepalive-stop")
	}()
	err := register.RegisterTask(ctx)
	require.NoError(t, err)

	time.Sleep(RegisterRetryInternal)
	list, err := GetImportTasksFrom(ctx, client)
	require.NoError(t, err)
	require.Equal(t, 0, len(list.Tasks), list)

	failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-keepalive-stop")
	failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/brie-task-register-failed-to-reput")
	time.Sleep(RegisterRetryInternal)
	list, err = GetImportTasksFrom(ctx, client)
	require.NoError(t, err)
	for _, task := range list.Tasks {
		t.Log(task.MessageToUser())
		require.Equal(t, "/tidb/brie/import/restore/test", task.Key)
	}
	require.True(t, len(list.Tasks) > 0)
	err = register.Close(ctx)
	// for flaky test, the lease would expire
	if err != nil && !strings.Contains(err.Error(), "requested lease not found") {
		require.NoError(t, err)
	}
}
