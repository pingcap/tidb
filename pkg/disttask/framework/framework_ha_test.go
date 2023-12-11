// Copyright 2023 PingCAP, Inc.
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

package framework_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
)

func TestHABasic(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 4)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestDispatcherExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "4*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown", "return(\":4000\")"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	distContext.Close()
}

func TestHAManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestDispatcherExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown", "return(\":4000\")"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	distContext.Close()
}

func TestHAFailInDifferentStage(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 6)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestDispatcherExt(ctrl), testContext, nil)
	// stage1 : server num from 6 to 3.
	// stage2 : server num from 3 to 2.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "6*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown2", "return()"))

	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown2"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	distContext.Close()
}

func TestHAFailInDifferentStageManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestDispatcherExt(ctrl), testContext, nil)
	// stage1 : server num from 30 to 27.
	// stage2 : server num from 27 to 26.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown2", "return()"))

	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown2"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	distContext.Close()
}

func TestHAReplacedButRunning(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 4)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestDispatcherExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBPartitionThenResume", "10*return(true)"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBPartitionThenResume"))
	distContext.Close()
}

func TestHAReplacedButRunningManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestDispatcherExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBPartitionThenResume", "30*return(true)"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBPartitionThenResume"))
	distContext.Close()
}
