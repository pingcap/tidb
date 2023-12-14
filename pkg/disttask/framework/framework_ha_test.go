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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/hook"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/stretchr/testify/require"
)

func registerHAHook() {
	hk := &hook.TestCallback{}
	hk.OnSubtaskFinishedBeforeExported = func(subtask *proto.Subtask) error {
		if subtask.ExecID == ":4000" || subtask.ExecID == ":4001" || subtask.ExecID == ":4002" {
			v, ok := taskexecutor.TestContexts.Load(subtask.ExecID)
			if ok {
				v.(*taskexecutor.TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*taskexecutor.TestContext).MockDown.Store(true)
				time.Sleep(2 * time.Second)
				return nil
			}
		}
		return nil
	}
	hk.OnSubtaskRunBeforeExported = func(subtask *proto.Subtask) bool {
		v, ok := taskexecutor.TestContexts.Load(subtask.ExecID)
		if ok {
			if v.(*taskexecutor.TestContext).MockDown.Load() {
				return true
			}
		}
		return false
	}
	taskexecutor.RegisterHook(proto.TaskTypeExample, func() hook.Callback {
		return hk
	})
}

func registerMultiStageHAHook() {
	hk := &hook.TestCallback{}
	hk.OnSubtaskFinishedBeforeExported = func(subtask *proto.Subtask) error {
		if subtask.ExecID == ":4000" || subtask.ExecID == ":4001" || subtask.ExecID == ":4002" {
			v, ok := taskexecutor.TestContexts.Load(subtask.ExecID)
			if ok {
				v.(*taskexecutor.TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*taskexecutor.TestContext).MockDown.Store(true)
				time.Sleep(2 * time.Second)
				return nil
			}
		}
		if subtask.ExecID == ":4003" && subtask.Step == proto.StepTwo {
			v, ok := taskexecutor.TestContexts.Load(subtask.ExecID)
			if ok {
				v.(*taskexecutor.TestContext).TestSyncSubtaskRun <- struct{}{}
				v.(*taskexecutor.TestContext).MockDown.Store(true)
				time.Sleep(2 * time.Second)
				return nil
			}
		}
		return nil
	}
	hk.OnSubtaskRunBeforeExported = func(subtask *proto.Subtask) bool {
		v, ok := taskexecutor.TestContexts.Load(subtask.ExecID)
		if ok {
			if v.(*taskexecutor.TestContext).MockDown.Load() {
				return true
			}
		}
		return false
	}
	taskexecutor.RegisterHook(proto.TaskTypeExample, func() hook.Callback {
		return hk
	})
}
func TestHABasic(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 4)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestSchedulerExt(ctrl), testContext, nil)
	registerHAHook()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "4*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	distContext.Close()
}

func TestHAManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestSchedulerExt(ctrl), testContext, nil)
	registerHAHook()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "30*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	distContext.Close()
}

func TestHAFailInDifferentStage(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 6)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestSchedulerExt(ctrl), testContext, nil)
	registerMultiStageHAHook()
	// stage1 : server num from 6 to 3.
	// stage2 : server num from 3 to 2.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "6*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	distContext.Close()
}

func TestHAFailInDifferentStageManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestSchedulerExt(ctrl), testContext, nil)
	registerMultiStageHAHook()
	// stage1 : server num from 30 to 27.
	// stage2 : server num from 27 to 26.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "30*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	distContext.Close()
}

func registerReplaceButRunningHook() {
	hk := &hook.TestCallback{}
	hk.OnSubtaskFinishedBeforeExported = func(subtask *proto.Subtask) error {
		if subtask.ExecID == ":4000" || subtask.ExecID == ":4001" || subtask.ExecID == ":4002" {
			_ = infosync.MockGlobalServerInfoManagerEntry.DeleteByID(subtask.ExecID)
			time.Sleep(20 * time.Second)
		}
		return nil
	}
	taskexecutor.RegisterHook(proto.TaskTypeExample, func() hook.Callback {
		return hk
	})
}
func TestHAReplacedButRunning(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 4)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestSchedulerExt(ctrl), testContext, nil)
	registerReplaceButRunningHook()
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	distContext.Close()
}

func TestHAReplacedButRunningManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockHATestSchedulerExt(ctrl), testContext, nil)
	registerReplaceButRunningHook()
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	distContext.Close()
}
