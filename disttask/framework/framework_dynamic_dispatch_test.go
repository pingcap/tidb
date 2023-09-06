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
	"context"
	"fmt"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type testDynamicDispatcherExt struct {
	cnt int
}

var _ dispatcher.Extension = (*testDynamicDispatcherExt)(nil)

func (*testDynamicDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {}

func (dsp *testDynamicDispatcherExt) OnNextStage(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	// move to step1
	if gTask.Step == proto.StepInit {
		gTask.Step = proto.StepOne
	}

	// step1
	if gTask.Step == proto.StepOne && dsp.cnt < 3 {
		dsp.cnt++
		logutil.BgLogger().Info("ywq test reach", zap.Any("cnt", dsp.cnt))
		return [][]byte{
			[]byte(fmt.Sprintf("task%d", dsp.cnt)),
		}, nil
	}

	// move to step2.
	if gTask.Step == proto.StepOne && dsp.cnt == 3 {
		gTask.Step = proto.StepTwo
	}

	// step2
	if gTask.Step == proto.StepTwo && dsp.cnt < 4 {
		dsp.cnt++
		return [][]byte{
			[]byte(fmt.Sprintf("task%d", dsp.cnt)),
		}, nil
	}
	return nil, nil
}

func (*testDynamicDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return nil, nil
}

func (dsp *testDynamicDispatcherExt) AllDispatched(task *proto.Task) bool {
	if task.Step == proto.StepOne && dsp.cnt == 3 {
		return true
	}
	if task.Step == proto.StepTwo && dsp.cnt == 4 {
		return true
	}
	return false
}

func (dsp *testDynamicDispatcherExt) Finished(task *proto.Task) bool {
	if task.Step == proto.StepTwo && dsp.cnt == 4 {
		dsp.cnt = 0
		return true
	}
	return false
}

func (*testDynamicDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*testDynamicDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func TestFrameworkDynamicBasic(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterTaskMeta(&m, &testDynamicDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess("key1", t, &m)
	distContext.Close()
}

func TestFrameworkDynamicHA(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterTaskMeta(&m, &testDynamicDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 2)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/mockDynamicDispatchErr", "5*return()"))
	DispatchTaskAndCheckSuccess("key1", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/mockDynamicDispatchErr"))
	distContext.Close()
}
