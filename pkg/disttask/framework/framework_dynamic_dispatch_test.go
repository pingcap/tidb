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
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type testDynamicDispatcherExt struct {
	cnt int
}

var _ dispatcher.Extension = (*testDynamicDispatcherExt)(nil)

func (*testDynamicDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {}

func (dsp *testDynamicDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, _ proto.Step) (metas [][]byte, err error) {
	// step1
	if gTask.Step == proto.StepInit {
		dsp.cnt++
		return [][]byte{
			[]byte(fmt.Sprintf("task%d", dsp.cnt)),
			[]byte(fmt.Sprintf("task%d", dsp.cnt)),
		}, nil
	}

	// step2
	if gTask.Step == proto.StepOne {
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

func (dsp *testDynamicDispatcherExt) GetNextStep(_ dispatcher.TaskHandle, task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		return proto.StepTwo
	default:
		return proto.StepDone
	}
}

func (*testDynamicDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*testDynamicDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func TestFrameworkDynamicBasic(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &testDynamicDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 3)
	DispatchTaskAndCheckSuccess("key1", t, &m)
	distContext.Close()
}

func TestFrameworkDynamicHA(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &testDynamicDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 3)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr", "5*return()"))
	DispatchTaskAndCheckSuccess("key1", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr1", "5*return()"))
	DispatchTaskAndCheckSuccess("key2", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr2", "5*return()"))
	DispatchTaskAndCheckSuccess("key3", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr2"))
	distContext.Close()
}
