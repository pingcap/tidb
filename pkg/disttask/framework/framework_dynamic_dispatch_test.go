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
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func getMockDynamicDispatchExt(ctrl *gomock.Controller) dispatcher.Extension {
	// init mockDispatcher
	mockDispatcher := mockDispatch.NewMockExtension(ctrl)
	mockDispatcher.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockDispatcher.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateSchedulerNodes4Test()
		},
	).AnyTimes()
	mockDispatcher.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockDispatcher.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			switch task.Step {
			case proto.StepInit:
				return proto.StepOne
			case proto.StepOne:
				return proto.StepTwo
			default:
				return proto.StepDone
			}
		},
	).AnyTimes()
	mockDispatcher.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if gTask.Step == proto.StepInit {
				return [][]byte{
					[]byte("task"),
					[]byte("task"),
				}, nil
			}

			// step2
			if gTask.Step == proto.StepOne {
				return [][]byte{
					[]byte("task"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnErrStage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
			return nil, nil
		},
	).AnyTimes()

	return mockDispatcher
}

func TestFrameworkDynamicBasic(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockDynamicDispatchExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 3)
	DispatchTaskAndCheckSuccess(ctx, "key1", t, &m)
	distContext.Close()
}

func TestFrameworkDynamicHA(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockDynamicDispatchExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 3)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr", "5*return()"))
	DispatchTaskAndCheckSuccess(ctx, "key1", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr1", "5*return()"))
	DispatchTaskAndCheckSuccess(ctx, "key2", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr2", "5*return()"))
	DispatchTaskAndCheckSuccess(ctx, "key3", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockDynamicDispatchErr2"))
	distContext.Close()
}
