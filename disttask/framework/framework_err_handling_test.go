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
	"errors"
	"sync"
	"testing"

	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	"go.uber.org/mock/gomock"
)

type planErrDispatcherExt struct {
	callTime int
}

var (
	_ dispatcher.Extension = (*planErrDispatcherExt)(nil)
	_ dispatcher.Extension = (*planNotRetryableErrDispatcherExt)(nil)
)

func (*planErrDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (p *planErrDispatcherExt) OnNextStage(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		if p.callTime == 0 {
			p.callTime++
			return nil, errors.New("retryable err")
		}
		gTask.Step = proto.StepOne
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}, nil
	}
	if gTask.Step == proto.StepOne {
		gTask.Step = proto.StepTwo
		return [][]byte{
			[]byte("task4"),
		}, nil
	}
	return nil, nil
}

func (p *planErrDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	if p.callTime == 1 {
		p.callTime++
		return nil, errors.New("not retryable err")
	}
	return []byte("planErrTask"), nil
}

func (*planErrDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*planErrDispatcherExt) IsRetryableErr(error) bool {
	return true
}

type planNotRetryableErrDispatcherExt struct {
}

func (*planNotRetryableErrDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (p *planNotRetryableErrDispatcherExt) OnNextStage(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	return nil, errors.New("not retryable err")
}

func (*planNotRetryableErrDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return nil, errors.New("not retryable err")
}

func (*planNotRetryableErrDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*planNotRetryableErrDispatcherExt) IsRetryableErr(error) bool {
	return false
}

func TestPlanErr(t *testing.T) {
	m := sync.Map{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &planErrDispatcherExt{0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess("key1", t, &m)
	distContext.Close()
}

func TestRevertPlanErr(t *testing.T) {
	m := sync.Map{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &planErrDispatcherExt{0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess("key1", t, &m)
	distContext.Close()
}

func TestPlanNotRetryableErr(t *testing.T) {
	m := sync.Map{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &planNotRetryableErrDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckState("key1", t, &m, proto.TaskStateFailed)
	distContext.Close()
}
