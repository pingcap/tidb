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

package txn

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"go.uber.org/zap"
)

type schedulerImpl struct {
	*scheduler.BaseScheduler
	logger *zap.Logger
	SQL    string
}

var _ scheduler.Scheduler = (*schedulerImpl)(nil)

func NewScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) *schedulerImpl {
	return &schedulerImpl{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
	}
}

func (s *schedulerImpl) Init() (err error) {
	taskMeta := &taskMeta{}
	if err = json.Unmarshal(s.BaseScheduler.GetTask().Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}
	s.SQL = taskMeta.SQL

	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

func (s *schedulerImpl) OnTick(context.Context, *proto.Task) {
	s.logger.Info("OnTick")
}

func (s *schedulerImpl) OnNextSubtasksBatch(_ context.Context, _ storage.TaskHandle,
	task *proto.Task, _ []string, nextStep proto.Step) (subtaskMetas [][]byte, err error) {
	switch nextStep {
	case proto.StepOne:
		bytes, err := json.Marshal(&subtaskMeta{
			SQL: s.SQL,
		})
		if err != nil {
			return nil, err
		}
		return [][]byte{bytes}, nil
	default:
		panic(fmt.Sprintf("unexpected nextStep: %s", proto.Step2Str(proto.TaskTypeExample, nextStep)))
	}
}

func (s *schedulerImpl) GetEligibleInstances(_ context.Context, task *proto.Task) ([]string, error) {
	taskMeta := &taskMeta{}
	if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
		return nil, errors.Annotate(err, "unmarshal task meta failed")
	}
	return []string{taskMeta.ServerID}, nil
}

func (s *schedulerImpl) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	s.logger.Info("OnDone")
	return nil
}

func (*schedulerImpl) IsRetryableErr(error) bool {
	return false
}

func (*schedulerImpl) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	default:
		// current step must be proto.StepOne
		return proto.StepDone
	}
}
