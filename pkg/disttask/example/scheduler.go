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

package example

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type schedulerImpl struct {
	*scheduler.BaseScheduler
	logger       *zap.Logger
	subtaskCount int
}

var _ scheduler.Scheduler = (*schedulerImpl)(nil)

func newScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) *schedulerImpl {
	return &schedulerImpl{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
		logger:        logutil.BgLogger().With(zap.Int64("taskID", task.ID)),
	}
}

func (s *schedulerImpl) Init() (err error) {
	taskMeta := &taskMeta{}
	if err = json.Unmarshal(s.BaseScheduler.GetTask().Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}

	s.subtaskCount = taskMeta.SubtaskCount

	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

func (s *schedulerImpl) OnTick(context.Context, *proto.Task) {
	s.logger.Info("OnTick")
}

func (s *schedulerImpl) OnNextSubtasksBatch(_ context.Context, _ storage.TaskHandle,
	task *proto.Task, _ []string, nextStep proto.Step) (subtaskMetas [][]byte, err error) {
	switch nextStep {
	case proto.StepOne, proto.StepTwo:
		metas := make([][]byte, s.subtaskCount)
		for i := 0; i < s.subtaskCount; i++ {
			bytes, err := json.Marshal(&subtaskMeta{
				Message: fmt.Sprintf("subtask %d of step %s", i, proto.Step2Str(task.Type, nextStep)),
			})
			if err != nil {
				return nil, err
			}
			metas[i] = bytes
		}
		return metas, nil
	default:
		panic(fmt.Sprintf("unexpected nextStep: %s", proto.Step2Str(proto.TaskTypeExample, nextStep)))
	}
}

func (s *schedulerImpl) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	s.logger.Info("OnDone")
	return nil
}

func (*schedulerImpl) GetEligibleInstances(context.Context, *proto.Task) ([]string, error) {
	return nil, nil
}

func (*schedulerImpl) IsRetryableErr(error) bool {
	return true
}

func (*schedulerImpl) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		return proto.StepTwo
	default:
		// current step must be proto.StepTwo
		return proto.StepDone
	}
}

type postCleanupImpl struct{}

func (*postCleanupImpl) CleanUp(_ context.Context, task *proto.Task) error {
	logutil.BgLogger().Info("clean up task", zap.Int64("taskID", task.ID))
	return nil
}
