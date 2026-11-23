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

package autoanalyze

import (
	"context"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
)

type schedulerImpl struct {
	*scheduler.BaseScheduler
}

var _ scheduler.Scheduler = (*schedulerImpl)(nil)

func newScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) *schedulerImpl {
	return &schedulerImpl{BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param)}
}

func (s *schedulerImpl) Init() error {
	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

func (*schedulerImpl) OnTick(context.Context, *proto.Task) {}

// OnNextSubtasksBatch generates the only subtask, which carries the task meta as is.
func (*schedulerImpl) OnNextSubtasksBatch(_ context.Context, _ storage.TaskHandle,
	task *proto.Task, _ []string, _ proto.Step) ([][]byte, error) {
	return [][]byte{task.Meta}, nil
}

func (*schedulerImpl) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

// GetEligibleInstances returns nil: every node of the target scope can run the
// statement.
func (*schedulerImpl) GetEligibleInstances(context.Context, *proto.Task) ([]string, error) {
	return nil, nil
}

// IsRetryableErr returns false: the priority queue of the stats owner analyzes
// the table again later if the task fails.
func (*schedulerImpl) IsRetryableErr(error) bool {
	return false
}

func (*schedulerImpl) GetNextStep(task *proto.TaskBase) proto.Step {
	if task.Step == proto.StepInit {
		return proto.AutoAnalyzeStepRun
	}
	return proto.StepDone
}

func (*schedulerImpl) OnPrepare(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

// ModifyMeta implements scheduler.Extension. The task meta is never modified.
func (*schedulerImpl) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}
