// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/distribute_framework/scheduler"
	"github.com/pingcap/tidb/util/logutil"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distribute_framework/proto"
)

type ExampleStepOneSubtaskExecutor struct {
	minimalTask proto.MinimalTask
}

type ExampleStepTwoSubtaskExecutor struct {
	minimalTask proto.MinimalTask
}

func (e *ExampleStepOneSubtaskExecutor) Run(ctx context.Context) error {
	globalNumberCounter.Add(e.minimalTask.(int64))
	logutil.BgLogger().Info("sub task executor run step one")
	return nil
}

func (e *ExampleStepTwoSubtaskExecutor) Run(ctx context.Context) error {
	globalNumberCounter.Add(-2 * e.minimalTask.(int64))
	logutil.BgLogger().Info("sub task executor run step one")
	return nil
}

func init() {
	scheduler.RegisterSubtaskExectorConstructor(
		proto.TaskTypeExample,
		// The order of the subtask executors is the same as the order of the subtasks.
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			switch step {
			case proto.StepOne:
				return &ExampleStepOneSubtaskExecutor{minimalTask: minimalTask}, nil
			case proto.StepTwo:
				return &ExampleStepTwoSubtaskExecutor{minimalTask: minimalTask}, nil
			}
			return nil, errors.Errorf("unknown step %d", step)
		},
	)
}
