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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/util/logutil"
)

// ExampleStepOneSubtaskExecutor is an example subtask executor.
type ExampleStepOneSubtaskExecutor struct {
	minimalTask proto.MinimalTask
}

// ExampleStepTwoSubtaskExecutor is an example subtask executor.
type ExampleStepTwoSubtaskExecutor struct {
	minimalTask proto.MinimalTask
}

// Run implements the SubtaskExecutor interface.
func (e *ExampleStepOneSubtaskExecutor) Run(ctx context.Context) error {
	logutil.BgLogger().Info("sub task executor run step one")

	failpoint.Inject("mockStepOneError", func() {
		if e.minimalTask.(int64) == 8 {
			failpoint.Return(errors.New("mock step one run error"))
		}
	})
	globalNumberCounter.Add(e.minimalTask.(int64))
	return nil
}

// Run implements the SubtaskExecutor interface.
func (e *ExampleStepTwoSubtaskExecutor) Run(ctx context.Context) error {
	logutil.BgLogger().Info("sub task executor run step two")

	failpoint.Inject("mockStepTwoError", func() {
		if e.minimalTask.(int64) == 8 {
			failpoint.Return(errors.New("mock step two run error"))
		}
	})
	globalNumberCounter.Add(-2 * e.minimalTask.(int64))
	return nil
}

func init() {
	scheduler.RegisterSubtaskExectorConstructor(
		TaskTypeExample,
		// The order of the subtask executors is the same as the order of the subtasks.
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			switch step {
			case StepOne:
				return &ExampleStepOneSubtaskExecutor{minimalTask: minimalTask}, nil
			case StepTwo:
				return &ExampleStepTwoSubtaskExecutor{minimalTask: minimalTask}, nil
			}
			return nil, errors.Errorf("unknown step %d", step)
		},
	)
}
