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

package execute

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// StepExecutor defines the executor for subtasks of a task step.
// the calling sequence is:
//
//	Init
//	for every subtask of this step:
//		if RunSubtask failed then break
//		else OnFinished
//	Cleanup
type StepExecutor interface {
	// Init is used to initialize the environment.
	// if failed, task executor will retry later.
	Init(context.Context) error
	// RunSubtask is used to run the subtask.
	RunSubtask(ctx context.Context, subtask *proto.Subtask) error

	// RealtimeSummary returns the realtime summary of the running subtask by this executor.
	RealtimeSummary() *SubtaskSummary

	// OnFinished is used to handle the subtask when it is finished.
	// The subtask meta can be updated in place.
	OnFinished(ctx context.Context, subtask *proto.Subtask) error
	// Cleanup is used to clean up the environment.
	Cleanup(context.Context) error
}

// SubtaskSummary contains the summary of a subtask.
type SubtaskSummary struct {
	RowCount int64
}
