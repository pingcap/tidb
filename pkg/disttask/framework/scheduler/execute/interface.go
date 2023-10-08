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

// SubtaskExecutor defines the executor of a subtask.
type SubtaskExecutor interface {
	// Init is used to initialize the environment for the subtask executor.
	Init(context.Context) error
	// RunSubtask is used to run the subtask.
	RunSubtask(ctx context.Context, subtask *proto.Subtask) error
	// Cleanup is used to clean up the environment for the subtask executor.
	Cleanup(context.Context) error
	// OnFinished is used to handle the subtask when it is finished.
	// The subtask meta can be updated in place.
	OnFinished(ctx context.Context, subtask *proto.Subtask) error
	// Rollback is used to roll back all subtasks.
	// TODO: right now all impl of Rollback is empty, maybe we can remove it.
	Rollback(context.Context) error
}
