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

package scheduler

import (
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// VerifyTaskStateTransform verifies whether the task state transform is valid.
func VerifyTaskStateTransform(from, to proto.TaskState) bool {
	rules := map[proto.TaskState][]proto.TaskState{
		proto.TaskStatePending: {
			proto.TaskStateRunning,
			proto.TaskStateCancelling,
			proto.TaskStatePausing,
			proto.TaskStateSucceed,
			proto.TaskStateFailed,
		},
		proto.TaskStateRunning: {
			proto.TaskStateSucceed,
			proto.TaskStateReverting,
			proto.TaskStateFailed,
			proto.TaskStateCancelling,
			proto.TaskStatePausing,
		},
		proto.TaskStateSucceed: {},
		proto.TaskStateReverting: {
			proto.TaskStateReverted,
			// no revert_failed now
			// proto.TaskStateRevertFailed,
		},
		proto.TaskStateFailed: {},
		proto.TaskStateCancelling: {
			proto.TaskStateReverting,
		},
		proto.TaskStatePausing: {
			proto.TaskStatePaused,
		},
		proto.TaskStatePaused: {
			proto.TaskStateResuming,
		},
		proto.TaskStateResuming: {
			proto.TaskStateRunning,
		},
		proto.TaskStateReverted: {},
	}

	if from == to {
		return true
	}

	for _, state := range rules[from] {
		if state == to {
			return true
		}
	}
	return false
}
