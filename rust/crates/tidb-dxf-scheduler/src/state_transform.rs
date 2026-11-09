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

//! Go `state_transform.go`.

use tidb_dxf::{
    TaskState, TASK_STATE_CANCELLING, TASK_STATE_FAILED, TASK_STATE_PAUSED, TASK_STATE_PAUSING, TASK_STATE_PENDING,
    TASK_STATE_RESUMING, TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};

/// Go `VerifyTaskStateTransform`: whether the task state transform is valid.
#[must_use]
pub fn verify_task_state_transform(from: &TaskState, to: &TaskState) -> bool {
    let rules: &[(TaskState, &[TaskState])] = &[
        (
            TASK_STATE_PENDING,
            &[TASK_STATE_RUNNING, TASK_STATE_CANCELLING, TASK_STATE_PAUSING, TASK_STATE_SUCCEED, TASK_STATE_FAILED],
        ),
        (
            TASK_STATE_RUNNING,
            &[TASK_STATE_SUCCEED, TASK_STATE_REVERTING, TASK_STATE_FAILED, TASK_STATE_CANCELLING, TASK_STATE_PAUSING],
        ),
        (TASK_STATE_SUCCEED, &[]),
        (
            TASK_STATE_REVERTING,
            &[
                TASK_STATE_REVERTED,
                // no revert_failed now
                // TASK_STATE_REVERT_FAILED,
            ],
        ),
        (TASK_STATE_FAILED, &[]),
        (TASK_STATE_CANCELLING, &[TASK_STATE_REVERTING]),
        (TASK_STATE_PAUSING, &[TASK_STATE_PAUSED]),
        (TASK_STATE_PAUSED, &[TASK_STATE_RESUMING]),
        (TASK_STATE_RESUMING, &[TASK_STATE_RUNNING]),
        (TASK_STATE_REVERTED, &[]),
    ];

    if from == to {
        return true;
    }

    rules.iter().find(|(state, _)| state == from).is_some_and(|(_, targets)| targets.contains(to))
}

#[cfg(test)]
mod tests {
    use tidb_dxf::{
        TASK_STATE_CANCELLING, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING, TASK_STATE_REVERTED,
        TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
    };

    use super::verify_task_state_transform;

    /// Go `TestVerifyTaskStateTransform`.
    #[test]
    fn test_verify_task_state_transform() {
        let test_cases = [
            (TASK_STATE_RUNNING, TASK_STATE_RUNNING, true),
            (TASK_STATE_PENDING, TASK_STATE_RUNNING, true),
            (TASK_STATE_PENDING, TASK_STATE_REVERTING, false),
            (TASK_STATE_RUNNING, TASK_STATE_REVERTING, true),
            (TASK_STATE_REVERTING, TASK_STATE_REVERTED, true),
            (TASK_STATE_REVERTING, TASK_STATE_SUCCEED, false),
            (TASK_STATE_RUNNING, TASK_STATE_PAUSING, true),
            (TASK_STATE_RUNNING, TASK_STATE_RESUMING, false),
            (TASK_STATE_CANCELLING, TASK_STATE_RUNNING, false),
        ];
        for (old_state, new_state, expect) in test_cases {
            assert_eq!(verify_task_state_transform(&old_state, &new_state), expect, "{old_state} -> {new_state}");
        }
    }
}
