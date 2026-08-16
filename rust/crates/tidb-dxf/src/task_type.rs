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

//! Go `type.go`: the known task types and their integer encoding.
//!
//! The module is named for what the Go file declares rather than for the file
//! itself, because `type` is a Rust keyword. The `TaskType` type declaration
//! lives in [`crate::task`], exactly as Go declares it in `task.go`.

use crate::task::TaskType;

/// Go `TaskTypeExample`: the task type of Example, for test.
pub const TASK_TYPE_EXAMPLE: TaskType = TaskType::from_static("Example");
/// Go `ImportInto`: the task type of `IMPORT INTO`.
pub const IMPORT_INTO: TaskType = TaskType::from_static("ImportInto");
/// Go `Backfill`: the task type of the add-index backfilling process.
pub const BACKFILL: TaskType = TaskType::from_static("backfill");

/// Go `Type2Int`: converts a task type to an int.
#[must_use]
pub fn type2int(t: &TaskType) -> i64 {
    if *t == TASK_TYPE_EXAMPLE {
        1
    } else if *t == IMPORT_INTO {
        2
    } else if *t == BACKFILL {
        3
    } else {
        0
    }
}

/// Go `Int2Type`: converts an int to a task type.
#[must_use]
pub fn int2type(i: i64) -> TaskType {
    match i {
        1 => TASK_TYPE_EXAMPLE,
        2 => IMPORT_INTO,
        3 => BACKFILL,
        _ => TaskType::from_static(""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestTaskType`.
    #[test]
    fn test_task_type() {
        let cases = [
            (TASK_TYPE_EXAMPLE, 1),
            (IMPORT_INTO, 2),
            (BACKFILL, 3),
            (TaskType::from_static(""), 0),
        ];
        for (tp, val) in &cases {
            assert_eq!(type2int(tp), *val);
        }

        for (tp, val) in &cases {
            assert_eq!(int2type(*val), *tp);
        }
    }
}
