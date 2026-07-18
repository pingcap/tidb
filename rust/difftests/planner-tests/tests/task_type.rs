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

//! Dependency-closed tests for `pkg/planner/property/task_type.go:17`.
//!
//! The Go property test anchor is
//! `pkg/planner/property/physical_property_test.go:26`, while these vectors
//! isolate the task-kind value/label contract from physical-property FD logic.

use tidb_planner::task_type::TaskType;

#[test]
fn known_task_types_preserve_raw_values_and_labels() {
    let cases = [
        (TaskType::Root, 0, "rootTask"),
        (TaskType::CopSingleRead, 1, "copSingleReadTask"),
        (TaskType::CopMultiRead, 2, "copMultiReadTask"),
        (TaskType::Mpp, 3, "mppTask"),
    ];
    for (task, raw, label) in cases {
        assert_eq!(TaskType::from_raw(raw), task);
        assert_eq!(task.raw(), raw);
        assert_eq!(task.as_str(), label);
        assert_eq!(task.to_string(), label);
    }
}

#[test]
fn unknown_task_types_keep_source_integer_and_label() {
    let task = TaskType::from_raw(99);
    assert_eq!(task, TaskType::Unknown(99));
    assert_eq!(task.raw(), 99);
    assert_eq!(task.as_str(), "UnknownTaskType");
    assert_eq!(task.to_string(), "UnknownTaskType");
}
