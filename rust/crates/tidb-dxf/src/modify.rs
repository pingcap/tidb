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

//! Go `modify.go`: the in-flight task modification request, stored in the task
//! table's `modify_params` JSON column.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::task::TaskState;

go_string_type! {
    /// Go `ModificationType`: the type of a task modification.
    ModificationType
}

/// Go `ModifyRequiredSlots`: modifies the task's required slots.
///
/// Note: required slots were introduced later and separated from the old
/// "concurrency" concept; the modification type stays `modify_concurrency` for
/// compatibility.
pub const MODIFY_REQUIRED_SLOTS: ModificationType =
    ModificationType::from_static("modify_concurrency");
/// Go `ModifyMaxNodeCount`: modifies the max node count of a task.
pub const MODIFY_MAX_NODE_COUNT: ModificationType =
    ModificationType::from_static("modify_max_node_count");
/// Go `ModifyBatchSize`: modifies the batch size of add-index.
pub const MODIFY_BATCH_SIZE: ModificationType = ModificationType::from_static("modify_batch_size");
/// Go `ModifyMaxWriteSpeed`: modifies the max write speed of add-index.
pub const MODIFY_MAX_WRITE_SPEED: ModificationType =
    ModificationType::from_static("modify_max_write_speed");

/// Go `Modification`: one modification for a task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Modification {
    /// What is being modified.
    #[serde(rename = "type")]
    pub tp: ModificationType,
    /// The value it is modified to.
    pub to: i64,
}

impl fmt::Display for Modification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{type: {}, to: {}}}", self.tp, self.to)
    }
}

/// Go `ModifyParam`: the parameter for a task modification.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModifyParam {
    /// The task state before the modification started.
    pub prev_state: TaskState,
    /// The modifications to apply.
    pub modifications: Vec<Modification>,
}

impl fmt::Display for ModifyParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{prev_state: {}, modifications: [", self.prev_state)?;
        for (i, m) in self.modifications.iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            write!(f, "{m}")?;
        }
        f.write_str("]}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TASK_STATE_RUNNING;

    /// Not a Go test: `ModifyParam` is persisted as the task table's
    /// `modify_params` JSON column and rendered into scheduler logs, so both
    /// forms are pinned here.
    #[test]
    fn test_modify_param() {
        let p = ModifyParam {
            prev_state: TASK_STATE_RUNNING,
            modifications: vec![
                Modification {
                    tp: MODIFY_REQUIRED_SLOTS,
                    to: 8,
                },
                Modification {
                    tp: MODIFY_MAX_NODE_COUNT,
                    to: 3,
                },
            ],
        };
        assert_eq!(
            serde_json::to_string(&p).unwrap(),
            r#"{"prev_state":"running","modifications":[{"type":"modify_concurrency","to":8},{"type":"modify_max_node_count","to":3}]}"#
        );
        // Go's `%v` over a slice writes the elements space-separated in
        // brackets.
        assert_eq!(
            p.to_string(),
            "{prev_state: running, modifications: [{type: modify_concurrency, to: 8} {type: modify_max_node_count, to: 3}]}"
        );
        let back: ModifyParam = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        assert_eq!(back, p);
    }
}
