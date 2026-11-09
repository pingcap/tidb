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

//! Go `proto.go`.

use serde::{Deserialize, Serialize};

/// Go `taskMeta`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMeta {
    /// Go `SubtaskCount`.
    #[serde(rename = "subtask_count")]
    pub subtask_count: isize,
}

/// Go `subtaskMeta`. Different steps could have different subtask metas.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubtaskMeta {
    /// Go `Message`.
    #[serde(rename = "message")]
    pub(crate) message: String,
}
