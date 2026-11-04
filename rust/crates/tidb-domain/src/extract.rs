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

//! Go `pkg/domain/extract.go`: the extract task directory the dump-file GC
//! walks. The `EXTRACT` handle itself is not ported.

/// Go `ExtractTaskDirName`.
pub const EXTRACT_TASK_DIR_NAME: &str = "extract";

/// Go `GetExtractTaskDirName`.
#[must_use]
pub const fn get_extract_task_dir_name() -> &'static str {
    EXTRACT_TASK_DIR_NAME
}
