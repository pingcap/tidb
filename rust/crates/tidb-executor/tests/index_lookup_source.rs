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

//! Go `IndexLookUpExecutor` handle ownership contracts.

#[test]
fn lookup_worker_takes_the_go_task_handle_slice_without_cloning() {
    let source = include_str!("../src/access_path.rs");
    assert!(
        source.contains("let worker_handles = handles;"),
        "the table worker must take ownership of Go's lookupTableTask.handles"
    );
    assert!(
        !source.contains("let worker_handles = handles.clone();"),
        "cloning every common-handle byte vector diverges from Go's task handoff"
    );
    assert!(
        source.contains("LocalFallback(Vec<TableHandle>)"),
        "a refused remote request must return the owned handles for local fallback"
    );
}
