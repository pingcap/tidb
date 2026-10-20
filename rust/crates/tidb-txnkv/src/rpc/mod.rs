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

//! TiDB's side of a storage call: its context (cancellation, deadline,
//! KV-exec counter) and the runtimes TiDB's own workers run on. The calls
//! themselves go through client-rust.

mod call_context;
mod execution;

pub use call_context::{UnaryCallContext, UnaryCancellation};
pub use execution::{
    execution_runtime, go_max_procs, query_worker_runtime, wait_with_call, CompletionError,
};
