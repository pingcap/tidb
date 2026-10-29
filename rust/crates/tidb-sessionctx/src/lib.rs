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

//! Go `pkg/sessionctx`: the context a session hands every statement's
//! planner and executor -- Go's `StatementContext` together with the session
//! state (`SessionVars`) and the `sessionctx.Context` reads they make.

pub mod statement_pushdown;
pub mod advisory_lock_state;
pub mod explain_format;
#[cfg(test)]
mod explain_tests;
pub mod show_retrieval;
pub mod select_lock;
mod stmt_context;
pub use stmt_context::*;
