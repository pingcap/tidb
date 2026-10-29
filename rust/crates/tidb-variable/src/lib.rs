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

//! Go `pkg/sessionctx/variable`: the system-variable catalog and its
//! validation, the variable errors, the settings derived from session
//! variables, and the slow-log rules. It sits below the executor, as Go's
//! package does.

mod error;
pub mod user_vars;
pub mod hypo_index;
pub mod variable_errors;
pub mod hint_updatable_vars;
pub mod phase_durations;
pub mod session_vars;
pub mod set_hooks;
pub mod slow_log;
pub mod slow_log_rules;
pub mod statusvar;
pub mod sysvar;
pub mod table_delta;
pub mod varsutil;
pub mod vars;

pub use error::VarError;
pub use global_accessor::{GlobalVarAccessor, MockGlobalAccessor};
pub use vars::{MdlRelatedTableRecorder, SessionVars, TlsConnectionState};

pub mod global_accessor;

/// The live GLOBAL-variable reader Go exposes through
/// `SessionVars.GlobalVarsAccessor`.
pub trait GlobalSysvarAccessor: Send + Sync {
    /// Reads the current GLOBAL value, or `None` when no session/global table
    /// is attached to this evaluation context.
    fn get_global_sysvar(&self, name: &str) -> Option<String>;
}
