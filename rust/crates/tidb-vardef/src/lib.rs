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

//! Constants and mode enums from `pkg/sessionctx/vardef/tidb_vars.go`.
//!
//! [`tidb_vars`] holds the system-variable **name** constants -- the string
//! identifiers used to reference session/global system variables throughout
//! parse -> plan -> execute. [`defaults`] holds the `Def*` **default-value**
//! constants for those variables. [`modes`] holds the small
//! `ClusteredIndexDefMode` / `ExchangeCompressionMode` enums and their helpers.
//! [`is_mdl_enabled`] and [`set_enable_mdl`] retain the source's exceptional
//! runtime MDL switch: NextGen always reports enabled even if the mutable
//! classic-kernel backing value is false.
//!
//! SCOPE (documented, not yet the whole `vardef` package): the name constants
//! (508), the `Def*` defaults (389), and the mode enums are ported; constants
//! are script-extracted and byte-verified against the Go source. `ScopeFlag`
//! and the sysvar `TypeFlag` already live in `tidb-exec`
//! (`sysvar_scope`/`sysvar_type`). Still DEFERRED from the full package: the
//! remainder of the mutable `var (...)` block of runtime-tunable global sysvar
//! backing stores (many need config/system-memory-derived initializers,
//! `rate.Limiter`, or typed pointers, and are runtime state better wired when
//! the session layer consumes them, not on the simple-query path),
//! `sysvar.go`'s `SysVar` struct together with the `GetSysVar`/`SetSysVar`
//! global registry (the singleton the rewrite deliberately replaces with
//! explicit wiring), and `runtime.go`.

use std::sync::atomic::{AtomicBool, Ordering};

static ENABLE_MDL: AtomicBool = AtomicBool::new(false);

/// Go `IsMDLEnabled` with the process-global kernel selection made explicit.
///
/// NextGen cannot disable metadata locking; classic mode reads the mutable
/// value changed by [`set_enable_mdl`].
#[must_use]
pub fn is_mdl_enabled(next_gen: bool) -> bool {
    next_gen || ENABLE_MDL.load(Ordering::SeqCst)
}

/// Go `SetEnableMDL`: changes the classic-kernel MDL backing value.
pub fn set_enable_mdl(enabled: bool) {
    ENABLE_MDL.store(enabled, Ordering::SeqCst);
}

pub mod defaults;
/// One function from `sessionctx/variable/sysvar.go` rather than from
/// `vardef`: `GlobalSystemVariableInitialValue`, which decides the value a
/// `Def*` constant above actually takes on a real install. It lives here
/// because both tiers that need it -- the bootstrap writer above this crate
/// and `SET <var> = DEFAULT` in `tidb-session` -- can only share it from a
/// leaf, and because it is pure policy over those same constants.
pub mod global_sysvar_initial;
pub mod modes;
pub mod tidb_vars;

#[cfg(test)]
mod tests {
    use super::*;

    struct RestoreMdl(bool);

    impl Drop for RestoreMdl {
        fn drop(&mut self) {
            set_enable_mdl(self.0);
        }
    }

    /// Source: `pkg/sessionctx/vardef/tidb_vars_test.go::TestIsMDLEnabledInNextGen`.
    #[test]
    fn test_is_mdl_enabled_in_next_gen() {
        let original = ENABLE_MDL.load(Ordering::SeqCst);
        let _restore = RestoreMdl(original);

        set_enable_mdl(false);
        assert!(is_mdl_enabled(true));
        set_enable_mdl(true);
        assert!(is_mdl_enabled(true));
    }
}
