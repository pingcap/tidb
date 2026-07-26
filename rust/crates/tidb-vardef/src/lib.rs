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
//!
//! SCOPE (documented, not yet the whole `vardef` package): the name constants
//! (508), the `Def*` defaults (389), and the mode enums are ported; constants
//! are script-extracted and byte-verified against the Go source. `ScopeFlag`
//! and the sysvar `TypeFlag` already live in `tidb-exec`
//! (`sysvar_scope`/`sysvar_type`). Still DEFERRED from the full package: the
//! mutable `var (...)` block of runtime-tunable global sysvar backing stores
//! (many need config/system-memory-derived initializers, `rate.Limiter`, or
//! typed pointers, and are runtime state better wired when the session layer
//! consumes them, not on the simple-query path), `sysvar.go`'s `SysVar` struct
//! together with the `GetSysVar`/`SetSysVar` global registry (the singleton the
//! rewrite deliberately replaces with explicit wiring), and `runtime.go`.

pub mod defaults;
pub mod modes;
pub mod tidb_vars;
