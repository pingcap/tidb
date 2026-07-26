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

//! Constants from `pkg/sessionctx/vardef/tidb_vars.go`.
//!
//! - [`tidb_vars`]: the system-variable **name** constants -- the string
//!   identifiers used to reference session/global system variables throughout
//!   parse -> plan -> execute.
//!
//! - [`defaults`]: the `Def*` **default-value** constants for those variables.
//!
//! SCOPE (documented, not yet the whole `vardef` package): the name constants
//! (508) and the `Def*` defaults (389) are ported, both script-extracted and
//! byte-verified against the Go source. Still DEFERRED from the full package:
//! the mutable `var (...)` block of runtime-tunable globals (atomics), the
//! small `ExchangeCompressionMode` / iota helper enums and their `Name`/`To*`
//! methods, `sysvar.go`'s `SysVar` struct + the `GetSysVar`/`SetSysVar` global
//! registry (the mutable singleton the rewrite deliberately replaces with
//! explicit wiring), and `runtime.go`. Those land in follow-up units.

pub mod defaults;
pub mod tidb_vars;
