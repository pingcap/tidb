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

//! System-variable **name** constants from `pkg/sessionctx/vardef/tidb_vars.go`.
//!
//! These are the string identifiers used to reference session/global system
//! variables throughout parse -> plan -> execute. Every consumer that names a
//! sysvar (session context, planner, executor) depends on these literals.
//!
//! SCOPE (documented seed, not the whole `vardef` package): this module ports
//! only the string-literal name constants (508 of them, script-extracted and
//! byte-verified verbatim against the Go source). Still DEFERRED from the full
//! package: the numeric/bool/float `Def*` default-value constants (mixed
//! types), the mutable `var (...)` block of runtime-tunable globals (atomics),
//! `sysvar.go`'s `SysVar` struct + the `GetSysVar`/`SetSysVar` global registry
//! (the mutable singleton the rewrite deliberately replaces with explicit
//! wiring), and `runtime.go`. Those land in follow-up units.

pub mod tidb_vars;
