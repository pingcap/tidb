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

//! Go `pkg/expression/exprstatic` lands as a complete package: the *static*
//! expression and evaluation contexts — the snapshot a session hands to code
//! that must keep evaluating after the session moved on.
//!
//! Every production symbol of both Go files is here, one Rust file per Go
//! file:
//!
//! - `evalctx.go` → [`evalctx`]: [`EvalContext`], [`EvalCtxOption`] with all
//!   fourteen `WithXxx` constructors, `timeOnce`, `defaultSQLMode`,
//!   `newSessionVarsWithSystemVariables`, `LoadSystemVars` and
//!   [`make_eval_context_static`].
//! - `exprctx.go` → [`exprctx`]: [`ExprContext`], [`ExprCtxOption`] with all
//!   twelve `WithXxx` constructors, `LoadSystemVars` and
//!   [`make_expr_context_static`].
//!
//! All thirteen upstream test functions are ported, eight in [`evalctx`] and
//! five in [`exprctx`].
//!
//! # Boundaries
//!
//! The Go package leans on `pkg/sessionctx/variable`, which this workspace
//! ports into `tidb-session` — a crate that sits *above* this one. Everything
//! reached through it is narrowed here, named at its definition site:
//!
//! - `// boundary:` Go `variable.SessionVars` /
//!   `newSessionVarsWithSystemVariables` — modeled as
//!   [`evalctx::SessionVarsSnapshot`] plus [`evalctx::StaticSessionVars`],
//!   which parses exactly the system variables these two files switch on.
//!   Validation of every *other* system variable belongs to the session's
//!   sysvar catalog and is not attempted; unknown names are accepted and
//!   ignored rather than rejected.
//! - `// boundary:` Go `variable.PlanCacheParamList` — [`evalctx::with_param_list`]
//!   takes the datum slice Go copies out of it (`AllParamValues`). The list
//!   itself is ported in `tidb-exec`, also above this crate.
//! - `// boundary:` Go `variable.TiDBOptOnOffWarn` / `OnInt` / `OffInt` /
//!   `WarnInt` — reimplemented as [`evalctx::tidb_opt_on_off_warn`] and its
//!   three constants, three lines that would otherwise pull in `tidb-session`.
//! - `// boundary:` Go `vardef.DefMaxAllowedPacket` and the MySQL-compatible
//!   system-variable *names* of `vardef/sysvar.go` (`time_zone`, `sql_mode`,
//!   `timestamp`, `max_allowed_packet`, `default_week_format`,
//!   `character_set_connection`, `collation_connection`,
//!   `default_collation_for_utf8mb4`, `block_encryption_mode`,
//!   `windowing_use_high_precision`, `group_concat_max_len`). `tidb-vardef`
//!   ports `vardef/tidb_vars.go` only, so these are declared in
//!   [`evalctx`]/[`exprctx`]; the four names that *are* in `tidb-vardef`
//!   (`tidb_redact_log`, `div_precision_increment`, `tidb_sysdate_is_now`,
//!   `tidb_enable_noop_functions`) are reused from it, as are every ported
//!   `DefXxx` default.
//! - `// boundary:` Go `exprctx.StaticConvertibleEvalContext` /
//!   `StaticConvertibleExprContext`. Both embed the `exprctx.EvalContext` /
//!   `ExprContext` umbrella interfaces, which [`crate::exprctx`] does not yet
//!   carry; they are declared here narrowed to the methods
//!   `MakeEvalContextStatic` / `MakeExprContextStatic` actually call.
//!
//! # Adaptations
//!
//! - Go's `EvalContext` passes *itself* as the warning sink of its
//!   `types.Context` and `errctx.Context`, so replacing the handler through
//!   `Apply` is picked up by both. Here the two contexts are derived on
//!   access from the current handler through one `WarnBridge`, which reaches
//!   the same late binding without a self-referential struct — and makes
//!   Go's "typeCtx and errCtx should be reset because warn handler changed"
//!   step in `Apply` unnecessary.
//! - `CurrentTime` returns the instant only. Go returns a `time.Time` already
//!   converted `.In(ctx.Location())`; the conversion changes no instant, and
//!   the location is [`EvalContext::location`], so the pair carries the same
//!   information.

pub mod evalctx;
pub mod exprctx;

pub use evalctx::{
    make_eval_context_static, with_current_db, with_current_time, with_default_week_format_mode,
    with_div_precision_increment, with_enable_redact_log, with_err_level_map, with_location,
    with_max_allowed_packet, with_optional_property, with_param_list, with_sql_mode,
    with_type_flags, with_user_vars_reader, with_warn_handler, EvalContext, EvalCtxError,
    EvalCtxOption, SessionVarsSnapshot, StaticConvertibleEvalContext, StaticSessionVars,
};
pub use exprctx::{
    make_expr_context_static, with_block_encryption_mode, with_charset, with_column_id_allocator,
    with_connection_id, with_default_collation_for_utf8mb4, with_eval_ctx,
    with_group_concat_max_len, with_noop_funcs_mode, with_plan_cache_tracker, with_rng,
    with_sysdate_is_now, with_windowing_use_high_precision, ExprContext, ExprCtxOption,
    StaticConvertibleExprContext,
};
