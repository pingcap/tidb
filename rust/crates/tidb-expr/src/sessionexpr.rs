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

//! Go `pkg/expression/sessionexpr` lands as a complete package: the
//! *live-session* expression and evaluation contexts — the same two interfaces
//! [`crate::exprstatic`] implements over a frozen snapshot, implemented here
//! over a running session, so every read goes back to the session on each call.
//!
//! Every production symbol of the package's single file `sessionctx.go` is
//! here:
//!
//! - [`ExprContext`] and [`EvalContext`] with all of their methods, including
//!   the `StaticConvertibleExprContext` / `StaticConvertibleEvalContext` halves
//!   and both `IntoStatic` conversions.
//! - the package-private helpers `getStmtTimestamp` ([`get_stmt_timestamp`]),
//!   `currentUserProp`, `infoSchemaProp`, `sqlExecutorProp`,
//!   `sequenceOperatorProp` and the `sequenceOperator` type
//!   ([`SessionSequenceOperator`]).
//!
//! All five upstream test functions of `sessionctx_test.go` are ported in
//! [`mod tests`](self#tests): `TestSessionEvalContextBasic`,
//! `TestSessionEvalContextCurrentTime`, `TestSessionEvalContextPrivilegeCheck`,
//! `TestSessionEvalContextOptProps` and `TestSessionBuildContext`.
//!
//! # Boundaries
//!
//! The whole package is written against `pkg/sessionctx`, which this workspace
//! ports into `tidb-session` — a crate that sits *above* this one, so a real
//! dependency is impossible. Every reach into it is narrowed here and named at
//! its definition site:
//!
//! - `// boundary:` Go `sessionctx.Context` — [`SessionContext`], carrying only
//!   the ten accessors this file calls on a session (`GetSessionVars`,
//!   `GetStore`, `IsDDLOwner`, `GetInfoSchema`, `GetLatestInfoSchema`,
//!   `GetRestrictedSQLExecutor`, the session's own `AdvisoryLockContext`, the
//!   bound privilege manager, sequence lookup, and the plan context's
//!   read-only user-variable map).
//! - `// boundary:` Go `sessionctx/variable.SessionVars` *and* the
//!   `stmtctx.StatementContext` reached through it —
//!   [`SessionVarsAccessor`]. It **extends
//!   [`crate::exprstatic::SessionVarsSnapshot`]** rather than declaring a
//!   second session view: that trait already names the twelve system-variable
//!   backed fields both packages read (`Location`, `SQLMode`,
//!   `MaxAllowedPacket`, `EnableRedactLog`, `DivPrecisionIncrement`,
//!   `GetCharsetInfo`, `DefaultCollationForUTF8MB4`, `GetSystemVar`,
//!   `SysdateIsNow`, `NoopFuncsMode`, `WindowingUseHighPrecision`,
//!   `GroupConcatMaxLen`), and reusing it keeps one description of "the session
//!   variables an expression context reads" for the workspace. The extension
//!   adds only what a *live* session has beyond a snapshot: the statement
//!   context's identity, type flags, error levels, warning handler, plan-cache
//!   tracker and stale TSO, plus `CurrentDB`, `User`, `ActiveRoles`,
//!   `PlanCacheParams`, `UserVars`, `Rng`, `PlanColumnID`, `ConnectionID` and
//!   `GetSessionOrGlobalSystemVar`.
//! - `// boundary:` Go `pkg/privilege.Manager` and
//!   `privilege.GetPrivilegeManager` — [`PrivilegeManager`], narrowed to
//!   `RequestVerification` and `RequestDynamicVerification`, the two methods
//!   [`EvalContext::request_verification`] and
//!   [`EvalContext::request_dynamic_verification`] call. An unbound manager is
//!   `None`, which is Go's nil interface.
//! - `// boundary:` Go `pkg/util.SequenceTable` / `util.GetSequenceByName` —
//!   [`SequenceTable`] plus [`SessionContext::get_sequence_by_name`]. The Go
//!   lookup goes through `infoschema`, whose table layer is not reachable from
//!   this crate.
//! - `// boundary:` Go `github.com/tikv/client-go/v2/oracle.GetTimeFromTS` —
//!   [`get_time_from_ts`], three lines of bit-shifting that would otherwise
//!   pull in the storage client.
//!
//! Reused from the sibling packages instead of re-modeled:
//! [`crate::expropt`]'s nine providers and its `KvStorage` / `SqlExecutor` /
//! `SessionVars` / `AdvisoryLockContext` / `PrivilegeChecker` boundary traits,
//! [`crate::exprctx`]'s optional-property keys,
//! [`crate::metabuild::MetaOnlyInfoSchema`], [`crate::user_vars::UserVarsReader`],
//! and [`crate::exprstatic`]'s `EvalCtxError` as this file's error type —
//! `IntoStatic` requires the two to agree anyway.
//!
//! # Adaptations
//!
//! - Go's `PrivilegeCheckerProvider(func() PrivilegeChecker { return ctx })`
//!   hands the `EvalContext` itself back as the checker. The Rust equivalent
//!   needs the context inside an `Arc` before its own providers can name it, so
//!   [`EvalContext::new`] builds through `Arc::new_cyclic` and the provider
//!   holds a `Weak`. The upgrade cannot fail: the provider lives inside the
//!   context it points at.
//! - Go embeds `*EvalContext` in `ExprContext`, which promotes every evaluation
//!   method onto the expression context. Rust has no embedding, so
//!   [`ExprContext::eval_context`] (Go's `ctx.EvalContext` field) and
//!   [`ExprContext::get_eval_ctx`] return the shared `Arc<EvalContext>` and
//!   callers go through it.
//! - `CurrentTime` returns the instant only, as in [`crate::exprstatic`]: Go
//!   returns a `time.Time` whose location is the context's, and the conversion
//!   moves no instant.
//! - `getStmtTimestamp`'s two `ctx == nil` branches are dropped —
//!   `Arc<dyn SessionContext>` is non-nullable — and its `logutil.BgLogger()`
//!   line for a failing stale-TSO provider is dropped with it; Go proceeds to
//!   the `timestamp` variable in exactly the same way after logging.

use std::sync::{Arc, Weak};

use chrono::{DateTime, Utc};
use tidb_datatype::{
    str_to_float, ConversionContext, ConversionFlags, ConversionLocation,
    ConversionWarningAppender, Datum,
};
use tidb_error::errctx::{
    new_context_with_levels, Context as ErrCtxContext, LevelMap, SharedError,
    WarnAppender as ErrCtxWarnAppender,
};
use tidb_error::terror::TerrorError;
use tidb_mysql::consts::SqlMode;
use tidb_mysql::privilege::PrivilegeType;
use tidb_parser::auth::{RoleIdentity, UserIdentity};
use tidb_util::context::{PlanCacheTracker, SqlWarn, WarnAppender, WarnErr, WarnHandler};
use tidb_util::mathutil::MysqlRng;
use tidb_util::timeutil::{zone_name, TimeZone};
use tidb_vardef::defaults::DEF_BLOCK_ENCRYPTION_MODE;

use crate::exprctx::{
    OptionalEvalPropKey, OptionalEvalPropKeySet, ERR_PARAM_INDEX_EXCEED_PARAM_COUNTS,
};
use crate::expropt::{
    new_session_vars_provider, AdvisoryLockContext, AdvisoryLockPropProvider,
    CurrentUserPropProvider, DdlOwnerInfoProvider, DynOptionalEvalPropProvider, EvalPropContext,
    ExprOptError, InfoSchemaPropProvider, KvStorage, KvStorePropProvider,
    OptionalEvalPropProviders, PrivilegeChecker, PrivilegeCheckerProvider, SequenceOperator,
    SequenceOperatorProvider, SessionVars as ExproptSessionVars, SessionVarsProvider, SqlExecutor,
    SqlExecutorPropProvider,
};
use crate::exprstatic::evalctx::{
    make_eval_context_static, StaticConvertibleEvalContext, BLOCK_ENCRYPTION_MODE,
    DEFAULT_WEEK_FORMAT, TIMESTAMP,
};
use crate::exprstatic::exprctx::{make_expr_context_static, StaticConvertibleExprContext};
use crate::exprstatic::{EvalCtxError, SessionVarsSnapshot};
use crate::metabuild::MetaOnlyInfoSchema;
use crate::user_vars::UserVarsReader;

/// boundary: Go `sessionctx/variable.SessionVars` together with the
/// `stmtctx.StatementContext` this package reaches through it
/// (`vars.StmtCtx.*`).
///
/// It extends [`SessionVarsSnapshot`], the view [`crate::exprstatic`] already
/// declared for the same Go type, with the accessors that only a *live*
/// session has. See the module header for why the two are one trait family
/// rather than two parallel ones.
pub trait SessionVarsAccessor: SessionVarsSnapshot + ExproptSessionVars {
    /// Go `SessionVars.StmtCtx.CtxID()`.
    fn ctx_id(&self) -> u64;
    /// Go `SessionVars.StmtCtx.TypeCtx().Flags()`.
    fn type_flags(&self) -> ConversionFlags;
    /// Go `SessionVars.StmtCtx.ErrCtx().LevelMap()`.
    fn err_level_map(&self) -> LevelMap;
    /// Go `SessionVars.StmtCtx.WarnHandler`, the sink of every warning the
    /// statement context, its type context and its error context share.
    fn warn_handler(&self) -> Arc<dyn WarnHandler + Send + Sync>;
    /// Go `SessionVars.CurrentDB`.
    fn current_db(&self) -> String;
    /// Go `SessionVars.User`.
    fn user(&self) -> Option<Arc<UserIdentity>>;
    /// Go `SessionVars.ActiveRoles`.
    fn active_roles(&self) -> Vec<Arc<RoleIdentity>>;
    /// Go `SessionVars.PlanCacheParams.AllParamValues()`.
    fn all_param_values(&self) -> Vec<Datum>;
    /// Go `SessionVars.UserVars`.
    fn user_vars_reader(&self) -> Arc<dyn UserVarsReader + Send + Sync>;
    /// Go `SessionVars.Rng`.
    fn rng(&self) -> Arc<MysqlRng>;
    /// Go `SessionVars.StmtCtx.PlanCacheTracker`.
    fn plan_cache_tracker(&self) -> Arc<PlanCacheTracker>;
    /// Go `SessionVars.AllocPlanColumnID()`.
    fn alloc_plan_column_id(&self) -> i64;
    /// Go `SessionVars.PlanColumnID.Load()`.
    fn last_plan_column_id(&self) -> i64;
    /// Go `SessionVars.ConnectionID`.
    fn connection_id(&self) -> u64;
    /// Go's assignment `GetSessionVars().GroupConcatMaxLen = val`, which this
    /// package performs only from `SetGroupConcatMaxLenForTest`.
    fn set_group_concat_max_len_for_test(&self, val: u64);
    /// Go `SessionVars.StmtCtx.GetStaleTSO()`: the cached stale-read TSO, or 0
    /// when no provider is installed.
    fn stale_tso(&self) -> Result<u64, EvalCtxError>;
    /// Go `SessionVars.GetSessionOrGlobalSystemVar(ctx, name)`, which unlike
    /// [`SessionVarsSnapshot::get_system_var`] runs the variable's `GetSession`
    /// hook — the reason `timestamp` resolves to the statement's cached "now"
    /// when it is left at its default.
    fn session_or_global_system_var(&self, name: &str) -> Result<String, EvalCtxError>;
}

/// boundary: Go `sessionctx.Context`, narrowed to the accessors this package
/// calls on a session. See the module header.
pub trait SessionContext: Send + Sync {
    /// Go `Context.GetSessionVars`.
    fn get_session_vars(&self) -> Arc<dyn SessionVarsAccessor>;
    /// Go `Context.GetStore`.
    fn get_store(&self) -> Arc<dyn KvStorage>;
    /// Go `Context.IsDDLOwner`.
    fn is_ddl_owner(&self) -> bool;
    /// Go `Context.GetInfoSchema`.
    fn get_info_schema(&self) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync>;
    /// Go `Context.GetLatestInfoSchema`.
    fn get_latest_info_schema(&self) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync>;
    /// Go `Context.GetRestrictedSQLExecutor`.
    fn get_restricted_sql_executor(&self) -> Arc<dyn SqlExecutor>;
    /// The session itself, which in Go *is* an `expropt.AdvisoryLockContext`
    /// and is handed to `NewAdvisoryLockPropProvider` unchanged.
    fn get_advisory_lock_context(&self) -> Arc<dyn AdvisoryLockContext>;
    /// boundary: Go `privilege.GetPrivilegeManager(sctx)`. `None` is Go's nil
    /// manager, under which every privilege check passes.
    fn get_privilege_manager(&self) -> Option<Arc<dyn PrivilegeManager>>;
    /// boundary: Go `util.GetSequenceByName(sctx.GetInfoSchema(), db, name)`.
    fn get_sequence_by_name(
        &self,
        db: &str,
        name: &str,
    ) -> Result<Arc<dyn SequenceTable>, ExprOptError>;
    /// Go `sctx.GetPlanCtx().GetReadonlyUserVarMap()` membership, which is all
    /// `IsReadonlyUserVar` asks of the map.
    fn is_readonly_user_var(&self, name: &str) -> bool;
}

/// boundary: Go `pkg/privilege.Manager`, narrowed to the two verification
/// methods this package calls.
pub trait PrivilegeManager: Send + Sync {
    /// Go `Manager.RequestVerification`.
    fn request_verification(
        &self,
        active_roles: &[Arc<RoleIdentity>],
        db: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool;

    /// Go `Manager.RequestDynamicVerification`.
    fn request_dynamic_verification(
        &self,
        active_roles: &[Arc<RoleIdentity>],
        priv_name: &str,
        grantable: bool,
    ) -> bool;
}

/// boundary: Go `pkg/util.SequenceTable`, the sequence half of a table handle.
/// Go's methods take the session as their first argument, and
/// [`SessionSequenceOperator`] passes the one it captured; that shape is kept.
pub trait SequenceTable: Send + Sync {
    /// Go `SequenceTable.GetSequenceID`.
    fn get_sequence_id(&self) -> i64;
    /// Go `SequenceTable.GetSequenceNextVal`.
    fn get_sequence_next_val(
        &self,
        sctx: &Arc<dyn SessionContext>,
        db: &str,
        name: &str,
    ) -> Result<i64, ExprOptError>;
    /// Go `SequenceTable.SetSequenceVal`, returning the value and whether
    /// `new_val` was already under the sequence's base.
    fn set_sequence_val(
        &self,
        sctx: &Arc<dyn SessionContext>,
        new_val: i64,
        db: &str,
        name: &str,
    ) -> Result<(i64, bool), ExprOptError>;
}

/// boundary: Go `github.com/tikv/client-go/v2/oracle.GetTimeFromTS`. A TSO's
/// high bits are physical milliseconds; the low 18 are the logical counter.
#[must_use]
pub fn get_time_from_ts(ts: u64) -> DateTime<Utc> {
    #[allow(clippy::cast_possible_wrap)]
    let millis = (ts >> 18) as i64;
    DateTime::from_timestamp_millis(millis).unwrap_or_else(Utc::now)
}

/// The single sink Go reaches by handing the statement context's warning
/// handler to its type context and error context alike: every warning raised
/// through this evaluation context lands in the session's own handler, read
/// live on each append.
struct SessionWarnBridge(Arc<dyn SessionContext>);

impl SessionWarnBridge {
    fn handler(&self) -> Arc<dyn WarnHandler + Send + Sync> {
        self.0.get_session_vars().warn_handler()
    }
}

impl WarnAppender for SessionWarnBridge {
    fn append_warning(&self, err: WarnErr) {
        self.handler().append_warning(err);
    }

    fn append_note(&self, err: WarnErr) {
        self.handler().append_note(err);
    }
}

impl ConversionWarningAppender for SessionWarnBridge {
    fn append_conversion_warning(&self, warning: TerrorError) {
        self.handler().append_warning(WarnErr::Terror(warning));
    }
}

impl ErrCtxWarnAppender for SessionWarnBridge {
    /// `errctx` hands over an opaque `SharedError`, so a typed terror that
    /// arrived that way is flattened to its message, exactly as
    /// [`crate::exprstatic`] does. Only the `SQLWarn.Err` identity differs,
    /// never the text.
    fn append_warning(&self, err: SharedError) {
        self.handler()
            .append_warning(WarnErr::Message(err.to_string()));
    }

    fn append_note(&self, err: SharedError) {
        self.handler()
            .append_note(WarnErr::Message(err.to_string()));
    }
}

/// Go `EvalContext`: implements the evaluation context over a live session.
pub struct EvalContext {
    sctx: Arc<dyn SessionContext>,
    props: OptionalEvalPropProviders,
    /// The shared sink of [`EvalContext::type_ctx`] and
    /// [`EvalContext::err_ctx`]. Go instead hands both the statement context's
    /// own handler.
    bridge: Arc<SessionWarnBridge>,
}

impl std::fmt::Debug for EvalContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EvalContext")
            .field("ctx_id", &self.ctx_id())
            .field("current_db", &self.current_db())
            .finish_non_exhaustive()
    }
}

/// Go `EvalContext.setOptionalProp`, including its `intest.AssertFunc` that no
/// property is installed twice.
fn set_optional_prop(
    props: &mut OptionalEvalPropProviders,
    prop: Arc<dyn DynOptionalEvalPropProvider>,
) {
    debug_assert!(
        !props.contains(prop.desc().key()),
        "optional property '{}' is set twice",
        prop.desc().key()
    );
    props.add(prop);
}

impl EvalContext {
    /// Go `NewEvalContext`, which installs *all* optional properties.
    ///
    /// The `Arc::new_cyclic` shape is the adaptation named in the module
    /// header: the privilege-checker provider must hand back this very
    /// context.
    #[must_use]
    pub fn new(sctx: Arc<dyn SessionContext>) -> Arc<EvalContext> {
        Arc::new_cyclic(|weak: &Weak<EvalContext>| {
            let mut props = OptionalEvalPropProviders::new();

            set_optional_prop(&mut props, Arc::new(current_user_prop(Arc::clone(&sctx))));
            set_optional_prop(
                &mut props,
                Arc::new(new_session_vars_provider(Arc::new(
                    SessionVarsProviderAdapter(Arc::clone(&sctx)),
                ))),
            );
            set_optional_prop(&mut props, Arc::new(info_schema_prop(Arc::clone(&sctx))));

            let store_sctx = Arc::clone(&sctx);
            set_optional_prop(
                &mut props,
                Arc::new(KvStorePropProvider::new(move || store_sctx.get_store())),
            );

            set_optional_prop(&mut props, Arc::new(sql_executor_prop(Arc::clone(&sctx))));
            set_optional_prop(
                &mut props,
                Arc::new(sequence_operator_prop(Arc::clone(&sctx))),
            );
            set_optional_prop(
                &mut props,
                Arc::new(AdvisoryLockPropProvider::new(
                    sctx.get_advisory_lock_context(),
                )),
            );

            let ddl_sctx = Arc::clone(&sctx);
            set_optional_prop(
                &mut props,
                Arc::new(DdlOwnerInfoProvider::new(move || ddl_sctx.is_ddl_owner())),
            );

            let this = weak.clone();
            set_optional_prop(
                &mut props,
                Arc::new(PrivilegeCheckerProvider::new(move || {
                    this.upgrade()
                        .expect("the provider lives inside the EvalContext it points at")
                        as Arc<dyn PrivilegeChecker>
                })),
            );

            // Go: "When EvalContext is created from a session, it should
            // contain all the optional properties."
            debug_assert!(props.prop_key_set().is_full());

            let bridge = Arc::new(SessionWarnBridge(Arc::clone(&sctx)));
            EvalContext {
                sctx,
                props,
                bridge,
            }
        })
    }

    /// Go `Sctx`: the inner session context.
    #[must_use]
    pub fn sctx(&self) -> &Arc<dyn SessionContext> {
        &self.sctx
    }

    fn vars(&self) -> Arc<dyn SessionVarsAccessor> {
        self.sctx.get_session_vars()
    }

    /// Go `CtxID`.
    #[must_use]
    pub fn ctx_id(&self) -> u64 {
        self.vars().ctx_id()
    }

    /// Go `SQLMode`.
    #[must_use]
    pub fn sql_mode(&self) -> SqlMode {
        self.vars().sql_mode()
    }

    /// Go `TypeCtx`.
    ///
    /// Go additionally runs `exprctx.AssertLocationWithSessionVars` under the
    /// `intest` build tag; the location here *is* the session's, by
    /// construction, so the assertion has nothing left to check.
    #[must_use]
    pub fn type_ctx(&self) -> ConversionContext<'_> {
        let vars = self.vars();
        ConversionContext::new(
            vars.type_flags(),
            ConversionLocation::named(zone_name(&vars.location())),
            &*self.bridge,
        )
    }

    /// The flags of [`EvalContext::type_ctx`], Go `TypeCtx().Flags()`.
    #[must_use]
    pub fn type_flags(&self) -> ConversionFlags {
        self.vars().type_flags()
    }

    /// Go `ErrCtx`.
    #[must_use]
    pub fn err_ctx(&self) -> ErrCtxContext {
        new_context_with_levels(self.vars().err_level_map(), Arc::clone(&self.bridge) as _)
    }

    /// The level map of [`EvalContext::err_ctx`], Go `ErrCtx().LevelMap()`.
    #[must_use]
    pub fn err_level_map(&self) -> LevelMap {
        self.vars().err_level_map()
    }

    /// Go `Location`.
    #[must_use]
    pub fn location(&self) -> TimeZone {
        self.vars().location()
    }

    /// Go `AppendWarning`.
    pub fn append_warning(&self, err: WarnErr) {
        self.vars().warn_handler().append_warning(err);
    }

    /// Go `AppendNote`.
    pub fn append_note(&self, err: WarnErr) {
        self.vars().warn_handler().append_note(err);
    }

    /// Go `WarningCount`.
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.vars().warn_handler().warning_count()
    }

    /// Go `TruncateWarnings`.
    #[must_use]
    pub fn truncate_warnings(&self, start: usize) -> Vec<SqlWarn> {
        self.vars().warn_handler().truncate_warnings(start)
    }

    /// Go `CopyWarnings`. Go appends into the caller's slice; ownership makes
    /// the returned vector the same result without the aliasing contract.
    #[must_use]
    pub fn copy_warnings(&self) -> Vec<SqlWarn> {
        self.vars().warn_handler().copy_warnings()
    }

    /// Go `CurrentDB`.
    #[must_use]
    pub fn current_db(&self) -> String {
        self.vars().current_db()
    }

    /// Go `CurrentTime`, as the instant only; see the module header.
    pub fn current_time(&self) -> Result<DateTime<Utc>, EvalCtxError> {
        get_stmt_timestamp(&self.sctx)
    }

    /// Go `GetMaxAllowedPacket`.
    #[must_use]
    pub fn get_max_allowed_packet(&self) -> u64 {
        self.vars().max_allowed_packet()
    }

    /// Go `GetTiDBRedactLog`.
    #[must_use]
    pub fn get_tidb_redact_log(&self) -> String {
        self.vars().enable_redact_log()
    }

    /// Go `GetDefaultWeekFormatMode`: an unset or empty variable reads as "0".
    #[must_use]
    pub fn get_default_week_format_mode(&self) -> String {
        match self.vars().get_system_var(DEFAULT_WEEK_FORMAT) {
            Some(mode) if !mode.is_empty() => mode,
            _ => "0".to_owned(),
        }
    }

    /// Go `GetDivPrecisionIncrement`.
    #[must_use]
    pub fn get_div_precision_increment(&self) -> i64 {
        self.vars().div_precision_increment()
    }

    /// Go `GetOptionalPropSet`.
    #[must_use]
    pub fn get_optional_prop_set(&self) -> OptionalEvalPropKeySet {
        self.props.prop_key_set()
    }

    /// Go `GetOptionalPropProvider`.
    #[must_use]
    pub fn get_optional_prop_provider(
        &self,
        key: OptionalEvalPropKey,
    ) -> Option<Arc<dyn DynOptionalEvalPropProvider>> {
        self.props.get(key)
    }

    /// Go `RequestVerification`: no bound privilege manager passes everything.
    #[must_use]
    pub fn request_verification(
        &self,
        db: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool {
        let Some(checker) = self.sctx.get_privilege_manager() else {
            return true;
        };
        checker.request_verification(&self.vars().active_roles(), db, table, column, privilege)
    }

    /// Go `RequestDynamicVerification`, for a DYNAMIC privilege.
    #[must_use]
    pub fn request_dynamic_verification(&self, priv_name: &str, grantable: bool) -> bool {
        let Some(checker) = self.sctx.get_privilege_manager() else {
            return true;
        };
        checker.request_dynamic_verification(&self.vars().active_roles(), priv_name, grantable)
    }

    /// Go `GetParamValue`.
    pub fn get_param_value(&self, idx: usize) -> Result<Datum, EvalCtxError> {
        self.vars()
            .all_param_values()
            .get(idx)
            .cloned()
            .ok_or_else(|| EvalCtxError::new(ERR_PARAM_INDEX_EXCEED_PARAM_COUNTS))
    }

    /// Go `GetUserVarsReader`.
    #[must_use]
    pub fn get_user_vars_reader(&self) -> Arc<dyn UserVarsReader + Send + Sync> {
        self.vars().user_vars_reader()
    }

    /// Go `AllParamValues`, implementing `StaticConvertibleEvalContext`.
    #[must_use]
    pub fn all_param_values(&self) -> Vec<Datum> {
        self.vars().all_param_values()
    }

    /// Go `GetWarnHandler`, implementing `StaticConvertibleEvalContext`.
    #[must_use]
    pub fn get_warn_handler(&self) -> Arc<dyn WarnHandler + Send + Sync> {
        self.vars().warn_handler()
    }

    /// Go `IntoStatic`: turns this session context into a static snapshot.
    #[must_use]
    pub fn into_static(&self) -> crate::exprstatic::EvalContext {
        make_eval_context_static(self)
    }
}

impl EvalPropContext for EvalContext {
    fn get_optional_prop_provider(
        &self,
        key: OptionalEvalPropKey,
    ) -> Option<Arc<dyn DynOptionalEvalPropProvider>> {
        EvalContext::get_optional_prop_provider(self, key)
    }
}

impl PrivilegeChecker for EvalContext {
    fn request_verification(
        &self,
        db: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool {
        EvalContext::request_verification(self, db, table, column, privilege)
    }

    fn request_dynamic_verification(&self, priv_name: &str, grantable: bool) -> bool {
        EvalContext::request_dynamic_verification(self, priv_name, grantable)
    }
}

impl StaticConvertibleEvalContext for EvalContext {
    fn sql_mode(&self) -> SqlMode {
        EvalContext::sql_mode(self)
    }

    fn type_flags(&self) -> ConversionFlags {
        EvalContext::type_flags(self)
    }

    fn location(&self) -> TimeZone {
        EvalContext::location(self)
    }

    fn err_level_map(&self) -> LevelMap {
        EvalContext::err_level_map(self)
    }

    fn current_db(&self) -> String {
        EvalContext::current_db(self)
    }

    fn current_time(&self) -> Result<DateTime<Utc>, EvalCtxError> {
        EvalContext::current_time(self)
    }

    fn get_max_allowed_packet(&self) -> u64 {
        EvalContext::get_max_allowed_packet(self)
    }

    fn get_default_week_format_mode(&self) -> String {
        EvalContext::get_default_week_format_mode(self)
    }

    fn get_div_precision_increment(&self) -> i64 {
        EvalContext::get_div_precision_increment(self)
    }

    fn get_tidb_redact_log(&self) -> String {
        EvalContext::get_tidb_redact_log(self)
    }

    fn get_user_vars_reader(&self) -> Arc<dyn UserVarsReader + Send + Sync> {
        EvalContext::get_user_vars_reader(self)
    }

    fn all_param_values(&self) -> Vec<Datum> {
        EvalContext::all_param_values(self)
    }

    fn get_warn_handler(&self) -> Arc<dyn WarnHandler + Send + Sync> {
        EvalContext::get_warn_handler(self)
    }
}

/// Go `ExprContext`: the expression-building context over a live session. Go
/// embeds `*EvalContext`; here it is a field, see the module header.
pub struct ExprContext {
    sctx: Arc<dyn SessionContext>,
    eval_ctx: Arc<EvalContext>,
}

impl std::fmt::Debug for ExprContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExprContext")
            .field("eval_ctx", &self.eval_ctx)
            .finish_non_exhaustive()
    }
}

impl ExprContext {
    /// Go `NewExprContext`.
    #[must_use]
    pub fn new(sctx: Arc<dyn SessionContext>) -> ExprContext {
        ExprContext {
            eval_ctx: EvalContext::new(Arc::clone(&sctx)),
            sctx,
        }
    }

    /// Go's embedded `ctx.EvalContext` field.
    #[must_use]
    pub fn eval_context(&self) -> &Arc<EvalContext> {
        &self.eval_ctx
    }

    /// Go `GetEvalCtx`.
    #[must_use]
    pub fn get_eval_ctx(&self) -> &Arc<EvalContext> {
        &self.eval_ctx
    }

    fn vars(&self) -> Arc<dyn SessionVarsAccessor> {
        self.sctx.get_session_vars()
    }

    /// Go `GetCharsetInfo`.
    #[must_use]
    pub fn get_charset_info(&self) -> (String, String) {
        self.vars().charset_info()
    }

    /// Go `GetDefaultCollationForUTF8MB4`.
    #[must_use]
    pub fn get_default_collation_for_utf8mb4(&self) -> String {
        self.vars().default_collation_for_utf8mb4()
    }

    /// Go `GetBlockEncryptionMode`: the value of `block_encryption_mode`,
    /// falling back to its default when the variable is unset.
    #[must_use]
    pub fn get_block_encryption_mode(&self) -> String {
        self.vars()
            .get_system_var(BLOCK_ENCRYPTION_MODE)
            .unwrap_or_else(|| DEF_BLOCK_ENCRYPTION_MODE.to_owned())
    }

    /// Go `GetSysdateIsNow`: the value of `tidb_sysdate_is_now`.
    #[must_use]
    pub fn get_sysdate_is_now(&self) -> bool {
        self.vars().sysdate_is_now()
    }

    /// Go `GetNoopFuncsMode`: OFF/ON/WARN as 0/1/2.
    #[must_use]
    pub fn get_noop_funcs_mode(&self) -> i64 {
        self.vars().noop_funcs_mode()
    }

    /// Go `Rng`.
    #[must_use]
    pub fn rng(&self) -> Arc<MysqlRng> {
        self.vars().rng()
    }

    /// Go `IsUseCache`.
    #[must_use]
    pub fn is_use_cache(&self) -> bool {
        self.vars().plan_cache_tracker().use_cache()
    }

    /// Go `SetSkipPlanCache`.
    pub fn set_skip_plan_cache(&self, reason: &str) {
        self.vars().plan_cache_tracker().set_skip_plan_cache(reason);
    }

    /// Go `AllocPlanColumnID`.
    #[must_use]
    pub fn alloc_plan_column_id(&self) -> i64 {
        self.vars().alloc_plan_column_id()
    }

    /// Go `IsInNullRejectCheck`, which always returns false.
    #[must_use]
    pub fn is_in_null_reject_check(&self) -> bool {
        false
    }

    /// Go `IsConstantPropagateCheck`, which always returns false.
    #[must_use]
    pub fn is_constant_propagate_check(&self) -> bool {
        false
    }

    /// Go `GetWindowingUseHighPrecision`.
    #[must_use]
    pub fn get_windowing_use_high_precision(&self) -> bool {
        self.vars().windowing_use_high_precision()
    }

    /// Go `GetGroupConcatMaxLen`.
    #[must_use]
    pub fn get_group_concat_max_len(&self) -> u64 {
        self.vars().group_concat_max_len()
    }

    /// Go `SetGroupConcatMaxLenForTest`.
    pub fn set_group_concat_max_len_for_test(&self, val: u64) {
        self.vars().set_group_concat_max_len_for_test(val);
    }

    /// Go `ConnectionID`, 0 when the context is not in a session.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        self.vars().connection_id()
    }

    /// Go `IsReadonlyUserVar`.
    #[must_use]
    pub fn is_readonly_user_var(&self, name: &str) -> bool {
        self.sctx.is_readonly_user_var(name)
    }

    /// Go `GetPlanCacheTracker`, implementing `StaticConvertibleExprContext`.
    #[must_use]
    pub fn get_plan_cache_tracker(&self) -> Arc<PlanCacheTracker> {
        self.vars().plan_cache_tracker()
    }

    /// Go `GetLastPlanColumnID`, implementing `StaticConvertibleExprContext`.
    #[must_use]
    pub fn get_last_plan_column_id(&self) -> i64 {
        self.vars().last_plan_column_id()
    }

    /// Go `IntoStatic`.
    #[must_use]
    pub fn into_static(&self) -> crate::exprstatic::ExprContext {
        make_expr_context_static(self)
    }
}

impl StaticConvertibleExprContext for ExprContext {
    fn get_static_convertible_eval_context(&self) -> &dyn StaticConvertibleEvalContext {
        self.eval_ctx.as_ref()
    }

    fn get_charset_info(&self) -> (String, String) {
        ExprContext::get_charset_info(self)
    }

    fn get_default_collation_for_utf8mb4(&self) -> String {
        ExprContext::get_default_collation_for_utf8mb4(self)
    }

    fn get_block_encryption_mode(&self) -> String {
        ExprContext::get_block_encryption_mode(self)
    }

    fn get_sysdate_is_now(&self) -> bool {
        ExprContext::get_sysdate_is_now(self)
    }

    fn get_noop_funcs_mode(&self) -> i64 {
        ExprContext::get_noop_funcs_mode(self)
    }

    fn rng(&self) -> Arc<MysqlRng> {
        ExprContext::rng(self)
    }

    fn get_plan_cache_tracker(&self) -> Arc<PlanCacheTracker> {
        ExprContext::get_plan_cache_tracker(self)
    }

    fn get_last_plan_column_id(&self) -> i64 {
        ExprContext::get_last_plan_column_id(self)
    }

    fn connection_id(&self) -> u64 {
        ExprContext::connection_id(self)
    }

    fn get_windowing_use_high_precision(&self) -> bool {
        ExprContext::get_windowing_use_high_precision(self)
    }

    fn get_group_concat_max_len(&self) -> u64 {
        ExprContext::get_group_concat_max_len(self)
    }
}

/// Go `getStmtTimestamp`: the statement's "now", which is the stale-read TSO
/// when one is installed, otherwise the `timestamp` system variable.
///
/// Go's `ctx == nil` branches are dropped; see the module header.
pub fn get_stmt_timestamp(sctx: &Arc<dyn SessionContext>) -> Result<DateTime<Utc>, EvalCtxError> {
    let vars = sctx.get_session_vars();
    if let Ok(stale_tso) = vars.stale_tso() {
        if stale_tso != 0 {
            return Ok(get_time_from_ts(stale_tso));
        }
    }

    let timestamp_str = vars.session_or_global_system_var(TIMESTAMP)?;

    // Go `types.StrToFloat(sessionVars.StmtCtx.TypeCtx(), timestampStr, false)`:
    // whether a truncation is an error is the statement type context's call.
    let converted = str_to_float(&timestamp_str, false);
    if converted.event.is_some() {
        let type_ctx = ConversionContext::new(
            vars.type_flags(),
            ConversionLocation::named(zone_name(&vars.location())),
            &tidb_datatype::IGNORE_CONVERSION_WARNINGS,
        );
        let truncated = tidb_datatype::ERR_TRUNCATED_WRONG_VALUE.generate_with_stack(format!(
            "Truncated incorrect DOUBLE value: '{timestamp_str}'"
        ));
        if let Some(err) = type_ctx.handle_truncate(Some(truncated)) {
            return Err(EvalCtxError::new(err.to_string()));
        }
    }

    let seconds = converted.value.trunc();
    let fractional = converted.value - seconds;
    #[allow(clippy::cast_possible_truncation)]
    let nanos = (fractional * 1e9).round() as i64;
    #[allow(clippy::cast_possible_truncation)]
    DateTime::from_timestamp(seconds as i64, nanos.unsigned_abs() as u32)
        .ok_or_else(|| EvalCtxError::new(format!("timestamp out of range: '{timestamp_str}'")))
}

/// Go `currentUserProp`.
fn current_user_prop(sctx: Arc<dyn SessionContext>) -> CurrentUserPropProvider {
    CurrentUserPropProvider::new(move || {
        let vars = sctx.get_session_vars();
        (vars.user(), vars.active_roles())
    })
}

/// Go `infoSchemaProp`.
fn info_schema_prop(sctx: Arc<dyn SessionContext>) -> InfoSchemaPropProvider {
    InfoSchemaPropProvider::new(move |is_domain| {
        if is_domain {
            sctx.get_latest_info_schema()
        } else {
            sctx.get_info_schema()
        }
    })
}

/// Go `sqlExecutorProp`.
fn sql_executor_prop(sctx: Arc<dyn SessionContext>) -> SqlExecutorPropProvider {
    SqlExecutorPropProvider::new(move || Ok(sctx.get_restricted_sql_executor()))
}

/// Go `sequenceOperator`: one sequence, bound to the session that reached it.
pub struct SessionSequenceOperator {
    sctx: Arc<dyn SessionContext>,
    db: String,
    name: String,
    tbl: Arc<dyn SequenceTable>,
}

impl SequenceOperator for SessionSequenceOperator {
    fn get_sequence_id(&self) -> i64 {
        self.tbl.get_sequence_id()
    }

    fn get_sequence_next_val(&self) -> Result<i64, ExprOptError> {
        self.tbl
            .get_sequence_next_val(&self.sctx, &self.db, &self.name)
    }

    fn set_sequence_val(&self, new_val: i64) -> Result<(i64, bool), ExprOptError> {
        self.tbl
            .set_sequence_val(&self.sctx, new_val, &self.db, &self.name)
    }
}

/// Go `sequenceOperatorProp`.
fn sequence_operator_prop(sctx: Arc<dyn SessionContext>) -> SequenceOperatorProvider {
    SequenceOperatorProvider::new(move |db, name| {
        let sequence = sctx.get_sequence_by_name(db, name)?;
        Ok(Arc::new(SessionSequenceOperator {
            sctx: Arc::clone(&sctx),
            db: db.to_owned(),
            name: name.to_owned(),
            tbl: sequence,
        }) as Arc<dyn SequenceOperator>)
    })
}

/// Go passes the session straight to `expropt.NewSessionVarsProvider`, because
/// a `sessionctx.Context` *is* a `variable.SessionVarsProvider`. Rust needs a
/// named adapter for the same hand-off.
struct SessionVarsProviderAdapter(Arc<dyn SessionContext>);

impl SessionVarsProvider for SessionVarsProviderAdapter {
    fn get_session_vars(&self) -> Arc<dyn ExproptSessionVars> {
        self.0.get_session_vars()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
    use std::sync::Mutex;

    use tidb_datatype::STRICT_FLAGS;
    use tidb_error::errctx::{ErrGroup, Level};
    use tidb_error::terror::TerrorCode;
    use tidb_mysql::consts::{get_sql_mode, ModeNoZeroDate, ModeStrictTransTables};
    use tidb_mysql::privilege::{CreatePriv, SuperPriv};
    use tidb_util::context::{
        gen_context_id, StaticWarnHandler, WARN_LEVEL_NOTE, WARN_LEVEL_WARNING,
    };
    use tidb_vardef::defaults::DEF_TIMESTAMP;

    use crate::expropt::{
        AdvisoryLockPropReader, CurrentUserPropReader, DdlOwnerPropReader, InfoSchemaPropReader,
        KvStorePropReader, PrivilegeCheckerPropReader, SequenceOperatorPropReader,
        SessionVarsPropReader, SqlExecutorPropReader,
    };
    use crate::exprstatic::StaticSessionVars;
    use crate::user_vars::UserVars;

    /// Go `require.Same` on two interface values: the same allocation, even
    /// when the two handles have different trait-object types.
    fn same_alloc<T: ?Sized, U: ?Sized>(a: &Arc<T>, b: &Arc<U>) -> bool {
        std::ptr::eq(Arc::as_ptr(a) as *const (), Arc::as_ptr(b) as *const ())
    }

    /// A warning raised through the *type* context, which carries a typed
    /// terror rather than Go's open `error`; its rendered text is
    /// `[0]<message>`.
    fn terror(message: &str) -> TerrorError {
        TerrorError::compatible(TerrorCode::new(0), message)
    }

    fn warn_texts(warnings: &[SqlWarn]) -> Vec<(String, String)> {
        warnings
            .iter()
            .map(|w| (w.level.clone(), w.err.to_string()))
            .collect()
    }

    // ---------------------------------------------------------------------
    // The test fixture, standing in for Go's `pkg/util/mock.Context`.
    // ---------------------------------------------------------------------

    /// Go `stmtctx.staleTSOProvider`.
    #[derive(Default)]
    struct MockStaleTso {
        value: Option<u64>,
        #[allow(clippy::type_complexity)]
        eval: Option<Arc<dyn Fn() -> Result<u64, EvalCtxError> + Send + Sync>>,
    }

    /// The session variables of the mock session: [`StaticSessionVars`] for
    /// everything a system variable drives, plus the live fields and the
    /// statement-context slice this package reads.
    struct MockSessionVars {
        systems: Mutex<StaticSessionVars>,
        warn_handler: Arc<StaticWarnHandler>,
        type_flags: Mutex<ConversionFlags>,
        level_map: Mutex<LevelMap>,
        current_db: Mutex<String>,
        user: Mutex<Option<Arc<UserIdentity>>>,
        active_roles: Mutex<Vec<Arc<RoleIdentity>>>,
        params: Mutex<Vec<Datum>>,
        user_vars: Arc<UserVars>,
        rng: Mutex<Arc<MysqlRng>>,
        plan_cache_tracker: Arc<PlanCacheTracker>,
        plan_column_id: AtomicI64,
        connection_id: AtomicU64,
        ctx_id: u64,
        stale_tso: Mutex<MockStaleTso>,
        /// Go `stmtctx.StmtNowTsCacheKey`, the statement's cached "now".
        stmt_now: Mutex<Option<DateTime<Utc>>>,
    }

    impl MockSessionVars {
        fn new() -> Arc<MockSessionVars> {
            let warn_handler = Arc::new(StaticWarnHandler::new(0));
            Arc::new(MockSessionVars {
                systems: Mutex::new(StaticSessionVars::default()),
                plan_cache_tracker: Arc::new(PlanCacheTracker::new(Arc::clone(&warn_handler) as _)),
                warn_handler,
                type_flags: Mutex::new(STRICT_FLAGS),
                level_map: Mutex::new(LevelMap::strict()),
                current_db: Mutex::new(String::new()),
                user: Mutex::new(None),
                active_roles: Mutex::new(Vec::new()),
                params: Mutex::new(Vec::new()),
                user_vars: Arc::new(UserVars::new()),
                rng: Mutex::new(Arc::new(MysqlRng::new_with_time())),
                plan_column_id: AtomicI64::new(0),
                connection_id: AtomicU64::new(0),
                ctx_id: gen_context_id(),
                stale_tso: Mutex::new(MockStaleTso::default()),
                stmt_now: Mutex::new(None),
            })
        }

        /// Go `SessionVars.SetSystemVar`.
        fn set_system_var(&self, name: &str, val: &str) -> Result<(), EvalCtxError> {
            self.systems.lock().unwrap().set_system_var(name, val)
        }

        /// Go `StatementContext.SetTypeFlags`.
        fn set_type_flags(&self, flags: ConversionFlags) {
            *self.type_flags.lock().unwrap() = flags;
        }

        /// Go `StatementContext.SetErrLevels`.
        fn set_err_levels(&self, levels: LevelMap) {
            *self.level_map.lock().unwrap() = levels;
        }

        /// Go `StatementContext.SetStaleTSOProviderIfNotExist`, including its
        /// "already installed" early return.
        fn set_stale_tso_provider_if_not_exist(
            &self,
            eval: Option<Arc<dyn Fn() -> Result<u64, EvalCtxError> + Send + Sync>>,
        ) {
            let mut provider = self.stale_tso.lock().unwrap();
            if provider.eval.is_some() {
                return;
            }
            provider.value = None;
            provider.eval = eval;
        }

        /// Go `StatementContext.Reset`, in the two pieces this package sees:
        /// the stale-TSO provider and the statement's cached "now".
        fn reset_stmt(&self) {
            let mut provider = self.stale_tso.lock().unwrap();
            provider.value = None;
            provider.eval = None;
            *self.stmt_now.lock().unwrap() = None;
        }
    }

    impl SessionVarsSnapshot for MockSessionVars {
        fn location(&self) -> TimeZone {
            self.systems.lock().unwrap().location()
        }
        fn sql_mode(&self) -> SqlMode {
            self.systems.lock().unwrap().sql_mode()
        }
        fn max_allowed_packet(&self) -> u64 {
            self.systems.lock().unwrap().max_allowed_packet()
        }
        fn enable_redact_log(&self) -> String {
            self.systems.lock().unwrap().enable_redact_log()
        }
        fn div_precision_increment(&self) -> i64 {
            self.systems.lock().unwrap().div_precision_increment()
        }
        fn charset_info(&self) -> (String, String) {
            self.systems.lock().unwrap().charset_info()
        }
        fn default_collation_for_utf8mb4(&self) -> String {
            self.systems.lock().unwrap().default_collation_for_utf8mb4()
        }
        fn get_system_var(&self, name: &str) -> Option<String> {
            self.systems.lock().unwrap().get_system_var(name)
        }
        fn sysdate_is_now(&self) -> bool {
            self.systems.lock().unwrap().sysdate_is_now()
        }
        fn noop_funcs_mode(&self) -> i64 {
            self.systems.lock().unwrap().noop_funcs_mode()
        }
        fn windowing_use_high_precision(&self) -> bool {
            self.systems.lock().unwrap().windowing_use_high_precision()
        }
        fn group_concat_max_len(&self) -> u64 {
            self.systems.lock().unwrap().group_concat_max_len()
        }
    }

    impl ExproptSessionVars for MockSessionVars {}

    impl SessionVarsAccessor for MockSessionVars {
        fn ctx_id(&self) -> u64 {
            self.ctx_id
        }

        fn type_flags(&self) -> ConversionFlags {
            *self.type_flags.lock().unwrap()
        }

        fn err_level_map(&self) -> LevelMap {
            *self.level_map.lock().unwrap()
        }

        fn warn_handler(&self) -> Arc<dyn WarnHandler + Send + Sync> {
            Arc::clone(&self.warn_handler) as _
        }

        fn current_db(&self) -> String {
            self.current_db.lock().unwrap().clone()
        }

        fn user(&self) -> Option<Arc<UserIdentity>> {
            self.user.lock().unwrap().clone()
        }

        fn active_roles(&self) -> Vec<Arc<RoleIdentity>> {
            self.active_roles.lock().unwrap().clone()
        }

        fn all_param_values(&self) -> Vec<Datum> {
            self.params.lock().unwrap().clone()
        }

        fn user_vars_reader(&self) -> Arc<dyn UserVarsReader + Send + Sync> {
            Arc::clone(&self.user_vars) as _
        }

        fn rng(&self) -> Arc<MysqlRng> {
            Arc::clone(&self.rng.lock().unwrap())
        }

        fn plan_cache_tracker(&self) -> Arc<PlanCacheTracker> {
            Arc::clone(&self.plan_cache_tracker)
        }

        fn alloc_plan_column_id(&self) -> i64 {
            self.plan_column_id.fetch_add(1, Ordering::SeqCst) + 1
        }

        fn last_plan_column_id(&self) -> i64 {
            self.plan_column_id.load(Ordering::SeqCst)
        }

        fn connection_id(&self) -> u64 {
            self.connection_id.load(Ordering::SeqCst)
        }

        fn set_group_concat_max_len_for_test(&self, val: u64) {
            self.set_system_var("group_concat_max_len", &val.to_string())
                .expect("a numeric group_concat_max_len is always accepted");
        }

        fn stale_tso(&self) -> Result<u64, EvalCtxError> {
            let mut provider = self.stale_tso.lock().unwrap();
            if let Some(value) = provider.value {
                return Ok(value);
            }
            let Some(eval) = provider.eval.clone() else {
                return Ok(0);
            };
            let tso = eval()?;
            provider.value = Some(tso);
            Ok(tso)
        }

        /// The `timestamp` variable's `GetSession` hook: an explicitly set
        /// value wins, otherwise the statement's cached "now" is rendered as
        /// float seconds.
        fn session_or_global_system_var(&self, name: &str) -> Result<String, EvalCtxError> {
            if name != TIMESTAMP {
                return self
                    .get_system_var(name)
                    .ok_or_else(|| EvalCtxError::new(format!("unknown system variable '{name}'")));
            }

            match self.get_system_var(TIMESTAMP) {
                Some(val) if val != DEF_TIMESTAMP => Ok(val),
                _ => {
                    let mut cached = self.stmt_now.lock().unwrap();
                    let now = *cached.get_or_insert_with(Utc::now);
                    #[allow(clippy::cast_precision_loss)]
                    let seconds = now.timestamp_nanos_opt().expect("in range") as f64 / 1e9;
                    Ok(seconds.to_string())
                }
            }
        }
    }

    struct MockStore;
    impl KvStorage for MockStore {}

    struct MockSqlExecutor;
    impl SqlExecutor for MockSqlExecutor {}

    struct MockInfoSchema(i64);
    impl MetaOnlyInfoSchema for MockInfoSchema {
        fn schema_meta_version(&self) -> i64 {
            self.0
        }
    }

    /// The mock session, Go's `mock.Context`.
    struct MockSession {
        this: Weak<MockSession>,
        vars: Arc<MockSessionVars>,
        ddl_owner: AtomicBool,
        privilege_manager: Mutex<Option<Arc<dyn PrivilegeManager>>>,
        store: Arc<MockStore>,
        executor: Arc<MockSqlExecutor>,
        readonly_user_vars: Mutex<Vec<String>>,
        /// Records the advisory-lock calls, so the fixture is more than a stub.
        advisory_locks: Mutex<Vec<String>>,
    }

    impl MockSession {
        fn new() -> Arc<MockSession> {
            Arc::new_cyclic(|this: &Weak<MockSession>| MockSession {
                this: this.clone(),
                vars: MockSessionVars::new(),
                ddl_owner: AtomicBool::new(false),
                privilege_manager: Mutex::new(None),
                store: Arc::new(MockStore),
                executor: Arc::new(MockSqlExecutor),
                readonly_user_vars: Mutex::new(Vec::new()),
                advisory_locks: Mutex::new(Vec::new()),
            })
        }

        fn arc(&self) -> Arc<MockSession> {
            self.this.upgrade().expect("the session is alive")
        }

        /// Go `mock.Context.SetIsDDLOwner`.
        fn set_is_ddl_owner(&self, owner: bool) {
            self.ddl_owner.store(owner, Ordering::SeqCst);
        }

        /// Go `privilege.BindPrivilegeManager`.
        fn bind_privilege_manager(&self, manager: Option<Arc<dyn PrivilegeManager>>) {
            *self.privilege_manager.lock().unwrap() = manager;
        }
    }

    impl AdvisoryLockContext for MockSession {
        fn get_advisory_lock(&self, name: &str, _timeout: i64) -> Result<(), ExprOptError> {
            self.advisory_locks.lock().unwrap().push(name.to_owned());
            Ok(())
        }

        fn is_used_advisory_lock(&self, name: &str) -> u64 {
            u64::from(
                self.advisory_locks
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|n| n == name),
            )
        }

        fn release_advisory_lock(&self, name: &str) -> bool {
            let mut locks = self.advisory_locks.lock().unwrap();
            let before = locks.len();
            locks.retain(|n| n != name);
            locks.len() != before
        }

        fn release_all_advisory_locks(&self) -> i64 {
            let mut locks = self.advisory_locks.lock().unwrap();
            let count = i64::try_from(locks.len()).expect("in range");
            locks.clear();
            count
        }
    }

    impl SessionContext for MockSession {
        fn get_session_vars(&self) -> Arc<dyn SessionVarsAccessor> {
            Arc::clone(&self.vars) as _
        }

        fn get_store(&self) -> Arc<dyn KvStorage> {
            Arc::clone(&self.store) as _
        }

        fn is_ddl_owner(&self) -> bool {
            self.ddl_owner.load(Ordering::SeqCst)
        }

        fn get_info_schema(&self) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync> {
            Arc::new(MockInfoSchema(1))
        }

        fn get_latest_info_schema(&self) -> Arc<dyn MetaOnlyInfoSchema + Send + Sync> {
            Arc::new(MockInfoSchema(2))
        }

        fn get_restricted_sql_executor(&self) -> Arc<dyn SqlExecutor> {
            Arc::clone(&self.executor) as _
        }

        fn get_advisory_lock_context(&self) -> Arc<dyn AdvisoryLockContext> {
            self.arc() as _
        }

        fn get_privilege_manager(&self) -> Option<Arc<dyn PrivilegeManager>> {
            self.privilege_manager.lock().unwrap().clone()
        }

        fn get_sequence_by_name(
            &self,
            db: &str,
            name: &str,
        ) -> Result<Arc<dyn SequenceTable>, ExprOptError> {
            Err(ExprOptError::new(format!(
                "[schema:1146]Table '{db}.{name}' doesn't exist"
            )))
        }

        fn is_readonly_user_var(&self, name: &str) -> bool {
            self.readonly_user_vars
                .lock()
                .unwrap()
                .iter()
                .any(|n| n == name)
        }
    }

    // ---------------------------------------------------------------------
    // The ported tests.
    // ---------------------------------------------------------------------

    // Go `TestSessionEvalContextBasic`.
    #[test]
    fn session_eval_context_basic() {
        let ctx = MockSession::new();
        let sctx: Arc<dyn SessionContext> = Arc::clone(&ctx) as _;
        let vars = Arc::clone(&ctx.vars);
        let eval_ctx = EvalContext::new(Arc::clone(&sctx));
        assert!(eval_ctx.get_optional_prop_set().is_full());

        // It should contain all the optional properties.
        for key in OptionalEvalPropKey::ALL {
            let provider = eval_ctx
                .get_optional_prop_provider(key)
                .expect("every property is provided");
            assert!(std::ptr::eq(provider.desc(), key.desc()));
        }

        // Go `ResetSessionAndStmtTimeZone(time.FixedZone("UTC+11", 11*3600))`;
        // here the same fixed zone is reached through the `time_zone`
        // variable, which is the mock session's only route to it.
        vars.set_system_var("time_zone", "+11:00").unwrap();
        vars.set_system_var("sql_mode", "STRICT_TRANS_TABLES,NO_ZERO_DATE")
            .unwrap();
        vars.set_type_flags(
            STRICT_FLAGS
                .with_ignore_invalid_date_err(true)
                .with_skip_utf8_check(true),
        );
        vars.set_err_levels(
            LevelMap::strict()
                .with_level(ErrGroup::DupKey, Level::Warn)
                .with_level(ErrGroup::BadNull, Level::Ignore)
                .with_level(ErrGroup::NoDefault, Level::Ignore),
        );
        *vars.current_db.lock().unwrap() = "db1".to_owned();
        vars.set_system_var("max_allowed_packet", "123456").unwrap();

        // Basic fields.
        assert_eq!(eval_ctx.type_ctx().flags(), vars.type_flags());
        assert_eq!(eval_ctx.type_flags(), vars.type_flags());
        assert_eq!(eval_ctx.err_ctx().level_map(), vars.err_level_map());
        assert_eq!(eval_ctx.err_level_map(), vars.err_level_map());
        assert_eq!(
            eval_ctx.sql_mode(),
            get_sql_mode("STRICT_TRANS_TABLES,NO_ZERO_DATE").unwrap()
        );
        assert_eq!(eval_ctx.sql_mode(), ModeStrictTransTables | ModeNoZeroDate);
        assert_eq!(eval_ctx.location(), vars.location());
        assert_eq!(
            eval_ctx.location(),
            TimeZone::Fixed {
                name: String::new(),
                offset_secs: 11 * 3600,
            }
        );
        assert_eq!(
            eval_ctx.type_ctx().location().name(),
            zone_name(&vars.location())
        );
        assert_eq!(eval_ctx.current_db(), "db1");
        assert_eq!(eval_ctx.get_max_allowed_packet(), 123_456);
        assert_eq!(eval_ctx.get_default_week_format_mode(), "0");
        vars.set_system_var("default_week_format", "5").unwrap();
        assert_eq!(eval_ctx.get_default_week_format_mode(), "5");
        assert!(same_alloc(
            &eval_ctx.get_user_vars_reader(),
            &vars.user_vars
        ));

        // The statement context, its type context and its error context share
        // one warning sink.
        assert_eq!(eval_ctx.warning_count(), 0);
        eval_ctx.append_warning(WarnErr::from("err1"));
        assert_eq!(eval_ctx.warning_count(), 1);
        eval_ctx.type_ctx().append_warning(terror("err2"));
        assert_eq!(eval_ctx.warning_count(), 2);
        eval_ctx
            .err_ctx()
            .append_warning(Arc::new(EvalCtxError::new("err3")));
        assert_eq!(eval_ctx.warning_count(), 3);

        let expected = vec![
            (WARN_LEVEL_WARNING.to_owned(), "err1".to_owned()),
            (WARN_LEVEL_WARNING.to_owned(), terror("err2").to_string()),
            (WARN_LEVEL_WARNING.to_owned(), "err3".to_owned()),
        ];
        // Go copies into four differently shaped destination slices; ownership
        // makes the result one value, so one call proves the same contract.
        assert_eq!(warn_texts(&eval_ctx.copy_warnings()), expected);

        let warnings = eval_ctx.truncate_warnings(1);
        assert_eq!(warn_texts(&warnings), expected[1..].to_vec());

        let warnings = eval_ctx.truncate_warnings(0);
        assert_eq!(warn_texts(&warnings), expected[..1].to_vec());
        assert_eq!(eval_ctx.warning_count(), 0);

        // A note keeps its own level, as the shared handler records it.
        eval_ctx.append_note(WarnErr::from("note1"));
        assert_eq!(
            warn_texts(&eval_ctx.copy_warnings()),
            vec![(WARN_LEVEL_NOTE.to_owned(), "note1".to_owned())]
        );
    }

    // Go `TestSessionEvalContextCurrentTime`.
    #[test]
    fn session_eval_context_current_time() {
        let ctx = MockSession::new();
        let sctx: Arc<dyn SessionContext> = Arc::clone(&ctx) as _;
        let vars = Arc::clone(&ctx.vars);
        let eval_ctx = EvalContext::new(Arc::clone(&sctx));

        let now = DateTime::from_timestamp_millis(123_456_789).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let calls_in_fn = Arc::clone(&calls);
        vars.set_stale_tso_provider_if_not_exist(Some(Arc::new(move || {
            // Go `require.True(now.CompareAndSwap(nil, &v))`: called once only.
            assert_eq!(calls_in_fn.fetch_add(1, Ordering::SeqCst), 0);
            // Go `oracle.GoTimeToTS(v)`: physical milliseconds shifted up.
            Ok((u64::try_from(now.timestamp_millis()).unwrap()) << 18)
        })));

        // `CurrentTime` returns the stale TSO when one is installed.
        let tm = eval_ctx.current_time().unwrap();
        assert_eq!(tm.timestamp_nanos_opt(), now.timestamp_nanos_opt());

        // The second call returns the same value, from the provider's cache.
        let tm = eval_ctx.current_time().unwrap();
        assert_eq!(tm.timestamp_nanos_opt(), now.timestamp_nanos_opt());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // With no stale TSO, `CurrentTime` returns the `timestamp` variable.
        vars.set_stale_tso_provider_if_not_exist(None);
        vars.reset_stmt();
        vars.set_system_var(TIMESTAMP, "7654321.875").unwrap();
        let tm = eval_ctx.current_time().unwrap();
        assert_eq!(tm.timestamp_nanos_opt(), Some(7_654_321_875_000_000));

        // The second call returns the same value.
        let tm = eval_ctx.current_time().unwrap();
        assert_eq!(tm.timestamp_nanos_opt(), Some(7_654_321_875_000_000));

        // With neither, it returns the system's current time...
        vars.set_system_var(TIMESTAMP, "0").unwrap();
        vars.reset_stmt();
        let tm = eval_ctx.current_time().unwrap();
        assert!((tm.timestamp() - Utc::now().timestamp()).abs() <= 5);

        // ...and the second call returns the same value, because the statement
        // caches its "now".
        let tm2 = eval_ctx.current_time().unwrap();
        assert_eq!(tm.timestamp_nanos_opt(), tm2.timestamp_nanos_opt());
    }

    /// Go's `mockPrivManager`: `testify/mock` records the expected calls, which
    /// here is a queue of expectations checked in order and asserted drained.
    /// One `RequestVerification` expectation: roles, db, table, column,
    /// privilege, and the answer to give.
    type VerificationCall = (Vec<String>, String, String, String, PrivilegeType, bool);
    /// One `RequestDynamicVerification` expectation: roles, privilege name,
    /// grantable, and the answer to give.
    type DynamicCall = (Vec<String>, String, bool, bool);

    #[derive(Default)]
    struct MockPrivManager {
        verification: Mutex<Vec<VerificationCall>>,
        dynamic: Mutex<Vec<DynamicCall>>,
    }

    impl MockPrivManager {
        fn role_names(roles: &[Arc<RoleIdentity>]) -> Vec<String> {
            roles.iter().map(|r| r.to_string()).collect()
        }

        fn on_request_verification(
            &self,
            roles: &[Arc<RoleIdentity>],
            db: &str,
            table: &str,
            column: &str,
            privilege: PrivilegeType,
            ret: bool,
        ) {
            self.verification.lock().unwrap().push((
                Self::role_names(roles),
                db.to_owned(),
                table.to_owned(),
                column.to_owned(),
                privilege,
                ret,
            ));
        }

        fn on_request_dynamic_verification(
            &self,
            roles: &[Arc<RoleIdentity>],
            priv_name: &str,
            grantable: bool,
            ret: bool,
        ) {
            self.dynamic.lock().unwrap().push((
                Self::role_names(roles),
                priv_name.to_owned(),
                grantable,
                ret,
            ));
        }

        /// Go `mgr.AssertExpectations(t)`.
        fn assert_expectations(&self) {
            assert!(self.verification.lock().unwrap().is_empty());
            assert!(self.dynamic.lock().unwrap().is_empty());
        }
    }

    impl PrivilegeManager for MockPrivManager {
        fn request_verification(
            &self,
            active_roles: &[Arc<RoleIdentity>],
            db: &str,
            table: &str,
            column: &str,
            privilege: PrivilegeType,
        ) -> bool {
            let expected = self.verification.lock().unwrap().remove(0);
            assert_eq!(
                (
                    Self::role_names(active_roles),
                    db.to_owned(),
                    table.to_owned(),
                    column.to_owned(),
                    privilege
                ),
                (expected.0, expected.1, expected.2, expected.3, expected.4)
            );
            expected.5
        }

        fn request_dynamic_verification(
            &self,
            active_roles: &[Arc<RoleIdentity>],
            priv_name: &str,
            grantable: bool,
        ) -> bool {
            let expected = self.dynamic.lock().unwrap().remove(0);
            assert_eq!(
                (
                    Self::role_names(active_roles),
                    priv_name.to_owned(),
                    grantable
                ),
                (expected.0, expected.1, expected.2)
            );
            expected.3
        }
    }

    fn role(username: &str, hostname: &str) -> Arc<RoleIdentity> {
        Arc::new(RoleIdentity {
            username: username.to_owned(),
            hostname: hostname.to_owned(),
        })
    }

    // Go `TestSessionEvalContextPrivilegeCheck`.
    #[test]
    fn session_eval_context_privilege_check() {
        let ctx = MockSession::new();
        let sctx: Arc<dyn SessionContext> = Arc::clone(&ctx) as _;
        let eval_ctx = EvalContext::new(Arc::clone(&sctx));
        let active_roles = vec![role("role1", "host1"), role("role2", "host2")];
        *ctx.vars.active_roles.lock().unwrap() = active_roles.clone();

        // With no privilege manager every check passes.
        ctx.bind_privilege_manager(None);
        assert!(eval_ctx.request_verification("test", "tbl1", "col1", SuperPriv));
        assert!(eval_ctx.request_dynamic_verification("RESTRICTED_TABLES_ADMIN", true));
        assert!(eval_ctx.request_dynamic_verification("RESTRICTED_TABLES_ADMIN", false));

        // With one bound, its answer is the answer.
        let manager = Arc::new(MockPrivManager::default());
        ctx.bind_privilege_manager(Some(Arc::clone(&manager) as _));

        manager.on_request_verification(&active_roles, "db1", "t1", "c1", CreatePriv, true);
        assert!(eval_ctx.request_verification("db1", "t1", "c1", CreatePriv));
        manager.assert_expectations();

        manager.on_request_verification(&active_roles, "db2", "t2", "c2", SuperPriv, false);
        assert!(!eval_ctx.request_verification("db2", "t2", "c2", SuperPriv));
        manager.assert_expectations();

        manager.on_request_dynamic_verification(
            &active_roles,
            "RESTRICTED_USER_ADMIN",
            false,
            true,
        );
        assert!(eval_ctx.request_dynamic_verification("RESTRICTED_USER_ADMIN", false));

        manager.on_request_dynamic_verification(
            &active_roles,
            "RESTRICTED_CONNECTION_ADMIN",
            true,
            false,
        );
        assert!(!eval_ctx.request_dynamic_verification("RESTRICTED_CONNECTION_ADMIN", true));
        manager.assert_expectations();
    }

    // Go `TestSessionEvalContextOptProps`.
    #[test]
    fn session_eval_context_opt_props() {
        let ctx = MockSession::new();
        let sctx: Arc<dyn SessionContext> = Arc::clone(&ctx) as _;
        let eval_ctx = EvalContext::new(Arc::clone(&sctx));

        // OptPropCurrentUser.
        let identity = Arc::new(UserIdentity {
            username: "user1".to_owned(),
            hostname: "host1".to_owned(),
            ..UserIdentity::default()
        });
        *ctx.vars.user.lock().unwrap() = Some(Arc::clone(&identity));
        *ctx.vars.active_roles.lock().unwrap() =
            vec![role("role1", "host1"), role("role2", "host2")];
        let user = CurrentUserPropReader
            .current_user(eval_ctx.as_ref())
            .unwrap()
            .unwrap();
        assert!(same_alloc(&user, &identity));
        let roles = CurrentUserPropReader
            .active_roles(eval_ctx.as_ref())
            .unwrap();
        assert_eq!(roles.len(), 2);
        assert!(same_alloc(
            &roles[0],
            &ctx.vars.active_roles.lock().unwrap()[0]
        ));
        assert!(same_alloc(
            &roles[1],
            &ctx.vars.active_roles.lock().unwrap()[1]
        ));

        // OptPropSessionVars: the reader hands back the session's own vars.
        let got_vars = SessionVarsPropReader
            .get_session_vars(eval_ctx.as_ref())
            .unwrap();
        assert!(same_alloc(&got_vars, &ctx.vars));

        // OptPropAdvisoryLock: the lock context is the session itself.
        let lock_provider = AdvisoryLockPropReader
            .advisory_lock_ctx(eval_ctx.as_ref())
            .unwrap();
        assert!(same_alloc(
            lock_provider.advisory_lock_context(),
            &Arc::clone(&ctx)
        ));

        // OptPropDDLOwnerInfo.
        assert!(!DdlOwnerPropReader.is_ddl_owner(eval_ctx.as_ref()).unwrap());
        ctx.set_is_ddl_owner(true);
        assert!(DdlOwnerPropReader.is_ddl_owner(eval_ctx.as_ref()).unwrap());

        // OptPropPrivilegeChecker: the checker is the eval context itself.
        let checker = PrivilegeCheckerPropReader
            .get_privilege_checker(eval_ctx.as_ref())
            .unwrap();
        assert!(same_alloc(&checker, &eval_ctx));

        // The remaining properties this package installs, which Go's test
        // leaves to the constructor's `IsFull` assertion.
        let store = KvStorePropReader.get_kv_store(eval_ctx.as_ref()).unwrap();
        assert!(same_alloc(&store, &ctx.store));
        let executor = SqlExecutorPropReader
            .get_sql_executor(eval_ctx.as_ref())
            .unwrap();
        assert!(same_alloc(&executor, &ctx.executor));
        assert_eq!(
            InfoSchemaPropReader
                .get_session_info_schema(eval_ctx.as_ref())
                .unwrap()
                .schema_meta_version(),
            1
        );
        assert_eq!(
            InfoSchemaPropReader
                .get_latest_info_schema(eval_ctx.as_ref())
                .unwrap()
                .schema_meta_version(),
            2
        );
        let sequence_err = SequenceOperatorPropReader
            .get_sequence_operator(eval_ctx.as_ref(), "db1", "seq1")
            .err()
            .expect("the mock session has no sequences");
        assert_eq!(
            sequence_err.message(),
            "[schema:1146]Table 'db1.seq1' doesn't exist"
        );
    }

    // Go `TestSessionBuildContext`.
    #[test]
    fn session_build_context() {
        let ctx = MockSession::new();
        let sctx: Arc<dyn SessionContext> = Arc::clone(&ctx) as _;
        let expr_ctx = ExprContext::new(Arc::clone(&sctx));
        let eval_ctx = expr_ctx.get_eval_ctx();
        assert!(Arc::ptr_eq(eval_ctx, expr_ctx.eval_context()));
        assert!(eval_ctx.get_optional_prop_set().is_full());
        assert!(same_alloc(eval_ctx.sctx(), &Arc::clone(&ctx)));

        // Charset and collation.
        let vars = Arc::clone(&ctx.vars);
        vars.set_system_var("character_set_connection", "gbk")
            .unwrap();
        vars.set_system_var("collation_connection", "gbk_chinese_ci")
            .unwrap();
        vars.set_system_var("default_collation_for_utf8mb4", "utf8mb4_0900_ai_ci")
            .unwrap();

        let (charset, collation) = expr_ctx.get_charset_info();
        assert_eq!(charset, "gbk");
        assert_eq!(collation, "gbk_chinese_ci");
        assert_eq!(
            expr_ctx.get_default_collation_for_utf8mb4(),
            "utf8mb4_0900_ai_ci"
        );

        // SysdateIsNow.
        vars.set_system_var("tidb_sysdate_is_now", "ON").unwrap();
        assert!(expr_ctx.get_sysdate_is_now());

        // NoopFuncsMode: OFF/ON/WARN as 0/1/2.
        vars.set_system_var("tidb_enable_noop_functions", "WARN")
            .unwrap();
        assert_eq!(expr_ctx.get_noop_funcs_mode(), 2);

        // Rng.
        let rng = Arc::new(MysqlRng::new_with_seed(123));
        *vars.rng.lock().unwrap() = Arc::clone(&rng);
        assert!(same_alloc(&expr_ctx.rng(), &rng));

        // Plan cache.
        vars.plan_cache_tracker.enable_plan_cache();
        assert!(expr_ctx.is_use_cache());
        expr_ctx.set_skip_plan_cache("mockReason");
        assert!(!expr_ctx.is_use_cache());

        // Column-id allocation, shared with the session.
        let prev_id = vars.last_plan_column_id();
        assert_eq!(expr_ctx.alloc_plan_column_id(), prev_id + 1);
        assert_eq!(expr_ctx.alloc_plan_column_id(), prev_id + 2);
        vars.alloc_plan_column_id();
        assert_eq!(expr_ctx.alloc_plan_column_id(), prev_id + 4);
        assert_eq!(expr_ctx.get_last_plan_column_id(), prev_id + 4);

        // Null-reject and constant-propagate checks are always false.
        assert!(!expr_ctx.is_in_null_reject_check());
        assert!(!expr_ctx.is_constant_propagate_check());

        // ConnectionID.
        vars.connection_id.store(123, Ordering::SeqCst);
        assert_eq!(expr_ctx.connection_id(), 123);
    }

    // The two `IntoStatic` conversions, which Go pins only through
    // `exprstatic`'s own `MakeXxxContextStatic` tests.
    #[test]
    fn into_static_snapshots_the_session() {
        let ctx = MockSession::new();
        let sctx: Arc<dyn SessionContext> = Arc::clone(&ctx) as _;
        let vars = Arc::clone(&ctx.vars);
        vars.set_system_var("max_allowed_packet", "9999").unwrap();
        vars.set_system_var("block_encryption_mode", "aes-256-cbc")
            .unwrap();
        *vars.current_db.lock().unwrap() = "db2".to_owned();
        vars.connection_id.store(77, Ordering::SeqCst);

        let expr_ctx = ExprContext::new(Arc::clone(&sctx));
        assert_eq!(expr_ctx.get_block_encryption_mode(), "aes-256-cbc");

        let static_expr = expr_ctx.into_static();
        assert_eq!(static_expr.connection_id(), 77);
        assert_eq!(static_expr.get_block_encryption_mode(), "aes-256-cbc");
        assert_eq!(static_expr.get_eval_ctx().current_db(), "db2");
        assert_eq!(static_expr.get_eval_ctx().get_max_allowed_packet(), 9999);

        let static_eval = expr_ctx.get_eval_ctx().into_static();
        assert_eq!(static_eval.current_db(), "db2");
        assert_eq!(static_eval.get_max_allowed_packet(), 9999);

        // The snapshot no longer follows the session.
        *vars.current_db.lock().unwrap() = "db3".to_owned();
        assert_eq!(static_eval.current_db(), "db2");
        assert_eq!(expr_ctx.get_eval_ctx().current_db(), "db3");
    }
}
