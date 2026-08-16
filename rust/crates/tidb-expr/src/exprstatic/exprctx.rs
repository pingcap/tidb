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

//! Go `pkg/expression/exprstatic/exprctx.go`: the static expression context.
//!
//! See the module header of [`super`] for the package's boundaries.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use tidb_util::context::PlanCacheTracker;
use tidb_util::mathutil::MysqlRng;
use tidb_vardef::defaults::{
    DEF_BLOCK_ENCRYPTION_MODE, DEF_GROUP_CONCAT_MAX_LEN, DEF_SYSDATE_IS_NOW,
    DEF_TIDB_ENABLE_NOOP_FUNCS,
};
use tidb_vardef::tidb_vars::{TIDB_ENABLE_NOOP_FUNCS, TIDB_SYSDATE_IS_NOW};

use super::evalctx::{
    make_eval_context_static, new_session_vars_with_system_variables, tidb_opt_on_off_warn,
    EvalContext, EvalCtxError, SessionVarsSnapshot, StaticConvertibleEvalContext,
    BLOCK_ENCRYPTION_MODE, CHARACTER_SET_CONNECTION, COLLATION_CONNECTION,
    DEFAULT_COLLATION_FOR_UTF8MB4, GROUP_CONCAT_MAX_LEN, OFF_INT, ON_INT, WARN_INT,
    WINDOWING_USE_HIGH_PRECISION,
};
use crate::exprctx::{PlanColumnIdAllocator, SimplePlanColumnIdAllocator};

/// Go `exprCtxState`: the internal state of an [`ExprContext`], kept separate
/// so that an [`ExprCtxOption`] can only run inside a constructor.
#[derive(Clone)]
struct ExprCtxState {
    /// Always `Some` after a constructor returns; `Option` exists only so the
    /// option closures can run before Go's nil fallbacks fill it in.
    eval_ctx: Option<Arc<EvalContext>>,
    charset: String,
    collation: String,
    default_collation_for_utf8mb4: String,
    block_encryption_mode: String,
    sysdate_is_now: bool,
    noop_funcs_mode: i64,
    rng: Arc<MysqlRng>,
    /// Always `Some` after a constructor returns; see `eval_ctx`.
    plan_cache_tracker: Option<Arc<PlanCacheTracker>>,
    column_id_allocator: Arc<dyn PlanColumnIdAllocator + Send + Sync>,
    connection_id: u64,
    windowing_use_high_precision: bool,
    group_concat_max_len: u64,
}

/// Go `ExprCtxOption`: one option of an [`ExprContext`].
pub struct ExprCtxOption(Box<dyn FnOnce(&mut ExprCtxState)>);

impl ExprCtxOption {
    fn new(f: impl FnOnce(&mut ExprCtxState) + 'static) -> Self {
        ExprCtxOption(Box::new(f))
    }
}

/// Go `WithEvalCtx`. Go's `intest.AssertNotNil(ctx)` is dropped: `Arc` is
/// non-nullable.
#[must_use]
pub fn with_eval_ctx(ctx: Arc<EvalContext>) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.eval_ctx = Some(ctx))
}

/// Go `WithCharset`: sets the charset *and* its collation.
#[must_use]
pub fn with_charset(charset: impl Into<String>, collation: impl Into<String>) -> ExprCtxOption {
    let (charset, collation) = (charset.into(), collation.into());
    ExprCtxOption::new(move |state| {
        state.charset = charset;
        state.collation = collation;
    })
}

/// Go `WithDefaultCollationForUTF8MB4`.
#[must_use]
pub fn with_default_collation_for_utf8mb4(collation: impl Into<String>) -> ExprCtxOption {
    let collation = collation.into();
    ExprCtxOption::new(move |state| state.default_collation_for_utf8mb4 = collation)
}

/// Go `WithBlockEncryptionMode`.
#[must_use]
pub fn with_block_encryption_mode(mode: impl Into<String>) -> ExprCtxOption {
    let mode = mode.into();
    ExprCtxOption::new(move |state| state.block_encryption_mode = mode)
}

/// Go `WithSysDateIsNow`.
#[must_use]
pub fn with_sysdate_is_now(now: bool) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.sysdate_is_now = now)
}

/// Go `WithNoopFuncsMode`. Go's `intest.Assert` that the mode is one of
/// `OnInt`/`OffInt`/`WarnInt` is kept as a `debug_assert`.
#[must_use]
pub fn with_noop_funcs_mode(mode: i64) -> ExprCtxOption {
    debug_assert!(
        mode == ON_INT || mode == OFF_INT || mode == WARN_INT,
        "noop funcs mode must be one of OnInt/OffInt/WarnInt"
    );
    ExprCtxOption::new(move |state| state.noop_funcs_mode = mode)
}

/// Go `WithRng`.
#[must_use]
pub fn with_rng(rng: Arc<MysqlRng>) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.rng = rng)
}

/// Go `WithPlanCacheTracker`.
#[must_use]
pub fn with_plan_cache_tracker(tracker: Arc<PlanCacheTracker>) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.plan_cache_tracker = Some(tracker))
}

/// Go `WithColumnIDAllocator`.
#[must_use]
pub fn with_column_id_allocator(
    allocator: Arc<dyn PlanColumnIdAllocator + Send + Sync>,
) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.column_id_allocator = allocator)
}

/// Go `WithConnectionID`.
#[must_use]
pub fn with_connection_id(id: u64) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.connection_id = id)
}

/// Go `WithWindowingUseHighPrecision`.
#[must_use]
pub fn with_windowing_use_high_precision(use_high_precision: bool) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.windowing_use_high_precision = use_high_precision)
}

/// Go `WithGroupConcatMaxLen`.
#[must_use]
pub fn with_group_concat_max_len(max_len: u64) -> ExprCtxOption {
    ExprCtxOption::new(move |state| state.group_concat_max_len = max_len)
}

/// Go `ExprContext`: a static expression-building context, whose state does
/// not rely on the session.
pub struct ExprContext {
    state: ExprCtxState,
}

impl fmt::Debug for ExprContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExprContext")
            .field("charset", &self.state.charset)
            .field("collation", &self.state.collation)
            .field("connection_id", &self.state.connection_id)
            .finish_non_exhaustive()
    }
}

impl Default for ExprContext {
    fn default() -> Self {
        ExprContext::new([])
    }
}

impl ExprContext {
    /// Go `NewExprContext`.
    #[must_use]
    pub fn new(opts: impl IntoIterator<Item = ExprCtxOption>) -> ExprContext {
        let charset_info = tidb_datatype::get_charset_info(tidb_mysql::charset::DefaultCharset)
            .expect("mysql.DefaultCharset is always registered");

        let mut state = ExprCtxState {
            eval_ctx: None,
            charset: charset_info.name,
            collation: charset_info.default_collation,
            default_collation_for_utf8mb4: tidb_mysql::charset::DefaultCollationName.to_owned(),
            block_encryption_mode: DEF_BLOCK_ENCRYPTION_MODE.to_owned(),
            sysdate_is_now: DEF_SYSDATE_IS_NOW,
            noop_funcs_mode: tidb_opt_on_off_warn(DEF_TIDB_ENABLE_NOOP_FUNCS),
            // Go leaves `rng` nil and falls back to `mathutil.NewWithTime()`
            // after the options ran; seeding it up front and letting an option
            // overwrite it is observably identical, and the same holds for the
            // column-ID allocator below.
            rng: Arc::new(MysqlRng::new_with_time()),
            plan_cache_tracker: None,
            column_id_allocator: Arc::new(SimplePlanColumnIdAllocator::new(0)),
            connection_id: 0,
            windowing_use_high_precision: true,
            group_concat_max_len: DEF_GROUP_CONCAT_MAX_LEN,
        };

        for opt in opts {
            (opt.0)(&mut state);
        }

        let eval_ctx = state
            .eval_ctx
            .get_or_insert_with(|| Arc::new(EvalContext::new([])));

        if state.plan_cache_tracker.is_none() {
            // Go `contextutil.NewPlanCacheTracker(ctx.evalCtx)` followed by
            // `EnablePlanCache()`; the eval context's warning handler is the
            // sink it appends through.
            let tracker = PlanCacheTracker::new(Arc::new(EvalCtxWarnSink(Arc::clone(eval_ctx))));
            tracker.enable_plan_cache();
            state.plan_cache_tracker = Some(Arc::new(tracker));
        }

        ExprContext { state }
    }

    /// Go `Apply`: a new context with the options applied on top of this one.
    #[must_use]
    pub fn apply(&self, opts: impl IntoIterator<Item = ExprCtxOption>) -> ExprContext {
        let mut state = self.state.clone();
        for opt in opts {
            (opt.0)(&mut state);
        }
        ExprContext { state }
    }

    /// Go `GetEvalCtx` / `GetStaticEvalCtx`, which return the same value under
    /// two static types Rust does not need to distinguish.
    #[must_use]
    pub fn get_eval_ctx(&self) -> &Arc<EvalContext> {
        self.state
            .eval_ctx
            .as_ref()
            .expect("a constructor always fills the evaluation context")
    }

    /// Go `GetCharsetInfo`.
    #[must_use]
    pub fn get_charset_info(&self) -> (&str, &str) {
        (&self.state.charset, &self.state.collation)
    }

    /// Go `GetDefaultCollationForUTF8MB4`.
    #[must_use]
    pub fn get_default_collation_for_utf8mb4(&self) -> &str {
        &self.state.default_collation_for_utf8mb4
    }

    /// Go `GetBlockEncryptionMode`.
    #[must_use]
    pub fn get_block_encryption_mode(&self) -> &str {
        &self.state.block_encryption_mode
    }

    /// Go `GetSysdateIsNow`.
    #[must_use]
    pub fn get_sysdate_is_now(&self) -> bool {
        self.state.sysdate_is_now
    }

    /// Go `GetNoopFuncsMode`.
    #[must_use]
    pub fn get_noop_funcs_mode(&self) -> i64 {
        self.state.noop_funcs_mode
    }

    /// Go `Rng`.
    #[must_use]
    pub fn rng(&self) -> &Arc<MysqlRng> {
        &self.state.rng
    }

    /// Go `IsUseCache`.
    #[must_use]
    pub fn is_use_cache(&self) -> bool {
        self.get_plan_cache_tracker().use_cache()
    }

    /// Go `SetSkipPlanCache`.
    pub fn set_skip_plan_cache(&self, reason: &str) {
        self.get_plan_cache_tracker().set_skip_plan_cache(reason);
    }

    /// Go `AllocPlanColumnID`.
    #[must_use]
    pub fn alloc_plan_column_id(&self) -> i64 {
        self.state.column_id_allocator.alloc_plan_column_id()
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

    /// Go `ConnectionID`.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        self.state.connection_id
    }

    /// Go `GetWindowingUseHighPrecision`.
    #[must_use]
    pub fn get_windowing_use_high_precision(&self) -> bool {
        self.state.windowing_use_high_precision
    }

    /// Go `GetGroupConcatMaxLen`.
    #[must_use]
    pub fn get_group_concat_max_len(&self) -> u64 {
        self.state.group_concat_max_len
    }

    /// Go `GetLastPlanColumnID`, implementing `StaticConvertibleExprContext`.
    #[must_use]
    pub fn get_last_plan_column_id(&self) -> i64 {
        self.state.column_id_allocator.last_plan_column_id()
    }

    /// Go `GetPlanCacheTracker`, implementing `StaticConvertibleExprContext`.
    #[must_use]
    pub fn get_plan_cache_tracker(&self) -> &Arc<PlanCacheTracker> {
        self.state
            .plan_cache_tracker
            .as_ref()
            .expect("a constructor always fills the plan cache tracker")
    }

    /// Go `IsReadonlyUserVar`, which always returns false "for simplicity,
    /// ensuring the safest behavior across all scenarios".
    #[must_use]
    pub fn is_readonly_user_var(&self, _name: &str) -> bool {
        false
    }

    /// Go `LoadSystemVars`.
    pub fn load_system_vars(
        &self,
        sys_vars: &HashMap<String, String>,
    ) -> Result<ExprContext, EvalCtxError> {
        let session_vars = new_session_vars_with_system_variables(sys_vars)?;
        Ok(self.load_session_vars_internal(&session_vars, sys_vars))
    }

    fn load_session_vars_internal(
        &self,
        session_vars: &dyn SessionVarsSnapshot,
        sys_vars: &HashMap<String, String>,
    ) -> ExprContext {
        let mut opts: Vec<ExprCtxOption> = Vec::with_capacity(8);
        opts.push(with_eval_ctx(Arc::new(
            self.get_eval_ctx()
                .load_session_vars_internal(session_vars, sys_vars),
        )));
        for name in sys_vars.keys() {
            match name.to_lowercase().as_str() {
                CHARACTER_SET_CONNECTION | COLLATION_CONNECTION => {
                    let (charset, collation) = session_vars.charset_info();
                    opts.push(with_charset(charset, collation));
                }
                DEFAULT_COLLATION_FOR_UTF8MB4 => opts.push(with_default_collation_for_utf8mb4(
                    session_vars.default_collation_for_utf8mb4(),
                )),
                BLOCK_ENCRYPTION_MODE => {
                    // Go asserts the variable is present and skips the option
                    // when it is not; a missing value is the same skip here.
                    if let Some(mode) = session_vars.get_system_var(BLOCK_ENCRYPTION_MODE) {
                        opts.push(with_block_encryption_mode(mode));
                    }
                }
                TIDB_SYSDATE_IS_NOW => {
                    opts.push(with_sysdate_is_now(session_vars.sysdate_is_now()))
                }
                TIDB_ENABLE_NOOP_FUNCS => {
                    opts.push(with_noop_funcs_mode(session_vars.noop_funcs_mode()));
                }
                WINDOWING_USE_HIGH_PRECISION => opts.push(with_windowing_use_high_precision(
                    session_vars.windowing_use_high_precision(),
                )),
                GROUP_CONCAT_MAX_LEN => opts.push(with_group_concat_max_len(
                    session_vars.group_concat_max_len(),
                )),
                _ => {}
            }
        }
        self.apply(opts)
    }
}

/// The warning sink Go reaches by passing the `EvalContext` itself to
/// `contextutil.NewPlanCacheTracker`.
struct EvalCtxWarnSink(Arc<EvalContext>);

impl tidb_util::context::WarnAppender for EvalCtxWarnSink {
    fn append_warning(&self, err: tidb_util::context::WarnErr) {
        self.0.append_warning(err);
    }

    fn append_note(&self, err: tidb_util::context::WarnErr) {
        self.0.append_note(err);
    }
}

/// boundary: Go `exprctx.StaticConvertibleExprContext`, narrowed to the
/// methods [`make_expr_context_static`] calls. Go's version embeds the
/// `exprctx.ExprContext` umbrella interface, which [`crate::exprctx`] does not
/// carry yet.
pub trait StaticConvertibleExprContext {
    /// Go `GetStaticConvertibleEvalContext`.
    fn get_static_convertible_eval_context(&self) -> &dyn StaticConvertibleEvalContext;
    /// Go `ExprContext.GetCharsetInfo`.
    fn get_charset_info(&self) -> (String, String);
    /// Go `ExprContext.GetDefaultCollationForUTF8MB4`.
    fn get_default_collation_for_utf8mb4(&self) -> String;
    /// Go `ExprContext.GetBlockEncryptionMode`.
    fn get_block_encryption_mode(&self) -> String;
    /// Go `ExprContext.GetSysdateIsNow`.
    fn get_sysdate_is_now(&self) -> bool;
    /// Go `ExprContext.GetNoopFuncsMode`.
    fn get_noop_funcs_mode(&self) -> i64;
    /// Go `ExprContext.Rng`.
    fn rng(&self) -> Arc<MysqlRng>;
    /// Go `StaticConvertibleExprContext.GetPlanCacheTracker`.
    fn get_plan_cache_tracker(&self) -> Arc<PlanCacheTracker>;
    /// Go `StaticConvertibleExprContext.GetLastPlanColumnID`.
    fn get_last_plan_column_id(&self) -> i64;
    /// Go `ExprContext.ConnectionID`.
    fn connection_id(&self) -> u64;
    /// Go `ExprContext.GetWindowingUseHighPrecision`.
    fn get_windowing_use_high_precision(&self) -> bool;
    /// Go `ExprContext.GetGroupConcatMaxLen`.
    fn get_group_concat_max_len(&self) -> u64;
}

impl StaticConvertibleExprContext for ExprContext {
    fn get_static_convertible_eval_context(&self) -> &dyn StaticConvertibleEvalContext {
        self.get_eval_ctx().as_ref()
    }

    fn get_charset_info(&self) -> (String, String) {
        let (charset, collation) = ExprContext::get_charset_info(self);
        (charset.to_owned(), collation.to_owned())
    }

    fn get_default_collation_for_utf8mb4(&self) -> String {
        ExprContext::get_default_collation_for_utf8mb4(self).to_owned()
    }

    fn get_block_encryption_mode(&self) -> String {
        ExprContext::get_block_encryption_mode(self).to_owned()
    }

    fn get_sysdate_is_now(&self) -> bool {
        ExprContext::get_sysdate_is_now(self)
    }

    fn get_noop_funcs_mode(&self) -> i64 {
        ExprContext::get_noop_funcs_mode(self)
    }

    fn rng(&self) -> Arc<MysqlRng> {
        Arc::clone(ExprContext::rng(self))
    }

    fn get_plan_cache_tracker(&self) -> Arc<PlanCacheTracker> {
        Arc::clone(ExprContext::get_plan_cache_tracker(self))
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

/// Go `MakeExprContextStatic`.
#[must_use]
pub fn make_expr_context_static(ctx: &dyn StaticConvertibleExprContext) -> ExprContext {
    let static_eval_context = make_eval_context_static(ctx.get_static_convertible_eval_context());
    let (charset, collation) = ctx.get_charset_info();

    ExprContext::new([
        with_eval_ctx(Arc::new(static_eval_context)),
        with_charset(charset, collation),
        with_default_collation_for_utf8mb4(ctx.get_default_collation_for_utf8mb4()),
        with_block_encryption_mode(ctx.get_block_encryption_mode()),
        with_sysdate_is_now(ctx.get_sysdate_is_now()),
        with_noop_funcs_mode(ctx.get_noop_funcs_mode()),
        with_rng(ctx.rng()),
        with_plan_cache_tracker(ctx.get_plan_cache_tracker()),
        with_column_id_allocator(Arc::new(SimplePlanColumnIdAllocator::new(
            ctx.get_last_plan_column_id(),
        ))),
        with_connection_id(ctx.connection_id()),
        with_windowing_use_high_precision(ctx.get_windowing_use_high_precision()),
        with_group_concat_max_len(ctx.get_group_concat_max_len()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exprstatic::evalctx::with_location;
    use tidb_util::context::{gen_context_id, StaticWarnHandler};
    use tidb_util::timeutil::TimeZone;

    // Go `checkDefaultStaticExprCtx`.
    fn check_default_static_expr_ctx(ctx: &ExprContext) {
        let (charset_name, collation) = ctx.get_charset_info();
        assert_eq!(charset_name, tidb_mysql::charset::DefaultCharset);
        let info = tidb_datatype::get_charset_info(charset_name).unwrap();
        assert_eq!(charset_name, info.name);
        assert_eq!(collation, info.default_collation);
        assert_eq!(
            ctx.get_default_collation_for_utf8mb4(),
            tidb_mysql::charset::DefaultCollationName
        );
        assert_eq!(ctx.get_block_encryption_mode(), DEF_BLOCK_ENCRYPTION_MODE);
        assert_eq!(ctx.get_sysdate_is_now(), DEF_SYSDATE_IS_NOW);
        assert_eq!(
            ctx.get_noop_funcs_mode(),
            tidb_opt_on_off_warn(DEF_TIDB_ENABLE_NOOP_FUNCS)
        );
        assert!(ctx.is_use_cache());
        assert_eq!(ctx.connection_id(), 0);
        assert!(ctx.get_windowing_use_high_precision());
        assert_eq!(ctx.get_group_concat_max_len(), DEF_GROUP_CONCAT_MAX_LEN);
        assert!(!ctx.is_in_null_reject_check());
        assert!(!ctx.is_constant_propagate_check());
        assert!(!ctx.is_readonly_user_var("a"));
    }

    // Go `exprCtxOptionsTestState`.
    struct ExprCtxOptionsTestState {
        eval_ctx: Arc<EvalContext>,
        col_id_alloc: Arc<dyn PlanColumnIdAllocator + Send + Sync>,
        rng: Arc<MysqlRng>,
    }

    // Go `getExprCtxOptionsForTest`.
    fn expr_ctx_options_for_test() -> (Vec<ExprCtxOption>, ExprCtxOptionsTestState) {
        let state = ExprCtxOptionsTestState {
            eval_ctx: Arc::new(EvalContext::new([with_location(TimeZone::Fixed {
                name: "UTC+11".to_owned(),
                offset_secs: 11 * 3600,
            })])),
            col_id_alloc: Arc::new(SimplePlanColumnIdAllocator::new(1024)),
            rng: Arc::new(MysqlRng::new_with_seed(12_345_678)),
        };
        let plan_cache_tracker =
            Arc::new(PlanCacheTracker::new(Arc::new(StaticWarnHandler::new(0))));

        let opts = vec![
            with_eval_ctx(Arc::clone(&state.eval_ctx)),
            with_charset("gbk", "gbk_bin"),
            with_default_collation_for_utf8mb4("utf8mb4_0900_ai_ci"),
            with_block_encryption_mode("aes-256-cbc"),
            with_sysdate_is_now(true),
            with_noop_funcs_mode(WARN_INT),
            with_rng(Arc::clone(&state.rng)),
            with_plan_cache_tracker(plan_cache_tracker),
            with_column_id_allocator(Arc::clone(&state.col_id_alloc)),
            with_connection_id(778_899),
            with_windowing_use_high_precision(false),
            with_group_concat_max_len(2_233_445_566),
        ];
        (opts, state)
    }

    // Go `checkOptionsStaticExprCtx`.
    fn check_options_static_expr_ctx(ctx: &ExprContext, state: &ExprCtxOptionsTestState) {
        assert!(Arc::ptr_eq(ctx.get_eval_ctx(), &state.eval_ctx));
        let (charset, collation) = ctx.get_charset_info();
        assert_eq!(charset, "gbk");
        assert_eq!(collation, "gbk_bin");
        assert_eq!(
            ctx.get_default_collation_for_utf8mb4(),
            "utf8mb4_0900_ai_ci"
        );
        assert_eq!(ctx.get_block_encryption_mode(), "aes-256-cbc");
        assert!(ctx.get_sysdate_is_now());
        assert_eq!(ctx.get_noop_funcs_mode(), WARN_INT);
        assert!(Arc::ptr_eq(ctx.rng(), &state.rng));
        // A freshly built tracker starts with the plan cache disabled, which
        // is Go's `require.False(t, ctx.IsUseCache())` here.
        assert!(!ctx.is_use_cache());
        ctx.set_skip_plan_cache("reason");
        assert!(Arc::ptr_eq(
            &ctx.state.column_id_allocator,
            &state.col_id_alloc
        ));
        assert_eq!(ctx.connection_id(), 778_899);
        assert!(!ctx.get_windowing_use_high_precision());
        assert_eq!(ctx.get_group_concat_max_len(), 2_233_445_566);
    }

    // Go `TestNewStaticExprCtx`.
    #[test]
    fn new_static_expr_ctx() {
        // Go asserts the id is exactly `prev+1`; the counter is process-wide
        // and Rust runs tests in parallel threads, so the assertion here is
        // that the new context took a LATER id.
        let prev_id = gen_context_id();
        let ctx = ExprContext::new([]);
        assert!(ctx.get_eval_ctx().ctx_id() > prev_id);
        check_default_static_expr_ctx(&ctx);

        let (opts, state) = expr_ctx_options_for_test();
        let ctx = ExprContext::new(opts);
        check_options_static_expr_ctx(&ctx, &state);
    }

    // Go `TestStaticExprCtxApplyOptions`.
    #[test]
    fn static_expr_ctx_apply_options() {
        let ctx = ExprContext::new([]);
        let old_eval_ctx = Arc::clone(ctx.get_eval_ctx());
        let old_allocator = Arc::clone(&ctx.state.column_id_allocator);

        let (opts, state) = expr_ctx_options_for_test();
        let ctx2 = ctx.apply(opts);
        assert!(Arc::ptr_eq(ctx.get_eval_ctx(), &old_eval_ctx));
        assert!(Arc::ptr_eq(&ctx.state.column_id_allocator, &old_allocator));
        check_default_static_expr_ctx(&ctx);
        check_options_static_expr_ctx(&ctx2, &state);

        // Apply with empty options keeps everything, including the tracker
        // that `checkOptionsStaticExprCtx` has already told to skip the cache.
        let ctx3 = ctx2.apply([]);
        assert!(Arc::ptr_eq(ctx3.get_eval_ctx(), &state.eval_ctx));
        assert!(!ctx3.is_use_cache());
        assert_eq!(ctx3.get_group_concat_max_len(), 2_233_445_566);
    }

    // Go `TestExprCtxColumnIDAllocator`.
    #[test]
    fn expr_ctx_column_id_allocator() {
        let ctx = ExprContext::new([]);
        assert_eq!(ctx.alloc_plan_column_id(), 1);

        // Apply without an allocator shares the old one.
        let ctx2 = ctx.apply([]);
        assert!(Arc::ptr_eq(
            &ctx2.state.column_id_allocator,
            &ctx.state.column_id_allocator
        ));
        assert_eq!(ctx2.alloc_plan_column_id(), 2);
        assert_eq!(ctx.alloc_plan_column_id(), 3);

        // Apply with a new allocator.
        let alloc: Arc<dyn PlanColumnIdAllocator + Send + Sync> =
            Arc::new(SimplePlanColumnIdAllocator::new(1024));
        let ctx3 = ctx.apply([with_column_id_allocator(Arc::clone(&alloc))]);
        assert!(Arc::ptr_eq(&ctx3.state.column_id_allocator, &alloc));
        assert!(!Arc::ptr_eq(
            &ctx.state.column_id_allocator,
            &ctx3.state.column_id_allocator
        ));
        assert_eq!(ctx3.alloc_plan_column_id(), 1025);
        assert_eq!(ctx.alloc_plan_column_id(), 4);

        // A new context with an allocator.
        let alloc: Arc<dyn PlanColumnIdAllocator + Send + Sync> =
            Arc::new(SimplePlanColumnIdAllocator::new(2048));
        let ctx4 = ExprContext::new([with_column_id_allocator(Arc::clone(&alloc))]);
        assert!(Arc::ptr_eq(&ctx4.state.column_id_allocator, &alloc));
        assert_eq!(ctx4.alloc_plan_column_id(), 2049);
    }

    // Go `TestMakeExprContextStatic`. Go drives the comparison with
    // `deeptest.AssertDeepClonedEqual`; the fields that walk covers are
    // compared field by field here.
    #[test]
    fn make_expr_context_static_copies_every_field() {
        let eval_ctx = Arc::new(EvalContext::new([]));
        let plan_cache_tracker =
            Arc::new(PlanCacheTracker::new(Arc::new(StaticWarnHandler::new(0))));
        let obj = ExprContext::new([
            with_eval_ctx(Arc::clone(&eval_ctx)),
            with_charset("a", "b"),
            with_default_collation_for_utf8mb4("c"),
            with_block_encryption_mode("d"),
            with_sysdate_is_now(true),
            with_noop_funcs_mode(1),
            with_rng(Arc::new(MysqlRng::new_with_seed(12_345_678))),
            with_plan_cache_tracker(Arc::clone(&plan_cache_tracker)),
            with_column_id_allocator(Arc::new(SimplePlanColumnIdAllocator::new(1))),
            with_connection_id(1),
            with_windowing_use_high_precision(false),
            with_group_concat_max_len(1),
        ]);

        // Go first proves every field differs from a default context.
        let default_ctx = ExprContext::new([]);
        assert_ne!(obj.get_charset_info(), default_ctx.get_charset_info());
        assert_ne!(
            obj.get_default_collation_for_utf8mb4(),
            default_ctx.get_default_collation_for_utf8mb4()
        );
        assert_ne!(
            obj.get_block_encryption_mode(),
            default_ctx.get_block_encryption_mode()
        );
        assert_ne!(obj.get_sysdate_is_now(), default_ctx.get_sysdate_is_now());
        assert_ne!(obj.get_noop_funcs_mode(), default_ctx.get_noop_funcs_mode());
        assert_ne!(obj.connection_id(), default_ctx.connection_id());
        assert_ne!(
            obj.get_windowing_use_high_precision(),
            default_ctx.get_windowing_use_high_precision()
        );
        assert_ne!(
            obj.get_group_concat_max_len(),
            default_ctx.get_group_concat_max_len()
        );
        assert_ne!(
            obj.get_last_plan_column_id(),
            default_ctx.get_last_plan_column_id()
        );

        let static_obj = make_expr_context_static(&obj);
        assert_eq!(
            StaticConvertibleExprContext::get_charset_info(&static_obj),
            StaticConvertibleExprContext::get_charset_info(&obj)
        );
        assert_eq!(
            static_obj.get_default_collation_for_utf8mb4(),
            obj.get_default_collation_for_utf8mb4()
        );
        assert_eq!(
            static_obj.get_block_encryption_mode(),
            obj.get_block_encryption_mode()
        );
        assert_eq!(static_obj.get_sysdate_is_now(), obj.get_sysdate_is_now());
        assert_eq!(static_obj.get_noop_funcs_mode(), obj.get_noop_funcs_mode());
        assert_eq!(static_obj.connection_id(), obj.connection_id());
        assert_eq!(
            static_obj.get_windowing_use_high_precision(),
            obj.get_windowing_use_high_precision()
        );
        assert_eq!(
            static_obj.get_group_concat_max_len(),
            obj.get_group_concat_max_len()
        );
        assert_eq!(
            static_obj.get_last_plan_column_id(),
            obj.get_last_plan_column_id()
        );

        // The rng and the plan-cache tracker are shared, not cloned.
        assert!(Arc::ptr_eq(static_obj.rng(), obj.rng()));
        assert!(Arc::ptr_eq(
            static_obj.get_plan_cache_tracker(),
            obj.get_plan_cache_tracker()
        ));

        // The eval context is a new one.
        assert!(!Arc::ptr_eq(static_obj.get_eval_ctx(), obj.get_eval_ctx()));
    }

    // Go `TestExprCtxLoadSystemVars`.
    #[test]
    fn expr_ctx_load_system_vars() {
        let vars: Vec<(&str, &str)> = vec![
            ("character_set_connection", "gbk"),
            ("collation_connection", "gbk_chinese_ci"),
            ("default_collation_for_utf8mb4", "utf8mb4_general_ci"),
            // Upper case on purpose: the name is folded.
            ("TIDB_SYSDATE_IS_NOW", "1"),
            ("tidb_enable_noop_functions", "warn"),
            ("block_encryption_mode", "aes-256-cbc"),
            ("group_concat_max_len", "123456"),
            ("windowing_use_high_precision", "0"),
        ];

        let mut vars_map = HashMap::new();
        for (name, val) in &vars {
            vars_map.insert((*name).to_owned(), (*val).to_owned());
        }
        let session_vars = new_session_vars_with_system_variables(&vars_map).unwrap();

        let default_ctx = ExprContext::new([]);
        let ctx = default_ctx.load_system_vars(&vars_map).unwrap();

        // Every variable-related field changed...
        assert_ne!(
            ExprContext::get_charset_info(&ctx),
            ExprContext::get_charset_info(&default_ctx)
        );
        assert_ne!(
            ctx.get_default_collation_for_utf8mb4(),
            default_ctx.get_default_collation_for_utf8mb4()
        );
        assert_ne!(
            ctx.get_block_encryption_mode(),
            default_ctx.get_block_encryption_mode()
        );
        assert_ne!(ctx.get_sysdate_is_now(), default_ctx.get_sysdate_is_now());
        assert_ne!(ctx.get_noop_funcs_mode(), default_ctx.get_noop_funcs_mode());
        assert_ne!(
            ctx.get_group_concat_max_len(),
            default_ctx.get_group_concat_max_len()
        );
        assert_ne!(
            ctx.get_windowing_use_high_precision(),
            default_ctx.get_windowing_use_high_precision()
        );

        // ...and every unrelated field is shared with the source context.
        assert!(Arc::ptr_eq(ctx.rng(), default_ctx.rng()));
        assert!(Arc::ptr_eq(
            ctx.get_plan_cache_tracker(),
            default_ctx.get_plan_cache_tracker()
        ));
        assert!(Arc::ptr_eq(
            &ctx.state.column_id_allocator,
            &default_ctx.state.column_id_allocator
        ));
        assert_eq!(ctx.connection_id(), default_ctx.connection_id());

        // Each variable's own assertion, against the session snapshot.
        let (charset, collation) = ctx.get_charset_info();
        assert_eq!(charset, "gbk");
        assert_eq!(collation, "gbk_chinese_ci");
        assert_eq!(
            (charset.to_owned(), collation.to_owned()),
            session_vars.charset_info()
        );

        assert_eq!(
            ctx.get_default_collation_for_utf8mb4(),
            "utf8mb4_general_ci"
        );
        assert_eq!(
            ctx.get_default_collation_for_utf8mb4(),
            session_vars.default_collation_for_utf8mb4()
        );

        assert!(ctx.get_sysdate_is_now());
        assert_eq!(ctx.get_sysdate_is_now(), session_vars.sysdate_is_now());

        assert_eq!(ctx.get_noop_funcs_mode(), WARN_INT);
        assert_eq!(ctx.get_noop_funcs_mode(), session_vars.noop_funcs_mode());

        assert_eq!(ctx.get_block_encryption_mode(), "aes-256-cbc");
        assert_eq!(
            ctx.get_block_encryption_mode(),
            session_vars.get_system_var(BLOCK_ENCRYPTION_MODE).unwrap()
        );

        assert_eq!(ctx.get_group_concat_max_len(), 123_456);
        assert_eq!(
            ctx.get_group_concat_max_len(),
            session_vars.group_concat_max_len()
        );

        assert!(!ctx.get_windowing_use_high_precision());
        assert_eq!(
            ctx.get_windowing_use_high_precision(),
            session_vars.windowing_use_high_precision()
        );

        // Setting the charset also sets the collation...
        let mut only_charset = HashMap::new();
        only_charset.insert("character_set_connection".to_owned(), "ascii".to_owned());
        let ctx = default_ctx.load_system_vars(&only_charset).unwrap();
        assert_eq!(ctx.get_charset_info(), ("ascii", "ascii_bin"));

        // ...and setting the collation also sets the charset.
        let mut only_collation = HashMap::new();
        only_collation.insert("collation_connection".to_owned(), "latin1_bin".to_owned());
        let ctx = default_ctx.load_system_vars(&only_collation).unwrap();
        assert_eq!(ctx.get_charset_info(), ("latin1", "latin1_bin"));

        // `LoadSystemVars` reaches the eval context too.
        let mut eval_vars = HashMap::new();
        eval_vars.insert("div_precision_increment".to_owned(), "9".to_owned());
        eval_vars.insert("time_zone".to_owned(), "Asia/Tokyo".to_owned());
        let ctx = default_ctx.load_system_vars(&eval_vars).unwrap();
        assert_eq!(ctx.get_eval_ctx().get_div_precision_increment(), 9);
        assert_eq!(
            tidb_util::timeutil::zone_name(ctx.get_eval_ctx().location()),
            "Asia/Tokyo"
        );
    }
}
