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

//! Go `pkg/util/ranger/context` lands as a complete package: the context
//! ranger uses to build ranges.
//!
//! Both production symbols (`RangerContext` and `Detach`) are here. Field
//! types map to their already-transcreated homes — `types.Context` is
//! [`tidb_datatype::ConversionContext`], `errctx.Context` is
//! [`tidb_error::errctx::Context`], and the two embedded `contextutil`
//! pointers are `Arc`s of [`tidb_util::context::RangeFallbackHandler`] /
//! [`tidb_util::context::PlanCacheTracker`] — except one:
//!
//! - `// boundary:` Go `pkg/expression/exprctx.BuildContext` — modeled as
//!   the local marker trait [`ExprBuildContext`]; this package only stores
//!   and swaps the value, never calls it, and the crate-local `exprctx`
//!   module is still a seed without the umbrella interface.
//!
//! Go embeds the two tracker pointers so their methods promote onto
//! `RangerContext`; Rust exposes them as public `Arc` fields instead, so
//! callers reach the same methods through one explicit field hop.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_datatype::ConversionContext;
use tidb_error::errctx;
use tidb_util::context::{PlanCacheTracker, RangeFallbackHandler};

/// boundary: Go `pkg/expression/exprctx.BuildContext` — ranger's context
/// stores the interface value and swaps it on detach without calling it, so
/// a marker trait is the whole consumed surface.
pub trait ExprBuildContext {}

/// Go `RangerContext`: the context used to build range.
#[derive(Clone)]
pub struct RangerContext<'a> {
    /// Go `TypeCtx types.Context`.
    pub type_ctx: ConversionContext<'a>,
    /// Go `ErrCtx errctx.Context`.
    pub err_ctx: errctx::Context,
    /// Go `ExprCtx exprctx.BuildContext`.
    pub expr_ctx: Arc<dyn ExprBuildContext>,
    /// Go embedded `*contextutil.RangeFallbackHandler`.
    pub range_fallback_handler: Arc<RangeFallbackHandler>,
    /// Go embedded `*contextutil.PlanCacheTracker`.
    pub plan_cache_tracker: Arc<PlanCacheTracker>,
    /// Go `OptimizerFixControl map[uint64]string` (an empty map is Go's nil).
    pub optimizer_fix_control: HashMap<u64, String>,
    /// Go `UseCache bool`.
    pub use_cache: bool,
    /// Go `RegardNULLAsPoint bool`.
    pub regard_null_as_point: bool,
    /// Go `OptPrefixIndexSingleScan bool`.
    pub opt_prefix_index_single_scan: bool,
}

impl<'a> RangerContext<'a> {
    /// Go `Detach`: detaches this context from the session context.
    ///
    /// NOTE: Though the session context can be used parallelly with this
    /// context after calling it, the `StatementContext` cannot. The session
    /// context should create a new `StatementContext` before executing
    /// another statement.
    ///
    /// Go shallow-copies the struct (sharing the tracker pointers — here the
    /// `Arc`s), replaces `ExprCtx`, and `maps.Clone`s `OptimizerFixControl`;
    /// `Clone` plus the swap below does exactly that.
    #[must_use]
    pub fn detach(&self, static_expr_ctx: Arc<dyn ExprBuildContext>) -> RangerContext<'a> {
        let mut new_ctx = self.clone();
        new_ctx.expr_ctx = static_expr_ctx;
        new_ctx
    }
}

// Go `context_test.go` `TestContextDetach`. The `deeptest` reflection
// assertions (`AssertRecursivelyNotEqual` proving the fixture sets every
// non-ignored field, `AssertDeepClonedEqual` proving the detached copy is a
// deep clone) become the explicit field assertions below; the trailing
// `require.Equal` pointer comparisons become `Arc::ptr_eq`. Go's
// `exprstatic.NewExprContext()` stand-in is a local unit implementing the
// [`ExprBuildContext`] boundary.
#[cfg(test)]
mod tests {
    use super::*;
    use tidb_util::context::StaticWarnHandler;

    struct StaticExprCtx;

    impl ExprBuildContext for StaticExprCtx {}

    #[test]
    fn context_detach() {
        let warn_handler = Arc::new(StaticWarnHandler::new(5));
        let plan_cache_tracker = Arc::new(PlanCacheTracker::new(warn_handler.clone()));
        let range_fallback_handler = Arc::new(RangeFallbackHandler::new(
            plan_cache_tracker.clone(),
            warn_handler,
        ));
        let expr_ctx: Arc<dyn ExprBuildContext> = Arc::new(StaticExprCtx);
        let obj = RangerContext {
            type_ctx: ConversionContext::default_statement_no_warning(),
            err_ctx: errctx::STRICT_NO_WARNING_CONTEXT.clone(),
            expr_ctx: Arc::clone(&expr_ctx),
            range_fallback_handler: Arc::clone(&range_fallback_handler),
            plan_cache_tracker: Arc::clone(&plan_cache_tracker),
            optimizer_fix_control: HashMap::from([(1_u64, "a".to_owned())]),
            use_cache: true,
            regard_null_as_point: true,
            opt_prefix_index_single_scan: true,
        };

        // Go: AssertRecursivelyNotEqual(obj, &RangerContext{}) ignoring the
        // context/handler fields — every remaining field differs from its
        // zero value.
        assert_ne!(obj.optimizer_fix_control, HashMap::new());
        assert!(obj.use_cache);
        assert!(obj.regard_null_as_point);
        assert!(obj.opt_prefix_index_single_scan);

        let static_obj = obj.detach(Arc::clone(&obj.expr_ctx));

        // Go: AssertDeepClonedEqual on the non-ignored fields — the map is a
        // clone (equal contents, distinct storage), the bools are copies.
        assert_eq!(obj.optimizer_fix_control, static_obj.optimizer_fix_control);
        assert_eq!(obj.use_cache, static_obj.use_cache);
        assert_eq!(obj.regard_null_as_point, static_obj.regard_null_as_point);
        assert_eq!(
            obj.opt_prefix_index_single_scan,
            static_obj.opt_prefix_index_single_scan
        );

        // Go: require.Equal on the shared context/handler fields.
        assert_eq!(obj.type_ctx.flags(), static_obj.type_ctx.flags());
        assert_eq!(obj.type_ctx.location(), static_obj.type_ctx.location());
        assert_eq!(obj.err_ctx.level_map(), static_obj.err_ctx.level_map());
        assert!(Arc::ptr_eq(&obj.expr_ctx, &static_obj.expr_ctx));
        assert!(Arc::ptr_eq(
            &obj.range_fallback_handler,
            &static_obj.range_fallback_handler
        ));
        assert!(Arc::ptr_eq(
            &obj.plan_cache_tracker,
            &static_obj.plan_cache_tracker
        ));
    }
}
