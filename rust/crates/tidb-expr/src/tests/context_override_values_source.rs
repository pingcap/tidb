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

//! GO PORT of the part9 test `TestCtxWithHandleTruncateErrLevel`
//! (`pkg/expression/exprctx/context_override_test.go:28`) and
//! `TestExpressionMemeoryUsage`
//! (`pkg/expression/expression_test.go:328`, also stubbed from
//! `vectorizable_and_chunk_eval_source.rs`). The context wrapper is active;
//! memory accounting remains a documented gap beside its receipt row.

/// GO PORT of `TestCtxWithHandleTruncateErrLevel`
/// (`exprctx/context_override_test.go:28`) exercises
/// `exprctx.CtxWithHandleTruncateErrLevel` (`exprctx/context.go:203`), which
/// overrides ONLY the truncate error group of an eval context while aliasing
/// every other field, and returns the SAME context when no override is needed.
/// The Rust static context carries the same two-state override through
/// `exprstatic::ctx_with_handle_truncate_err_level`; the full umbrella
/// interface remains a higher-layer boundary.
///
/// Once the helper lands, the port must assert, per LevelWarn/LevelIgnore/
/// LevelError: flags swap TruncateAsWarning/IgnoreTruncateErr exactly as
/// `errctx.LevelMap{ErrGroupTruncate: level}` says, DividedByZero keeps its own
/// error level, location/connection id pass through unchanged, the ORIGINAL
/// eval context is untouched, and re-wrapping with the same level returns the
/// same context object.
#[test]
fn test_ctx_with_handle_truncate_err_level() {
    use std::sync::Arc;

    use crate::exprstatic::{
        ctx_with_handle_truncate_err_level, with_connection_id, with_err_level_map, with_eval_ctx,
        with_location, with_type_flags, EvalContext, ExprContext,
    };
    use tidb_datatype::DEFAULT_STATEMENT_FLAGS;
    use tidb_error::errctx::{ErrGroup, Level, LevelMap};
    use tidb_util::timeutil::TimeZone;

    for level in [Level::Warn, Level::Ignore, Level::Error] {
        let mut original_flags = DEFAULT_STATEMENT_FLAGS;
        let mut original_levels = LevelMap::strict()
            .with_level(ErrGroup::DividedByZero, Level::Error)
            .with_level(ErrGroup::Truncate, level);
        let expected_flags = match level {
            Level::Error => {
                original_flags = original_flags.with_truncate_as_warning(true);
                original_levels = original_levels.with_level(ErrGroup::Truncate, Level::Warn);
                original_flags.with_truncate_as_warning(false)
            }
            Level::Warn => original_flags.with_truncate_as_warning(true),
            Level::Ignore => original_flags.with_ignore_truncate_err(true),
        };
        let expected_levels = original_levels.with_level(ErrGroup::Truncate, level);
        let original_location = TimeZone::Fixed {
            name: "tz1".to_owned(),
            offset_secs: 7200,
        };
        let eval_ctx = Arc::new(EvalContext::new([
            with_type_flags(original_flags),
            with_location(original_location.clone()),
            with_err_level_map(original_levels),
        ]));
        let ctx = ExprContext::new([
            with_eval_ctx(Arc::clone(&eval_ctx)),
            with_connection_id(1234),
        ]);

        let new_ctx = ctx_with_handle_truncate_err_level(&ctx, level);
        assert_eq!(new_ctx.get_eval_ctx().type_flags(), expected_flags);
        assert_eq!(new_ctx.get_eval_ctx().err_level_map(), expected_levels);
        assert_eq!(new_ctx.get_eval_ctx().location(), &original_location);
        assert_eq!(new_ctx.connection_id(), 1234);

        // The original context is not mutated.
        assert!(Arc::ptr_eq(ctx.get_eval_ctx(), &eval_ctx));
        assert_eq!(ctx.get_eval_ctx().type_flags(), original_flags);
        assert_eq!(ctx.get_eval_ctx().err_level_map(), original_levels);
        assert_eq!(ctx.get_eval_ctx().location(), &original_location);

        // A second application of the same policy keeps the evaluation
        // context allocation, matching Go's no-wrapper fast path.
        let new_ctx2 = ctx_with_handle_truncate_err_level(&new_ctx, level);
        assert!(Arc::ptr_eq(new_ctx2.get_eval_ctx(), new_ctx.get_eval_ctx()));
    }
}
