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

//! Go `pkg/meta/metabuild` lands as a complete package: the options/context
//! object DDL uses while building meta like `TableInfo` and `IndexInfo`.
//!
//! Every production symbol of the Go package (`Option`, the eight `WithXxx`
//! option constructors, `Context`, `NewContext`, `NewNonStrictContext`, and
//! all `Context` accessors) is here. The package's imports from neighbors
//! that this crate cannot reach are narrowed to local boundary shapes:
//!
//! - `// boundary:` Go `pkg/expression/exprctx.ExprContext` /
//!   `exprctx.EvalContext` — modeled as the local [`ExprContext`] /
//!   [`EvalContext`] traits carrying exactly the methods metabuild consumes
//!   (`GetEvalCtx`, `GetDefaultCollationForUTF8MB4`, `GetCharsetInfo`,
//!   `SQLMode`, `AppendWarning`, `AppendNote`). The crate-local
//!   `exprctx` module is still a seed without these interfaces.
//! - `// boundary:` Go `pkg/expression/exprstatic.ExprContext` — modeled as
//!   [`StaticExprContext`], a snapshot providing only the defaults metabuild
//!   observes (default charset/collation, default SQL mode, the
//!   `WithSQLMode` override used by `NewNonStrictContext`, and a
//!   `StaticWarnHandler`-backed warning sink).
//! - `// boundary:` Go `pkg/infoschema/context.MetaOnlyInfoSchema` — modeled
//!   as the local [`MetaOnlyInfoSchema`] trait; metabuild only stores and
//!   returns the value, so a single identifying method suffices.
//!
//! Go's `intest.AssertNotNil(exprCtx)` in `WithExprCtx` is dropped: `Arc` is
//! non-nullable, so the assertion holds by construction.
//!
//! Go's option-function pattern (`funcCtxOption` wrapping `func(*Context)`)
//! becomes [`ContextOption`], a struct holding a boxed `FnOnce(&mut
//! Context)`; observable behavior — later options overwrite earlier ones,
//! and the expression-context default is applied only after all options ran
//! — is identical.

use std::sync::Arc;

use tidb_mysql::consts::SqlMode;
use tidb_util::context::WarnErr;
use tidb_vardef::defaults::{
    DEF_PRE_SPLIT_REGIONS, DEF_SHARD_ROW_ID_BITS, DEF_TIDB_ENABLE_AUTO_INCREMENT_IN_GENERATED,
    DEF_TIDB_ENABLE_CLUSTERED_INDEX,
};
use tidb_vardef::modes::ClusteredIndexDefMode;

/// The slice of Go `exprctx.EvalContext` that metabuild consumes.
///
/// boundary: Go `pkg/expression/exprctx.EvalContext` (`SQLMode`,
/// `AppendWarning`, `AppendNote`).
pub trait EvalContext {
    /// Go `EvalContext.SQLMode`: the SQL mode the context evaluates under.
    fn sql_mode(&self) -> SqlMode;
    /// Go `EvalContext.AppendWarning`: appends a warning.
    fn append_warning(&self, err: WarnErr);
    /// Go `EvalContext.AppendNote`: appends a note-level warning.
    fn append_note(&self, note: WarnErr);
}

/// The slice of Go `exprctx.ExprContext` that metabuild consumes.
///
/// boundary: Go `pkg/expression/exprctx.ExprContext` (`GetEvalCtx`,
/// `GetDefaultCollationForUTF8MB4`) plus the embedded
/// `exprctx.BuildContext.GetCharsetInfo` its tests observe.
pub trait ExprContext {
    /// Go `ExprContext.GetEvalCtx`: the evaluation context.
    fn get_eval_ctx(&self) -> &dyn EvalContext;
    /// Go `ExprContext.GetDefaultCollationForUTF8MB4`.
    fn get_default_collation_for_utf8mb4(&self) -> &str;
    /// Go `exprctx.BuildContext.GetCharsetInfo`: `(charset, collation)`.
    fn get_charset_info(&self) -> (&str, &str);
}

/// The slice of Go `infoschemactx.MetaOnlyInfoSchema` that metabuild stores.
///
/// boundary: Go `pkg/infoschema/context.MetaOnlyInfoSchema` — metabuild only
/// keeps the value for later constraint checks, so one identifying method
/// stands in for the full meta-only interface.
pub trait MetaOnlyInfoSchema {
    /// Go `MetaOnlyInfoSchema.SchemaMetaVersion`.
    fn schema_meta_version(&self) -> i64;
}

/// boundary: Go `pkg/expression/exprstatic.ExprContext` — the static
/// expression context metabuild falls back to, narrowed to the defaults it
/// observes.
///
/// Defaults mirror `exprstatic.NewExprContext()`: the default
/// charset/collation pair, `mysql.GetSQLMode(mysql.DefaultSQLMode)` as the
/// SQL mode, and `mysql.DefaultCollationName` for utf8mb4. Warnings and
/// notes land in a [`tidb_util::context::StaticWarnHandler`].
pub struct StaticExprContext {
    sql_mode: SqlMode,
    charset: &'static str,
    collation: &'static str,
    default_collation_for_utf8mb4: &'static str,
    warn_handler: tidb_util::context::StaticWarnHandler,
}

impl StaticExprContext {
    /// Go `exprstatic.NewExprContext()` with default options.
    #[must_use]
    pub fn new() -> Self {
        let (charset, collation) = tidb_datatype::get_default_charset_and_collate();
        StaticExprContext {
            sql_mode: tidb_mysql::consts::get_sql_mode(tidb_mysql::consts::DefaultSQLMode)
                .expect("mysql.DefaultSQLMode always parses"),
            charset,
            collation,
            default_collation_for_utf8mb4: tidb_mysql::charset::DefaultCollationName,
            warn_handler: tidb_util::context::StaticWarnHandler::new(0),
        }
    }

    /// Go `exprstatic.NewExprContext(exprstatic.WithEvalCtx(
    /// exprstatic.NewEvalContext(exprstatic.WithSQLMode(mode))))`.
    #[must_use]
    pub fn with_sql_mode(mode: SqlMode) -> Self {
        StaticExprContext {
            sql_mode: mode,
            ..StaticExprContext::new()
        }
    }

    /// The warning store backing [`EvalContext::append_warning`] /
    /// [`EvalContext::append_note`].
    #[must_use]
    pub fn warn_handler(&self) -> &tidb_util::context::StaticWarnHandler {
        &self.warn_handler
    }
}

impl Default for StaticExprContext {
    fn default() -> Self {
        StaticExprContext::new()
    }
}

impl EvalContext for StaticExprContext {
    fn sql_mode(&self) -> SqlMode {
        self.sql_mode
    }

    fn append_warning(&self, err: WarnErr) {
        use tidb_util::context::WarnAppender as _;
        self.warn_handler.append_warning(err);
    }

    fn append_note(&self, note: WarnErr) {
        use tidb_util::context::WarnAppender as _;
        self.warn_handler.append_note(note);
    }
}

impl ExprContext for StaticExprContext {
    fn get_eval_ctx(&self) -> &dyn EvalContext {
        self
    }

    fn get_default_collation_for_utf8mb4(&self) -> &str {
        self.default_collation_for_utf8mb4
    }

    fn get_charset_info(&self) -> (&str, &str) {
        (self.charset, self.collation)
    }
}

/// Go `Option`: sets one [`Context`] option.
///
/// Go's `funcCtxOption`/`funcOpt` plumbing collapses into the boxed closure
/// this struct owns; `applyCtx` is [`ContextOption::apply_ctx`].
pub struct ContextOption(Box<dyn FnOnce(&mut Context)>);

impl ContextOption {
    fn new(f: impl FnOnce(&mut Context) + 'static) -> Self {
        ContextOption(Box::new(f))
    }

    fn apply_ctx(self, ctx: &mut Context) {
        (self.0)(ctx);
    }
}

/// Go `WithExprCtx`: sets the expression context.
///
/// Go's `intest.AssertNotNil(exprCtx)` is dropped: `Arc` cannot be null.
#[must_use]
pub fn with_expr_ctx(expr_ctx: Arc<dyn ExprContext>) -> ContextOption {
    ContextOption::new(move |ctx| ctx.expr_ctx = Some(expr_ctx))
}

/// Go `WithEnableAutoIncrementInGenerated`: sets whether auto increment is
/// enabled in a generated column.
#[must_use]
pub fn with_enable_auto_increment_in_generated(enable: bool) -> ContextOption {
    ContextOption::new(move |ctx| ctx.enable_auto_increment_in_generated = enable)
}

/// Go `WithPrimaryKeyRequired`: sets whether a primary key is required.
#[must_use]
pub fn with_primary_key_required(required: bool) -> ContextOption {
    ContextOption::new(move |ctx| ctx.primary_key_required = required)
}

/// Go `WithClusteredIndexDefMode`: sets the clustered index mode.
#[must_use]
pub fn with_clustered_index_def_mode(mode: ClusteredIndexDefMode) -> ContextOption {
    ContextOption::new(move |ctx| ctx.clustered_index_def_mode = mode)
}

/// Go `WithShardRowIDBits`: sets the shard row id bits.
#[must_use]
pub fn with_shard_row_id_bits(bits: u64) -> ContextOption {
    ContextOption::new(move |ctx| ctx.shard_row_id_bits = bits)
}

/// Go `WithPreSplitRegions`: sets the pre-split regions.
#[must_use]
pub fn with_pre_split_regions(regions: u64) -> ContextOption {
    ContextOption::new(move |ctx| ctx.pre_split_regions = regions)
}

/// Go `WithSuppressTooLongIndexErr`: sets whether to suppress the too-long
/// index error.
#[must_use]
pub fn with_suppress_too_long_index_err(suppress: bool) -> ContextOption {
    ContextOption::new(move |ctx| ctx.suppress_too_long_index_err = suppress)
}

/// Go `WithInfoSchema`: sets the info schema. `None` is Go's nil schema.
#[must_use]
pub fn with_info_schema(schema: Option<Arc<dyn MetaOnlyInfoSchema>>) -> ContextOption {
    ContextOption::new(move |ctx| ctx.is = schema)
}

/// Go `Context`: used to build meta like `TableInfo`, `IndexInfo`, etc.
pub struct Context {
    /// Always `Some` after [`Context::new`]; `Option` only exists so the
    /// option closures can run before the Go nil-fallback fills it in.
    expr_ctx: Option<Arc<dyn ExprContext>>,
    enable_auto_increment_in_generated: bool,
    primary_key_required: bool,
    clustered_index_def_mode: ClusteredIndexDefMode,
    shard_row_id_bits: u64,
    pre_split_regions: u64,
    suppress_too_long_index_err: bool,
    is: Option<Arc<dyn MetaOnlyInfoSchema>>,
}

impl Context {
    /// Go `NewContext`: creates a new context for meta-building.
    pub fn new(opts: impl IntoIterator<Item = ContextOption>) -> Context {
        let mut ctx = Context {
            expr_ctx: None,
            enable_auto_increment_in_generated: DEF_TIDB_ENABLE_AUTO_INCREMENT_IN_GENERATED,
            primary_key_required: false,
            clustered_index_def_mode: ClusteredIndexDefMode(DEF_TIDB_ENABLE_CLUSTERED_INDEX),
            // Go declares the two defaults as untyped zero constants; the
            // casts are lossless for these values.
            shard_row_id_bits: DEF_SHARD_ROW_ID_BITS as u64,
            pre_split_regions: DEF_PRE_SPLIT_REGIONS as u64,
            suppress_too_long_index_err: false,
            is: None,
        };

        for opt in opts {
            opt.apply_ctx(&mut ctx);
        }

        if ctx.expr_ctx.is_none() {
            ctx.expr_ctx = Some(Arc::new(StaticExprContext::new()));
        }

        ctx
    }

    /// Go `NewNonStrictContext`: creates a new context for meta-building with
    /// non-strict mode — `mysql.ModeNone` avoids special values like the
    /// datetime `0000-00-00 00:00:00`.
    #[must_use]
    pub fn new_non_strict() -> Context {
        Context::new([with_expr_ctx(Arc::new(StaticExprContext::with_sql_mode(
            tidb_mysql::consts::ModeNone,
        )))])
    }

    /// Go `GetExprCtx`: returns the expression context of the session.
    #[must_use]
    pub fn get_expr_ctx(&self) -> &Arc<dyn ExprContext> {
        self.expr_ctx
            .as_ref()
            .expect("Context::new always sets the expression context")
    }

    /// Go `GetDefaultCollationForUTF8MB4`: the default collation for utf8mb4.
    #[must_use]
    pub fn get_default_collation_for_utf8mb4(&self) -> &str {
        self.get_expr_ctx().get_default_collation_for_utf8mb4()
    }

    /// Go `GetSQLMode`: returns the SQL mode.
    #[must_use]
    pub fn get_sql_mode(&self) -> SqlMode {
        self.get_expr_ctx().get_eval_ctx().sql_mode()
    }

    /// Go `AppendWarning`: appends a warning.
    pub fn append_warning(&self, err: WarnErr) {
        self.get_expr_ctx().get_eval_ctx().append_warning(err);
    }

    /// Go `AppendNote`: appends a note.
    pub fn append_note(&self, note: WarnErr) {
        self.get_expr_ctx().get_eval_ctx().append_note(note);
    }

    /// Go `EnableAutoIncrementInGenerated`: whether auto increment is enabled
    /// in a generated column.
    #[must_use]
    pub fn enable_auto_increment_in_generated(&self) -> bool {
        self.enable_auto_increment_in_generated
    }

    /// Go `PrimaryKeyRequired`: whether a primary key is required.
    #[must_use]
    pub fn primary_key_required(&self) -> bool {
        self.primary_key_required
    }

    /// Go `GetClusteredIndexDefMode`: returns the clustered index mode.
    #[must_use]
    pub fn get_clustered_index_def_mode(&self) -> ClusteredIndexDefMode {
        self.clustered_index_def_mode
    }

    /// Go `GetShardRowIDBits`: returns the shard row id bits.
    #[must_use]
    pub fn get_shard_row_id_bits(&self) -> u64 {
        self.shard_row_id_bits
    }

    /// Go `GetPreSplitRegions`: returns the pre-split regions.
    #[must_use]
    pub fn get_pre_split_regions(&self) -> u64 {
        self.pre_split_regions
    }

    /// Go `SuppressTooLongIndexErr`: whether the too-long index error is
    /// suppressed.
    #[must_use]
    pub fn suppress_too_long_index_err(&self) -> bool {
        self.suppress_too_long_index_err
    }

    /// Go `GetInfoSchema`: returns the info schema for checking constraints
    /// between tables. `None` is Go's `(nil, false)`: no cross-table
    /// constraint checks are needed.
    #[must_use]
    pub fn get_info_schema(&self) -> Option<&Arc<dyn MetaOnlyInfoSchema>> {
        self.is.as_ref()
    }
}

impl Default for Context {
    fn default() -> Self {
        Context::new([])
    }
}

// Go `context_test.go` `TestMetaBuildContext`, split into the same
// per-field default/option subtests the table drives. The closing
// `deeptest.AssertRecursivelyNotEqual` reflection guard (proving the table
// names every `Context` field) has no Rust counterpart and is replaced by
// the tests below exercising each field explicitly; Go-side helpers
// `variable.NewSessionVars` and `infoschema.MockInfoSchema` become the
// vardef defaults they resolve to and a local mock.
#[cfg(test)]
mod tests {
    use super::*;

    struct MockInfoSchema;

    impl MetaOnlyInfoSchema for MockInfoSchema {
        fn schema_meta_version(&self) -> i64 {
            0
        }
    }

    #[test]
    fn default_of_expr_ctx() {
        let ctx = Context::new([]);
        let (cs, col) = ctx.get_expr_ctx().get_charset_info();
        let (def_cs, def_col) = tidb_datatype::get_default_charset_and_collate();
        assert_eq!(def_cs, cs);
        assert_eq!(def_col, col);
        let def_sql_mode = tidb_mysql::consts::get_sql_mode(tidb_mysql::consts::DefaultSQLMode)
            .expect("mysql.DefaultSQLMode always parses");
        assert_eq!(def_sql_mode, ctx.get_sql_mode());
        assert_eq!(
            ctx.get_expr_ctx().get_eval_ctx().sql_mode(),
            ctx.get_sql_mode()
        );
        // Go compares against `variable.NewSessionVars(nil).DefaultCollationForUTF8MB4`,
        // whose default is `mysql.DefaultCollationName`.
        assert_eq!(
            tidb_mysql::charset::DefaultCollationName,
            ctx.get_default_collation_for_utf8mb4()
        );
        assert_eq!(
            ctx.get_expr_ctx().get_default_collation_for_utf8mb4(),
            ctx.get_default_collation_for_utf8mb4()
        );
    }

    #[test]
    fn default_of_scalar_fields() {
        let ctx = Context::new([]);
        // Go checks each default against `variable.NewSessionVars(nil)`; those
        // session defaults resolve to the vardef constants used here.
        assert_eq!(
            DEF_TIDB_ENABLE_AUTO_INCREMENT_IN_GENERATED,
            ctx.enable_auto_increment_in_generated()
        );
        assert!(!ctx.primary_key_required());
        assert_eq!(
            ClusteredIndexDefMode(DEF_TIDB_ENABLE_CLUSTERED_INDEX),
            ctx.get_clustered_index_def_mode()
        );
        assert_eq!(
            ClusteredIndexDefMode::ON,
            ctx.get_clustered_index_def_mode()
        );
        assert_eq!(0, ctx.get_shard_row_id_bits());
        assert_eq!(0, ctx.get_pre_split_regions());
        assert!(!ctx.suppress_too_long_index_err());
        assert!(ctx.get_info_schema().is_none());
    }

    #[test]
    fn option_of_expr_ctx() {
        let expr_ctx: Arc<dyn ExprContext> = Arc::new(StaticExprContext::new());
        let ctx = Context::new([with_expr_ctx(Arc::clone(&expr_ctx))]);
        assert!(Arc::ptr_eq(ctx.get_expr_ctx(), &expr_ctx));
    }

    #[test]
    fn option_of_enable_auto_increment_in_generated() {
        for val in [true, false] {
            let ctx = Context::new([with_enable_auto_increment_in_generated(val)]);
            assert_eq!(val, ctx.enable_auto_increment_in_generated());
        }
    }

    #[test]
    fn option_of_primary_key_required() {
        for val in [true, false] {
            let ctx = Context::new([with_primary_key_required(val)]);
            assert_eq!(val, ctx.primary_key_required());
        }
    }

    #[test]
    fn option_of_clustered_index_def_mode() {
        for val in [ClusteredIndexDefMode::ON, ClusteredIndexDefMode::OFF] {
            let ctx = Context::new([with_clustered_index_def_mode(val)]);
            assert_eq!(val, ctx.get_clustered_index_def_mode());
        }
    }

    #[test]
    fn option_of_shard_row_id_bits() {
        for val in [6_u64, 8] {
            let ctx = Context::new([with_shard_row_id_bits(val)]);
            assert_eq!(val, ctx.get_shard_row_id_bits());
        }
    }

    #[test]
    fn option_of_pre_split_regions() {
        for val in [123_u64, 456] {
            let ctx = Context::new([with_pre_split_regions(val)]);
            assert_eq!(val, ctx.get_pre_split_regions());
        }
    }

    #[test]
    fn option_of_suppress_too_long_index_err() {
        for val in [true, false] {
            let ctx = Context::new([with_suppress_too_long_index_err(val)]);
            assert_eq!(val, ctx.suppress_too_long_index_err());
        }
    }

    #[test]
    fn option_of_info_schema() {
        let schema: Arc<dyn MetaOnlyInfoSchema> = Arc::new(MockInfoSchema);
        let ctx = Context::new([with_info_schema(Some(Arc::clone(&schema)))]);
        let got = ctx.get_info_schema().expect("schema was set");
        assert!(Arc::ptr_eq(got, &schema));

        let ctx = Context::new([with_info_schema(None)]);
        assert!(ctx.get_info_schema().is_none());
    }

    // `NewNonStrictContext` is not covered by Go's test file; its SQL-mode
    // contract (`mysql.ModeNone`) is asserted here to keep the ported symbol
    // honest.
    #[test]
    fn non_strict_context_uses_mode_none() {
        let ctx = Context::new_non_strict();
        assert_eq!(tidb_mysql::consts::ModeNone, ctx.get_sql_mode());
    }
}
