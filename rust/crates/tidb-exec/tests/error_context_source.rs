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

#![allow(missing_docs)]

#[path = "../src/error_context.rs"]
mod error_context;

use error_context::{
    resolve_err_level, ErrGroup, ErrorContext, ErrorContextFlags, ErrorDisposition, Level, LevelMap,
};
use tidb_datatype::{ConversionContext, ConversionFlags, ConversionLocation, STRICT_FLAGS};
use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_exec::{StaticWarningHandler, WarningHandler};

#[test]
fn source_groups_default_to_error_and_keep_declaration_order() {
    // Source: pkg/errctx/context.go:189-210 (the seven ErrGroup values).
    let levels = LevelMap::default();
    assert_eq!(ErrGroup::COUNT, 7);
    for group in ErrGroup::ALL {
        assert_eq!(levels.get(group), Level::Error);
    }
    assert_eq!(levels.as_array(), [Level::Error; ErrGroup::COUNT]);
    assert_eq!(ErrGroup::ALL[0], ErrGroup::Truncate);
    assert_eq!(ErrGroup::ALL[4], ErrGroup::DividedByZero);
    assert_eq!(ErrGroup::ALL[6], ErrGroup::NoMatchedPartition);
}

#[test]
fn copy_on_write_group_levels_and_strict_reset_match_context_methods() {
    // Source: pkg/errctx/context.go:55-89 (WithStrictErrGroupLevel,
    // WithErrGroupLevel, and WithErrGroupLevels).
    let ctx = ErrorContext::new().with_group_level(ErrGroup::Truncate, Level::Warn);
    assert_eq!(ctx.level_for(ErrGroup::Truncate), Level::Warn);
    assert_eq!(
        ErrorContext::new().level_for(ErrGroup::Truncate),
        Level::Error
    );

    let strict = ctx.with_strict_group_levels();
    assert_eq!(strict.levels(), LevelMap::default());
    assert_eq!(strict.flags(), ErrorContextFlags::default());

    let explicitly_built = ErrorContext::with_levels(
        LevelMap::default().with_level(ErrGroup::BadNull, Level::Warn),
        ErrorContextFlags::new(),
    );
    assert_eq!(explicitly_built.level_for(ErrGroup::BadNull), Level::Warn);

    let replaced = ctx.with_group_levels(
        LevelMap::default().with_level(ErrGroup::AutoIncReadFailed, Level::Ignore),
    );
    assert_eq!(
        replaced.level_for(ErrGroup::AutoIncReadFailed),
        Level::Ignore
    );
    assert_eq!(ctx.level_for(ErrGroup::AutoIncReadFailed), Level::Error);
}

#[test]
fn ignore_precedes_warning_for_resolve_err_level() {
    // Source: pkg/errctx/context.go:254-265.
    assert_eq!(resolve_err_level(false, false), Level::Error);
    assert_eq!(resolve_err_level(false, true), Level::Warn);
    assert_eq!(resolve_err_level(true, false), Level::Ignore);
    assert_eq!(resolve_err_level(true, true), Level::Ignore);
}

#[test]
fn statement_flags_derive_truncate_and_division_policy_without_a_warning_sink() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1275-1285 and 1457-1466.
    let flags = ErrorContextFlags::new()
        .with_ignore_truncate(true)
        .with_truncate_as_warning(true)
        .with_ignore_zero_in_date(true)
        .with_divided_by_zero_as_warning(true);
    let ctx = ErrorContext::from_flags(flags);

    // Ignore-truncate wins over truncate-as-warning, while the divided-by-zero
    // push-down flag is an independent warning decision.
    assert_eq!(ctx.level_for(ErrGroup::Truncate), Level::Ignore);
    assert_eq!(ctx.level_for(ErrGroup::DividedByZero), Level::Warn);
    assert_eq!(ctx.level_for(ErrGroup::DupKey), Level::Error);
    assert!(ctx.flags().ignore_zero_in_date());
    assert!(ctx.flags().ignore_truncate());
    assert!(ctx.flags().truncate_as_warning());
    assert!(ctx.flags().divided_by_zero_as_warning());
    assert_eq!(
        ctx.flags().conversion_flags().bits(),
        ConversionFlags::IGNORE_TRUNCATE_ERR
            | ConversionFlags::TRUNCATE_AS_WARNING
            | ConversionFlags::IGNORE_ZERO_IN_DATE_ERR
    );
}

#[test]
fn statement_defaults_warn_on_division_but_keep_other_groups_strict() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:561-563
    // (DefaultStmtErrLevels) and context.go:254-265.
    let defaults = ErrorContext::statement_defaults();
    assert_eq!(defaults.level_for(ErrGroup::DividedByZero), Level::Warn);
    assert_eq!(defaults.level_for(ErrGroup::Truncate), Level::Error);
    assert_eq!(defaults.level_for(ErrGroup::DupKey), Level::Error);
    assert!(defaults.flags().divided_by_zero_as_warning());
}

#[test]
fn disposition_is_typed_and_does_not_mutate_warning_state() {
    // Source: pkg/errctx/context.go:128-168. The Rust leaf returns a typed
    // action; the future statement owner will append the warning itself.
    let ctx = ErrorContext::new()
        .with_group_level(ErrGroup::Truncate, Level::Warn)
        .with_group_level(ErrGroup::DupKey, Level::Ignore);
    assert_eq!(ctx.disposition(ErrGroup::Truncate), ErrorDisposition::Warn);
    assert_eq!(ctx.disposition(ErrGroup::DupKey), ErrorDisposition::Ignore);
    assert_eq!(ctx.disposition(ErrGroup::BadNull), ErrorDisposition::Return);
}

#[test]
fn executor_warning_handler_is_the_conversion_context_sink() {
    let warnings = StaticWarningHandler::new(0);
    let context = ConversionContext::new(
        STRICT_FLAGS.with_truncate_as_warning(true),
        ConversionLocation::UTC,
        &warnings,
    );
    context.append_warning(TerrorError::registered(
        TerrorClass::Types,
        TerrorCode::new(1292),
        "truncated",
    ));
    assert_eq!(warnings.warning_count(), 1);
    assert_eq!(
        warnings.warnings_snapshot()[0].message,
        "[types:1292]truncated"
    );
}
