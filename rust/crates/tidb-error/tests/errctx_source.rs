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

//! Source tests from `pkg/errctx/context_test.go`.

#![allow(missing_docs)]

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use tidb_error::errctx::{
    err_group_for_code, new_context, new_context_with_levels, resolve_err_level, ErrGroup, Level,
    LevelMap, MultiError, SharedError, WarnAppender, STRICT_NO_WARNING_CONTEXT,
};
use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_error::tidb::errcode;

/// A plain error, standing in for Go's `errors.New`.
#[derive(Debug)]
struct PlainError(&'static str);

impl fmt::Display for PlainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for PlainError {}

fn code_value(code: u16) -> isize {
    isize::try_from(code).expect("u16 fits isize")
}

fn plain(message: &'static str) -> SharedError {
    Arc::new(PlainError(message))
}

/// `contextutil.NewFuncWarnAppenderForTest`: records each appended warning
/// with its level so the test can assert `WarnLevelWarning`.
#[derive(Default)]
struct FuncWarnAppender {
    recorded: Mutex<Vec<(&'static str, SharedError)>>,
}

impl FuncWarnAppender {
    fn last(&self) -> Option<SharedError> {
        let recorded = self.recorded.lock().unwrap();
        recorded.last().map(|(level, err)| {
            // Go's test closure: require.Equal(t, WarnLevelWarning, level).
            assert_eq!(*level, "Warning");
            err.clone()
        })
    }

    fn clear(&self) {
        self.recorded.lock().unwrap().clear();
    }
}

impl WarnAppender for FuncWarnAppender {
    fn append_warning(&self, err: SharedError) {
        self.recorded.lock().unwrap().push(("Warning", err));
    }

    fn append_note(&self, err: SharedError) {
        self.recorded.lock().unwrap().push(("Note", err));
    }
}

fn same_error(left: &SharedError, right: &SharedError) -> bool {
    Arc::ptr_eq(left, right)
}

// Go `TestContext` (pkg/errctx/context_test.go:28).
#[test]
fn test_context() {
    let handler = Arc::new(FuncWarnAppender::default());
    let ctx = new_context(handler.clone());

    // `types.ErrOverflow` is `ClassTypes.NewStdErr(mysql.ErrDataOutOfRange, ...)`;
    // only its code (1690, ErrGroupTruncate) matters to errctx.
    let test_internal_err: SharedError = Arc::new(TerrorError::synthesize(
        TerrorClass::Types,
        TerrorCode::new(code_value(errcode::ErrDataOutOfRange)),
        "overflow",
    ));
    let test_err = plain("error");
    let test_warn = plain("warn");

    // By default, all errors will be returned directly.
    let returned = ctx
        .handle_error_with_alias(
            Some(test_internal_err.clone()),
            test_err.clone(),
            test_warn.clone(),
        )
        .expect("strict context returns the error");
    assert!(same_error(&returned, &test_err));

    // Set level to "warn".
    let new_ctx = ctx.with_err_group_level(ErrGroup::Truncate, Level::Warn);
    // `ctx` is not affected.
    let returned = ctx
        .handle_error_with_alias(
            Some(test_internal_err.clone()),
            test_err.clone(),
            test_warn.clone(),
        )
        .expect("strict context still returns the error");
    assert!(same_error(&returned, &test_err));
    // `new_ctx` will handle the error as a warn.
    assert!(new_ctx
        .handle_error_with_alias(
            Some(test_internal_err.clone()),
            test_err.clone(),
            test_warn.clone(),
        )
        .is_none());
    assert!(same_error(
        &handler.last().expect("warn recorded"),
        &test_warn
    ));
    let levels = new_ctx.level_map();
    for group in ErrGroup::ALL {
        if group == ErrGroup::Truncate {
            assert_eq!(levels.get(group), Level::Warn);
        } else {
            assert_eq!(levels.get(group), Level::Error);
            assert_eq!(levels.get(group), new_ctx.level_for_group(group));
        }
    }

    handler.clear();
    let new_ctx2 = new_ctx.with_strict_err_group_level();
    // `new_ctx` is not affected.
    assert!(new_ctx
        .handle_error_with_alias(
            Some(test_internal_err.clone()),
            test_err.clone(),
            test_warn.clone(),
        )
        .is_none());
    assert!(same_error(
        &handler.last().expect("warn recorded"),
        &test_warn
    ));
    // `new_ctx2` will return all errors.
    let returned = new_ctx2
        .handle_error_with_alias(
            Some(test_internal_err.clone()),
            test_err.clone(),
            test_warn.clone(),
        )
        .expect("strict copy returns the error");
    assert!(same_error(&returned, &test_err));
    assert_eq!(new_ctx2.level_map(), LevelMap::strict());

    // Test `multierr`.
    let test_errs: SharedError = Arc::new(MultiError::new(vec![
        test_internal_err.clone(),
        test_err.clone(),
    ]));
    let returned = ctx
        .handle_error(Some(test_errs.clone()))
        .expect("strict context returns the first error");
    assert!(same_error(&returned, &test_internal_err));
    let returned = new_ctx
        .handle_error(Some(test_errs))
        .expect("warn context returns the non-terror error");
    assert!(same_error(&returned, &test_err));
    assert!(same_error(
        &handler.last().expect("warn recorded"),
        &test_internal_err
    ));

    // Test nil.
    assert!(ctx.handle_error(None).is_none());

    // Test with a level map.
    let mut levels = LevelMap::strict();
    levels[ErrGroup::AutoIncReadFailed] = Level::Warn;
    let ctx = new_context_with_levels(levels, handler.clone());
    assert_eq!(levels, ctx.level_map());
    let mut levels2 = LevelMap::strict();
    levels2[ErrGroup::AutoIncReadFailed] = Level::Ignore;
    let ctx = ctx.with_err_group_levels(levels2);
    assert_eq!(levels2, ctx.level_map());

    // Original levels should not change.
    let ctx = ctx.with_err_group_levels(LevelMap::strict());
    assert_eq!(LevelMap::strict(), ctx.level_map());
    assert_eq!(Level::Warn, levels[ErrGroup::AutoIncReadFailed]);
    assert_eq!(Level::Ignore, levels2[ErrGroup::AutoIncReadFailed]);
}

// Not part of the Go test file: pins the full `errGroupMap` membership from
// `pkg/errctx/context.go`'s `init()`, so a code silently dropping out of a
// group is caught here rather than in a downstream statement.
#[test]
fn err_group_map_matches_source_init() {
    let expect = [
        (errcode::ErrTruncatedWrongValue, ErrGroup::Truncate),
        (errcode::ErrDataTooLong, ErrGroup::Truncate),
        (errcode::ErrTruncatedWrongValueForField, ErrGroup::Truncate),
        (errcode::ErrWarnDataOutOfRange, ErrGroup::Truncate),
        (errcode::ErrDataOutOfRange, ErrGroup::Truncate),
        (errcode::ErrBadNumber, ErrGroup::Truncate),
        (errcode::ErrWrongValueForType, ErrGroup::Truncate),
        (errcode::ErrDatetimeFunctionOverflow, ErrGroup::Truncate),
        (errcode::WarnDataTruncated, ErrGroup::Truncate),
        (errcode::ErrIncorrectDatetimeValue, ErrGroup::Truncate),
        (errcode::ErrBadNull, ErrGroup::BadNull),
        (errcode::ErrWarnNullToNotnull, ErrGroup::BadNull),
        (errcode::ErrNoDefaultForField, ErrGroup::NoDefault),
        (errcode::ErrDivisionByZero, ErrGroup::DividedByZero),
        (errcode::ErrAutoincReadFailed, ErrGroup::AutoIncReadFailed),
        (
            errcode::ErrNoPartitionForGivenValue,
            ErrGroup::NoMatchedPartition,
        ),
        (
            errcode::ErrRowDoesNotMatchGivenPartitionSet,
            ErrGroup::NoMatchedPartition,
        ),
        (errcode::ErrDupEntry, ErrGroup::DupKey),
    ];
    for (code, group) in expect {
        assert_eq!(err_group_for_code(code_value(code)), Some(group), "{code}");
    }
    // Codes outside the table are unmapped: their errors pass through.
    assert_eq!(err_group_for_code(0), None);
    assert_eq!(err_group_for_code(1064), None);
    assert_eq!(err_group_for_code(-1), None);

    // The strict shared context returns everything and swallows warnings.
    assert_eq!(STRICT_NO_WARNING_CONTEXT.level_map(), LevelMap::strict());
    assert_eq!(resolve_err_level(true, true), Level::Ignore);
    assert_eq!(resolve_err_level(false, true), Level::Warn);
    assert_eq!(resolve_err_level(false, false), Level::Error);
}
