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

//! One-to-one ports of every test function in
//! `pkg/parser/terror/terror_test.go` (origin/master):
//!
//! - `TestErrCode`
//! - `TestTError`
//! - `TestJson`
//! - `TestErrorEqual`
//! - `TestLog`
//! - `TestTraceAndLocation`

use std::error::Error;
use std::fmt;

use tidb_error::mysql::FormatArg;
use tidb_error::terror::{
    log, terror_error_equal, TerrorClass, TerrorCode, TerrorError, CODE_EXEC_RESULT_IS_EMPTY,
    CODE_MISS_CONNECTION_ID, CODE_RESULT_UNDETERMINED, ERR_CRITICAL,
};

/// Stand-in for a plain `errors.New` value with no terror identity.
#[derive(Debug)]
struct MessageError(&'static str);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for MessageError {}

/// Stand-in for `pingcap/errors.Trace`: a context wrapper whose root cause is
/// the boxed inner error, reachable through `Error::source`.
#[derive(Debug)]
struct TraceError {
    message: &'static str,
    source: Box<dyn Error>,
}

impl fmt::Display for TraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.message, self.source)
    }
}

impl Error for TraceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn trace(error: impl Error + 'static, message: &'static str) -> TraceError {
    TraceError {
        message,
        source: Box::new(error),
    }
}

/// Port of `terror_test.go` `TestErrCode`: the special sentinel error codes
/// keep their source numeric values.
#[test]
fn err_code_preserves_sentinel_values() {
    // go: pkg/parser/terror/terror_test.go TestErrCode
    assert_eq!(CODE_MISS_CONNECTION_ID.value(), 1);
    assert_eq!(CODE_RESULT_UNDETERMINED.value(), 2);
}

/// Port of `terror_test.go` `TestTError`: class descriptions are registered,
/// class membership follows generated errors to their root cause, identity
/// equality ignores context wrappers and message changes, `FastGen` renders
/// Go verbs, and `ToSQLError` strips the `[class:code]` prefix.
#[test]
fn terror_class_membership_identity_and_sql_conversion() {
    // go: pkg/parser/terror/terror_test.go TestTError
    assert!(!TerrorClass::Parser.description().is_empty());
    assert!(!TerrorClass::Optimizer.description().is_empty());
    assert!(!TerrorClass::Kv.description().is_empty());
    assert!(!TerrorClass::Server.description().is_empty());

    let parser_err = TerrorError::registered(TerrorClass::Parser, TerrorCode::new(100), "error 100");
    assert!(!parser_err.to_string().is_empty());
    assert!(TerrorClass::Parser.equal_class(Some(&parser_err)));
    assert!(!TerrorClass::Parser.not_equal_class(Some(&parser_err)));

    let optimizer_err =
        TerrorError::registered(TerrorClass::Optimizer, TerrorCode::new(2), "abc");
    assert!(!TerrorClass::Optimizer.equal_class(Some(&parser_err)));
    assert!(!TerrorClass::Optimizer.equal_class(Some(&MessageError("abc"))));
    assert!(!TerrorClass::Optimizer.equal_class(None));

    // GenWithStack keeps the identity: the wrapped error compares equal.
    assert!(optimizer_err.equal(Some(&optimizer_err.generate_with_stack("def"))));
    assert!(!optimizer_err.equal(None));
    assert!(!optimizer_err.equal(Some(&MessageError("abc"))));

    // FastGen cases, including printf-style formatting.
    let kv_err = TerrorError::registered(
        TerrorClass::Kv,
        TerrorCode::new(1062),
        "key already exist",
    );
    let formatted = kv_err.fast_generate(
        "Duplicate entry '%d' for key 'PRIMARY'",
        &[FormatArg::from(1_i64)],
    );
    assert_eq!(
        formatted.to_string(),
        "[kv:1062]Duplicate entry '1' for key 'PRIMARY'"
    );
    let sql_err = formatted.to_sql_error();
    assert_eq!(sql_err.message, "Duplicate entry '1' for key 'PRIMARY'");
    assert_eq!(sql_err.code, 1062);

    // ErrCritical stays equal to itself across a trace wrapper and bare.
    assert!(ERR_CRITICAL.equal(Some(&trace(
        ERR_CRITICAL.clone(),
        "wrapped",
    ))));
    assert!(ERR_CRITICAL.equal(Some(&ERR_CRITICAL.clone())));
}

/// Port of `terror_test.go` `TestJson`: a pingcap/errors-shaped error
/// survives a JSON round trip with its identity intact.
#[test]
fn json_round_trip_preserves_compatible_error_identity() {
    // go: pkg/parser/terror/terror_test.go TestJson
    // errors.Normalize("json test", errors.MySQLErrorCode(CodeExecResultIsEmpty))
    // produces a compatibility-form error carrying only the raw code.
    let prev_terr = TerrorError::compatible(CODE_EXEC_RESULT_IS_EMPTY, "json test");
    let buf = serde_json::to_string(&prev_terr).expect("serialize");
    let cur_terr: TerrorError = serde_json::from_str(&buf).expect("deserialize");
    assert!(prev_terr == cur_terr);
}

/// Port of `terror_test.go` `TestErrorEqual`: cause traversal, string-based
/// equality for plain errors, and RFC-identity-based (not message-based)
/// equality for terror errors.
#[test]
fn error_equal_follows_causes_and_rfc_identity() {
    // go: pkg/parser/terror/terror_test.go TestErrorEqual
    let e1 = MessageError("test error");
    let e2 = trace(MessageError("test error"), "trace1");
    let e3 = trace(trace(MessageError("test error"), "trace2"), "trace3");

    // errors.Cause chains resolve to the original error.
    assert!(terror_error_equal(Some(&e1), Some(&e2)));
    assert!(terror_error_equal(Some(&e1), Some(&e3)));
    assert!(terror_error_equal(
        Some(e2.source().expect("e2 has a cause")),
        Some(e3.source().and_then(Error::source).expect("e3 nested cause"))
    ));

    let e4 = MessageError("test error");
    let e5 = DisplayError("test error");
    assert!(terror_error_equal(Some(&e1), Some(&e4)));
    assert!(terror_error_equal(Some(&e1), Some(&e5)));

    // nil cases.
    assert!(terror_error_equal(None, None));
    assert!(!terror_error_equal(Some(&e1), None));

    // Same code, different class: unequal. Same class, different code:
    // unequal.
    let te1 = TerrorError::synthesize(TerrorClass::Parser, TerrorCode::new(9001), "abc");
    let te3 = TerrorError::registered(TerrorClass::Kv, TerrorCode::new(9001), "abc");
    let te4 = TerrorError::registered(TerrorClass::Kv, TerrorCode::new(9002), "abc");
    assert!(!terror_error_equal(Some(&te1), Some(&te3)));
    assert!(!terror_error_equal(Some(&te3), Some(&te4)));
}

/// A second plain-error type so same-text errors from different constructors
/// (`errors.New` vs `errors.Errorf`) can be distinguished structurally, as in
/// the Go test's e1/e5 pair.
#[derive(Debug)]
struct DisplayError(&'static str);

impl fmt::Display for DisplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for DisplayError {}

/// Port of `terror_test.go` `TestLog`: logging an error must not panic.
#[test]
fn log_accepts_an_error_without_panicking() {
    // go: pkg/parser/terror/terror_test.go TestLog
    let err = MessageError("xxx");
    log(Some(&err));
}

/// Port of `terror_test.go` `TestTraceAndLocation`: a stack-generating error
/// captures a backtrace whose rendering names this test's source file. The
/// Go-specific line-count arithmetic over GOROOT frames does not apply to
/// Rust's native backtrace format and is intentionally omitted.
#[test]
fn trace_and_location_names_the_test_source_file() {
    // go: pkg/parser/terror/terror_test.go TestTraceAndLocation
    let prototype =
        TerrorError::registered(TerrorClass::Executor, TerrorCode::new(123), "predefiend error");
    let err = prototype.generate_with_stack("error message:abc");
    let stack = err.stack().expect("GenWithStack must capture a backtrace");
    let rendered = stack.to_string();
    assert!(
        rendered.contains("tests_terror"),
        "stack must name this test file, got:\n{rendered}"
    );
}
