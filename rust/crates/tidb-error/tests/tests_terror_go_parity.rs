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

//! Direct ports of `origin/master:pkg/parser/terror/terror_test.go`
//! (Go package `terror`, 6 test functions) onto the `tidb-error` crate.

use std::error::Error;
use std::fmt;

use tidb_error::mysql::FormatArg;
use tidb_error::terror::{
    call, log, terror_error_equal, TerrorClass, TerrorCode, TerrorError,
    CODE_EXEC_RESULT_IS_EMPTY, CODE_MISS_CONNECTION_ID, CODE_RESULT_UNDETERMINED,
};

/// Go `errors.New`/`errors.Errorf` stand-in: an owned-message error.
#[derive(Debug)]
struct MessageError(&'static str);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for MessageError {}

/// Go `errors.Trace` stand-in: wraps an error as `source()`, mirroring the
/// wrapped-cause chain that `errors.Cause` walks in Go.
#[derive(Debug)]
struct TraceError(Box<dyn Error>);

impl fmt::Display for TraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "traced: {}", self.0)
    }
}

impl Error for TraceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

// Go: TestErrCode — the special global codes keep their master values.
#[test]
fn err_code_keeps_master_values() {
    assert_eq!(CODE_MISS_CONNECTION_ID.value(), 1);
    assert_eq!(CODE_RESULT_UNDETERMINED.value(), 2);
}

// Go: TestTError — class descriptions are non-empty; class identity, Gen /
// FastGen equality, ToSQLError message+code, and ErrCritical trace equality.
#[test]
fn terror_class_generation_equality_and_sql_conversion() {
    assert!(!TerrorClass::Parser.description().is_empty());
    assert!(!TerrorClass::Optimizer.description().is_empty());
    assert!(!TerrorClass::Kv.description().is_empty());
    assert!(!TerrorClass::Server.description().is_empty());

    let parser_err =
        TerrorError::registered(TerrorClass::Parser, TerrorCode::new(100), "error 100");
    assert!(!parser_err.to_string().is_empty());
    assert!(TerrorClass::Parser.equal_class(Some(&parser_err)));
    assert!(!TerrorClass::Parser.not_equal_class(Some(&parser_err)));

    assert!(!TerrorClass::Optimizer.equal_class(Some(&parser_err)));
    let optimizer_err =
        TerrorError::registered(TerrorClass::Optimizer, TerrorCode::new(2), "abc");
    assert!(!TerrorClass::Optimizer.equal_class(Some(&MessageError("abc"))));
    assert!(!TerrorClass::Optimizer.equal_class(None));
    // GenWithStack keeps the identity of its prototype.
    assert!(optimizer_err.equal(Some(&optimizer_err.generate("def"))));
    assert!(!optimizer_err.equal(None));
    assert!(!optimizer_err.equal(Some(&MessageError("abc"))));

    // FastGen cases.
    assert!(optimizer_err.equal(Some(&optimizer_err.fast_generate("def", &[]))));
    assert!(optimizer_err.equal(Some(
        &optimizer_err.fast_generate("def: %s", &[FormatArg::from("def")])
    )));
    let kv_err = TerrorError::registered(
        TerrorClass::Kv,
        TerrorCode::new(1062),
        "key already exist",
    );
    let generated = kv_err.fast_generate(
        "Duplicate entry '%d' for key 'PRIMARY'",
        &[FormatArg::from(1_i32)],
    );
    assert_eq!(
        generated.to_string(),
        "[kv:1062]Duplicate entry '1' for key 'PRIMARY'"
    );
    let sql_err = generated.to_sql_error();
    assert_eq!(sql_err.message, "Duplicate entry '1' for key 'PRIMARY'");
    assert_eq!(sql_err.code, 1062);

    // errors.Trace(ErrCritical.GenWithStackByArgs(...)) and
    // errors.Trace(ErrCritical) both stay equal to ErrCritical.
    let traced_generated = TraceError(Box::new(TerrorError::synthesize(
        TerrorClass::Global,
        TerrorCode::new(3),
        "critical",
    )
    .generate_with_stack("test")));
    let prototype = TerrorError::synthesize(TerrorClass::Global, TerrorCode::new(3), "critical");
    assert!(prototype.equal(Some(&traced_generated)));
    let traced_prototype = TraceError(Box::new(prototype));
    assert!(TerrorError::synthesize(TerrorClass::Global, TerrorCode::new(3), "critical")
        .equal(Some(&traced_prototype)));
}

// Go: TestJson — JSON round trip preserves the error.
#[test]
fn json_round_trip_preserves_the_error() {
    // Go builds prevTErr via errors.Normalize with MySQLErrorCode
    // CodeExecResultIsEmpty (3); `compatible` is the same unclassified shape.
    let prev = TerrorError::compatible(CODE_EXEC_RESULT_IS_EMPTY, "json test");
    let buf = serde_json::to_string(&prev).expect("marshal must succeed");
    let cur: TerrorError = serde_json::from_str(&buf).expect("unmarshal must succeed");
    assert!(prev.equal(Some(&cur)));
    assert_eq!(cur.to_string(), "[3]json test");
}


// Go: TestErrorEqual — cause traversal and terror class/code identity.
#[test]
fn error_equal_follows_causes_and_identity() {
    let e1 = MessageError("test error");
    let e2 = TraceError(Box::new(MessageError("test error")));
    let e3 = TraceError(Box::new(TraceError(Box::new(MessageError("test error")))));

    // Cause(e2) == Cause(e3) == e1 by message.
    assert!(terror_error_equal(Some(&e1), Some(&e2)));
    assert!(terror_error_equal(Some(&e1), Some(&e3)));
    assert!(terror_error_equal(Some(&e2), Some(&e3)));

    // A distinct instance with the same message compares equal through
    // ErrorEqual's message fallback, matching Go's pingcap/errors behavior.
    let e4 = MessageError("test error");
    assert!(terror_error_equal(Some(&e1), Some(&e4)));

    assert!(terror_error_equal(None, None));
    assert!(!terror_error_equal(Some(&e1), None));

    let te1 = TerrorError::synthesize(TerrorClass::Parser, TerrorCode::new(9001), "abc");
    let te3 =
        TerrorError::registered(TerrorClass::Kv, TerrorCode::new(9001), "abc");
    let te4 =
        TerrorError::registered(TerrorClass::Kv, TerrorCode::new(9002), "abc");
    assert!(!terror_error_equal(Some(&te1), Some(&te3)));
    assert!(!terror_error_equal(Some(&te3), Some(&te4)));
}

// Go: TestLog — Log must accept any error (including nil) without panicking.
#[test]
fn log_accepts_any_error_without_panicking() {
    log(Some(&MessageError("xxx")));
    log(None);
    call(|| Err(MessageError("call error")));
    call(|| Ok::<(), MessageError>(()));
}

// Go: TestTraceAndLocation — a traced error's stack must name this call site.
#[test]
fn trace_and_location_stack_names_the_call_site() {
    fn inner() -> TerrorError {
        TerrorError::synthesize(TerrorClass::Parser, TerrorCode::new(123), "predefined error")
            .generate_with_stack("error message:abc")
    }
    let err = inner();
    let stack = err.stack().expect("GenWithStack must capture a stack");
    let rendered = stack.to_string();
    // Go asserts the stack mentions `terror_test.go`; the Rust equivalent is
    // this source file's module/function appearing in the backtrace.
    assert!(
        rendered.contains("tests_terror_go_parity")
            || rendered.contains("trace_and_location_stack_names_the_call_site"),
        "stack =\n{rendered}"
    );
}

// go-parity-gap: TestTraceAndLocation also asserts exact Go-runtime stack
// arithmetic (`len(lines) - 2*sysStack == 9` after filtering GOROOT lines).
// std::backtrace renders an implementation-defined format with no GOROOT
// concept, so that numeric assertion has no faithful Rust equivalent.
#[test]
#[ignore]
fn trace_and_location_go_runtime_line_arithmetic_is_not_portable()
{
    unreachable!("see go-parity-gap comment above")
}
