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

//! Direct class, RFC identity, conversion, and equality obligations from
//! `pkg/parser/terror/terror_test.go`.

use std::error::Error;
use std::fmt;
use std::process::Command;

use tidb_error::mysql::{errcode, errname, FormatArg};
use tidb_error::terror::{
    call, get_error_class, log, must_nil, register_error_class, register_finish,
    registration_frozen, terror_error_equal, TerrorClass, TerrorCode, TerrorError,
    CODE_MISS_CONNECTION_ID, CODE_RESULT_UNDETERMINED, ERR_CRITICAL, ERR_RESULT_UNDETERMINED,
};

#[derive(Debug)]
struct ContextError {
    message: &'static str,
    source: Box<dyn Error>,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.message, self.source)
    }
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
struct MessageError(&'static str);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for MessageError {}

#[test]
fn test_err_code_preserves_source_values() {
    assert_eq!(CODE_MISS_CONNECTION_ID.value(), 1);
    assert_eq!(CODE_RESULT_UNDETERMINED.value(), 2);
    assert_eq!(TerrorCode::new(isize::MIN).value(), isize::MIN);
    assert_eq!(TerrorCode::new(isize::MAX).value(), isize::MAX);
}

#[test]
fn test_error_class_catalog_is_complete_and_stable() {
    let expected = [
        (1, "autoid"),
        (2, "ddl"),
        (3, "domain"),
        (4, "evaluator"),
        (5, "executor"),
        (6, "expression"),
        (7, "admin"),
        (8, "kv"),
        (9, "meta"),
        (10, "planner"),
        (11, "parser"),
        (12, "perfschema"),
        (13, "privilege"),
        (14, "schema"),
        (15, "server"),
        (16, "structure"),
        (17, "variable"),
        (18, "xeval"),
        (19, "table"),
        (20, "types"),
        (21, "global"),
        (22, "mocktikv"),
        (23, "json"),
        (24, "tikv"),
        (25, "session"),
        (26, "plugin"),
        (27, "util"),
    ];
    assert_eq!(TerrorClass::ALL.len(), expected.len());
    for (class, (code, description)) in TerrorClass::ALL.into_iter().zip(expected) {
        assert_eq!(class.code(), code);
        assert_eq!(class.description(), description);
        assert_eq!(class.to_string(), description);
    }
    assert_eq!(
        TerrorClass::from_value(isize::MIN).to_string(),
        isize::MIN.to_string()
    );
    assert_eq!(
        TerrorClass::from_value(isize::MAX).to_string(),
        isize::MAX.to_string()
    );
    let unknown = TerrorError::synthesize(
        TerrorClass::from_value(isize::MAX),
        TerrorCode::new(7),
        "unknown",
    );
    assert_eq!(unknown.rfc_code(), ":7");
    assert_eq!(get_error_class(&unknown), None);
}

#[test]
fn test_dynamic_registration_duplicate_detection_and_class_lookup() {
    let dynamic_code = isize::MAX - 1;
    let class = register_error_class(dynamic_code, "test-dynamic");
    assert_eq!(class.code(), dynamic_code);
    assert_eq!(class.to_string(), "test-dynamic");
    let error = TerrorError::registered(class, TerrorCode::new(isize::MAX), "dynamic");
    assert_eq!(error.rfc_code(), format!("test-dynamic:{}", isize::MAX));
    assert_eq!(get_error_class(&error), Some(class));
    assert_eq!(error.to_sql_error().code, errcode::ErrUnknown);

    let duplicate = std::panic::catch_unwind(|| register_error_class(dynamic_code, "duplicate"));
    assert!(duplicate.is_err());
}

#[test]
fn freeze_registration_helper() {
    if std::env::var_os("TIDB_TERROR_FREEZE_HELPER").is_none() {
        return;
    }
    register_finish();
    assert!(registration_frozen());
    let blocked = std::panic::catch_unwind(|| {
        TerrorError::registered(TerrorClass::Parser, TerrorCode::new(777), "blocked")
    });
    assert!(blocked.is_err());
    let class = register_error_class(778, "class-after-freeze");
    assert_eq!(class.to_string(), "class-after-freeze");
}

#[test]
fn must_nil_helper() {
    let Some(marker) = std::env::var_os("TIDB_TERROR_MUST_NIL_MARKER") else {
        return;
    };
    let marker = std::path::PathBuf::from(marker);
    must_nil(
        Some(&MessageError("fatal")),
        [Box::new(move || {
            std::fs::write(marker, "closed").expect("cleanup marker must be written");
        }) as Box<dyn FnOnce()>],
    );
    unreachable!("MustNil must terminate the process after cleanup");
}

#[test]
fn test_register_finish_blocks_errors_without_polluting_other_tests() {
    let output = Command::new(std::env::current_exe().expect("test executable must exist"))
        .args(["--exact", "freeze_registration_helper", "--nocapture"])
        .env("TIDB_TERROR_FREEZE_HELPER", "1")
        .output()
        .expect("freeze helper must run");
    assert!(
        output.status.success(),
        "stdout =\n{}\nstderr =\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_must_nil_runs_cleanup_before_terminating() {
    let marker = std::env::temp_dir().join(format!(
        "tidb-terror-must-nil-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let output = Command::new(std::env::current_exe().expect("test executable must exist"))
        .args(["--exact", "must_nil_helper", "--nocapture"])
        .env("TIDB_TERROR_MUST_NIL_MARKER", &marker)
        .output()
        .expect("MustNil helper must run");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        std::fs::read_to_string(&marker).expect("cleanup marker must exist"),
        "closed"
    );
    std::fs::remove_file(marker).expect("cleanup marker must be removable");
}

#[test]
fn test_terror_identity_generation_class_and_sql_conversion() {
    let parser = TerrorError::registered(TerrorClass::Parser, TerrorCode::new(100), "error 100");
    let wrapped = ContextError {
        message: "trace",
        source: Box::new(parser.clone()),
    };
    assert!(TerrorClass::Parser.equal_class(Some(&wrapped)));
    assert!(!TerrorClass::Parser.not_equal_class(Some(&wrapped)));
    assert!(!TerrorClass::Optimizer.equal_class(Some(&wrapped)));
    assert!(!TerrorClass::Optimizer.equal_class(Some(&MessageError("abc"))));
    assert!(!TerrorClass::Optimizer.equal_class(None));

    let optimizer = TerrorError::registered(TerrorClass::Optimizer, TerrorCode::new(2), "abc");
    assert!(optimizer.equal(Some(&optimizer.generate("def"))));
    assert!(optimizer.equal(Some(&optimizer.fast_generate("def", &[]))));
    assert!(optimizer.equal(Some(
        &optimizer.fast_generate("def: %s", &[FormatArg::from("def")],)
    )));
    assert!(!optimizer.equal(None));
    assert!(!optimizer.equal(Some(&MessageError("abc"))));

    let duplicate = TerrorError::registered(TerrorClass::Kv, TerrorCode::new(1062), "key exists")
        .fast_generate(
            "Duplicate entry '%d' for key 'PRIMARY'",
            &[FormatArg::from(1_i32)],
        );
    assert_eq!(
        duplicate.to_string(),
        "[kv:1062]Duplicate entry '1' for key 'PRIMARY'"
    );
    let sql_error = duplicate.to_sql_error();
    assert_eq!(sql_error.code, 1062);
    assert_eq!(sql_error.message, "Duplicate entry '1' for key 'PRIMARY'");

    let truncated = TerrorError::registered_from_catalog(
        TerrorClass::Types,
        TerrorCode::new(
            isize::try_from(errcode::ErrTruncatedWrongValue)
                .expect("MySQL error code must fit the source int domain"),
        ),
    );
    assert_eq!(truncated.rfc_code(), "types:1292");
    assert_eq!(truncated.to_sql_error().code, 1292);
    assert_eq!(truncated.message(), errname::ErrTruncatedWrongValue.raw);

    let critical = ContextError {
        message: "trace",
        source: Box::new(ERR_CRITICAL.generate("critical error test")),
    };
    assert!(ERR_CRITICAL.equal(Some(&critical)));
    let traced_prototype = ContextError {
        message: "trace",
        source: Box::new(ERR_CRITICAL.clone()),
    };
    assert!(ERR_CRITICAL.equal(Some(&traced_prototype)));
}

#[test]
fn test_error_equal_follows_causes_and_prefers_rfc_identity() {
    let first = MessageError("test error");
    let second = MessageError("test error");
    let wrapped = ContextError {
        message: "trace",
        source: Box::new(MessageError("test error")),
    };
    assert!(terror_error_equal(Some(&first), Some(&second)));
    assert!(terror_error_equal(Some(&first), Some(&wrapped)));
    assert!(terror_error_equal(None, None));
    assert!(!terror_error_equal(Some(&first), None));

    let parser = TerrorError::synthesize(TerrorClass::Parser, TerrorCode::new(9001), "abc");
    let same_code_other_class =
        TerrorError::registered(TerrorClass::Kv, TerrorCode::new(9001), "abc");
    let other_code = TerrorError::registered(TerrorClass::Kv, TerrorCode::new(9002), "abc");
    assert!(!terror_error_equal(
        Some(&parser),
        Some(&same_code_other_class)
    ));
    assert!(!terror_error_equal(
        Some(&same_code_other_class),
        Some(&other_code)
    ));
    assert_eq!(parser.to_sql_error().code, errcode::ErrUnknown);
}

#[test]
fn test_json_compatibility_round_trips_pingcap_errors_shape() {
    let previous = TerrorError::compatible(TerrorCode::new(3), "json test");
    let json = serde_json::to_string(&previous).expect("compatible error must serialize");
    assert_eq!(
        json,
        r#"{"class":0,"code":3,"message":"json test","rfccode":""}"#
    );
    let current: TerrorError =
        serde_json::from_str(&json).expect("compatible error must deserialize");
    assert_eq!(previous, current);
    assert_eq!(current.to_string(), "[3]json test");

    let _parser_registration = TerrorError::registered(
        TerrorClass::Parser,
        TerrorCode::new(100),
        "registered parser",
    );
    let legacy: TerrorError =
        serde_json::from_str(r#"{"class":11,"code":100,"message":"legacy","rfccode":""}"#)
            .expect("legacy class JSON must deserialize");
    assert_eq!(legacy.rfc_code(), "parser:100");
    assert_eq!(get_error_class(&legacy), Some(TerrorClass::Parser));

    let structure = TerrorError::registered(
        TerrorClass::Structure,
        TerrorCode::new(200),
        "current structure",
    );
    assert_eq!(structure.to_sql_error().code, 200);
    let legacy_structure: TerrorError = serde_json::from_str(
        r#"{"class":16,"code":200,"message":"legacy structure","rfccode":""}"#,
    )
    .expect("legacy structure JSON must deserialize");
    assert_eq!(legacy_structure.rfc_code(), "struct:200");
    assert_eq!(get_error_class(&legacy_structure), None);
    assert_eq!(legacy_structure.to_sql_error().code, errcode::ErrUnknown);

    let escaped = TerrorError::compatible(TerrorCode::new(4), "quote: \"line\"\nnext");
    let escaped_json = serde_json::to_string(&escaped).expect("escaped error must serialize");
    let escaped_round_trip: TerrorError =
        serde_json::from_str(&escaped_json).expect("escaped error must deserialize");
    assert_eq!(escaped, escaped_round_trip);
}

#[test]
fn test_log_call_and_rust_native_stack_capture() {
    log(None);
    log(Some(&MessageError("xxx")));
    call(|| Err(MessageError("call error")));
    call(|| Ok::<(), MessageError>(()));

    let traced = TerrorError::synthesize(TerrorClass::Parser, TerrorCode::new(123), "prototype")
        .generate_with_stack("stacked");
    let stack = traced
        .stack()
        .expect("GenWithStack must capture a backtrace");
    let rendered = stack.to_string();
    assert!(
        rendered.contains("test_log_call_and_rust_native_stack_capture")
            || rendered.contains("terror_source"),
        "stack =\n{rendered}"
    );
}

#[test]
fn global_prototypes_share_the_authoritative_rfc_identity() {
    assert_eq!(ERR_CRITICAL.rfc_code(), "global:3");
    assert_eq!(ERR_RESULT_UNDETERMINED.rfc_code(), "global:2");
    assert_eq!(
        ERR_RESULT_UNDETERMINED.message(),
        "execution result undetermined"
    );
    let synthesized = TerrorError::synthesize(
        TerrorClass::Global,
        CODE_RESULT_UNDETERMINED,
        "consumer-specific context",
    );
    assert_eq!(synthesized.to_sql_error().code, 2);
}
