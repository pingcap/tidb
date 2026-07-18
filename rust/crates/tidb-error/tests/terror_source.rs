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

use tidb_error::mysql::{errcode, errname, FormatArg};
use tidb_error::terror::{
    terror_error_equal, TerrorClass, TerrorCode, TerrorError, CODE_MISS_CONNECTION_ID,
    CODE_RESULT_UNDETERMINED, ERR_CRITICAL, ERR_RESULT_UNDETERMINED,
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

    let truncated = TerrorError::registered_standard(
        TerrorClass::Types,
        TerrorCode::new(i32::from(errcode::ErrTruncatedWrongValue)),
        errname::ErrTruncatedWrongValue,
    );
    assert_eq!(truncated.rfc_code(), "types:1292");
    assert_eq!(truncated.to_sql_error().code, 1292);

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
