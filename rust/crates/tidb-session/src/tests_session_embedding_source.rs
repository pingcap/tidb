// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Source-derived regressions for Go's `pkg/sessionctx/variable` embedding
//! settings.  The tests exercise the actual Rust system-variable registry and
//! GLOBAL write path rather than a test-only map.

use crate::embedding;
use crate::sysvar::{get_sys_var, VarType};
use crate::tests_support::session_with_privileges;
use crate::vars::GlobalSysvars;
use crate::{Datum, Session, StmtResult};
use std::sync::{Mutex, OnceLock};

fn embedding_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn one(session: &mut Session, sql: &str) -> String {
    match session.run(sql).expect(sql) {
        StmtResult::Rows(rows) => match rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
        {
            Some(Datum::Bytes(value)) => String::from_utf8(value).expect("UTF-8 scalar"),
            Some(Datum::String(value)) => String::from_utf8_lossy(value.bytes()).into_owned(),
            Some(value) => panic!("expected string scalar, got {value:?}"),
            None => String::new(),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Go `embedding_vars_test.go::TestNormalizeOpenAIEmbeddingAPIBase`.
#[test]
fn normalize_openai_embedding_api_base_matches_go() {
    let cases = [
        ("", ""),
        ("https://api.openai.com/v1/", "https://api.openai.com/v1"),
        (
            "https://api.openai.com/v1/embeddings",
            "https://api.openai.com/v1",
        ),
        (
            "https://my-resource.openai.azure.com:8443/openai/v1",
            "https://my-resource.openai.azure.com:8443/openai/v1",
        ),
        (
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
        ),
        (
            "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
            "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
        ),
        (
            "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
            "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
        ),
        // net/url keeps credentials in URL.User but Go rebuilds from u.Host,
        // so user-info must not leak into the normalized endpoint.
        (
            "https://user:secret@api.openai.com/v1",
            "https://api.openai.com/v1",
        ),
        ("HTTPS://API.OPENAI.COM/v1", "https://API.OPENAI.COM/v1"),
    ];
    for (input, expected) in cases {
        assert_eq!(
            embedding::normalize_openai_embedding_api_base(input).as_deref(),
            Ok(expected)
        );
    }
    for (input, expected) in [
        (
            "https://example.com/v1",
            embedding::OPENAI_ENDPOINT_WHITELIST_ERR_MSG,
        ),
        (
            "https://example.azure.com/openai/v1",
            embedding::OPENAI_ENDPOINT_WHITELIST_ERR_MSG,
        ),
        ("https://api.openai.com/v1?foo=bar", "query parameters"),
        ("http://api.openai.com/v1", "only https scheme is supported"),
        ("/v1", "absolute https URL is required"),
    ] {
        let error = embedding::normalize_openai_embedding_api_base(input).unwrap_err();
        assert!(error.contains(expected), "{input}: {error}");
    }
}

/// Go `embedding_vars_test.go::TestGetOpenAIEmbeddingBaseURL`.
#[test]
fn openai_embedding_global_write_normalizes_and_versions() {
    let _lock = embedding_test_lock();
    let mut session = session_with_privileges();
    session.attach_globals(GlobalSysvars::new()).unwrap();
    let before = embedding::config_version();

    session
        .run("SET GLOBAL tidb_exp_embed_openai_api_base = 'https://api.openai.com/v2/'")
        .unwrap();
    assert_eq!(
        one(
            &mut session,
            "SELECT @@global.tidb_exp_embed_openai_api_base"
        ),
        "https://api.openai.com/v2"
    );
    assert_eq!(embedding::config_version(), before + 1);

    // The normalized value is idempotent, so the cache generation does not
    // churn when a caller repeats an equivalent endpoint.
    session
        .run("SET GLOBAL tidb_exp_embed_openai_api_base = 'https://api.openai.com/v2'")
        .unwrap();
    assert_eq!(embedding::config_version(), before + 1);

    session
        .run("SET GLOBAL tidb_exp_embed_openai_api_base = DEFAULT")
        .unwrap();
    assert_eq!(
        one(
            &mut session,
            "SELECT @@global.tidb_exp_embed_openai_api_base"
        ),
        "https://api.openai.com/v1"
    );
}

/// Go `embedding_vars_test.go::TestEmbeddingAPIKeySysVars`.
#[test]
fn embedding_api_keys_are_masked_and_versioned() {
    let _lock = embedding_test_lock();
    let mut session = session_with_privileges();
    session.attach_globals(GlobalSysvars::new()).unwrap();
    let key_names = [
        "tidb_exp_embed_jina_ai_api_key",
        "tidb_exp_embed_openai_api_key",
        "tidb_exp_embed_cohere_api_key",
        "tidb_exp_embed_huggingface_api_key",
        "tidb_exp_embed_nvidia_nim_api_key",
        "tidb_exp_embed_gemini_api_key",
    ];
    for name in key_names {
        let before = embedding::config_version();
        let sql = format!("SET GLOBAL {name} = 'secret-1234567890'");
        session.run(&sql).unwrap();
        let sql = format!("SELECT @@global.{name}");
        assert_eq!(one(&mut session, &sql), "******7890");
        assert_eq!(
            embedding::embedding_api_key(name).as_deref(),
            Some("secret-1234567890")
        );
        assert_eq!(embedding::config_version(), before + 1);

        let sql = format!("SET GLOBAL {name} = 'secret-1234567890'");
        session.run(&sql).unwrap();
        assert_eq!(embedding::config_version(), before + 1);

        let sql = format!("SET GLOBAL {name} = DEFAULT");
        session.run(&sql).unwrap();
        assert_eq!(embedding::embedding_api_key(name).as_deref(), Some(""));
    }
    assert_eq!(embedding::mask_embedding_api_key(""), "");
    assert_eq!(embedding::mask_embedding_api_key("short"), "******");
    assert_eq!(
        embedding::mask_embedding_api_key("1234567890"),
        "******7890"
    );
}

/// Current Go master added these registrations after the original Rust
/// catalog snapshot. Keep the registry/count and the source defaults aligned
/// so a new variable cannot silently become "unknown" at SET time.
#[test]
fn current_go_master_variable_additions_are_registered() {
    for (name, scope, value, var_type) in [
        ("tidb_analyze_store_batch_size", 3, "4", VarType::Unsigned),
        ("tidb_enable_connection_event_log", 1, "OFF", VarType::Bool),
        ("tidb_enable_full_outer_join", 3, "OFF", VarType::Bool),
        ("tidb_enable_txn_file", 3, "OFF", VarType::Bool),
        (
            "tidb_plan_replayer_file_retention_time",
            1,
            "168h0m0s",
            VarType::Duration,
        ),
        ("tidb_txn_file_min_mutation_size", 3, "0", VarType::Unsigned),
        (
            "tidb_exp_embed_openai_api_base",
            1,
            "https://api.openai.com/v1",
            VarType::Str,
        ),
    ] {
        let definition = get_sys_var(name).unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(
            (definition.scope, definition.value, definition.var_type),
            (scope, value, var_type)
        );
    }
    for name in [
        "tidb_exp_embed_cohere_api_key",
        "tidb_exp_embed_gemini_api_key",
        "tidb_exp_embed_huggingface_api_key",
        "tidb_exp_embed_jina_ai_api_key",
        "tidb_exp_embed_nvidia_nim_api_key",
        "tidb_exp_embed_openai_api_key",
    ] {
        let definition = get_sys_var(name).unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(
            (definition.scope, definition.value, definition.var_type),
            (1, "", VarType::Str)
        );
    }
    let mutation_size = get_sys_var("tidb_txn_file_min_mutation_size").unwrap();
    assert!(mutation_size.validate("1").is_err());
    assert_eq!(mutation_size.validate("1048576").unwrap().value, "1048576");
}
