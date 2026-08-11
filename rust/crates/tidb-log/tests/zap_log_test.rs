// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Transcreation of pinned `zap_log_test.go`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tidb_log::{
    init_logger, init_test_logger, Config, Field, FileLogConfig, Level, LoggerOptions, MemorySink,
    Value,
};

fn temp_dir(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("tidb-log-{name}-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn file_config(path: &Path) -> Config {
    Config {
        level: "info".into(),
        file: FileLogConfig {
            filename: path.join("test.log").to_string_lossy().into_owned(),
            max_size: 1,
            ..FileLogConfig::default()
        },
        ..Config::default()
    }
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(condition(), "condition was not met before timeout");
}

#[test]
fn test_log() {
    let sink = Arc::new(MemorySink::default());
    let cfg = Config {
        level: "debug".into(),
        disable_timestamp: true,
        ..Config::default()
    };
    let (logger, _) = init_test_logger(sink.clone(), &cfg).unwrap();
    let sugar = logger.sugar();
    sugar.infow(
        "failed to fetch URL",
        &[
            Field::new("url", Value::Str("http://example.com".into())),
            Field::new("attempt", Value::I64(3)),
            Field::new("backoff", Value::Duration(1_000_000_000)),
        ],
    );
    sugar.infof(format_args!(
        "failed to \"fetch\" [URL]: {}",
        "http://example.com"
    ));
    sugar.debugw(
        "Slow query",
        &[
            Field::new(
                "sql",
                Value::Str("SELECT * FROM TABLE\n\tWHERE ID=\"abc\"".into()),
            ),
            Field::new("duration", Value::Duration(1_300_000_000)),
            Field::new("process keys", Value::I64(1500)),
        ],
    );
    sugar.info("Welcome");
    sugar.info("Welcome TiDB");
    sugar.info("欢迎");
    sugar.info("欢迎来到 TiDB");
    sugar.warnw(
        "Type",
        &[
            Field::new("Counter", Value::F64(f64::NAN)),
            Field::new("Score", Value::F64(f64::INFINITY)),
        ],
    );
    logger
        .with_fields(&[
            Field::new("connID", Value::Str("1".into())),
            Field::new("traceID", Value::Str("dse1121".into())),
        ])
        .info("new connection", &[]);
    logger.info(
        "Testing typs",
        &[
            Field::new("filed1", Value::Str("noquote".into())),
            Field::new("filed2", Value::Str("in quote".into())),
            Field::new(
                "urls",
                Value::Array(vec![
                    Value::Str("http://mock1.com:2347".into()),
                    Value::Str("http://mock2.com:2432".into()),
                ]),
            ),
            Field::new(
                "urls-peer",
                Value::Array(vec![Value::Str("t1".into()), Value::Str("t2 fine".into())]),
            ),
            Field::new(
                "store ids",
                Value::Array(vec![Value::U64(1), Value::U64(4), Value::U64(5)]),
            ),
            Field::new(
                "object",
                Value::Object(vec![Field::new("username", Value::Str("user1".into()))]),
            ),
            Field::new(
                "object2",
                Value::Object(vec![Field::new("username", Value::Str("user 2".into()))]),
            ),
            Field::new("binary", Value::Binary(b"ab123".to_vec())),
            Field::new("is processed", Value::Bool(true)),
            Field::new("bytestring", Value::ByteString(b"noquote".to_vec())),
            Field::new("bytestring", Value::ByteString(b"in quote".to_vec())),
            Field::new("int8", Value::I64(1)),
            Field::new("ptr", Value::U64(0xa)),
            Field::new("reflect", Value::Reflect("[1,2]".into())),
            Field::new("stringer", Value::Str("127.0.0.1".into())),
            Field::new("array bools", Value::Array(vec![Value::Bool(true)])),
            Field::new(
                "array bools",
                Value::Array(vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                ]),
            ),
            Field::new(
                "complex128",
                Value::Complex {
                    real: 1.0,
                    imag: 2.0,
                },
            ),
            Field::new(
                "test",
                Value::Array(vec![
                    Value::Str("💖".into()),
                    Value::Str("�".into()),
                    Value::Str("☺☻☹".into()),
                    Value::Str("日a本b語ç日ð本Ê語þ日¥本¼語i日©".into()),
                    Value::Str(
                        "日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©"
                            .into(),
                    ),
                    Value::ByteString(vec![0x80, 0x80, 0x80, 0x80]),
                    Value::Str("<car><mirror>XML</mirror></car>".into()),
                ]),
            ),
            Field::new("duration", Value::Duration(10_000_000_000)),
        ],
    );
    let output = sink.string();
    for fragment in [
        "failed to fetch URL",
        "url=http://example.com",
        "attempt=3",
        "backoff=1s",
        "failed to \\\"fetch\\\" [URL]: http://example.com",
        "sql=\"SELECT * FROM TABLE\\n\\tWHERE ID=\\\"abc\\\"\"",
        "duration=1.3s",
        "\"process keys\"=1500",
        "[Welcome]",
        "[\"Welcome TiDB\"]",
        "[欢迎]",
        "[\"欢迎来到 TiDB\"]",
        "[Type] [Counter=NaN] [Score=+Inf]",
        "connID=1",
        "traceID=dse1121",
        "filed1=noquote",
        "filed2=\"in quote\"",
        "urls=\"[http://mock1.com:2347,http://mock2.com:2432]\"",
        "urls-peer=\"[t1,\\\"t2 fine\\\"]\"",
        "\"store ids\"=\"[1,4,5]\"",
        "object=\"{username=user1}\"",
        "object2=\"{username=\\\"user 2\\\"}\"",
        "binary=\"YWIxMjM=\"",
        "\"is processed\"=true",
        "bytestring=noquote",
        "bytestring=\"in quote\"",
        "int8=1",
        "ptr=10",
        "reflect=\"[1,2]\"",
        "stringer=127.0.0.1",
        "\"array bools\"=\"[true]\"",
        "\"array bools\"=\"[true,true,false]\"",
        "complex128=1+2i",
        "test=\"[💖,�,☺☻☹",
        "\\\\ufffd\\\\ufffd\\\\ufffd\\\\ufffd",
        "<car><mirror>XML</mirror></car>",
        "duration=10s",
    ] {
        assert!(
            output.contains(fragment),
            "missing fragment: {fragment}\n{output}"
        );
    }

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sugar.panic("unknown");
    }));
    let payload = panic.unwrap_err();
    assert_eq!(payload.downcast_ref::<String>().unwrap(), "unknown");
}

#[test]
fn test_rotate_log() {
    let dir = temp_dir("rotate");
    let cfg = file_config(&dir);
    let (logger, _) = init_logger(&cfg).unwrap();
    let mut data = String::new();
    for i in 1..=1024 * 1024 {
        if i % 1000 != 0 {
            data.push('d');
            continue;
        }
        logger.info(&data, &[]);
        data.clear();
    }
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 2);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn test_buffered_log() {
    let dir = temp_dir("buffered");
    let mut cfg = file_config(&dir);
    cfg.file.max_size = 10;
    cfg.file.is_buffered = true;
    cfg.file.buffer_size = 1024;
    cfg.file.buffer_flush_interval = 3_000_000_000;
    let (logger, _) = init_logger(&cfg).unwrap();
    logger.info("first message", &[]);
    assert!(!Path::new(&cfg.file.filename).exists());
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(&cfg.file.filename)
            .map(|s| s.contains("first message"))
            .unwrap_or(false)
    });

    let long = "x".repeat(200);
    for _ in 0..20 {
        logger.info(
            "big message",
            &[Field::new("msg", Value::Str(long.clone()))],
        );
    }
    wait_until(Duration::from_secs(1), || {
        fs::read_to_string(&cfg.file.filename)
            .map(|s| s.contains("big message"))
            .unwrap_or(false)
    });
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn test_buffered_log_with_rotate() {
    let dir = temp_dir("buffered-rotate");
    let mut cfg = file_config(&dir);
    cfg.file.is_buffered = true;
    cfg.file.buffer_size = 4096;
    cfg.file.buffer_flush_interval = 5_000_000_000;
    let (logger, _) = init_logger(&cfg).unwrap();
    let large = "x".repeat(200 * 1024);
    for _ in 0..10 {
        logger.info(
            "rotating message",
            &[Field::new("msg", Value::Str(large.clone()))],
        );
    }
    wait_until(Duration::from_secs(5), || {
        fs::read_dir(&dir)
            .map(|it| it.count() >= 2)
            .unwrap_or(false)
    });
    let total: u64 = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    assert!(total > 1024 * 1024);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn test_error_log() {
    let sink = Arc::new(MemorySink::default());
    let cfg = Config {
        level: "debug".into(),
        disable_timestamp: true,
        ..Config::default()
    };
    let (logger, _) = init_test_logger(sink.clone(), &cfg).unwrap();
    logger.error(
        "",
        &[Field::new(
            "err",
            Value::Error {
                basic: "log-stack-test".into(),
                verbose: Some("log-stack-test\nstack".into()),
            },
        )],
    );
    assert!(sink.string().contains("[err=log-stack-test]"));
    assert!(sink.string().contains("errVerbose="));
}

#[test]
fn test_with_options() {
    let sink = Arc::new(MemorySink::default());
    let cfg = Config {
        level: "debug".into(),
        disable_timestamp: true,
        disable_error_verbose: true,
        ..Config::default()
    };
    let (logger, _) = init_test_logger(sink.clone(), &cfg).unwrap();
    logger
        .with_options(LoggerOptions::default().with_stacktrace_level(Level::Fatal))
        .error(
            "Testing",
            &[Field::new(
                "error",
                Value::Error {
                    basic: "log-with-option".into(),
                    verbose: Some("log-with-option\nstack".into()),
                },
            )],
        );
    assert!(!sink.string().contains("errorVerbose"));
    assert!(!sink.string().contains("[stack="));
}

#[test]
fn test_log_json() {
    let sink = Arc::new(MemorySink::default());
    let cfg = Config {
        level: "debug".into(),
        format: "json".into(),
        disable_timestamp: true,
        ..Config::default()
    };
    let (logger, _) = init_test_logger(sink.clone(), &cfg).unwrap();
    logger.info(
        "failed to fetch URL",
        &[
            Field::new("url", Value::Str("http://example.com".into())),
            Field::new("attempt", Value::I64(3)),
            Field::new("backoff", Value::Duration(1_000_000_000)),
        ],
    );
    let value: serde_json::Value = serde_json::from_str(&sink.last_line().unwrap()).unwrap();
    assert_eq!(value["level"], "INFO");
    assert_eq!(value["message"], "failed to fetch URL");
    assert_eq!(value["url"], "http://example.com");
    assert_eq!(value["attempt"], 3);
    assert_eq!(value["backoff"], "1s");
}

#[test]
fn test_rotate_log_with_compress() {
    let dir = temp_dir("compress");
    let mut cfg = file_config(&dir);
    cfg.file.compression = "gzip".into();
    let (logger, _) = init_logger(&cfg).unwrap();
    let mut data = String::new();
    for i in 1..=1024 * 1024 {
        if i % 1000 != 0 {
            data.push('d');
            continue;
        }
        logger.info(&data, &[]);
        data.clear();
    }
    let files: Vec<_> = fs::read_dir(&dir).unwrap().map(Result::unwrap).collect();
    assert_eq!(files.len(), 2);
    assert!(files
        .iter()
        .any(|file| file.path().extension().is_some_and(|ext| ext == "gz")));
    assert!(files
        .iter()
        .all(|file| file.metadata().unwrap().len() < 512 * 1024));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn test_compress_error() {
    let dir = temp_dir("compress-error");
    let mut cfg = file_config(&dir);
    cfg.file.compression = "xxx".into();
    assert!(init_logger(&cfg)
        .unwrap_err()
        .contains("can't set compression"));
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn test_log_file_no_permission() {
    use std::os::unix::fs::PermissionsExt;

    let dir = temp_dir("permissions");
    let no_perm = dir.join("noperm-dir");
    fs::create_dir(&no_perm).unwrap();
    let cfg = file_config(&no_perm);
    init_logger(&cfg).unwrap();
    fs::set_permissions(&no_perm, fs::Permissions::from_mode(0o0)).unwrap();
    assert!(init_logger(&cfg).unwrap_err().contains("permission denied"));
    fs::set_permissions(&no_perm, fs::Permissions::from_mode(0o755)).unwrap();

    let read_only = dir.join("readonly-dir");
    fs::create_dir(&read_only).unwrap();
    fs::set_permissions(&read_only, fs::Permissions::from_mode(0o555)).unwrap();
    let cfg = file_config(&read_only);
    assert!(init_logger(&cfg).unwrap_err().contains("permission denied"));
    fs::set_permissions(&read_only, fs::Permissions::from_mode(0o755)).unwrap();

    let as_file = dir.join("dir-as-log");
    fs::create_dir(&as_file).unwrap();
    let mut cfg = file_config(&dir);
    cfg.file.filename = as_file.to_string_lossy().into_owned();
    assert!(init_logger(&cfg)
        .unwrap_err()
        .contains("can't use directory as log file name"));

    let read_only_file = dir.join("readonly.log");
    fs::File::create(&read_only_file).unwrap();
    fs::set_permissions(&read_only_file, fs::Permissions::from_mode(0o444)).unwrap();

    let nested = dir.join("nested/path/to");
    let cfg = file_config(&nested);
    init_logger(&cfg).unwrap();
    assert!(nested.exists());
    fs::remove_dir_all(dir).unwrap();
}
