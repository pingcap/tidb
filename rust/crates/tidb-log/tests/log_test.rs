// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Transcreation of pinned `log_test.go`.

use std::io;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use tidb_log::{
    debug, error, info, init_test_logger, l, lock_with_timeout, replace_globals, s, warn, with,
    Config, Field, LoggerOptions, MemorySink, Value, WriteSyncer, ZAP_ENCODING_NAME,
};

fn global_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn test_config() -> Config {
    Config {
        level: "debug".into(),
        disable_timestamp: true,
        ..Config::default()
    }
}

#[test]
fn test_export() {
    let _guard = global_test_lock();
    let sink = Arc::new(MemorySink::default());
    let (logger, _) = init_test_logger(sink.clone(), &test_config()).unwrap();
    let restore = replace_globals(logger);

    info("Testing", &[]);
    debug("Testing", &[]);
    warn("Testing", &[]);
    error("Testing", &[]);
    assert!(sink.string().contains("log_test.rs:"));

    l().info("Testing", &[]);
    assert!(sink.string().contains("log_test.rs:"));
    s().info("Testing");
    assert!(sink.string().contains("log_test.rs:"));
    l().with_options(LoggerOptions::default().with_caller_skip(1))
        .info("Testing", &[]);
    assert!(!sink.last_line().unwrap().contains("log_test.rs:"));

    let child = with(&[
        Field::new("name", Value::Str("tester".into())),
        Field::new("age", Value::I64(42)),
    ]);
    child.info("hello", &[]);
    child.debug("world", &[]);
    assert!(sink.string().contains("name=tester"));
    assert!(sink.string().contains("age=42"));
    restore.restore();
}

#[test]
fn test_replace_globals() {
    let _guard = global_test_lock();
    let first = Arc::new(MemorySink::default());
    let (first_logger, _) = init_test_logger(first.clone(), &test_config()).unwrap();
    let initial_restore = replace_globals(first_logger);
    info("foo_1", &[]);

    let second = Arc::new(MemorySink::default());
    let (second_logger, _) = init_test_logger(second.clone(), &test_config()).unwrap();
    let restore = replace_globals(second_logger);
    info("foo_2", &[]);
    assert!(!first.string().contains("foo_2"));
    assert!(second.string().contains("foo_2"));

    restore.restore();
    info("foo_3", &[]);
    assert!(first.string().contains("foo_3"));
    assert!(!second.string().contains("foo_3"));
    initial_restore.restore();
}

#[test]
fn test_zap_text_encoder() {
    let sink = Arc::new(MemorySink::default());
    let cfg = Config {
        level: "debug".into(),
        disable_timestamp: true,
        disable_caller: true,
        disable_stacktrace: true,
        ..Config::default()
    };
    let (logger, _) = init_test_logger(sink.clone(), &cfg).unwrap();
    logger.info("this is a message from zap", &[]);
    assert_eq!(sink.string(), "[INFO] [\"this is a message from zap\"]\n");
}

#[test]
fn test_registered_text_encoder() {
    assert_eq!(ZAP_ENCODING_NAME, "pingcap-log");
    let cfg = Config {
        format: "text".into(),
        ..Config::default()
    };
    assert!(tidb_log::TextEncoder::new(&cfg).is_ok());
}

#[derive(Default)]
struct HangingSink {
    entered: (Mutex<bool>, Condvar),
}

impl WriteSyncer for HangingSink {
    fn write(&self, _bytes: &[u8]) -> io::Result<usize> {
        let (lock, ready) = &self.entered;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        std::thread::park();
        Ok(0)
    }
}

#[test]
fn test_timeout() {
    let sink = Arc::new(MemorySink::default());
    let wrapped = lock_with_timeout(sink.clone(), 3);
    wrapped.write(b"abc").unwrap();
    wrapped.sync().unwrap();
    assert!(sink.string().contains("abc"));

    let hanging = Arc::new(HangingSink::default());
    let wrapped = lock_with_timeout(hanging.clone(), 3);
    let blocked = wrapped.clone();
    std::thread::spawn(move || {
        let _ = blocked.write(b"abc");
    });
    let (lock, ready) = &hanging.entered;
    let mut entered = lock.lock().unwrap();
    while !*entered {
        entered = ready.wait(entered).unwrap();
    }
    drop(entered);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = wrapped.write(b"abc");
    }));
    assert!(panic.is_err());
}
