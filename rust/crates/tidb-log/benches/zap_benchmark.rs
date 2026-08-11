// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Transcreation of pinned `zap_benchmark_test.go::BenchmarkLog`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use tidb_log::{
    debug, get_level, init_test_logger, replace_globals, set_level, Config, Level, MemorySink,
};

fn main() {
    let sink = Arc::new(MemorySink::default());
    let config = Config {
        level: "info".into(),
        ..Config::default()
    };
    let (logger, _) = init_test_logger(sink, &config).unwrap();
    let restore = replace_globals(logger);
    set_level(Level::Info);
    assert_eq!(get_level(), Level::Info);

    let start = Instant::now();
    for _ in 0..1_000_000 {
        debug(black_box("test"), &[]);
    }
    eprintln!("BenchmarkLog: {:?}", start.elapsed());
    restore.restore();
}
