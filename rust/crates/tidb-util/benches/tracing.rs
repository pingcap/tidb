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

//! Executable translations of the four `pkg/util/tracing` benchmarks.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_util::tracing::{child_span_from_context, span_from_context, Span, TraceContext, Tracer};

const SAMPLE_WINDOW: Duration = Duration::from_millis(100);

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    let mut iterations = 0_u64;
    while started.elapsed() < SAMPLE_WINDOW {
        operation();
        iterations += 1;
    }
    println!(
        "{name}: {:?} across {iterations} iterations",
        started.elapsed()
    );
}

trait NoopLogKv {
    fn log_kv(&self, key: &str, value: &str);
}

impl NoopLogKv for Span {
    fn log_kv(&self, key: &str, value: &str) {
        black_box((self, key, value));
    }
}

fn main() {
    let span = Arc::new(Tracer::noop()).start_span("DefaultSpan");
    measure("BenchmarkNoopLogKV", || {
        span.log_kv("event", "noop is finished");
    });

    measure("BenchmarkNoopLogKVWithF", || {
        let value = format!("this is format {}", "noop is finished");
        span.log_kv("event", black_box(&value));
    });

    let context = TraceContext::background();
    measure("BenchmarkSpanFromContext", || {
        black_box(span_from_context(&context));
    });
    measure("BenchmarkChildFromContext", || {
        black_box(child_span_from_context(&context, "child"));
    });
}
