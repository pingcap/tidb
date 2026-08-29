// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Executable translations of both `pkg/util/traceevent` benchmarks.

use std::hint::black_box;
use std::time::{Duration, Instant};

use tidb_log::{Field, Value};
use tidb_util::traceevent::{
    enable, get_flight_recorder, set_mode, start_log_flight_recorder, trace_event,
    FlightRecorderConfig, MODE_FULL, MODE_OFF, TXN_LIFECYCLE,
};
use tidb_util::tracing::TraceContext;

const SAMPLE_WINDOW: Duration = Duration::from_millis(100);

fn measure(name: &str, mut operation: impl FnMut(u64)) {
    let started = Instant::now();
    let mut iterations = 0_u64;
    while started.elapsed() < SAMPLE_WINDOW {
        operation(iterations);
        iterations += 1;
    }
    println!(
        "{name}: {:?} across {iterations} iterations",
        started.elapsed()
    );
}

fn configuration() -> FlightRecorderConfig {
    let mut config = FlightRecorderConfig::default();
    config.initialize();
    config.enabled_categories = vec!["*".to_owned()];
    config
}

fn run(name: &str, mode: &str) {
    start_log_flight_recorder(configuration()).unwrap();
    enable(TXN_LIFECYCLE);
    set_mode(mode).unwrap();
    let context = TraceContext::background();
    measure(name, |iteration| {
        trace_event(
            &context,
            TXN_LIFECYCLE,
            if mode == MODE_OFF {
                "benchmark-disabled"
            } else {
                "benchmark-enabled"
            },
            vec![
                Field::new("key", Value::Str("value".to_owned())),
                Field::new("iteration", Value::I64(iteration as i64)),
            ],
        );
        black_box(());
    });
    get_flight_recorder().unwrap().close();
}

fn main() {
    run("BenchmarkTraceEventDisabled", MODE_OFF);
    run("BenchmarkTraceEventEnabled", MODE_FULL);
}
