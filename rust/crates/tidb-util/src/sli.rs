// Copyright 2021 PingCAP, Inc.
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

//! Transaction write-throughput SLI accounting from `pkg/util/sli`.

use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use prometheus::{exponential_buckets, Histogram, HistogramOpts};

const SMALL_TXN_AFFECT_ROW: u64 = 20;
const SMALL_TXN_SIZE: i64 = 1024 * 1024;

/// Reports transaction write-throughput metrics for SLI.
#[derive(Default)]
pub struct TxnWriteThroughputSli {
    invalid: bool,
    affect_row: u64,
    write_size: i64,
    read_keys: i64,
    write_keys: i64,
    write_time: Duration,
}

impl TxnWriteThroughputSli {
    /// Records the cost of a write statement and reports the transaction when
    /// the statement leaves the transaction.
    pub fn finish_execute_stmt(&mut self, cost: Duration, affect_row: u64, in_txn: bool) {
        if affect_row > 0 {
            self.write_time += cost;
            self.affect_row = self.affect_row.wrapping_add(affect_row);
        }

        if !in_txn {
            if affect_row == 0 {
                self.write_time += cost;
            }
            self.report_metric();
            self.reset();
        }
    }

    /// Adds the read keys.
    pub fn add_read_keys(&mut self, read_keys: i64) {
        self.read_keys = self.read_keys.wrapping_add(read_keys);
    }

    /// Adds the transaction write size and keys.
    pub fn add_txn_write_size(&mut self, size: i64, keys: i64) {
        self.write_size = self.write_size.wrapping_add(size);
        self.write_keys = self.write_keys.wrapping_add(keys);
    }

    fn report_metric(&self) {
        if self.is_invalid() {
            return;
        }
        if self.is_small_txn() {
            small_txn_write_duration().observe(self.write_time.as_secs_f64());
        } else {
            #[expect(clippy::cast_precision_loss, reason = "Go converts int to float64")]
            txn_write_throughput().observe(self.write_size as f64 / self.write_time.as_secs_f64());
        }
    }

    /// Marks this transaction invalid for SLI reporting.
    pub fn set_invalid(&mut self) {
        self.invalid = true;
    }

    /// Whether this transaction is invalid for SLI reporting.
    #[must_use]
    pub fn is_invalid(&self) -> bool {
        self.invalid
            || self.read_keys > self.write_keys
            || self.write_size == 0
            || self.write_time.is_zero()
    }

    /// Whether this is a small transaction.
    #[must_use]
    pub fn is_small_txn(&self) -> bool {
        self.affect_row <= SMALL_TXN_AFFECT_ROW && self.write_size <= SMALL_TXN_SIZE
    }

    /// Resets all accumulated transaction state.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for TxnWriteThroughputSli {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid: {}, affectRow: {}, writeSize: {}, readKeys: {}, writeKeys: {}, writeTime: {}",
            self.invalid,
            self.affect_row,
            self.write_size,
            self.read_keys,
            self.write_keys,
            format_go_duration(self.write_time),
        )
    }
}

fn small_txn_write_duration() -> &'static Histogram {
    static METRIC: OnceLock<Histogram> = OnceLock::new();
    METRIC.get_or_init(|| {
        new_registered_histogram(
            HistogramOpts::new(
                "small_txn_write_duration_seconds",
                "Bucketed histogram of small transaction write time (s).",
            )
            .namespace("tidb")
            .subsystem("sli")
            .buckets(exponential_buckets(0.001, 2.0, 28).expect("valid SLI metric buckets")),
        )
    })
}

fn txn_write_throughput() -> &'static Histogram {
    static METRIC: OnceLock<Histogram> = OnceLock::new();
    METRIC.get_or_init(|| {
        new_registered_histogram(
            HistogramOpts::new(
                "txn_write_throughput",
                "Bucketed histogram of transaction write throughput (bytes/second).",
            )
            .namespace("tidb")
            .subsystem("sli")
            .buckets(exponential_buckets(64.0, 1.3, 40).expect("valid SLI metric buckets")),
        )
    })
}

fn new_registered_histogram(options: HistogramOpts) -> Histogram {
    let histogram = Histogram::with_opts(options).expect("valid SLI metric options");
    prometheus::default_registry()
        .register(Box::new(histogram.clone()))
        .expect("SLI metric is registered once");
    histogram
}

fn format_go_duration(duration: Duration) -> String {
    let total = duration.as_nanos();
    if total == 0 {
        return "0s".to_owned();
    }
    if total < 1_000_000_000 {
        let (scale, precision, unit) = if total < 1_000 {
            (1, 0, "ns")
        } else if total < 1_000_000 {
            (1_000, 3, "µs")
        } else {
            (1_000_000, 6, "ms")
        };
        let mut output = (total / scale).to_string();
        push_fraction(&mut output, total % scale, precision);
        output.push_str(unit);
        return output;
    }
    let seconds = total / 1_000_000_000;
    let mut tail = (seconds % 60).to_string();
    push_fraction(&mut tail, total % 1_000_000_000, 9);
    tail.push('s');
    let minutes = seconds / 60;
    if minutes == 0 {
        return tail;
    }
    let hours = minutes / 60;
    if hours == 0 {
        return format!("{minutes}m{tail}");
    }
    format!("{hours}h{}m{tail}", minutes % 60)
}

fn push_fraction(output: &mut String, fraction: u128, precision: usize) {
    if fraction == 0 || precision == 0 {
        return;
    }
    let mut digits = format!("{fraction:0precision$}");
    while digits.ends_with('0') {
        digits.pop();
    }
    if !digits.is_empty() {
        output.push('.');
        output.push_str(&digits);
    }
}
