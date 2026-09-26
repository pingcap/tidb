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

//! Go clientConn.addQueryMetrics command counters.
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::LazyLock;

thread_local! {
    static COMMAND_FAILED: Cell<bool> = const { Cell::new(false) };
    static GROUPS: RefCell<HashMap<String, Rc<GroupMetrics>>> = RefCell::default();
    /// The resource group the CURRENT connection last ran a command in, on
    /// the worker serving that connection. Go `clientConn` moves
    /// `ConnGauge` between groups when a statement switches groups
    /// (`conn.go:461-470`); the same worker-local value decides which label
    /// the connection's end decrements.
    static CONNECTION_RESOURCE_GROUP: RefCell<String> = RefCell::new(String::from("default"));
}

static QUERIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let metric = IntCounterVec::new(
        Opts::new("tidb_server_query_total", "Counter of queries."),
        &["type", "result", "resource_group"],
    )
    .expect("valid query counter");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("query counter registered once");
    metric
});

static DURATIONS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let metric = HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_handle_query_duration_seconds",
            "Bucketed histogram of processing time (s) of handled queries.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["sql_type", "db", "resource_group"],
    )
    .expect("valid query histogram");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("query histogram registered once");
    metric
});

struct GroupMetrics {
    name: String,
    counters: RefCell<HashMap<u8, (prometheus::IntCounter, prometheus::IntCounter)>>,
    durations: RefCell<HashMap<&'static str, prometheus::Histogram>>,
}

fn group_metrics(name: &str) -> Rc<GroupMetrics> {
    GROUPS.with(|groups| {
        let mut groups = groups.borrow_mut();
        if let Some(group) = groups.get(name) {
            return Rc::clone(group);
        }
        // Bound worker-local cache memory independently of registry cardinality.
        if groups.len() >= 64 {
            groups.clear();
        }
        let group = Rc::new(GroupMetrics {
            name: name.to_owned(),
            counters: RefCell::default(),
            durations: RefCell::default(),
        });
        groups.insert(name.to_owned(), Rc::clone(&group));
        group
    })
}

pub(crate) fn record_error() {
    COMMAND_FAILED.set(true);
}

/// Go `server.go:303`: every accepted connection enters `ConnGauge` under
/// the default group label before any session exists.
pub(crate) fn note_connection_start() {
    CONNECTION_RESOURCE_GROUP.with(|group| *group.borrow_mut() = "default".to_owned());
    crate::server_metrics::CONN_GAUGE
        .with_label_values(&["default"])
        .inc();
}

/// Go `conn.go:435-439`: the connection's end decrements the gauge under
/// the group its session last belonged to (the default label when no
/// session opened).
pub(crate) fn note_connection_end() {
    let group = CONNECTION_RESOURCE_GROUP.with(|group| group.borrow().clone());
    crate::server_metrics::CONN_GAUGE
        .with_label_values(&[&group])
        .dec();
}

/// The resource group label Go reads from the session vars at the dispatch
/// error site (`conn.go:1302`).
pub(crate) fn current_connection_resource_group() -> String {
    CONNECTION_RESOURCE_GROUP.with(|group| group.borrow().clone())
}

/// Go `conn.go:461-470` `moveResourceGroupCounter`: a group switch moves
/// the connection gauge between labels exactly once.
fn note_resource_group_move(previous: &str, current: &str) {
    let previous = if previous.is_empty() {
        "default"
    } else {
        previous
    };
    if previous != current {
        crate::server_metrics::CONN_GAUGE
            .with_label_values(&[previous])
            .dec();
        crate::server_metrics::CONN_GAUGE
            .with_label_values(&[current])
            .inc();
    }
    CONNECTION_RESOURCE_GROUP.with(|group| *group.borrow_mut() = current.to_owned());
}

/// Each connection dispatch runs synchronously on one worker. A scope guard
/// includes early returns and rejected commands without counting idle time.
pub(crate) struct CommandMetrics {
    ok: prometheus::IntCounter,
    error: prometheus::IntCounter,
    pub(crate) skip: bool,
    pub(crate) sql_type: &'static str,
    group: Rc<GroupMetrics>,
    duration_group: Rc<GroupMetrics>,
    code: u8,
    started: std::time::Instant,
}

impl CommandMetrics {
    pub(crate) fn start(code: u8, resource_group: &str) -> Self {
        let started = std::time::Instant::now();
        COMMAND_FAILED.set(false);
        let group = group_metrics(resource_group);
        let (ok, error) = group
            .counters
            .borrow_mut()
            .entry(code)
            .or_insert_with(|| {
                // Go pkg/server/metrics.CmdToString.
                let label = match code {
                    0 => "Sleep".to_owned(),
                    1 => "Quit".to_owned(),
                    2 => "InitDB".to_owned(),
                    3 => "Query".to_owned(),
                    4 => "FieldList".to_owned(),
                    14 => "Ping".to_owned(),
                    22 => "StmtPrepare".to_owned(),
                    23 => "StmtExecute".to_owned(),
                    24 => "StmtSendLongData".to_owned(),
                    25 => "StmtClose".to_owned(),
                    26 => "StmtReset".to_owned(),
                    27 => "SetOption".to_owned(),
                    28 => "StmtFetch".to_owned(),
                    _ => code.to_string(),
                };
                (
                    QUERIES.with_label_values(&[&label, "OK", resource_group]),
                    QUERIES.with_label_values(&[&label, "Error", resource_group]),
                )
            })
            .clone();
        Self {
            ok,
            error,
            skip: false,
            sql_type: "general",
            duration_group: Rc::clone(&group),
            group,
            code,
            started,
        }
    }
}

impl CommandMetrics {
    pub(crate) fn set_resource_groups(&mut self, current: &str, statement: &str) {
        note_resource_group_move(&self.group.name, current);
        if self.group.name != current {
            // Rebind without changing the command's start or error state.
            let failed = COMMAND_FAILED.get();
            let mut rebound = Self::start(self.code, current);
            COMMAND_FAILED.set(failed);
            self.ok = rebound.ok.clone();
            self.error = rebound.error.clone();
            self.group = Rc::clone(&rebound.group);
            rebound.skip = true;
        }
        if self.duration_group.name != statement {
            self.duration_group = group_metrics(statement);
        }
    }
}

impl Drop for CommandMetrics {
    fn drop(&mut self) {
        if !self.skip {
            // Go GetDBNames returns the empty label by default:
            // Status.RecordDBLabel is disabled in the server configuration.
            self.duration_group
                .durations
                .borrow_mut()
                .entry(self.sql_type)
                .or_insert_with(|| {
                    DURATIONS.with_label_values(&[self.sql_type, "", &self.duration_group.name])
                })
                .observe(self.started.elapsed().as_secs_f64());
            if COMMAND_FAILED.get() {
                self.error.inc();
            } else {
                self.ok.inc();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_scope_counts_errors_once_and_resets_for_next_command() {
        let group = group_metrics("scope-test");
        {
            let _guard = CommandMetrics::start(3, "scope-test");
            record_error();
            record_error();
        }
        drop(CommandMetrics::start(3, "scope-test"));
        {
            let mut ddl = CommandMetrics::start(3, "scope-test");
            ddl.skip = true;
        }
        let counters = group.counters.borrow();
        let (ok, error) = &counters[&3];
        assert_eq!(ok.get(), 1);
        assert_eq!(error.get(), 1);
        assert_eq!(group.durations.borrow()["general"].get_sample_count(), 2);
    }

    #[test]
    fn worker_metric_handles_are_reused_and_cache_is_bounded() {
        let first = group_metrics("cache-test");
        assert!(Rc::ptr_eq(&first, &group_metrics("cache-test")));
        for _ in 0..2 {
            let mut guard = CommandMetrics::start(23, "cache-test");
            guard.sql_type = "Select";
        }
        assert_eq!(first.counters.borrow().len(), 1);
        assert_eq!(first.durations.borrow().len(), 1);
        for index in 0..128 {
            group_metrics(&format!("cache-{index}"));
        }
        GROUPS.with(|groups| assert!(groups.borrow().len() <= 64));
        // Cache eviction must not remove registered collectors or invalidate handles.
        assert_eq!(first.counters.borrow()[&23].0.get(), 2);
    }
}
