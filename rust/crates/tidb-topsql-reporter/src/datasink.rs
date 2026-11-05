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

//! Go `pkg/util/topsql/reporter/datasink.go`.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use tidb_proto::tipb::{PlanMeta, SqlMeta, TopRuRecord, TopSqlRecord};
use tidb_topsql_state::ItemInterval;

/// Go `DataSink` collects and sends data to a target.
pub trait DataSink: Send + Sync {
    /// Go `TrySend` pushes a report data into the sink, which will later be
    /// sent to a target by the sink. A deadline can be specified to control
    /// how late it should be sent. If the sink is kept full and cannot
    /// schedule a send within the specified deadline, or the sink is closed,
    /// an error will be returned.
    ///
    /// # Errors
    /// The sink is full or closed.
    fn try_send(&self, data: Arc<ReportData>, deadline: Instant) -> Result<(), String>;

    /// Go `OnReporterClosing` notifies DataSink that the reporter is closing.
    fn on_reporter_closing(&self);

    /// Go's `dataSink.(*pubSubDataSink)` assertion: a pub/sub sink's
    /// subscription; every other sink has none.
    fn pub_sub_subscription(&self) -> Option<PubSubSubscription> {
        None
    }
}

/// The subscription fields Go's registerer reads off a `pubSubDataSink`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PubSubSubscription {
    /// Go `pubSubDataSink.enableTopSQL`.
    pub enable_top_sql: bool,
    /// Go `pubSubDataSink.enableTopRU`.
    pub enable_top_ru: bool,
    /// Go `pubSubDataSink.itemInterval`.
    pub item_interval: ItemInterval,
}

/// Go `DataSinkRegisterer` is for registering DataSink.
pub trait DataSinkRegisterer: Send + Sync {
    /// Go `Register`.
    ///
    /// # Errors
    /// The registerer is closed, full, or the sink's Top-RU interval is
    /// invalid.
    fn register(&self, data_sink: Arc<dyn DataSink>) -> Result<(), String>;

    /// Go `Deregister`.
    fn deregister(&self, data_sink: &Arc<dyn DataSink>);
}

/// Go `ReportData` contains the payload sent from reporter to agent.
/// `data_records` stores TopSQL CPU records, and `ru_records` stores TopRU
/// records. `sql_metas` and `plan_metas` are shared by both record types.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReportData {
    /// Go `DataRecords` contains the topN records of each second and the
    /// `others` record which aggregation all []tipb.TopSQLRecord that is out
    /// of Top N.
    pub data_records: Vec<TopSqlRecord>,
    /// Go `RURecords` contains TopRU records aggregated by (user,
    /// sql_digest, plan_digest).
    pub ru_records: Vec<TopRuRecord>,
    /// Go `SQLMetas`.
    pub sql_metas: Vec<SqlMeta>,
    /// Go `PlanMetas`.
    pub plan_metas: Vec<PlanMeta>,
}

impl ReportData {
    /// Go `ReportData.hasData`.
    #[must_use]
    pub fn has_data(&self) -> bool {
        !self.data_records.is_empty()
            || !self.ru_records.is_empty()
            || !self.sql_metas.is_empty()
            || !self.plan_metas.is_empty()
    }
}

/// The state Go guards with `DefaultDataSinkRegisterer`'s embedded mutex.
#[derive(Default)]
pub(crate) struct Registered {
    /// Go `dataSinks`, keyed by pointer identity.
    pub(crate) data_sinks: Vec<Arc<dyn DataSink>>,
    /// Go `topSQLSinkCount` tracks the number of sinks that require TopSQL
    /// enabled.
    top_sql_sink_count: usize,
}

/// Go `DefaultDataSinkRegisterer` implements DataSinkRegisterer.
pub struct DefaultDataSinkRegisterer {
    /// Go `ctx`.
    ctx: Arc<crate::reporter::Context>,
    pub(crate) inner: Mutex<Registered>,
}

impl DefaultDataSinkRegisterer {
    /// Go `NewDefaultDataSinkRegisterer`.
    #[must_use]
    pub fn new(ctx: Arc<crate::reporter::Context>) -> Self {
        Self { ctx, inner: Mutex::default() }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Registered> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Go's type switch: non-pubsub sinks do not carry subscription collectors;
/// keep TopSQL enabled by default.
fn enable_top_sql(data_sink: &dyn DataSink) -> bool {
    data_sink.pub_sub_subscription().is_none_or(|subscription| subscription.enable_top_sql)
}

impl DataSinkRegisterer for DefaultDataSinkRegisterer {
    fn register(&self, data_sink: Arc<dyn DataSink>) -> Result<(), String> {
        let mut registered = self.lock();
        if self.ctx.is_done() {
            return Err("DefaultDataSinkRegisterer closed".to_owned());
        }
        if registered.data_sinks.iter().any(|ds| Arc::ptr_eq(ds, &data_sink)) {
            return Ok(());
        }
        if registered.data_sinks.len() >= 10 {
            return Err("too many datasinks".to_owned());
        }
        if let Some(subscription) = data_sink.pub_sub_subscription().filter(|s| s.enable_top_ru) {
            tidb_topsql_state::set_top_ru_item_interval(subscription.item_interval)
                .map_err(|error| error.to_string())?;
            tidb_topsql_state::enable_top_ru();
        }
        let enable_top_sql = enable_top_sql(data_sink.as_ref());
        registered.data_sinks.push(data_sink);
        if enable_top_sql {
            tidb_topsql_state::enable_top_sql();
            registered.top_sql_sink_count += 1;
        }
        Ok(())
    }

    fn deregister(&self, data_sink: &Arc<dyn DataSink>) {
        let mut registered = self.lock();
        if self.ctx.is_done() {
            return;
        }
        let Some(position) = registered.data_sinks.iter().position(|ds| Arc::ptr_eq(ds, data_sink)) else {
            return;
        };
        registered.data_sinks.remove(position);
        if enable_top_sql(data_sink.as_ref()) {
            registered.top_sql_sink_count = registered.top_sql_sink_count.saturating_sub(1);
            if registered.top_sql_sink_count == 0 {
                tidb_topsql_state::disable_top_sql();
            }
        }
        if data_sink.pub_sub_subscription().is_some_and(|s| s.enable_top_ru) {
            tidb_topsql_state::disable_top_ru();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDataSink2(Option<PubSubSubscription>);

    impl DataSink for MockDataSink2 {
        fn try_send(&self, _data: Arc<ReportData>, _deadline: Instant) -> Result<(), String> {
            Ok(())
        }

        fn on_reporter_closing(&self) {}

        fn pub_sub_subscription(&self) -> Option<PubSubSubscription> {
            self.0
        }
    }

    fn reset_state() {
        while tidb_topsql_state::top_ru_enabled() {
            tidb_topsql_state::disable_top_ru();
        }
        tidb_topsql_state::disable_top_sql();
        tidb_topsql_state::reset_top_ru_item_interval();
    }

    /// Go `TestDefaultDataSinkRegisterer`.
    #[test]
    fn default_data_sink_registerer() {
        reset_state();
        let r = DefaultDataSinkRegisterer::new(crate::reporter::Context::new());
        let m1: Arc<dyn DataSink> = Arc::new(MockDataSink2(None));
        let m2: Arc<dyn DataSink> = Arc::new(MockDataSink2(None));
        r.register(Arc::clone(&m1)).unwrap();
        r.register(Arc::clone(&m2)).unwrap();
        // Registering the same sink again is a no-op.
        r.register(Arc::clone(&m2)).unwrap();
        assert_eq!(r.lock().data_sinks.len(), 2);
        assert!(tidb_topsql_state::top_sql_enabled());
        assert!(!tidb_topsql_state::top_ru_enabled());
        r.deregister(&m1);
        assert!(tidb_topsql_state::top_sql_enabled());
        r.deregister(&m2);
        assert!(r.lock().data_sinks.is_empty());
        assert!(!tidb_topsql_state::top_sql_enabled());
        assert!(!tidb_topsql_state::top_ru_enabled());
    }

    /// Go `TestDefaultDataSinkRegistererTopSQLRespectsTopRUOnly` and the
    /// ten-sink cap: a TopRU-only subscriber turns TopRU, not TopSQL, on.
    #[test]
    fn top_ru_only_sink_and_the_sink_cap() {
        reset_state();
        let r = DefaultDataSinkRegisterer::new(crate::reporter::Context::new());
        let top_ru: Arc<dyn DataSink> = Arc::new(MockDataSink2(Some(PubSubSubscription {
            enable_top_sql: false,
            enable_top_ru: true,
            item_interval: ItemInterval::FIFTEEN_SECONDS,
        })));
        r.register(Arc::clone(&top_ru)).unwrap();
        assert!(!tidb_topsql_state::top_sql_enabled());
        assert!(tidb_topsql_state::top_ru_enabled());
        assert_eq!(tidb_topsql_state::get_top_ru_item_interval(), 15);
        for _ in 0..9 {
            r.register(Arc::new(MockDataSink2(None))).unwrap();
        }
        assert_eq!(r.register(Arc::new(MockDataSink2(None))).unwrap_err(), "too many datasinks");
        r.deregister(&top_ru);
        assert!(!tidb_topsql_state::top_ru_enabled());
        assert!(tidb_topsql_state::top_sql_enabled());
        reset_state();
    }
}
