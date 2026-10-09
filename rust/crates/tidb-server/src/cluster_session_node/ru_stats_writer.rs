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

//! Go `Domain.requestUnitsWriterLoop` on this node.
//!
//! `tidb_domain::ru_stats` has long carried Go's writer -- the daily totals,
//! the probe, the GC -- but nothing ran it, so a live server never wrote
//! `mysql.request_unit_by_group`. This is the half it was missing: the
//! production [`RuStatsDeps`] over PD, meta and the domain's internal-session
//! pool, and the owner-gated worker that drives Go's loop.
//!
//! Each dependency is the one Go's writer reaches through `*Domain`
//! (`pkg/domain/ru_stats.go:45-60`): `RMClient` is the PD ResourceManager,
//! `InfoCache` resolves resource groups, the store reads and writes the
//! `RUStats` meta key, and `sessPool` runs `ExecRCRestrictedSQL`.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use tidb_domain::ru_stats::{
    Consumption, DailyRuStats, GroupRuStats, ResourceGroupInfo, ResourceGroupWithRuStats,
    RuStats, RuStatsDeps, RuStatsError, RuStatsWriter,
};
use tidb_exec::meta_txn::{MetaTxnError, MetaTxnStorage};
use tidb_meta::transaction::Mutator;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};
use tidb_txnkv::{run_in_new_txn, NewTxnStorage, NewTxnTransaction, RunInNewTxnContext};

use super::{ClusterSessionFactory, ClusterStatsSessionContext, UsageWorkerStop};

/// Go `kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)`, the context
/// `ExecRCRestrictedSQL` executes under (`runaway/record.go:401`).
struct InternalTxnOthers;

/// The production dependencies of Go's RU statistics writer.
pub(super) struct ClusterRuStatsDeps<C, L, P: StorePdCapability> {
    pd: tidb_pd_client::PdClient,
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    pool: Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>,
    keyspace_id: Option<u32>,
    timeout: Duration,
}

impl<C, L, P> ClusterRuStatsDeps<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    pub(super) fn new(
        pd: tidb_pd_client::PdClient,
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        pool: Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>,
        keyspace_id: Option<u32>,
        timeout: Duration,
    ) -> Self {
        Self {
            pd,
            opener,
            pool,
            keyspace_id,
            timeout,
        }
    }

    /// Go `runaway.ExecRCRestrictedSQL`: one pooled internal session, the
    /// current-session option, the rows back.
    fn exec_rc_restricted_sql(
        &self,
        sql: &str,
        params: &[&str],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, RuStatsError> {
        use tidb_sqlexec::RestrictedSqlExecutor;
        let arguments: Vec<tidb_util::sqlescape::SqlArg<'_>> = params
            .iter()
            .map(|param| tidb_util::sqlescape::SqlArg::String(param.as_bytes()))
            .collect();
        let use_current_session: tidb_sqlexec::OptionFuncAlias =
            Arc::new(tidb_sqlexec::exec_option_use_current_session);
        self.pool
            .with_session(|session| {
                session
                    .exec_restricted_sql(&InternalTxnOthers, &[use_current_session], sql, &arguments)
                    .map(|(rows, _)| rows)
                    .map_err(|error| tidb_syssession::SysSessionError::new(error.to_string()))
            })
            .map_err(|error| RuStatsError::Other(format!("get session failed: {error}")))
    }

    /// One read-only meta transaction, rolled back afterwards.
    fn read_meta<T>(
        &self,
        read: impl FnOnce(&mut tidb_exec::meta_txn::MetaTxn<C, L, P>) -> Result<T, RuStatsError>,
    ) -> Result<T, RuStatsError> {
        let mut storage = MetaTxnStorage::new(&self.opener, self.timeout);
        let mut transaction = storage
            .begin()
            .map_err(|error| RuStatsError::Other(error.to_string()))?;
        let result = read(&mut transaction);
        let _ = NewTxnTransaction::rollback(&mut transaction);
        result
    }
}

impl<C, L, P> RuStatsDeps for ClusterRuStatsDeps<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn list_resource_groups_with_ru_stats(
        &self,
    ) -> Result<Vec<ResourceGroupWithRuStats>, RuStatsError> {
        let groups = self
            .pd
            .list_resource_groups(self.keyspace_id, true)
            .map_err(|error| RuStatsError::Other(error.to_string()))?;
        Ok(groups
            .into_iter()
            .map(|group| ResourceGroupWithRuStats {
                name: group.name,
                ru_stats: group.ru_stats.map(|consumption| Consumption {
                    rru: consumption.r_r_u,
                    wru: consumption.w_r_u,
                }),
            })
            .collect())
    }

    /// Go `InfoCache.GetLatest().ResourceGroupByName(ast.NewCIStr(name))`. The
    /// info schema is built from the meta resource-group hash, read here at
    /// a fresh snapshot; the match is on the lowercase form, as a `CIStr` key
    /// is, and the original-case name is what the row keeps.
    fn resource_group_by_name(&self, name: &str) -> Option<ResourceGroupInfo> {
        let wanted = name.to_lowercase();
        self.read_meta(|transaction| {
            Mutator::new(transaction)
                .resource_groups()
                .map_err(|error| RuStatsError::Other(error.to_string()))
        })
        .ok()?
        .into_iter()
        .find(|group| group.name.lowercase() == wanted)
        .map(|group| ResourceGroupInfo {
            id: group.id,
            name: group.name.original().to_owned(),
        })
    }

    fn load_ru_stats(&self) -> Result<Option<RuStats>, RuStatsError> {
        let stored = self.read_meta(|transaction| {
            Mutator::new(transaction)
                .ru_stats()
                .map_err(|error| RuStatsError::Other(error.to_string()))
        })?;
        Ok(stored.map(meta_to_domain))
    }

    /// Go `kv.RunInNewTxn(..., true, meta.NewMutator(txn).SetRUStats)`.
    fn persist_ru_stats(&self, stats: &RuStats) -> Result<(), RuStatsError> {
        let stored = domain_to_meta(stats);
        let mut storage = MetaTxnStorage::new(&self.opener, self.timeout);
        run_in_new_txn(&RunInNewTxnContext::default(), &mut storage, true, |transaction| {
            Mutator::new(&mut *transaction)
                .set_ru_stats(Some(&stored))
                .map_err(MetaTxnError::from)
        })
        .map_err(|error| RuStatsError::Other(error.to_string()))
    }

    fn query_row_exists(&self, sql: &str, params: &[&str]) -> Result<bool, RuStatsError> {
        Ok(!self.exec_rc_restricted_sql(sql, params)?.is_empty())
    }

    fn query_single_count(&self, sql: &str) -> Result<Option<i64>, RuStatsError> {
        Ok(self
            .exec_rc_restricted_sql(sql, &[])?
            .first()
            .and_then(|row| row.first())
            .map(tidb_datatype::Datum::get_int64))
    }

    fn exec_statement(&self, sql: &str) -> Result<(), RuStatsError> {
        self.exec_rc_restricted_sql(sql, &[]).map(|_| ())
    }
}

/// The meta key's stored form to the writer's. Go keeps the whole
/// `rmpb.Consumption`; only the read and write request units take part in
/// the daily totals, which is all the writer's type carries.
fn meta_to_domain(stored: tidb_meta::transaction::RuStats) -> RuStats {
    let daily = |day: Box<tidb_meta::transaction::DailyRuStats>| DailyRuStats {
        end_time: day.end_time.fixed_offset(),
        stats: day
            .stats
            .unwrap_or_default()
            .into_iter()
            .map(|group| GroupRuStats {
                id: group.id,
                name: group.name,
                ru_consumption: group.ru_consumption.map(|consumption| Consumption {
                    rru: consumption.read_request_units,
                    wru: consumption.write_request_units,
                }),
            })
            .collect(),
    };
    RuStats {
        latest: stored.latest.map(daily),
        previous: stored.previous.map(daily),
    }
}

fn domain_to_meta(stats: &RuStats) -> tidb_meta::transaction::RuStats {
    let daily = |day: &DailyRuStats| {
        Box::new(tidb_meta::transaction::DailyRuStats {
            end_time: day.end_time.with_timezone(&chrono::Utc),
            stats: Some(
                day.stats
                    .iter()
                    .map(|group| tidb_meta::transaction::GroupRuStats {
                        id: group.id,
                        name: group.name.clone(),
                        ru_consumption: group.ru_consumption.as_ref().map(|consumption| {
                            tidb_meta::transaction::RuConsumption {
                                read_request_units: consumption.rru,
                                write_request_units: consumption.wru,
                                ..Default::default()
                            }
                        }),
                    })
                    .collect(),
            ),
        })
    };
    tidb_meta::transaction::RuStats {
        latest: stats.latest.as_ref().map(daily),
        previous: stats.previous.as_ref().map(daily),
    }
}

/// The node's `requestUnitsWriterLoop`: one thread, Go's round, and a wait
/// that shutdown interrupts.
pub(super) struct RuStatsWriterWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RuStatsWriterWorker {
    pub(super) fn start<D>(factory: &Arc<ClusterSessionFactory>, deps: D) -> Self
    where
        D: RuStatsDeps + Send + 'static,
    {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak: Weak<ClusterSessionFactory> = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("ru-stats-writer".to_owned())
            .spawn(move || {
                let local = chrono::Local;
                let mut writer = RuStatsWriter::new(deps, chrono::Local::now(), local);
                loop {
                    let Some(factory) = weak.upgrade() else {
                        return;
                    };
                    // Go asks the DDL owner manager every round.
                    let is_owner = factory.ddl.is_owner();
                    drop(factory);
                    let wait = tidb_domain::ru_stats::request_units_writer_round(
                        &mut writer,
                        &chrono::Local::now,
                        is_owner,
                        |delay| {
                            let _ = running.wait(delay);
                        },
                    );
                    if running.wait(wait) {
                        return;
                    }
                }
            })
            .expect("RU statistics writer spawns");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for RuStatsWriterWorker {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RuStats {
        let day = |hour: u32, rru: f64| DailyRuStats {
            end_time: chrono::DateTime::parse_from_rfc3339(&format!(
                "2026-10-0{hour}T00:00:00-07:00"
            ))
            .unwrap(),
            stats: vec![GroupRuStats {
                id: 7,
                name: "rg1".to_owned(),
                ru_consumption: Some(Consumption { rru, wru: 2.5 }),
            }],
        };
        RuStats {
            latest: Some(day(8, 10.0)),
            previous: Some(day(7, 4.0)),
        }
    }

    /// What the writer persists is what it reads back: the instant, each
    /// group's identity and both request-unit totals survive the meta key's
    /// stored form, which is the round trip `needFetchData` and the daily
    /// difference depend on.
    #[test]
    fn ru_stats_survive_the_meta_round_trip() {
        let original = sample();
        let restored = meta_to_domain(domain_to_meta(&original));
        let instants = |stats: &RuStats| {
            [&stats.latest, &stats.previous]
                .map(|day| day.as_ref().map(|day| day.end_time.timestamp()))
        };
        assert_eq!(instants(&restored), instants(&original), "the instants survive");
        let groups = |stats: &RuStats| {
            [&stats.latest, &stats.previous].map(|day| day.as_ref().unwrap().stats.clone())
        };
        assert_eq!(groups(&restored), groups(&original));
    }

    /// A day with no stored stats reads as an empty list, never a missing
    /// day: Go's `DailyRUStats.Stats` is a nil slice, not a nil pointer.
    #[test]
    fn a_day_without_stats_reads_as_empty() {
        let stored = tidb_meta::transaction::RuStats {
            latest: Some(Box::new(tidb_meta::transaction::DailyRuStats {
                end_time: chrono::Utc::now(),
                stats: None,
            })),
            previous: None,
        };
        let restored = meta_to_domain(stored);
        assert!(restored.latest.unwrap().stats.is_empty());
        assert!(restored.previous.is_none());
    }
}
