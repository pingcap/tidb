// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;

use super::factory::AnalysisJobFactory;
use super::model::{PartitionStats, RuntimeError, RuntimeResult};
use super::ports::{ClockPort, InfoSchemaPort, QueueMutationPort, SessionPort, StatisticsPort};
use crate::priority_heap::LiveAnalysisQueue;
use crate::PriorityHeapItem;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlQueueDisposition {
    Dispatch,
    RetryLater,
    Ignore,
}

#[must_use]
pub const fn ddl_queue_disposition(initialized: bool, enabled: bool) -> DdlQueueDisposition {
    if initialized {
        DdlQueueDisposition::Dispatch
    } else if enabled {
        DdlQueueDisposition::RetryLater
    } else {
        DdlQueueDisposition::Ignore
    }
}

/// Source DDL events reduced to the IDs that drive queue mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DdlEvent {
    AddIndex {
        table_id: i64,
        already_analyzed: bool,
    },
    TruncateTable {
        old_table_id: i64,
        old_partition_ids: Vec<i64>,
    },
    DropTable {
        table_id: i64,
        partition_ids: Vec<i64>,
    },
    TruncatePartitions {
        table_id: i64,
        dropped_partition_ids: Vec<i64>,
    },
    DropPartitions {
        table_id: i64,
        dropped_partition_ids: Vec<i64>,
    },
    ExchangePartition {
        table_id: i64,
        partition_id: i64,
        non_partitioned_table_id: i64,
    },
    ReorganizePartitions {
        table_id: i64,
        dropped_partition_ids: Vec<i64>,
    },
    AlterTablePartitioning {
        old_table_id: i64,
        new_table_id: i64,
    },
    RemovePartitioning {
        old_table_id: i64,
        new_table_id: i64,
        dropped_partition_ids: Vec<i64>,
    },
    DropSchema {
        table_and_partition_ids: Vec<(i64, Vec<i64>)>,
    },
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DdlHandleOutcome {
    Handled {
        suppressed_errors: Vec<RuntimeError>,
    },
    RetryLater,
    Ignored,
}

/// Public-API-only adapter over the integrated live queue.
pub struct LiveQueueAdapter<'a> {
    pub queue: &'a LiveAnalysisQueue,
}

impl QueueMutationPort for LiveQueueAdapter<'_> {
    fn is_initialized(&self) -> bool {
        self.queue.is_initialized()
    }

    fn remove(&self, table_id: i64) -> RuntimeResult<()> {
        let snapshot = self
            .queue
            .snapshot()
            .map_err(|error| RuntimeError(error.to_string()))?;
        let remaining = snapshot
            .current_jobs
            .into_iter()
            .filter(|job| job.table_id != table_id)
            .map(|job| job.table_id);
        self.queue
            .refresh_jobs([], remaining)
            .map_err(|error| RuntimeError(error.to_string()))
    }

    fn upsert(&self, job: PriorityHeapItem, locked: &BTreeSet<i64>) -> RuntimeResult<()> {
        let version = self
            .queue
            .snapshot()
            .map_err(|error| RuntimeError(error.to_string()))?
            .last_dml_update_version
            .saturating_add(1);
        self.queue
            .process_dml_changes([job], locked.iter().copied(), version)
            .map_err(|error| RuntimeError(error.to_string()))
    }
}

/// Concrete DDL dispatcher over injected metadata/statistics/queue ports.
pub struct DdlRuntime<'a, S, C, I, T, Q> {
    pub session: &'a S,
    pub clock: &'a C,
    pub info_schema: &'a I,
    pub statistics: &'a T,
    pub queue: &'a Q,
}

impl<S, C, I, T, Q> DdlRuntime<'_, S, C, I, T, Q>
where
    S: SessionPort,
    C: ClockPort,
    I: InfoSchemaPort,
    T: StatisticsPort,
    Q: QueueMutationPort,
{
    pub fn handle(&self, event: &DdlEvent) -> DdlHandleOutcome {
        if !self.queue.is_initialized() {
            return if self.session.auto_analyze_enabled() {
                DdlHandleOutcome::RetryLater
            } else {
                DdlHandleOutcome::Ignored
            };
        }
        let mut errors = Vec::new();
        let mut apply = |result: RuntimeResult<()>| {
            if let Err(error) = result {
                errors.push(error);
            }
        };
        match event {
            DdlEvent::AddIndex {
                table_id,
                already_analyzed,
            } => {
                if !already_analyzed {
                    apply(self.recreate(*table_id));
                }
            }
            DdlEvent::TruncateTable {
                old_table_id,
                old_partition_ids,
            }
            | DdlEvent::DropTable {
                table_id: old_table_id,
                partition_ids: old_partition_ids,
            } => {
                apply(self.remove_many(
                    std::iter::once(*old_table_id).chain(old_partition_ids.iter().copied()),
                ));
            }
            DdlEvent::TruncatePartitions {
                table_id,
                dropped_partition_ids,
            }
            | DdlEvent::DropPartitions {
                table_id,
                dropped_partition_ids,
            }
            | DdlEvent::ReorganizePartitions {
                table_id,
                dropped_partition_ids,
            } => {
                apply(
                    self.remove_many(
                        dropped_partition_ids
                            .iter()
                            .copied()
                            .chain(std::iter::once(*table_id)),
                    ),
                );
                apply(self.recreate(*table_id));
            }
            DdlEvent::ExchangePartition {
                table_id,
                partition_id,
                non_partitioned_table_id,
            } => {
                apply(self.remove_many([*partition_id, *non_partitioned_table_id, *table_id]));
                apply(self.recreate(*table_id));
                apply(self.recreate(*partition_id));
            }
            DdlEvent::AlterTablePartitioning {
                old_table_id,
                new_table_id,
            } => {
                apply(self.remove_many([*old_table_id, *new_table_id]));
                apply(self.recreate(*new_table_id));
            }
            DdlEvent::RemovePartitioning {
                old_table_id,
                new_table_id,
                dropped_partition_ids,
            } => {
                apply(
                    self.remove_many(
                        dropped_partition_ids
                            .iter()
                            .copied()
                            .chain(std::iter::once(*old_table_id)),
                    ),
                );
                apply(self.recreate(*new_table_id));
            }
            DdlEvent::DropSchema {
                table_and_partition_ids,
            } => {
                for (table, partitions) in table_and_partition_ids {
                    // Drop-schema is best effort: preserve every error and continue.
                    for id in partitions.iter().copied().chain(std::iter::once(*table)) {
                        apply(self.queue.remove(id));
                    }
                }
            }
            DdlEvent::Other => {}
        }
        DdlHandleOutcome::Handled {
            suppressed_errors: errors,
        }
    }

    fn remove_many(&self, ids: impl IntoIterator<Item = i64>) -> RuntimeResult<()> {
        for id in ids {
            self.queue.remove(id)?;
        }
        Ok(())
    }

    fn recreate(&self, table_id: i64) -> RuntimeResult<()> {
        let Some(table) = self.info_schema.table_by_id(table_id) else {
            return Ok(());
        };
        let locked = self.statistics.locked_table_ids()?;
        let factory = AnalysisJobFactory::new(self.session, self.clock);
        if !table.partitions.is_empty() && !self.session.dynamic_partition_pruning() {
            for partition in &table.partitions {
                let Some(stats) = self.statistics.stats_by_id(partition.id) else {
                    continue;
                };
                if let Some(mut job) = factory.create_static_partition(&table, partition.id, &stats)
                {
                    job.calculate_and_set_weight();
                    self.queue.upsert(job.heap_item(), &locked)?;
                }
            }
        } else if !table.partitions.is_empty() {
            let Some(global) = self.statistics.stats_by_id(table.id) else {
                return Ok(());
            };
            let partitions: Vec<_> = table
                .partitions
                .iter()
                .filter_map(|partition| {
                    self.statistics
                        .stats_by_id(partition.id)
                        .filter(|stats| stats.eligible)
                        .map(|stats| PartitionStats {
                            partition: partition.clone(),
                            stats,
                        })
                })
                .collect();
            if let Some(mut job) = factory.create_dynamic_partitioned(&table, &global, &partitions)
            {
                job.calculate_and_set_weight();
                self.queue.upsert(job.heap_item(), &locked)?;
            }
        } else if let Some(stats) = self.statistics.stats_by_id(table.id) {
            if let Some(mut job) = factory.create_non_partitioned(&table, &stats) {
                job.calculate_and_set_weight();
                self.queue.upsert(job.heap_item(), &locked)?;
            }
        }
        Ok(())
    }
}
