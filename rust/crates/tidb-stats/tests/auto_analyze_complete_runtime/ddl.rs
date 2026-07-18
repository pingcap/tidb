// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};

use super::factory::{stats, table, Clock, Session};
use tidb_stats::auto_analyze_runtime::*;
use tidb_stats::PriorityHeapItem;

struct Info(BTreeMap<i64, TableMeta>);
impl InfoSchemaPort for Info {
    fn table_by_id(&self, id: i64) -> Option<TableMeta> {
        self.0.get(&id).cloned()
    }
}

struct Stats(BTreeMap<i64, TableStats>);
impl StatisticsPort for Stats {
    fn stats_by_id(&self, id: i64) -> Option<TableStats> {
        self.0.get(&id).cloned()
    }
    fn locked_table_ids(&self) -> RuntimeResult<BTreeSet<i64>> {
        Ok(BTreeSet::new())
    }
    fn update_after_analyze(&mut self, _: i64) -> RuntimeResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Queue {
    initialized: Cell<bool>,
    removed: RefCell<Vec<i64>>,
    upserted: RefCell<Vec<PriorityHeapItem>>,
    fail_remove: Cell<bool>,
}
impl QueueMutationPort for Queue {
    fn is_initialized(&self) -> bool {
        self.initialized.get()
    }
    fn remove(&self, id: i64) -> RuntimeResult<()> {
        self.removed.borrow_mut().push(id);
        if self.fail_remove.get() {
            Err(RuntimeError("remove".into()))
        } else {
            Ok(())
        }
    }
    fn upsert(&self, job: PriorityHeapItem, _: &BTreeSet<i64>) -> RuntimeResult<()> {
        self.upserted.borrow_mut().push(job);
        Ok(())
    }
}

fn fixtures(dynamic: bool) -> (Session, Clock, Info, Stats, Queue) {
    let table = table();
    let info = Info(BTreeMap::from([
        (10, table.clone()),
        (
            11,
            TableMeta {
                id: 11,
                partitions: vec![],
                ..table.clone()
            },
        ),
    ]));
    let stats = Stats(BTreeMap::from([
        (10, stats(10)),
        (11, stats(11)),
        (12, stats(12)),
    ]));
    let queue = Queue::default();
    queue.initialized.set(true);
    (
        Session {
            version: 2,
            ratio: 0.5,
            enabled: true,
            dynamic,
        },
        Clock(11_000_000_000),
        info,
        stats,
        queue,
    )
}

fn handle(event: DdlEvent, dynamic: bool) -> (DdlHandleOutcome, Queue) {
    let (session, clock, info, stats, queue) = fixtures(dynamic);
    let outcome = DdlRuntime {
        session: &session,
        clock: &clock,
        info_schema: &info,
        statistics: &stats,
        queue: &queue,
    }
    .handle(&event);
    (outcome, queue)
}

#[test]
fn readiness_retries_enabled_queue_initialization_and_ignores_disabled() {
    let (mut session, clock, info, stats, queue) = fixtures(true);
    queue.initialized.set(false);
    assert_eq!(
        DdlRuntime {
            session: &session,
            clock: &clock,
            info_schema: &info,
            statistics: &stats,
            queue: &queue
        }
        .handle(&DdlEvent::Other),
        DdlHandleOutcome::RetryLater
    );
    session.enabled = false;
    assert_eq!(
        DdlRuntime {
            session: &session,
            clock: &clock,
            info_schema: &info,
            statistics: &stats,
            queue: &queue
        }
        .handle(&DdlEvent::Other),
        DdlHandleOutcome::Ignored
    );
}

#[test]
fn add_index_recreates_dynamic_or_static_jobs_and_skips_ddl_analyzed_index() {
    let (_, dynamic) = handle(
        DdlEvent::AddIndex {
            table_id: 10,
            already_analyzed: false,
        },
        true,
    );
    assert_eq!(dynamic.upserted.borrow().len(), 1);
    let (_, static_queue) = handle(
        DdlEvent::AddIndex {
            table_id: 10,
            already_analyzed: false,
        },
        false,
    );
    assert_eq!(static_queue.upserted.borrow().len(), 2);
    let (_, skipped) = handle(
        DdlEvent::AddIndex {
            table_id: 10,
            already_analyzed: true,
        },
        true,
    );
    assert!(skipped.upserted.borrow().is_empty());
}

#[test]
fn truncate_and_drop_table_delete_global_and_static_partition_identities() {
    for event in [
        DdlEvent::TruncateTable {
            old_table_id: 10,
            old_partition_ids: vec![11, 12],
        },
        DdlEvent::DropTable {
            table_id: 10,
            partition_ids: vec![11, 12],
        },
    ] {
        let (_, queue) = handle(event, true);
        assert_eq!(&*queue.removed.borrow(), &[10, 11, 12]);
    }
}

#[test]
fn truncate_drop_and_reorganize_partition_delete_then_recreate_global_job() {
    for event in [
        DdlEvent::TruncatePartitions {
            table_id: 10,
            dropped_partition_ids: vec![11],
        },
        DdlEvent::DropPartitions {
            table_id: 10,
            dropped_partition_ids: vec![11],
        },
        DdlEvent::ReorganizePartitions {
            table_id: 10,
            dropped_partition_ids: vec![11],
        },
    ] {
        let (_, queue) = handle(event, true);
        assert_eq!(&*queue.removed.borrow(), &[11, 10]);
        assert_eq!(queue.upserted.borrow().len(), 1);
    }
}

#[test]
fn exchange_partition_replaces_partition_nonpartitioned_and_global_jobs() {
    let (_, queue) = handle(
        DdlEvent::ExchangePartition {
            table_id: 10,
            partition_id: 11,
            non_partitioned_table_id: 20,
        },
        true,
    );
    assert_eq!(&*queue.removed.borrow(), &[11, 20, 10]);
    assert_eq!(queue.upserted.borrow().len(), 2);
}

#[test]
fn alter_and_remove_partitioning_delete_old_shape_before_new_job() {
    let (_, altered) = handle(
        DdlEvent::AlterTablePartitioning {
            old_table_id: 20,
            new_table_id: 10,
        },
        true,
    );
    assert_eq!(&*altered.removed.borrow(), &[20, 10]);
    assert_eq!(altered.upserted.borrow().len(), 1);
    let (_, removed) = handle(
        DdlEvent::RemovePartitioning {
            old_table_id: 10,
            new_table_id: 11,
            dropped_partition_ids: vec![12],
        },
        true,
    );
    assert_eq!(&*removed.removed.borrow(), &[12, 10]);
    assert_eq!(removed.upserted.borrow().len(), 1);
}

#[test]
fn drop_schema_attempts_every_partition_and_table() {
    let (_, queue) = handle(
        DdlEvent::DropSchema {
            table_and_partition_ids: vec![(10, vec![11, 12]), (20, vec![])],
        },
        true,
    );
    assert_eq!(&*queue.removed.borrow(), &[11, 12, 10, 20]);
}

#[test]
fn dispatched_handler_suppresses_port_errors_like_go_notifier_callback() {
    let (session, clock, info, stats, queue) = fixtures(true);
    queue.fail_remove.set(true);
    let outcome = DdlRuntime {
        session: &session,
        clock: &clock,
        info_schema: &info,
        statistics: &stats,
        queue: &queue,
    }
    .handle(&DdlEvent::DropTable {
        table_id: 10,
        partition_ids: vec![],
    });
    let DdlHandleOutcome::Handled { suppressed_errors } = outcome else {
        panic!()
    };
    assert_eq!(suppressed_errors, vec![RuntimeError("remove".into())]);

    queue.removed.borrow_mut().clear();
    let outcome = DdlRuntime {
        session: &session,
        clock: &clock,
        info_schema: &info,
        statistics: &stats,
        queue: &queue,
    }
    .handle(&DdlEvent::DropSchema {
        table_and_partition_ids: vec![(10, vec![11, 12])],
    });
    let DdlHandleOutcome::Handled { suppressed_errors } = outcome else {
        panic!()
    };
    assert_eq!(&*queue.removed.borrow(), &[11, 12, 10]);
    assert_eq!(suppressed_errors.len(), 3);
}

#[test]
fn unrelated_ddl_is_a_successful_noop() {
    let (outcome, queue) = handle(DdlEvent::Other, true);
    assert_eq!(
        outcome,
        DdlHandleOutcome::Handled {
            suppressed_errors: vec![]
        }
    );
    assert!(queue.removed.borrow().is_empty());
    assert!(queue.upserted.borrow().is_empty());
}
