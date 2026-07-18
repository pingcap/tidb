// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use super::factory::{stats, table, Clock, Session};
use tidb_stats::auto_analyze_runtime::*;
use tidb_stats::priority_heap::{LiveAnalysisQueue, LiveQueueSnapshot};
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

#[test]
fn ddl_factory_and_live_queue_close_the_running_job_retry_loop() {
    let queue = LiveAnalysisQueue::new();
    queue
        .initialize([PriorityHeapItem::new(10, 1.0)], [], 1)
        .unwrap();
    let running = queue.pop().unwrap();
    let session = Session {
        version: 2,
        ratio: 0.5,
        enabled: true,
        dynamic: true,
    };
    let clock = Clock(11_000_000_000);
    let table = table();
    let info = Info(BTreeMap::from([(10, table)]));
    let stats = Stats(BTreeMap::from([
        (10, stats(10)),
        (11, stats(11)),
        (12, stats(12)),
    ]));
    let adapter = LiveQueueAdapter { queue: &queue };
    let outcome = DdlRuntime {
        session: &session,
        clock: &clock,
        info_schema: &info,
        statistics: &stats,
        queue: &adapter,
    }
    .handle(&DdlEvent::AddIndex {
        table_id: 10,
        already_analyzed: false,
    });
    assert_eq!(
        outcome,
        DdlHandleOutcome::Handled {
            suppressed_errors: vec![]
        }
    );
    assert_eq!(queue.snapshot().unwrap().must_retry_jobs, [10]);
    queue.complete(running, false);
    queue.requeue_must_retry_jobs().unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 10);
}

#[test]
fn package_build_obligation_has_one_public_runtime_entrypoint() {
    let _: fn() -> LiveAnalysisQueue = LiveAnalysisQueue::new;
    let snapshot = LiveQueueSnapshot::default();
    assert!(snapshot.current_jobs.is_empty());
}
