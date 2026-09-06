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

//! Go `collector_test.go`.

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::*;

fn pending_usage(
    collector: &SessionIndexUsageCollector,
    table_id: i64,
    index_id: i64,
) -> Option<&Sample> {
    collector
        .index_usage
        .get(&GlobalIndexId { table_id, index_id })
}

#[deny(unused_must_use)]
#[test]
fn source_return_values_may_be_ignored_like_go() {
    new_sample(0, 0, 0, 0);
    Collector::new();

    let collector = Collector::new();
    collector.get_index_usage(1, 2);
    collector.spawn_session_collector();

    let session = Arc::new(Mutex::new(collector.spawn_session_collector()));
    StmtIndexUsageCollector::new(session);
}

#[test]
fn get_bucket() {
    for (value, expected) in [
        (0.0, 0),
        (0.005, 1),
        (0.01, 2),
        (0.05, 2),
        (0.1, 3),
        (0.15, 3),
        (0.2, 4),
        (0.4, 4),
        (0.5, 5),
        (0.7, 5),
        (1.0, 6),
    ] {
        assert_eq!(get_index_usage_access_bucket(value), expected);
    }
}

#[test]
fn update_index() {
    let global_collector = Collector::new();
    global_collector.start_worker();
    let mut collector = global_collector.spawn_session_collector();

    collector.update(1, 1, new_sample(1, 1, 1, 1));
    let usage = pending_usage(&collector, 1, 1).expect("entry recorded");
    assert_eq!(usage.query_total, 1);
    assert_eq!(usage.kv_req_total, 1);
    assert_eq!(usage.row_access_total, 1);
    assert_eq!(usage.percentage_access, [0, 0, 0, 0, 0, 0, 1]);

    collector.update(1, 1, new_sample(10, 10, 5, 50));
    let usage = pending_usage(&collector, 1, 1).expect("entry recorded");
    assert_eq!(usage.query_total, 11);
    assert_eq!(usage.kv_req_total, 11);
    assert_eq!(usage.row_access_total, 6);
    assert_eq!(usage.percentage_access, [0, 0, 0, 1, 0, 0, 1]);

    collector.update(1, 1, new_sample(10, 10, 5, 0));
    let usage = pending_usage(&collector, 1, 1).expect("entry recorded");
    assert_eq!(usage.query_total, 21);
    assert_eq!(usage.kv_req_total, 21);
    assert_eq!(usage.row_access_total, 11);
    assert_eq!(usage.percentage_access, [0, 0, 0, 1, 0, 0, 2]);
}

#[derive(Clone)]
struct TestOp {
    info: Sample,
    index: GlobalIndexId,
    report: bool,
}

struct TestOpGenerator(u64);

impl TestOpGenerator {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.0 = value;
        value.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn generate(&mut self) -> TestOp {
        let index = GlobalIndexId {
            table_id: (self.next() % 10) as i64,
            index_id: (self.next() % 10) as i64,
        };
        let query_total = self.next() % 10_000;
        let kv_req_total = self.next() % 10_000;
        let total_rows = self.next() % 10_000;
        let row_access = if total_rows > 0 {
            self.next() % total_rows
        } else {
            0
        };
        let report = self.next() % 4 == 1;
        TestOp {
            info: new_sample(query_total, kv_req_total, row_access, total_rows),
            index,
            report,
        }
    }
}

#[test]
fn flush_concurrent_index_collector() {
    const SESSION_COUNT: usize = 64;
    const OP_PER_SESSION: usize = 100_000;
    const OP_COUNT: usize = OP_PER_SESSION * SESSION_COUNT;

    let expected_collector = Collector::new();
    expected_collector.start_worker();
    let mut expected_session = expected_collector.spawn_session_collector();

    let mut generator = TestOpGenerator(0x1234_5678_9abc_def0);
    let mut operations = Vec::with_capacity(OP_COUNT);
    for _ in 0..OP_COUNT {
        let operation = generator.generate();
        expected_session.update(
            operation.index.table_id,
            operation.index.index_id,
            operation.info.clone(),
        );
        operations.push(operation);
    }
    expected_session.flush();

    let collector = Arc::new(Collector::new());
    collector.start_worker();
    thread::scope(|scope| {
        for session_operations in operations.chunks(OP_PER_SESSION) {
            let collector = Arc::clone(&collector);
            scope.spawn(move || {
                let mut local_collector = collector.spawn_session_collector();
                for operation in session_operations {
                    local_collector.update(
                        operation.index.table_id,
                        operation.index.index_id,
                        operation.info.clone(),
                    );
                    if operation.report {
                        local_collector.report();
                    }
                }
                local_collector.flush();
            });
        }
    });

    expected_collector.close();
    collector.close();
    assert_eq!(
        *expected_collector
            .index_usage
            .read()
            .expect("index usage lock poisoned"),
        *collector
            .index_usage
            .read()
            .expect("index usage lock poisoned")
    );
}

fn eventually(mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    predicate()
}

#[test]
fn stmt_index_usage_collector() {
    let collector = Collector::new();
    collector.start_worker();
    let session = Arc::new(Mutex::new(collector.spawn_session_collector()));
    let statement = StmtIndexUsageCollector::new(Arc::clone(&session));

    statement.update(1, 1, new_sample(10, 0, 0, 0));
    session.lock().expect("session lock poisoned").flush();
    assert!(eventually(
        || collector.get_index_usage(1, 1) != Sample::default()
    ));
    assert_eq!(collector.get_index_usage(1, 1).query_total, 1);

    statement.update(1, 1, new_sample(10, 0, 0, 0));
    session.lock().expect("session lock poisoned").flush();
    assert!(eventually(
        || collector.get_index_usage(1, 1).query_total == 1
    ));

    statement.update(1, 2, new_sample(10, 0, 0, 0));
    session.lock().expect("session lock poisoned").flush();
    assert!(eventually(
        || collector.get_index_usage(1, 2) != Sample::default()
    ));
    assert_eq!(collector.get_index_usage(1, 2).query_total, 1);

    statement.update(1, 3, new_sample(0, 0, 0, 0));
    session.lock().expect("session lock poisoned").flush();
    assert!(eventually(
        || collector.get_index_usage(1, 3) != Sample::default()
    ));
    assert_eq!(collector.get_index_usage(1, 3).query_total, 1);

    collector.close();
}
