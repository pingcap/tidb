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

//! Source-backed tests for [`tidb_exec::batch_flusher`], ported from
//! `pkg/resourcegroup/runaway/flusher_test.go`: `TestBatchFlusherAdd`,
//! `TestBatchFlusherMergeFn`, `TestBatchFlusherFlush`, and
//! `TestBatchFlusherFlushEmpty`. Expected values are byte-exact with the Go
//! originals; `newTestBatchFlusher`'s one-hour ticker interval is preserved
//! so the ticker never fires during a test run.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_exec::batch_flusher::BatchFlusher;

/// Go `flusher_test.go`'s locally defined `Record` (used only by
/// `TestBatchFlusherMergeFn`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    sql_digest: String,
    repeats: i32,
}

const TEST_TICKER_INTERVAL: Duration = Duration::from_secs(3600);

#[test]
fn test_batch_flusher_add() {
    // Source: pkg/resourcegroup/runaway/flusher_test.go:47-70.
    let flush_count = Arc::new(AtomicI32::new(0));
    let counted = Arc::clone(&flush_count);
    let mut flusher: BatchFlusher<String, i32> = BatchFlusher::new(
        "test",
        TEST_TICKER_INTERVAL,
        3,
        Box::new(|m, k, v| {
            m.insert(k, v);
        }),
        Box::new(move |_m| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );
    assert!(flusher.is_empty());

    flusher.add("a".to_owned(), 1);
    flusher.add("b".to_owned(), 2);
    assert_eq!(flusher.len(), 2);
    assert_eq!(flush_count.load(Ordering::SeqCst), 0);

    flusher.add("c".to_owned(), 3);
    assert_eq!(flusher.len(), 0);
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);

    flusher.add("d".to_owned(), 4);
    assert_eq!(flusher.len(), 1);
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_batch_flusher_merge_fn() {
    // Source: pkg/resourcegroup/runaway/flusher_test.go:72-100.
    let last_buffer: Arc<std::sync::Mutex<HashMap<String, Record>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let captured = Arc::clone(&last_buffer);
    let mut flusher: BatchFlusher<String, Record> = BatchFlusher::new(
        "test",
        TEST_TICKER_INTERVAL,
        10,
        Box::new(|m, k, v: Record| {
            if let Some(existing) = m.get_mut(&k) {
                existing.repeats += 1;
            } else {
                m.insert(k, v);
            }
        }),
        Box::new(move |m| {
            *captured.lock().unwrap() = m.clone();
            Ok(())
        }),
    );

    flusher.add(
        "key1".to_owned(),
        Record {
            sql_digest: "d1".to_owned(),
            repeats: 1,
        },
    );
    flusher.add(
        "key1".to_owned(),
        Record {
            sql_digest: "d1".to_owned(),
            repeats: 1,
        },
    );
    flusher.add(
        "key1".to_owned(),
        Record {
            sql_digest: "d1".to_owned(),
            repeats: 1,
        },
    );
    flusher.add(
        "key2".to_owned(),
        Record {
            sql_digest: "d2".to_owned(),
            repeats: 1,
        },
    );

    assert_eq!(flusher.len(), 2);
    assert_eq!(flusher.buffer()["key1"].repeats, 3);
    assert_eq!(flusher.buffer()["key2"].repeats, 1);

    flusher.flush();
    assert_eq!(flusher.len(), 0);
    assert_eq!(last_buffer.lock().unwrap()["key1"].repeats, 3);
}

#[test]
fn test_batch_flusher_flush() {
    // Source: pkg/resourcegroup/runaway/flusher_test.go:102-124.
    let flush_count = Arc::new(AtomicI32::new(0));
    let counted = Arc::clone(&flush_count);
    let mut flusher: BatchFlusher<String, i32> = BatchFlusher::new(
        "test",
        TEST_TICKER_INTERVAL,
        100,
        Box::new(|m, k, v| {
            m.insert(k, v);
        }),
        Box::new(move |_m| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );
    assert!(flusher.is_empty());

    flusher.add("a".to_owned(), 1);
    assert_eq!(flusher.len(), 1);
    assert_eq!(flush_count.load(Ordering::SeqCst), 0);

    flusher.flush();
    assert_eq!(flusher.len(), 0);
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);

    flusher.add("b".to_owned(), 2);
    assert_eq!(flusher.len(), 1);
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_batch_flusher_flush_empty() {
    // Source: pkg/resourcegroup/runaway/flusher_test.go:126-145.
    let flush_count = Arc::new(AtomicI32::new(0));
    let counted = Arc::clone(&flush_count);
    let mut flusher: BatchFlusher<String, i32> = BatchFlusher::new(
        "test",
        TEST_TICKER_INTERVAL,
        10,
        Box::new(|m, k, v| {
            m.insert(k, v);
        }),
        Box::new(move |_m| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );

    flusher.flush();
    assert_eq!(flush_count.load(Ordering::SeqCst), 0);

    flusher.add("a".to_owned(), 1);
    flusher.flush();
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);

    flusher.flush();
    assert_eq!(flush_count.load(Ordering::SeqCst), 1);
}
