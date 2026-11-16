// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `conflictrows/cleanup_test.go`, over a local store.

use std::sync::Mutex;

use chrono::{TimeZone, Utc};
use tidb_dxf::TaskType;

use super::*;

struct Getter<F>(Mutex<F>);

impl<F> TaskCleanupInfoGetter for Getter<F>
where
    F: FnMut(&[i64]) -> tidb_dxf_storage::Result<BTreeMap<i64, TaskCleanupInfo>> + Send,
{
    fn get_task_cleanup_info_by_ids(&self, _: &Context, ids: &[i64]) -> tidb_dxf_storage::Result<BTreeMap<i64, TaskCleanupInfo>> {
        (self.0.lock().unwrap())(ids)
    }
}

fn getter<F>(f: F) -> Getter<F> {
    Getter(Mutex::new(f))
}

fn now() -> DateTime<FixedOffset> {
    Utc.with_ymd_and_hms(2026, 8, 12, 12, 0, 0).unwrap().fixed_offset()
}

fn info(id: i64, tp: TaskType, state: tidb_dxf::TaskState, end_time: Option<DateTime<FixedOffset>>) -> TaskCleanupInfo {
    TaskCleanupInfo { id, tp, state, end_time }
}

fn failed(id: i64) -> TaskCleanupInfo {
    info(id, IMPORT_INTO, TASK_STATE_FAILED, None)
}

fn store_with(files: &[String]) -> (tempfile::TempDir, tidb_objstore::LocalStorage) {
    let dir = tempfile::tempdir().unwrap();
    let store = tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap();
    for f in files {
        store.write_file(&Context::background(), f, b"row").unwrap();
    }
    (dir, store)
}

fn exists(store: &tidb_objstore::LocalStorage, name: &str) -> bool {
    store.file_exists(&Context::background(), name).unwrap()
}

/// Go `TestParseTaskID`.
#[test]
fn parse_task_ids() {
    let cases: &[(&str, Option<i64>)] = &[
        ("conflicted-rows/42/data-0001.txt", Some(42)),
        ("conflicted-rows/9223372036854775807/subtask/data", Some(i64::MAX)),
        ("conflicted-rows/007/data", Some(7)),
        ("other/42/data", None),
        ("conflicted-rows-old/42/data", None),
        ("conflicted-rows//data", None),
        ("conflicted-rows/0/data", None),
        ("conflicted-rows/-1/data", None),
        ("conflicted-rows/+1/data", None),
        ("conflicted-rows/1a/data", None),
        ("conflicted-rows/9223372036854775808/data", None),
        ("conflicted-rows/", None),
        ("conflicted-rows/42", None),
        ("conflicted-rows/42/", None),
        ("conflicted-rows/42//", None),
        ("conflicted-rows/42///", None),
        ("conflicted-rows/42//data", Some(42)),
    ];
    for (path, want) in cases {
        assert_eq!(parse_task_id(path), *want, "{path}");
    }
}

/// Go `TestShouldDelete`.
#[test]
fn should_delete_by_state() {
    let now = now();
    let just_too_young = now - RETENTION + Duration::nanoseconds(1);
    let exactly_expired = now - RETENTION;
    let old = now - Duration::hours(200);
    let example = TaskType::new("Example".to_owned());
    let cases = [
        (info(1, IMPORT_INTO, TASK_STATE_SUCCEED, Some(just_too_young)), false),
        (info(1, IMPORT_INTO, TASK_STATE_SUCCEED, Some(exactly_expired)), true),
        (info(1, IMPORT_INTO, TASK_STATE_SUCCEED, None), false),
        (info(1, IMPORT_INTO, TASK_STATE_FAILED, None), true),
        (info(1, IMPORT_INTO, TASK_STATE_REVERTED, None), true),
        (info(1, IMPORT_INTO, tidb_dxf::TASK_STATE_RUNNING, Some(old)), false),
        (info(1, example, TASK_STATE_FAILED, Some(old)), false),
    ];
    for (i, (info, want)) in cases.iter().enumerate() {
        assert_eq!(should_delete(info, now), *want, "case {i}");
    }
}

/// Go `TestCleanFiles/record task diagnostics` and the empty stats' JSON.
#[test]
fn task_diagnostics() {
    let files = vec!["conflicted-rows/1/a".to_owned(), "conflicted-rows/1/b".to_owned()];
    let mut stats = CleanupStats::default();
    stats.record_task_diagnostics(1, &files, None);
    assert_eq!(stats.missing_tasks, CountWithSamples { count: 1, samples: vec!["1".to_owned()] });
    assert_eq!(stats.missing_task_files, CountWithSamples { count: 2, samples: files.clone() });
    let mut stats = CleanupStats::default();
    stats.record_task_diagnostics(1, &files, Some(&info(1, TaskType::new("Example".to_owned()), TASK_STATE_FAILED, None)));
    assert_eq!(stats.non_import_into_task_files, CountWithSamples { count: 2, samples: files });
    assert_eq!(serde_json::to_string(&CleanupStats::default()).unwrap(), "{}");
    let (_dir, store) = store_with(&[]);
    let g = getter(|_: &[i64]| -> tidb_dxf_storage::Result<_> { panic!("metadata lookup should not run") });
    let (stats, result) = clean_files(&Context::background(), &store, &g, now());
    result.unwrap();
    assert_eq!(stats, CleanupStats::default());
}

/// Go `TestCleanFiles/mixed decisions`.
#[test]
fn mixed_decisions() {
    let files: Vec<String> = [
        "conflicted-rows/1/a/data",
        "conflicted-rows/2/b/data",
        "conflicted-rows/3/c/data",
        "conflicted-rows/4/d/data",
        "conflicted-rows/bad/data",
        "conflicted-rows-old/1/data",
        "other/data",
    ]
    .map(str::to_owned)
    .to_vec();
    let (_dir, store) = store_with(&files);
    let old = now() - RETENTION;
    let g = getter(move |ids: &[i64]| {
        assert_eq!(ids, [1, 2, 3, 4]);
        Ok(BTreeMap::from([
            (1, failed(1)),
            (2, info(2, IMPORT_INTO, tidb_dxf::TASK_STATE_RUNNING, None)),
            (3, info(3, TaskType::new("Example".to_owned()), TASK_STATE_FAILED, None)),
            (4, info(4, IMPORT_INTO, TASK_STATE_SUCCEED, Some(old))),
        ]))
    });
    let (stats, result) = clean_files(&Context::background(), &store, &g, now());
    result.unwrap();
    assert_eq!(stats.deleted_files, 4);
    assert_eq!(stats.missing_tasks, CountWithSamples::default());
    assert_eq!(stats.non_import_into_task_files.count, 1);
    assert_eq!(stats.unparsed_task_id_files.count, 1);
    assert_eq!(stats.failures, 0);
    let want = [false, true, false, false, false, true, true];
    for (f, want) in files.iter().zip(want) {
        assert_eq!(exists(&store, f), want, "{f}");
    }
}

/// Go `TestCleanFiles/task ID bound` and `object bound keeps current
/// callback`: one batch may exceed a bound by one.
#[test]
fn flush_bounds() {
    let files: Vec<String> = (1..=MAX_TASK_IDS_PER_FLUSH as i64 + 1).map(|id| format!("conflicted-rows/{id:03}/data")).collect();
    let (_dir, store) = store_with(&files);
    let lookups = std::sync::Arc::new(Mutex::new(Vec::new()));
    let seen = std::sync::Arc::clone(&lookups);
    let g = getter(move |ids: &[i64]| {
        seen.lock().unwrap().push(ids.len());
        Ok(ids.iter().map(|id| (*id, failed(*id))).collect())
    });
    let (stats, result) = clean_files(&Context::background(), &store, &g, now());
    result.unwrap();
    assert_eq!(*lookups.lock().unwrap(), [MAX_TASK_IDS_PER_FLUSH + 1]);
    assert_eq!(stats.deleted_files, MAX_TASK_IDS_PER_FLUSH as i64 + 1);

    let files: Vec<String> = (0..=MAX_OBJECTS_PER_FLUSH).map(|i| format!("conflicted-rows/1/data-{i:04}")).collect();
    let (_dir, store) = store_with(&files);
    let lookups = std::sync::Arc::new(Mutex::new(Vec::new()));
    let seen = std::sync::Arc::clone(&lookups);
    let g = getter(move |ids: &[i64]| {
        seen.lock().unwrap().push(ids.len());
        Ok(BTreeMap::from([(1, failed(1))]))
    });
    let (stats, result) = clean_files(&Context::background(), &store, &g, now());
    result.unwrap();
    assert_eq!(*lookups.lock().unwrap(), [1]);
    assert_eq!(stats.deleted_files, MAX_OBJECTS_PER_FLUSH as i64 + 1);
}

/// Go `TestCleanFiles/later lookup failure and retry`: a failed flush
/// counts a failure, and the next run deletes what is left.
#[test]
fn lookup_failure_then_retry() {
    let files: Vec<String> = (1..=MAX_TASK_IDS_PER_FLUSH as i64 + 2).map(|id| format!("conflicted-rows/{id:03}/data")).collect();
    let (_dir, store) = store_with(&files);
    let calls = std::sync::Arc::new(Mutex::new(0));
    let seen = std::sync::Arc::clone(&calls);
    let g = getter(move |ids: &[i64]| {
        *seen.lock().unwrap() += 1;
        if *seen.lock().unwrap() == 2 {
            return Err(tidb_dxf_storage::StorageError::Other("lookup failed".to_owned()));
        }
        Ok(ids.iter().map(|id| (*id, failed(*id))).collect())
    });
    let (stats, result) = clean_files(&Context::background(), &store, &g, now());
    assert_eq!(result.unwrap_err().to_string(), "lookup failed");
    assert_eq!((stats.deleted_files, stats.failures), (MAX_TASK_IDS_PER_FLUSH as i64 + 1, 1));
    let (stats, result) = clean_files(&Context::background(), &store, &g, now());
    result.unwrap();
    assert_eq!(stats.deleted_files, 1);
    assert!(files.iter().all(|f| !exists(&store, f)));
}

/// Go `TestCleanConflictRowFiles/empty URI`.
#[test]
fn empty_uri_is_a_no_op() {
    let g = getter(|_: &[i64]| -> tidb_dxf_storage::Result<_> { panic!("no lookup") });
    clean_conflict_row_files(&Context::background(), &g, "").unwrap();
}
