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

//! Port of `pkg/domain/plan_replayer_test.go` (origin/master):
//! `TestPlanReplayerDifferentGC`, `TestDumpGCFileParseTime`, and
//! `TestSendTask`, against `tidb_domain::plan_replayer` — the
//! transcreation of `pkg/domain/plan_replayer.go`.
//!
//! Go's failpoint-injected timestamps are represented as fixture data in the
//! GC test. The parse-time test calls the real transcreated
//! `GeneratePlanReplayerFileName` for all eight flag combinations.

#![cfg(test)]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, Utc};

use tidb_domain::plan_replayer::{
    parse_time, DumpFileGcChecker, DumpFileStorage, InternalSqlExecutor, PlanReplayerDumpTask,
    PlanReplayerError, PlanReplayerHandle, PlanReplayerTaskCollectorHandle, RestrictedSqlExecutor,
};
use tidb_domain::replayer::{generate_plan_replayer_file_name, get_plan_replayer_dir_name};

/// `extstore.NewExtStorage(ctx, "file://<root>", "")`: real files under a
/// scratch root, addressed by storage-relative paths.
struct DirStorage {
    root: PathBuf,
}

impl DirStorage {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "tidb_rust_plan_replayer_{}_{tag}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(&root).expect("create scratch root");
        Self { root }
    }

    /// Go `storage.FileExists(ctx, path)`.
    fn exists(&self, path: &str) -> bool {
        self.root.join(path).exists()
    }

    fn write(&self, path: &str) {
        let full = self.root.join(path);
        fs::create_dir_all(full.parent().expect("path has a parent")).expect("create dir");
        fs::write(&full, b"zip-bytes").expect("write fixture file");
    }
}

impl Drop for DirStorage {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl DumpFileStorage for DirStorage {
    fn walk_dir(&self, sub_dir: &str) -> Result<Vec<String>, PlanReplayerError> {
        let dir = self.root.join(sub_dir);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut names: Vec<String> = fs::read_dir(&dir)
            .map_err(|error| PlanReplayerError::Other(error.to_string()))?
            .map(|entry| {
                let name = entry
                    .map_err(|error| PlanReplayerError::Other(error.to_string()))?
                    .file_name()
                    .to_string_lossy()
                    .into_owned();
                Ok(format!("{sub_dir}/{name}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        Ok(names)
    }

    fn delete_file(&self, file_name: &str) -> Result<(), PlanReplayerError> {
        fs::remove_file(self.root.join(file_name))
            .map_err(|error| PlanReplayerError::Other(error.to_string()))
    }
}

/// The GC checker runs with no session attached — the shape Go's own GC test
/// builds (`paths` only, `sctx == nil`).
struct NoopExec;

impl RestrictedSqlExecutor for NoopExec {
    fn exec_restricted_sql(&self, _sql: &str, _params: &[&str]) -> Result<(), PlanReplayerError> {
        Ok(())
    }
}

impl InternalSqlExecutor for NoopExec {
    fn query_row_count(&self, _sql: &str) -> Result<Option<usize>, PlanReplayerError> {
        Ok(None)
    }
    fn query_digest_pairs(
        &self,
        _sql: &str,
    ) -> Result<Option<Vec<(String, String)>>, PlanReplayerError> {
        Ok(None)
    }
}

fn nanos(at: DateTime<Utc>) -> i64 {
    at.timestamp_nanos_opt().expect("in-range timestamp")
}

/// Go `replayer.GeneratePlanReplayerFileName`'s three output shapes
/// (`pkg/util/replayer/replayer.go:77-97`), which the GC's `parseTime` must
/// accept.
fn fixture_file_name_at(
    is_capture: bool,
    is_continues_capture: bool,
    hist: bool,
    time_nanos: i64,
) -> String {
    // base64.URLEncoding of 16 random bytes; the character family (including
    // '-' and '_') is what matters to the parser, not the entropy.
    let key = "aB12-_cd==";
    if is_continues_capture || (is_capture && hist) {
        format!("capture_replayer_{key}_{time_nanos}.zip")
    } else if is_capture && !hist {
        format!("capture_normal_replayer_{key}_{time_nanos}.zip")
    } else {
        format!("replayer_{key}_{time_nanos}.zip")
    }
}

/// Go `pkg/domain/plan_replayer_test.go:30::TestPlanReplayerDifferentGC`.
#[test]
fn plan_replayer_different_gc() {
    let storage = DirStorage::new("different_gc");
    let dir_name = get_plan_replayer_dir_name();

    let now = Utc::now();
    let hour = chrono::Duration::hours(1);

    // Four files at Go's four ages: two captures straddling the 7-day
    // capture cutoff, one plain replayer past the 1-hour default cutoff, and
    // one brand-new plain replayer.
    let time1 = nanos(now - hour * 7 * 25);
    let file_name1 = fixture_file_name_at(true, false, false, time1);
    let file_path1 = format!("{dir_name}/{file_name1}");
    storage.write(&file_path1);

    let time2 = nanos(now - hour * 7 * 23);
    let file_name2 = fixture_file_name_at(true, false, false, time2);
    let file_path2 = format!("{dir_name}/{file_name2}");
    storage.write(&file_path2);

    let time3 = nanos(now - hour * 2);
    let file_name3 = fixture_file_name_at(false, false, false, time3);
    let file_path3 = format!("{dir_name}/{file_name3}");
    storage.write(&file_path3);

    let time4 = nanos(now);
    let file_name4 = fixture_file_name_at(false, false, false, time4);
    let file_path4 = format!("{dir_name}/{file_name4}");
    storage.write(&file_path4);

    let handler: DumpFileGcChecker<NoopExec> =
        DumpFileGcChecker::new(chrono::Duration::zero(), vec![dir_name.to_owned()]);
    for result in handler.gc_dump_files(&storage, now, hour, hour * 24 * 7) {
        result.expect("walk succeeds");
    }
    assert!(
        !storage.exists(&file_path1),
        "capture 175h old is past the 7d cutoff"
    );
    assert!(
        storage.exists(&file_path2),
        "capture 161h old is inside the 7d cutoff"
    );
    assert!(
        !storage.exists(&file_path3),
        "plain file 2h old is past the 1h cutoff"
    );
    assert!(
        storage.exists(&file_path4),
        "plain file just written survives"
    );

    for result in handler.gc_dump_files(
        &storage,
        now,
        chrono::Duration::zero(),
        chrono::Duration::zero(),
    ) {
        result.expect("walk succeeds");
    }
    assert!(!storage.exists(&file_path2), "zero cutoff removes the rest");
    assert!(!storage.exists(&file_path4));
}

/// Go `pkg/domain/plan_replayer_test.go:101::TestDumpGCFileParseTime`.
#[test]
fn dump_gc_file_parse_time() {
    let now_time = Utc::now();
    let now_nanos = nanos(now_time);

    let name1 = format!("replayer_single_xxxxxx_{now_nanos}.zip");
    let pt = parse_time(&name1).expect("name1 parses");
    assert_eq!(
        pt.timestamp_nanos_opt(),
        Some(now_nanos),
        "pt.Equal(nowTime)"
    );

    // Appending one digit overflows ParseInt's int64, as in Go.
    let name2 = format!("replayer_single_xxxxxx_{now_nanos}1.zip");
    assert!(parse_time(&name2).is_err(), "name2 must not parse");

    let name3 = format!("replayer_single_xxxxxx_{now_nanos}._zip");
    assert!(parse_time(&name3).is_err(), "name3 must not parse");

    let name4 = "extract_-brq6zKMarD9ayaifkHc4A==_1678168728477502000.zip";
    assert!(parse_time(name4).is_ok(), "name4 parses");

    // Every shape GeneratePlanReplayerFileName can produce parses.
    for is_capture in [false, true] {
        for is_continues_capture in [false, true] {
            for hist in [false, true] {
                let name = generate_plan_replayer_file_name(is_capture, is_continues_capture, hist)
                    .expect("name generation succeeds");
                assert!(
                    parse_time(&name).is_ok(),
                    "generated name {name} must parse"
                );
            }
        }
    }
}

/// Go `pkg/domain/plan_replayer_test.go:162::TestSendTask`: a task channel of
/// capacity one accepts the first task and refuses the second.
#[test]
fn send_task_discards_when_the_channel_is_full() {
    let h = PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(NoopExec), 1);
    let task1 = PlanReplayerDumpTask::default();
    let task2 = PlanReplayerDumpTask::default();
    h.send_task(task1);
    let success = h.send_task(task2);
    assert!(!success);
}
