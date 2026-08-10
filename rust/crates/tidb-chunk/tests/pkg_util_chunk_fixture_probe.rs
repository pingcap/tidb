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

//! Measured fixture-resolution probes for dynamic spill-file names in
//! `pkg/util/chunk/row_in_disk_test.go`.

mod pkg_util_chunk_fixture_observation;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row_in_disk::DataInDiskByRows;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

const CONCLUSION: &str = "the dynamic os.Stat/os.Open arguments are runtime-created spill paths, not repository fixture artifacts";
const LIVE_RESULT: &str = "exists=true,temp-root=true,repository=false";
const CLOSED_RESULT: &str = "exists=false,temp-root=true,repository=false";
const OPEN_RESULT: &str = "open=true,temp-root=true,repository=false";
const REOPEN_CLOSED_RESULT: &str = "open=false,temp-root=true,repository=false";

static TEMP_STORAGE_LOCK: Mutex<()> = Mutex::new(());

fn lock_temp_storage() -> MutexGuard<'static, ()> {
    TEMP_STORAGE_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("crate lives below rust/crates")
        .canonicalize()
        .expect("repository root")
}

fn scratch_dir(case: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "tidb_chunk_fixture_probe_{case}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create scratch directory");
    path.canonicalize().expect("canonical scratch directory")
}

fn spilled_container(case: &str) -> (DataInDiskByRows, PathBuf, PathBuf, PathBuf) {
    let scratch = scratch_dir(case);
    let storage = Arc::new(
        SpillStorage::open(SpillStorageSpec {
            path: scratch.clone(),
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        })
        .expect("spill storage"),
    );
    let repository = repository_root();
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut chunk = Chunk::new_with_capacity(&fields, 1);
    chunk.append_int64(0, 7);
    let mut container = DataInDiskByRows::new(fields, storage);
    container.add(&chunk).expect("spill one row");
    let path = container
        .data_file_path()
        .cloned()
        .expect("data spill path");
    assert!(path.starts_with(&scratch));
    assert!(!path.starts_with(&repository));
    (container, path, scratch, repository)
}

fn path_state(path: &Path, scratch: &Path, repository: &Path) -> String {
    format!(
        "exists={},temp-root={},repository={}",
        path.exists(),
        path.starts_with(scratch),
        path.starts_with(repository)
    )
}

fn open_state(path: &Path, scratch: &Path, repository: &Path) -> String {
    format!(
        "open={},temp-root={},repository={}",
        std::fs::File::open(path).is_ok(),
        path.starts_with(scratch),
        path.starts_with(repository)
    )
}

#[test]
fn stat_dynamic_spill_path_has_no_repository_artifact() {
    let _guard = lock_temp_storage();
    let (mut container, path, scratch, repository) = spilled_container("stat");
    let live = path_state(&path, &scratch, &repository);
    assert_eq!(live, LIVE_RESULT);
    container.close();
    let closed = path_state(&path, &scratch, &repository);
    assert_eq!(closed, CLOSED_RESULT);

    pkg_util_chunk_fixture_observation::emit(
        "chunk-row-in-disk-stat-path",
        CONCLUSION,
        &[
            (
                "live-spill-file",
                "the data spill path before DataInDiskByRows::close",
                &live,
            ),
            (
                "closed-spill-file",
                "the same data spill path after DataInDiskByRows::close",
                &closed,
            ),
        ],
    );
    drop(container);
    std::fs::remove_dir_all(scratch).expect("remove scratch directory");
}

#[test]
fn open_dynamic_spill_path_has_no_repository_artifact() {
    let _guard = lock_temp_storage();
    let (mut container, path, scratch, repository) = spilled_container("open");
    let opened = open_state(&path, &scratch, &repository);
    assert_eq!(opened, OPEN_RESULT);
    container.close();
    let reopened = open_state(&path, &scratch, &repository);
    assert_eq!(reopened, REOPEN_CLOSED_RESULT);

    pkg_util_chunk_fixture_observation::emit(
        "chunk-row-in-disk-open-path",
        CONCLUSION,
        &[
            (
                "open-live-spill-file",
                "open the data spill path before DataInDiskByRows::close",
                &opened,
            ),
            (
                "open-closed-spill-file",
                "open the same data spill path after DataInDiskByRows::close",
                &reopened,
            ),
        ],
    );
    drop(container);
    std::fs::remove_dir_all(scratch).expect("remove scratch directory");
}
