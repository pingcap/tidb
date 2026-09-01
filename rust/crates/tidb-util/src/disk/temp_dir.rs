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

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const LOCK_FILE: &str = "_dir.lock";
const RECORD_DIR: &str = "record";

struct TempDirLock {
    file: File,
    path: PathBuf,
}

fn temp_dir_lock() -> &'static Mutex<Option<TempDirLock>> {
    static TEMP_DIR_LOCK: OnceLock<Mutex<Option<TempDirLock>>> = OnceLock::new();
    TEMP_DIR_LOCK.get_or_init(|| Mutex::new(None))
}

fn configured_temp_dir() -> PathBuf {
    tidb_config::config_tree::config::get_global_config()
        .temp_storage_path
        .into()
}

/// Go `CheckAndInitTempDir`.
pub fn check_and_init_temp_dir() -> io::Result<()> {
    let mut lock = temp_dir_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !check_temp_dir_exist() {
        tracing::info!("Tmp-storage-path not found. Try to initialize TempDir.");
        initialize_temp_dir_locked(&mut lock)?;
    }
    Ok(())
}

fn check_temp_dir_exist() -> bool {
    configured_temp_dir().exists()
}

/// Go `InitializeTempDir`.
pub fn initialize_temp_dir() -> io::Result<()> {
    let mut lock = temp_dir_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    initialize_temp_dir_locked(&mut lock)
}

fn initialize_temp_dir_locked(lock: &mut Option<TempDirLock>) -> io::Result<()> {
    let temp_dir = configured_temp_dir();
    if !temp_dir.exists() {
        create_dir_all_0750(&temp_dir)?;
    }

    let lock_path = temp_dir.join(LOCK_FILE);
    if lock
        .as_ref()
        .is_some_and(|held| held.path.exists() && held.path == lock_path)
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "temporary storage directory lock is held",
        ));
    }

    // A removed directory leaves the old descriptor attached to an unlinked
    // inode. Go's next fslock acquisition replaces that stale global handle.
    drop(lock.take());
    let file = open_lock_file(&lock_path)?;
    acquire_exclusive_lock(&file)?;
    *lock = Some(TempDirLock {
        file,
        path: lock_path,
    });

    let entries = fs::read_dir(&temp_dir)?.collect::<Result<Vec<_>, _>>()?;
    if entries.len() > 2 {
        std::thread::spawn(move || {
            for entry in entries {
                let name = entry.file_name();
                if name == LOCK_FILE || name == RECORD_DIR {
                    continue;
                }
                let path = entry.path();
                let result = if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    fs::remove_dir_all(&path)
                } else {
                    fs::remove_file(&path)
                };
                if let Err(error) = result {
                    tracing::warn!(temp_storage_sub_dir = %path.display(), %error, "Remove temporary file error");
                }
            }
        });
    }
    Ok(())
}

/// Go `CleanUp`.
pub fn clean_up() {
    let mut lock = temp_dir_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(held) = lock.take() {
        if let Err(error) = unlock_file(&held.file) {
            tracing::error!(%error, "release temporary storage directory lock");
        }
    }
}

/// Go `CheckAndCreateDir`.
pub fn check_and_create_dir(path: &Path) -> io::Result<()> {
    if !path.exists() {
        create_dir_all_0750(path)?;
    }
    Ok(())
}

fn create_dir_all_0750(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o750);
    }
    builder.create(path)
}

fn open_lock_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o640);
    }
    options.open(path)
}

#[cfg(unix)]
fn acquire_exclusive_lock(file: &File) -> io::Result<()> {
    rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(Into::into)
}

#[cfg(unix)]
fn unlock_file(file: &File) -> io::Result<()> {
    rustix::fs::flock(file, rustix::fs::FlockOperation::Unlock).map_err(Into::into)
}

#[cfg(windows)]
fn acquire_exclusive_lock(file: &File) -> io::Result<()> {
    fs4::FileExt::try_lock_exclusive(file)
}

#[cfg(windows)]
fn unlock_file(file: &File) -> io::Result<()> {
    fs4::FileExt::unlock(file)
}

#[cfg(not(any(unix, windows)))]
fn acquire_exclusive_lock(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exclusive temporary-storage locking is unsupported on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn unlock_file(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_dir() {
        let _guard = crate::global_logger_test_guard();
        let restore = tidb_config::config_tree::config::restore_func();
        let path = tempfile::tempdir().expect("temporary parent").keep();
        tidb_config::config_tree::config::update_global(|config| {
            config.temp_storage_path = path.to_string_lossy().into_owned();
        });

        fs::remove_dir_all(&path).expect("remove old temporary directory");
        fs::create_dir_all(&path).expect("recreate temporary directory");
        check_and_init_temp_dir().expect("existing directory needs no initialization");
        assert!(check_temp_dir_exist());
        fs::remove_dir_all(configured_temp_dir()).expect("remove configured directory");
        assert!(!check_temp_dir_exist());

        let mut workers = Vec::new();
        for _ in 0..10 {
            workers.push(std::thread::spawn(check_and_init_temp_dir));
        }
        for worker in workers {
            worker.join().expect("worker panicked").expect("initialize");
        }
        check_and_init_temp_dir().expect("directory remains initialized");
        assert!(check_temp_dir_exist());

        clean_up();
        fs::remove_dir_all(&path).expect("remove temporary directory");
        restore();
    }
}
