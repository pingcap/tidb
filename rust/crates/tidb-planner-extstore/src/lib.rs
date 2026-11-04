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

//! Go `pkg/planner/extstore`: the process-wide external storage the plan
//! replayer, optimizer trace and extract task files live in. A classic
//! kernel always uses a local directory -- the log file's directory when its
//! `replayer` subdirectory is writable, else the configured temp dir -- over
//! Go `objstore.LocalStorage`.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Go `objstore.LocalStorage`: storage-relative paths under `base`.
#[derive(Clone, Debug)]
pub struct LocalStorage {
    base: PathBuf,
}

impl LocalStorage {
    /// Go `NewLocalStorage`: creates `base` when missing.
    pub fn new(base: impl Into<PathBuf>) -> io::Result<Self> {
        let base = base.into();
        std::fs::create_dir_all(&base)?;
        Ok(Self { base })
    }

    /// The storage's root directory.
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Go `LocalStorage.PresignFile`: the file name alone.
    pub fn presign_file(&self, file_name: &str) -> String {
        Path::new(file_name)
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
    }

    /// Go `ReadFile`.
    pub fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        std::fs::read(self.base.join(name))
    }

    /// Go `DeleteFile`: `failed to delete file <name>` on failure.
    pub fn delete_file(&self, name: &str) -> Result<(), String> {
        std::fs::remove_file(self.base.join(name)).map_err(|error| format!("failed to delete file {name}: {error}"))
    }

    /// Go `WalkDir(&WalkOption{SubDir})`: every regular file under the
    /// subdirectory, as paths relative to the storage base, in Go
    /// `filepath.Walk`'s lexical order. A missing subdirectory walks nothing.
    pub fn walk_dir(&self, sub_dir: &str) -> io::Result<Vec<String>> {
        fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) -> io::Result<()> {
            let mut entries: Vec<_> = match std::fs::read_dir(dir) {
                Ok(entries) => entries.collect::<Result<_, _>>()?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            entries.sort_by_key(std::fs::DirEntry::file_name);
            for entry in entries {
                let path = entry.path();
                if entry.file_type()?.is_dir() {
                    walk(base, &path, out)?;
                } else if let Ok(relative) = path.strip_prefix(base) {
                    out.push(relative.to_string_lossy().into_owned());
                }
            }
            Ok(())
        }
        let mut out = Vec::new();
        walk(&self.base, &self.base.join(sub_dir), &mut out)?;
        Ok(out)
    }
}

/// Go `objstore`'s local writer behind `Create`.
pub struct LocalWriter(std::io::BufWriter<std::fs::File>);

impl tidb_replayer::ObjectWriter for LocalWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        std::io::Write::write(&mut self.0, bytes)
    }

    fn close(&mut self) -> io::Result<()> {
        std::io::Write::flush(&mut self.0)
    }
}

impl tidb_replayer::Storage for LocalStorage {
    type Writer = LocalWriter;

    /// Go `LocalStorage.Create`: makes the file's directory, then creates it.
    fn create(&self, path: &Path) -> io::Result<LocalWriter> {
        let file_name = self.base.join(path);
        if let Some(dir) = file_name.parent() {
            std::fs::create_dir_all(dir)?;
        }
        Ok(LocalWriter(std::io::BufWriter::new(std::fs::File::create(file_name)?)))
    }
}

/// Go `globalExtStorage`.
static GLOBAL_EXT_STORAGE: Mutex<Option<Arc<LocalStorage>>> = Mutex::new(None);

/// Go `GetGlobalExtStorage`: created once, then shared.
pub fn get_global_ext_storage() -> Result<Arc<LocalStorage>, String> {
    let mut global = GLOBAL_EXT_STORAGE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(storage) = global.as_ref() {
        return Ok(Arc::clone(storage));
    }
    let storage = Arc::new(create_global_ext_storage()?);
    *global = Some(Arc::clone(&storage));
    Ok(storage)
}

/// Go `SetGlobalExtStorageForTest`.
pub fn set_global_ext_storage_for_test(storage: Option<Arc<LocalStorage>>) {
    *GLOBAL_EXT_STORAGE.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = storage;
}

/// Go `createGlobalExtStorage` on a classic kernel: the local directory.
/// A next-gen kernel's `tidb_cloud_storage_uri` storage has no port (O15).
fn create_global_ext_storage() -> Result<LocalStorage, String> {
    if !tidb_config_kerneltype::is_classic() {
        return Err("external storage of a next-gen kernel is not supported".to_owned());
    }
    let local_path = get_local_path_dir_name();
    let local_path = std::path::absolute(&local_path).unwrap_or(local_path);
    tidb_logutil::bg_logger().warn(
        "using default local storage",
        &[
            tidb_log::Field::new("category", tidb_log::Value::Str("extstore".to_owned())),
            tidb_log::Field::new("localPath", tidb_log::Value::Str(local_path.display().to_string())),
        ],
    );
    LocalStorage::new(local_path).map_err(|error| error.to_string())
}

/// Go `getLocalPathDirName`: the log file's directory when a file can be
/// written in its `replayer` subdirectory, else the configured temp dir.
pub fn get_local_path_dir_name() -> PathBuf {
    let config = tidb_config::config_tree::config::get_global_config();
    let log_dir = Path::new(&config.log.file.filename)
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    if can_write_to_replayer_dir_file(&log_dir) {
        return log_dir;
    }
    PathBuf::from(&config.temp_dir)
}

/// Go `canWriteToReplayerDirFile`: opens, writes one byte to and removes
/// `<dir>/replayer/test_<time>.txt` (Go does not create the directory).
fn can_write_to_replayer_dir_file(dir: &Path) -> bool {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    let path = dir.join("replayer").join(format!("test_{time}.txt"));
    let written = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .and_then(|mut file| std::io::Write::write_all(&mut file, &[0]));
    let _ = std::fs::remove_file(&path);
    written.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_replayer::{ObjectWriter as _, Storage as _};

    /// Go `LocalStorage`: `Create` makes the directories; `WalkDir` lists a
    /// subdirectory's files relative to the base; `DeleteFile` removes one.
    #[test]
    fn local_storage_creates_walks_and_deletes_like_go() {
        let base = std::env::temp_dir().join(format!("extstore-{}", std::process::id()));
        let storage = LocalStorage::new(&base).unwrap();
        for name in ["replayer/b.zip", "replayer/a.zip", "other/c.txt"] {
            let mut writer = storage.create(Path::new(name)).unwrap();
            writer.write(name.as_bytes()).unwrap();
            writer.close().unwrap();
        }
        assert_eq!(storage.walk_dir("replayer").unwrap(), ["replayer/a.zip", "replayer/b.zip"]);
        assert_eq!(storage.read_file("other/c.txt").unwrap(), b"other/c.txt");
        storage.delete_file("replayer/a.zip").unwrap();
        assert_eq!(storage.walk_dir("replayer").unwrap(), ["replayer/b.zip"]);
        assert!(storage.walk_dir("missing").unwrap().is_empty());
        assert!(storage.delete_file("replayer/a.zip").unwrap_err().starts_with("failed to delete file replayer/a.zip"));
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// Go `canWriteToReplayerDirFile` needs an existing `replayer`
    /// subdirectory; it leaves no probe file behind.
    #[test]
    fn the_replayer_dir_probe_needs_an_existing_subdirectory() {
        let base = std::env::temp_dir().join(format!("extstore-probe-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        assert!(!can_write_to_replayer_dir_file(&base));
        std::fs::create_dir_all(base.join("replayer")).unwrap();
        assert!(can_write_to_replayer_dir_file(&base));
        assert!(std::fs::read_dir(base.join("replayer")).unwrap().next().is_none());
        std::fs::remove_dir_all(&base).unwrap();
    }
}
