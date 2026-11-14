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

mod azblob;
mod cloud;
mod gcs;
mod hdfs;
mod noop;
mod object_storage;
mod oss;
mod s3_transport;
pub mod storage;
pub use storage::Storage;

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

    /// Go `LocalStorage.FileExists`.
    pub fn file_exists(&self, name: &str) -> io::Result<bool> {
        match std::fs::metadata(self.base.join(name)) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Go `ReadFile`.
    pub fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        std::fs::read(self.base.join(name))
    }

    /// Go `LocalStorage.URI`.
    pub fn uri(&self) -> String {
        format!("file://{}", self.base.display())
    }

    /// Go `LocalStorage.WriteFile`: publish a completed temporary file by rename.
    pub fn write_file(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt as _;
        let path = self.base.join(name);
        let temporary = PathBuf::from(format!("{}.tmp.{}", path.display(), uuid::Uuid::new_v4()));
        let write = || -> io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o644)
                .open(&temporary)?;
            std::io::Write::write_all(&mut file, bytes)
        };
        if let Err(error) = write() {
            let parent = temporary.parent().unwrap_or(Path::new("."));
            if parent.exists() {
                return Err(error);
            }
            std::fs::create_dir_all(parent)?;
            write()?;
        }
        std::fs::rename(temporary, path)
    }

    /// Go `LocalStorage.Open` with default reader options.
    pub fn open(&self, name: &str) -> io::Result<std::fs::File> {
        std::fs::File::open(self.base.join(name))
    }

    /// Go `LocalStorage.Rename`.
    pub fn rename(&self, old: &str, new: &str) -> io::Result<()> {
        std::fs::rename(self.base.join(old), self.base.join(new))
    }

    /// Go `LocalStorage.DeleteFiles`: stop at the first failure.
    pub fn delete_files(&self, names: &[&str]) -> Result<(), String> {
        for name in names {
            self.delete_file(name)?;
        }
        Ok(())
    }

    /// Go `DeleteFile`: `failed to delete file <name>` on failure.
    pub fn delete_file(&self, name: &str) -> Result<(), String> {
        std::fs::remove_file(self.base.join(name))
            .map_err(|error| format!("failed to delete file {name}: {error}"))
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
        Ok(LocalWriter(std::io::BufWriter::new(std::fs::File::create(
            file_name,
        )?)))
    }
}

/// Go `globalExtStorage`.
static GLOBAL_EXT_STORAGE: Mutex<Option<Arc<dyn Storage>>> = Mutex::new(None);

/// Go `GetGlobalExtStorage`: created once, then shared.
pub fn get_global_ext_storage() -> Result<Arc<dyn Storage>, String> {
    let mut global = GLOBAL_EXT_STORAGE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(storage) = global.as_ref() {
        return Ok(Arc::clone(storage));
    }
    let storage = create_global_ext_storage()?;
    *global = Some(Arc::clone(&storage));
    Ok(storage)
}

/// Go `SetGlobalExtStorageForTest`.
pub fn set_global_ext_storage_for_test(storage: Option<Arc<dyn Storage>>) {
    *GLOBAL_EXT_STORAGE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = storage;
}

/// Go `createGlobalExtStorage`.
fn create_global_ext_storage() -> Result<Arc<dyn Storage>, String> {
    let namespace = tidb_keyspace::get_keyspace_name_by_settings();
    let configured_uri = tidb_vardef::CLOUD_STORAGE_URI
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if !tidb_config_kerneltype::is_classic() && !configured_uri.is_empty() {
        return new_ext_storage(&configured_uri, &namespace);
    }
    let local_path = get_local_path_dir_name();
    let local_path = std::path::absolute(&local_path).unwrap_or(local_path);
    tidb_logutil::bg_logger().warn(
        "using default local storage",
        &[
            tidb_log::Field::new("category", tidb_log::Value::Str("extstore".to_owned())),
            tidb_log::Field::new(
                "localPath",
                tidb_log::Value::Str(local_path.display().to_string()),
            ),
        ],
    );
    new_ext_storage(&format!("file://{}", local_path.display()), &namespace)
}

/// Go `NewExtStorage`: parse the backend URL and append the keyspace namespace.
pub fn new_ext_storage(raw_url: &str, namespace: &str) -> Result<Arc<dyn Storage>, String> {
    // Go `objstore.ParseRawURL` protects plus signs, including credential query values.
    let escaped = raw_url.replace('+', "%2B");
    let scheme_end = escaped.find(':').filter(|at| {
        let scheme = &escaped[..*at];
        !scheme.is_empty()
            && scheme.as_bytes()[0].is_ascii_alphabetic()
            && scheme
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"+-.".contains(&c))
    });
    let parsed = scheme_end
        .map(|_| {
            url::Url::parse(&escaped)
                .map_err(|error| format!("parse {}: {error}", tidb_ast::redact_url(raw_url)))
        })
        .transpose()?;
    let scheme = parsed.as_ref().map_or("", url::Url::scheme);
    // Go net/url retains dot components, and opaque URLs have an empty Path.
    let path_and_suffix = if let Some(at) = scheme_end {
        let rest = &escaped[at + 1..];
        if let Some(authority) = rest.strip_prefix("//") {
            authority
                .find(['/', '?', '#'])
                .map(|at| &authority[at..])
                .unwrap_or("")
        } else if rest.starts_with('/') {
            rest
        } else {
            ""
        }
    } else {
        escaped.as_str()
    };
    let split = path_and_suffix
        .find(['?', '#'])
        .unwrap_or(path_and_suffix.len());
    let raw_path = &path_and_suffix[..split];
    let suffix = &path_and_suffix[split..];
    for (at, byte) in raw_path.bytes().enumerate() {
        if byte == b'%' && !raw_path.as_bytes().get(at + 1..at + 3)
            .is_some_and(|pair| pair.iter().all(u8::is_ascii_hexdigit)) {
            return Err("invalid storage URI path escape".into());
        }
    }
    let decoded = percent_encoding::percent_decode_str(raw_path)
        .decode_utf8()
        .map_err(|_| "invalid storage URI path".to_owned())?;
    let path = PathBuf::from(decoded.as_ref());
    let mut joined = path;
    if !namespace.is_empty() {
        // Go filepath.Join cleans components after concatenation; a leading slash
        // in the namespace does not discard the storage prefix.
        joined.push(namespace.trim_start_matches('/'));
        joined = clean_path(&joined);
    }
    if scheme == "hdfs" {
        let mut url = url::Url::parse(&escaped).map_err(|error| error.to_string())?;
        url.set_path(&joined.to_string_lossy());
        return Ok(Arc::new(hdfs::HDFSStorage::new(url.into())));
    }
    if scheme == "noop" {
        return Ok(Arc::new(noop::NoopStorage));
    }
    if matches!(scheme, "s3" | "ks3") && !parsed.as_ref().is_some_and(uses_oss) {
        let url = url::Url::parse(&escaped).map_err(|error| error.to_string())?;
        return cloud::S3Storage::new(&url, &joined.to_string_lossy())
            .map(|storage| storage as Arc<dyn Storage>);
    }
    if parsed.as_ref().is_some_and(uses_oss) {
        let url = url::Url::parse(&escaped).map_err(|_| "invalid OSS storage URI".to_owned())?;
        return oss::new(&url, &joined.to_string_lossy(), None).map(|s| s as Arc<dyn Storage>);
    }
    if matches!(scheme, "gs" | "gcs") {
        let url = url::Url::parse(&escaped).map_err(|_| "invalid GCS storage URI".to_owned())?;
        return gcs::new(&url, &joined.to_string_lossy()).map(|s| s as Arc<dyn Storage>);
    }
    if matches!(scheme, "azure" | "azblob") {
        let url = url::Url::parse(&escaped).map_err(|_| "invalid Azure storage URI".to_owned())?;
        return azblob::new(&url, &joined.to_string_lossy()).map(|s| s as Arc<dyn Storage>);
    }
    if !matches!(scheme, "" | "file" | "local") {
        return Err(format!("storage {scheme} not support yet"));
    }
    if scheme.is_empty() {
        // Go ParseBackendFromURL applies filepath.Abs to URL.String, including
        // its query/fragment. RawPath survives only while Path is unchanged.
        let path = if namespace.is_empty() && raw_path.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || b"!$&'()*+,-./:;=@[]_%~".contains(&byte)
        }) {
            raw_path.to_owned()
        } else {
            escape_url_path(&joined.to_string_lossy())
        };
        joined = PathBuf::from(format!("{path}{suffix}"));
        // Go filepath.Abs("") returns the working directory. Rust rejects an
        // empty argument, so make that same directory explicit.
        joined = std::path::absolute(if joined.as_os_str().is_empty() {
            Path::new(".")
        } else {
            &joined
        })
        .map_err(|error| error.to_string())?;
        joined = clean_path(&joined);
    }
    LocalStorage::new(joined)
        .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
        .map_err(|error| error.to_string())
}

/// Go `ParseBackendFromURL`: explicit schemes override the S3 provider option.
fn uses_oss(url: &url::Url) -> bool {
    url.scheme() == "oss"
        || url.scheme() == "s3"
            && cloud::query_options(url)
                .get("provider")
                .is_some_and(|value| value == "oss")
}
/// Go `net/url.URL.EscapedPath` after changing Path with filepath.Join.
fn escape_url_path(path: &str) -> String {
    let mut result = String::new();
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~$&+,/:;=@".contains(&byte) {
            result.push(char::from(byte));
        } else {
            use std::fmt::Write;
            write!(result, "%{byte:02X}").unwrap();
        }
    }
    result
}

/// Go `filepath.Clean` after joining the external-storage namespace.
fn clean_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut result = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                if result.file_name().is_some_and(|name| name != "..") {
                    result.pop();
                } else if !result.has_root() {
                    result.push("..");
                }
            }
            other => result.push(other.as_os_str()),
        }
    }
    if result.as_os_str().is_empty() {
        result.push(".");
    }
    result
}

/// Go `getLocalPathDirName`: the log file's directory when a file can be
/// written in its `replayer` subdirectory, else the configured temp dir.
pub fn get_local_path_dir_name() -> PathBuf {
    let config = tidb_config::config_tree::config::get_global_config();
    let log_dir = Path::new(&config.log.file.filename)
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let log_dir=clean_path(&log_dir);
    if can_write_to_replayer_dir_file(&log_dir) {
        return log_dir;
    }
    PathBuf::from(&config.temp_dir)
}

/// Go `canWriteToReplayerDirFile`: opens, writes one byte to and removes
/// `<dir>/replayer/test_<time>.txt` (Go does not create the directory).
fn can_write_to_replayer_dir_file(dir: &Path) -> bool {
    let time = chrono::Local::now().format("%Y%m%d%H%M%S");
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
        assert_eq!(
            storage.walk_dir("replayer").unwrap(),
            ["replayer/a.zip", "replayer/b.zip"]
        );
        assert_eq!(storage.read_file("other/c.txt").unwrap(), b"other/c.txt");
        storage.delete_file("replayer/a.zip").unwrap();
        assert_eq!(storage.walk_dir("replayer").unwrap(), ["replayer/b.zip"]);
        assert!(storage.walk_dir("missing").unwrap().is_empty());
        assert!(
            storage
                .delete_file("replayer/a.zip")
                .unwrap_err()
                .starts_with("failed to delete file replayer/a.zip")
        );
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
        assert!(
            std::fs::read_dir(base.join("replayer"))
                .unwrap()
                .next()
                .is_none()
        );
        std::fs::remove_dir_all(&base).unwrap();
    }
}

#[cfg(test)]
mod go_tests {
    use super::*;
    use std::io::Read as _;
    use tidb_replayer::{ObjectWriter as _, Storage as _};

    /// Go `zzprobe/o15_extstore.TestExtStorageParse`.
    #[test]
    fn test_ext_storage_parse() {
        let root = std::env::current_dir()
            .unwrap()
            .join(format!("o15-parse-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        for (raw, namespace, expected) in [
            ("relative+file", "", "relative%2Bfile"),
            ("relative+file", "tenant", "relative+file/tenant"),
            ("relative%20file", "tenant", "relative%20file/tenant"),
            ("relative file", "", "relative%20file"),
            ("relative[abc]", "", "relative[abc]"),
            ("relative[abc]", "tenant", "relative%5Babc%5D/tenant"),
            ("relative!file", "", "relative!file"),
            ("relative!file", "tenant", "relative%21file/tenant"),
            ("relative?x=1", "tenant", "relative/tenant?x=1"),
        ] {
            let storage = new_ext_storage(&format!("{}/{raw}", root.display()), namespace).unwrap();
            assert_eq!(
                storage.uri(),
                format!("file://{}/{expected}", root.display())
            );
        }
        for raw in ["relative%zz", "relative%"] {
            assert!(new_ext_storage(raw, "").is_err());
        }
        assert!(
            new_ext_storage("s3:bucket", "tenant")
                .err()
                .unwrap()
                .contains("please specify the bucket")
        );
        assert_eq!(
            new_ext_storage("noop:opaque", "tenant").unwrap().uri(),
            "noop:///"
        );
        for (uri, expected) in [
            ("s3://bucket/base?PROVIDER=oss", true),
            ("ks3://bucket/base?provider=oss", false),
        ] {
            assert_eq!(uses_oss(&url::Url::parse(uri).unwrap()), expected);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Go `TestExtStorage` (`pkg/planner/extstore/extstore_test.go`).
    #[test]
    fn test_ext_storage() {
        let base = std::env::temp_dir().join(format!("o15-extstore-{}", uuid::Uuid::new_v4()));
        let storage =
            new_ext_storage(&format!("file://{}", base.display()), "test_namespace").unwrap();
        assert!(storage.uri().contains(base.to_str().unwrap()));
        assert!(storage.uri().contains("test_namespace"));
        storage.write_file("test_file.txt", b"hello world").unwrap();
        assert_eq!(storage.read_file("test_file.txt").unwrap(), b"hello world");
        assert!(storage.file_exists("test_file.txt").unwrap());
        assert_eq!(storage.walk_dir("").unwrap(), ["test_file.txt"]);
        assert_eq!(
            storage
                .open("test_file.txt")
                .unwrap()
                .get_file_size()
                .unwrap(),
            11
        );
        storage.delete_file("test_file.txt").unwrap();
        assert!(!storage.file_exists("test_file.txt").unwrap());
        let mut writer = storage.create(Path::new("test_writer.txt")).unwrap();
        writer.write(b"test writer").unwrap();
        writer.close().unwrap();
        assert_eq!(
            storage.read_file("test_writer.txt").unwrap(),
            b"test writer"
        );
        let mut data = [0; 11];
        storage
            .open("test_writer.txt")
            .unwrap()
            .read_exact(&mut data)
            .unwrap();
        assert_eq!(&data, b"test writer");
        storage
            .rename("test_writer.txt", "test_writer_renamed.txt")
            .unwrap();
        assert!(!storage.file_exists("test_writer.txt").unwrap());
        assert!(storage.file_exists("test_writer_renamed.txt").unwrap());
        storage.write_file("file1", b"1").unwrap();
        storage.write_file("file2", b"2").unwrap();
        storage.delete_files(&["file1", "file2"]).unwrap();
        assert!(!storage.file_exists("file1").unwrap());
        assert!(!storage.file_exists("file2").unwrap());
        std::fs::remove_dir_all(base).unwrap();
    }

    struct RestoreConfig(
        std::sync::Arc<tidb_config::config_tree::config::Config>,
        String,
    );
    impl Drop for RestoreConfig {
        fn drop(&mut self) {
            tidb_config::config_tree::config::store_global_config(self.0.clone());
            *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = self.1.clone();
            set_global_ext_storage_for_test(None);
        }
    }

    /// Go `TestGetLocalPathDirNameWithWritePerm`.
    #[test]
    fn test_get_local_path_dir_name_with_write_perm() {test_local_path(true);}

    /// Go `TestGetLocalPathDirNameWithoutWritePerm`.
    #[test]
    fn test_get_local_path_dir_name_without_write_perm() {test_local_path(false);}

    /// Go extstore local-directory fixtures, using isolated native directories.
    fn test_local_path(writable:bool) {
        use tidb_config::config_tree::config::{get_global_config,update_global};
        use std::os::unix::fs::PermissionsExt;
        let _restore=RestoreConfig(get_global_config(),tidb_vardef::CLOUD_STORAGE_URI.read().unwrap().clone());
        let base=std::env::temp_dir().join(format!("o15-local-path-{}",uuid::Uuid::new_v4()));
        let log=base.join("log");let temp=base.join("tmp");let replayer=log.join("replayer");
        std::fs::create_dir_all(&replayer).unwrap();
        if !writable {std::fs::set_permissions(&replayer,std::fs::Permissions::from_mode(0o555)).unwrap();}
        update_global(|config|{config.log.file.filename=log.join("tidb.log").to_string_lossy().into_owned();config.temp_dir=temp.to_string_lossy().into_owned();});
        let actual=get_local_path_dir_name();
        std::fs::set_permissions(&replayer,std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(base).unwrap();
        assert_eq!(actual,if writable{log}else{temp});
    }

    /// Go `TestGetGlobalExtStorageWithWritePerm`.
    #[test]
    fn test_get_global_ext_storage_with_write_perm() {
        test_global_storage(true);
    }

    /// Go `TestGetGlobalExtStorageWithoutWritePerm`.
    #[test]
    fn test_get_global_ext_storage_without_write_perm() {
        test_global_storage(false);
    }

    /// Go `TestGetGlobalExtStorageWithWritePerm` / `WithoutWritePerm` setup.
    fn test_global_storage(writable: bool) {
        use tidb_config::config_tree::config::{get_global_config, update_global};
        let _restore = RestoreConfig(
            get_global_config(),
            tidb_vardef::CLOUD_STORAGE_URI.read().unwrap().clone(),
        );
        let base = std::env::temp_dir().join(format!("o15-extstore-{}", uuid::Uuid::new_v4()));
        let log = base.join("log");
        let temp = base.join("tmp");
        if writable {
            std::fs::create_dir_all(log.join("replayer")).unwrap();
        }
        update_global(|config| {
            config.log.file.filename = log.join("tidb.log").to_string_lossy().into_owned();
            config.temp_dir = temp.to_string_lossy().into_owned();
        });
        *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = String::new();
        set_global_ext_storage_for_test(None);
        let storage = get_global_ext_storage().unwrap();
        assert!(storage.uri().starts_with(&format!(
            "file://{}",
            if writable { &log } else { &temp }.display()
        )));
        assert!(Arc::ptr_eq(&storage, &get_global_ext_storage().unwrap()));
        std::fs::remove_dir_all(base).unwrap();
    }
}

/// Go `objstore.ValidateCloudStorageURI`: construction includes permission checks.
pub fn validate_cloud_storage_uri(ctx: &tidb_gocontext::Context, uri: &str) -> Result<(), String> {
    let escaped = uri.replace('+', "%2B");
    if let Ok(url) = url::Url::parse(&escaped) {
        if uses_oss(&url) {
            let prefix = percent_encoding::percent_decode_str(url.path())
                .decode_utf8()
                .map_err(|_| "invalid storage prefix".to_owned())?;
            let storage = oss::new(&url, &prefix, Some(ctx))?;
            storage.close();
            return Ok(());
        }
    }
    if escaped.starts_with("s3://") || escaped.starts_with("ks3://") {
        let url = url::Url::parse(&escaped).map_err(|_| {
            format!(
                "invalid external storage URI: {}",
                tidb_ast::redact_url(uri)
            )
        })?;
        let prefix = percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .map_err(|_| "invalid storage prefix".to_owned())?;
        let storage = cloud::S3Storage::new_with_context(&url, &prefix, Some(ctx))?;
        storage.check_permissions(ctx)?;
        storage.close();
        return Ok(());
    }
    let storage = new_ext_storage(uri, "")?;
    storage.close();
    Ok(())
}

#[cfg(test)]
mod permission_tests;
