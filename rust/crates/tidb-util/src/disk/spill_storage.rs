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

//! Immutable process authority for TiDB spill storage.
//!
//! This module owns the observable storage contract: one resolved directory,
//! one encryption policy, one process-wide quota tracker, an exclusive
//! directory lease, and private atomic file creation. It deliberately leaves
//! random-name generation and lock bookkeeping to audited Rust/OS primitives.

use std::collections::HashSet;
#[cfg(unix)]
use std::fs::Metadata;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

use crate::{
    memory::{Tracker, LABEL_FOR_GLOBAL_STORAGE},
    sys::storage::get_target_directory_capacity,
};

const LOCK_FILE: &str = "_dir.lock";
const RECORD_DIR: &str = "record";

/// The accepted MySQL-visible error text for local temporary-space exhaustion.
pub const LOCAL_TEMPORARY_SPACE_QUOTA_ERROR: &str = "Out Of Quota For Local Temporary Space!";

/// The configured spill-file encryption method.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SpillEncryptionMethod {
    /// Checksum framing over plaintext bytes.
    #[default]
    Plaintext,
    /// Checksum framing over AES-128-CTR encrypted bytes.
    Aes128Ctr,
}

impl SpillEncryptionMethod {
    /// The normalized TiDB config spelling.
    #[must_use]
    pub const fn as_config_value(self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Aes128Ctr => "aes128-ctr",
        }
    }
}

/// An invalid `security.spilled-file-encryption-method` value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillEncryptionParseError {
    value: String,
}

impl std::fmt::Display for SpillEncryptionParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "unsupported [security]spilled-file-encryption-method {}",
            self.value
        )
    }
}

impl std::error::Error for SpillEncryptionParseError {}

impl FromStr for SpillEncryptionMethod {
    type Err = SpillEncryptionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("plaintext") {
            Ok(Self::Plaintext)
        } else if value.eq_ignore_ascii_case("aes128-ctr") {
            Ok(Self::Aes128Ctr)
        } else {
            Err(SpillEncryptionParseError {
                value: value.to_owned(),
            })
        }
    }
}

/// Fully resolved immutable startup policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillStorageSpec {
    /// Final directory after endpoint/UID path derivation.
    pub path: PathBuf,
    /// Process-wide disk quota. Values `<= 0` are unlimited at query time;
    /// every nonnegative value is still checked against filesystem capacity.
    pub quota_bytes: i64,
    /// Encryption applied to every file created by this authority.
    pub encryption: SpillEncryptionMethod,
}

/// Failure to acquire and validate the process spill authority.
#[derive(Debug)]
pub enum SpillStorageOpenError {
    /// Another process already owns the configured directory.
    AlreadyInUse {
        /// Configured path already leased by another process.
        path: PathBuf,
    },
    /// The configured quota is larger than space available to this process.
    QuotaExceedsAvailable {
        /// Configured storage path.
        path: PathBuf,
        /// Requested startup quota.
        quota_bytes: i64,
        /// Bytes available to an unprivileged process.
        available_bytes: u64,
    },
    /// Filesystem operation failure with its exact target.
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Filesystem target of the operation.
        path: PathBuf,
        /// Underlying operating-system error.
        source: io::Error,
    },
}

impl SpillStorageOpenError {
    fn io(operation: &'static str, path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.into(),
            source,
        }
    }
}

impl std::fmt::Display for SpillStorageOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInUse { path } => write!(
                formatter,
                "the current temporary storage dir has been occupied by another instance: {}",
                path.display()
            ),
            Self::QuotaExceedsAvailable {
                path,
                quota_bytes,
                available_bytes,
            } => write!(
                formatter,
                "tmp-storage-quota {quota_bytes} exceeds available space {available_bytes} at {}",
                path.display()
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for SpillStorageOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::AlreadyInUse { .. } | Self::QuotaExceedsAvailable { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProcessLeaseKey {
    Path(PathBuf),
    #[cfg(unix)]
    File {
        device: u64,
        inode: u64,
    },
}

fn process_leases() -> &'static Mutex<HashSet<ProcessLeaseKey>> {
    static LEASES: OnceLock<Mutex<HashSet<ProcessLeaseKey>>> = OnceLock::new();
    LEASES.get_or_init(|| Mutex::new(HashSet::new()))
}

struct ProcessLease {
    keys: Vec<ProcessLeaseKey>,
}

impl Drop for ProcessLease {
    fn drop(&mut self) {
        let mut leases = process_leases()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in &self.keys {
            leases.remove(key);
        }
    }
}

struct TempDirLease {
    lock_file: Option<File>,
    _process_lease: ProcessLease,
}

impl Drop for TempDirLease {
    fn drop(&mut self) {
        // POSIX record locks are released when the descriptor closes. Keep
        // the process registry entry alive until after that close so another
        // local opener cannot race into the gap.
        drop(self.lock_file.take());
    }
}

/// One process-wide spill-storage authority.
pub struct SpillStorage {
    path: PathBuf,
    quota_bytes: i64,
    encryption: SpillEncryptionMethod,
    global_tracker: Arc<Tracker>,
    _lease: TempDirLease,
}

impl SpillStorage {
    /// Creates, exclusively leases, sweeps, and capacity-checks `spec.path`.
    pub fn open(spec: SpillStorageSpec) -> Result<Self, SpillStorageOpenError> {
        create_dir_all_0750(&spec.path).map_err(|source| {
            SpillStorageOpenError::io("create temporary storage directory", &spec.path, source)
        })?;

        let lock_path = spec.path.join(LOCK_FILE);
        let lease = acquire_temp_dir_lease(&lock_path, &spec.path)?;

        sweep_stale_entries(&spec.path).map_err(|source| {
            SpillStorageOpenError::io("read temporary storage directory", &spec.path, source)
        })?;

        if spec.quota_bytes >= 0 {
            let available_bytes = get_target_directory_capacity(&spec.path).map_err(|source| {
                SpillStorageOpenError::io("read temporary storage capacity", &spec.path, source)
            })?;
            if u64::try_from(spec.quota_bytes).is_ok_and(|quota| quota > available_bytes) {
                return Err(SpillStorageOpenError::QuotaExceedsAvailable {
                    path: spec.path,
                    quota_bytes: spec.quota_bytes,
                    available_bytes,
                });
            }
        }

        let global_tracker = Tracker::new_global(LABEL_FOR_GLOBAL_STORAGE, spec.quota_bytes);
        global_tracker.set_bytes_limit(spec.quota_bytes);
        Ok(Self {
            path: spec.path,
            quota_bytes: spec.quota_bytes,
            encryption: spec.encryption,
            global_tracker,
            _lease: lease,
        })
    }

    /// The final configured spill directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The configured process quota.
    #[must_use]
    pub const fn quota_bytes(&self) -> i64 {
        self.quota_bytes
    }

    /// The immutable file-encryption policy.
    #[must_use]
    pub const fn encryption(&self) -> SpillEncryptionMethod {
        self.encryption
    }

    /// Process-global disk tracker to which statement disk roots attach.
    #[must_use]
    pub fn global_tracker(&self) -> &Arc<Tracker> {
        &self.global_tracker
    }

    /// Creates one private, atomically unique spill file in this authority.
    pub fn create_file(&self, prefix: &str) -> io::Result<(File, PathBuf)> {
        tempfile::Builder::new()
            .prefix(prefix)
            .tempfile_in(&self.path)?
            .keep()
            .map_err(Into::into)
    }
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

fn acquire_temp_dir_lease(
    lock_path: &Path,
    spill_path: &Path,
) -> Result<TempDirLease, SpillStorageOpenError> {
    let parent = lock_path
        .parent()
        .expect("a spill lock is always inside its storage directory");
    let canonical_parent = fs::canonicalize(parent).map_err(|source| {
        SpillStorageOpenError::io("resolve temporary storage directory", parent, source)
    })?;
    let path_key = ProcessLeaseKey::Path(canonical_parent.join(LOCK_FILE));

    // POSIX record locks are process-owned: a second open in this process can
    // succeed, and merely closing that descriptor can release the first
    // authority's kernel lock. Serialize the complete local open/lock path and
    // reject both canonical-path and inode aliases before opening another fd.
    let mut process_leases = process_leases()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if process_leases.contains(&path_key) {
        return Err(SpillStorageOpenError::AlreadyInUse {
            path: spill_path.to_owned(),
        });
    }

    let mut keys = vec![path_key];
    #[cfg(unix)]
    match fs::metadata(lock_path) {
        Ok(metadata) => {
            let key = file_lease_key(&metadata);
            if process_leases.contains(&key) {
                return Err(SpillStorageOpenError::AlreadyInUse {
                    path: spill_path.to_owned(),
                });
            }
            keys.push(key);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(SpillStorageOpenError::io(
                "inspect temporary storage lock",
                lock_path,
                source,
            ));
        }
    }

    let lock_file = open_lock_file(lock_path).map_err(|source| {
        SpillStorageOpenError::io("open temporary storage lock", lock_path, source)
    })?;
    #[cfg(unix)]
    {
        let key = file_lease_key(&lock_file.metadata().map_err(|source| {
            SpillStorageOpenError::io("inspect temporary storage lock", lock_path, source)
        })?);
        if !keys.contains(&key) {
            if process_leases.contains(&key) {
                return Err(SpillStorageOpenError::AlreadyInUse {
                    path: spill_path.to_owned(),
                });
            }
            keys.push(key);
        }
    }

    acquire_exclusive_lock(&lock_file).map_err(|source| {
        if source.kind() == io::ErrorKind::WouldBlock {
            SpillStorageOpenError::AlreadyInUse {
                path: spill_path.to_owned(),
            }
        } else {
            SpillStorageOpenError::io("lock temporary storage directory", lock_path, source)
        }
    })?;
    for key in &keys {
        process_leases.insert(key.clone());
    }
    drop(process_leases);

    Ok(TempDirLease {
        lock_file: Some(lock_file),
        _process_lease: ProcessLease { keys },
    })
}

#[cfg(unix)]
fn file_lease_key(metadata: &Metadata) -> ProcessLeaseKey {
    use std::os::unix::fs::MetadataExt;

    ProcessLeaseKey::File {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
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

#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "android",
    target_os = "solaris",
))]
fn acquire_exclusive_lock(file: &File) -> io::Result<()> {
    rustix::fs::fcntl_lock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(Into::into)
}

#[cfg(windows)]
fn acquire_exclusive_lock(file: &File) -> io::Result<()> {
    file.try_lock()
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "android",
    target_os = "solaris",
    windows,
)))]
fn acquire_exclusive_lock(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exclusive temporary-storage locking is unsupported on this platform",
    ))
}

fn sweep_stale_entries(path: &Path) -> io::Result<()> {
    let entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    if entries.len() <= 2 {
        return Ok(());
    }
    for entry in entries {
        let name = entry.file_name();
        if name == LOCK_FILE || name == RECORD_DIR {
            continue;
        }
        let stale_path = entry.path();
        let removed = if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            fs::remove_dir_all(&stale_path)
        } else {
            fs::remove_file(&stale_path)
        };
        if let Err(error) = removed {
            tracing::warn!(path = %stale_path.display(), %error, "Remove temporary file error");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command};
    use std::thread;
    use std::time::{Duration, Instant};

    const LEASE_CHILD_PATH: &str = "TIDB_SPILL_LEASE_CHILD_PATH";

    struct LeaseChild {
        child: Option<Child>,
        release_path: PathBuf,
    }

    impl LeaseChild {
        fn finish(mut self) -> std::process::ExitStatus {
            fs::write(&self.release_path, b"release").expect("release child");
            self.child
                .take()
                .expect("child")
                .wait()
                .expect("wait for child")
        }
    }

    impl Drop for LeaseChild {
        fn drop(&mut self) {
            let _ = fs::write(&self.release_path, b"release");
            if let Some(mut child) = self.child.take() {
                if child.try_wait().ok().flatten().is_none() {
                    let _ = child.kill();
                }
                let _ = child.wait();
            }
        }
    }

    fn spec(path: PathBuf) -> SpillStorageSpec {
        SpillStorageSpec {
            path,
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        }
    }

    #[test]
    fn encryption_config_is_case_insensitive_and_normalized() {
        assert_eq!("PlAiNtExT".parse(), Ok(SpillEncryptionMethod::Plaintext));
        assert_eq!("AES128-CTR".parse(), Ok(SpillEncryptionMethod::Aes128Ctr));
        assert_eq!(
            SpillEncryptionMethod::Aes128Ctr.as_config_value(),
            "aes128-ctr"
        );
        assert!("aes256-ctr".parse::<SpillEncryptionMethod>().is_err());
    }

    #[test]
    fn private_files_are_unique_and_stale_entries_are_swept() {
        let parent = tempfile::tempdir().expect("parent");
        let path = parent.path().join("spill");
        create_dir_all_0750(&path).expect("spill directory");
        fs::create_dir_all(path.join(RECORD_DIR)).expect("record");
        fs::write(path.join("stale"), b"old").expect("stale file");

        let storage = SpillStorage::open(spec(path.clone())).expect("storage");
        assert!(!path.join("stale").exists());
        let (_, first) = storage.create_file("rows").expect("first file");
        let (_, second) = storage.create_file("rows").expect("second file");
        assert_ne!(first, second);
        assert_eq!(first.parent(), Some(path.as_path()));
        assert_eq!(second.parent(), Some(path.as_path()));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let directory_mode = fs::metadata(&path)
                .expect("directory metadata")
                .permissions()
                .mode();
            let file_mode = fs::metadata(&first)
                .expect("file metadata")
                .permissions()
                .mode();
            let lock_mode = fs::metadata(path.join(LOCK_FILE))
                .expect("lock metadata")
                .permissions()
                .mode();
            assert_eq!((directory_mode & 0o777) & !0o750, 0);
            assert_eq!((file_mode & 0o777) & !0o600, 0);
            assert_eq!((lock_mode & 0o777) & !0o640, 0);
        }
    }

    #[test]
    fn same_process_is_refused_before_any_sweep() {
        let parent = tempfile::tempdir().expect("parent");
        let path = parent.path().join("spill");
        create_dir_all_0750(&path).expect("spill directory");
        fs::create_dir_all(path.join(RECORD_DIR)).expect("record");

        let storage = SpillStorage::open(spec(path.clone())).expect("first storage");
        let (live_file, live_path) = storage.create_file("live").expect("live spill file");
        drop(live_file);
        fs::write(&live_path, b"owned by first authority").expect("live contents");

        assert!(matches!(
            SpillStorage::open(spec(path.clone())),
            Err(SpillStorageOpenError::AlreadyInUse { .. })
        ));
        assert!(
            live_path.exists(),
            "same-process contender swept the owner's live spill file"
        );

        drop(storage);
        let _replacement = SpillStorage::open(spec(path)).expect("replacement storage");
        assert!(!live_path.exists(), "replacement did not sweep stale data");
    }

    #[test]
    fn impossible_startup_quota_is_rejected() {
        let parent = tempfile::tempdir().expect("parent");
        let path = parent.path().join("spill");
        let error = match SpillStorage::open(SpillStorageSpec {
            path: path.clone(),
            quota_bytes: i64::MAX,
            encryption: SpillEncryptionMethod::Plaintext,
        }) {
            Ok(_) => panic!("quota must exceed the test filesystem"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SpillStorageOpenError::QuotaExceedsAvailable { .. }
        ));
        let _replacement = SpillStorage::open(spec(path))
            .expect("startup rejection must release process and OS leases");
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn lease_child() {
        let Some(path) = std::env::var_os(LEASE_CHILD_PATH).map(PathBuf::from) else {
            return;
        };
        let ready_path = path.join("child-ready");
        let release_path = path.join("child-release");
        let _storage = SpillStorage::open(spec(path)).expect("child storage");
        fs::write(ready_path, b"ready").expect("announce child lease");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !release_path.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(release_path.exists(), "parent never released child");
    }

    #[test]
    fn second_process_is_refused_before_any_sweep() {
        let parent = tempfile::tempdir().expect("parent");
        let path = parent.path().join("spill");
        create_dir_all_0750(&path).expect("spill directory");
        fs::create_dir_all(path.join(RECORD_DIR)).expect("record");
        let ready_path = path.join("child-ready");
        let release_path = path.join("child-release");

        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "disk::spill_storage::tests::lease_child",
                "--ignored",
                "--nocapture",
            ])
            .env(LEASE_CHILD_PATH, &path)
            .spawn()
            .expect("spawn lease child");
        let mut child = LeaseChild {
            child: Some(child),
            release_path,
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready_path.exists() && Instant::now() < deadline {
            if let Some(status) = child
                .child
                .as_mut()
                .expect("child")
                .try_wait()
                .expect("poll child")
            {
                panic!("lease child exited before ready: {status}");
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready_path.exists(), "lease child did not become ready");

        let sentinel = path.join("must-not-be-swept");
        fs::write(&sentinel, b"owned by child").expect("sentinel");
        assert!(matches!(
            SpillStorage::open(spec(path.clone())),
            Err(SpillStorageOpenError::AlreadyInUse { .. })
        ));
        assert!(sentinel.exists(), "failed contender swept the owner's file");

        assert!(child.finish().success());
        let _replacement = SpillStorage::open(spec(path)).expect("replacement storage");
        assert!(!sentinel.exists(), "replacement did not sweep stale file");
    }
}
