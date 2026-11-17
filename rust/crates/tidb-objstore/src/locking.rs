//! Go `pkg/objstore/locking.go`: locks as files in an external storage,
//! written with the put-and-verify protocol (an intention file, a check
//! that no other intention or conflicting lock exists, then the write).

use std::time::{Duration, SystemTime};

use tidb_gocontext::Context;

use crate::error::{Error, Result};
use crate::gopath;
use crate::storeapi::{Storage, WalkOption};

/// Go `lockRetryTimes`.
const LOCK_RETRY_TIMES: u32 = 60;
/// Go `lockBlockerMetaLimit`.
const LOCK_BLOCKER_META_LIMIT: usize = 3;
/// Go `lockBlockerErrorLimit`.
const LOCK_BLOCKER_ERROR_LIMIT: usize = 3;

/// Go `LockMetaInput`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LockMetaInput {
    /// Go `OwnerID`.
    pub owner_id: String,
    /// Go `LockType`: a diagnostic label only.
    pub lock_type: String,
    /// Go `Hint`.
    pub hint: String,
}

impl LockMetaInput {
    fn is_zero(&self) -> bool {
        self.owner_id.is_empty() && self.lock_type.is_empty() && self.hint.is_empty()
    }
}

impl std::fmt::Display for LockMetaInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fields = vec![format!("hint: {}", self.hint)];
        if !self.owner_id.is_empty() {
            fields.push(format!("owner_id: {}", self.owner_id));
        }
        if !self.lock_type.is_empty() {
            fields.push(format!("lock_type: {}", self.lock_type));
        }
        write!(f, "LockMetaInput({})", fields.join(", "))
    }
}

mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize as _, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if v.is_empty() {
            return s.serialize_none();
        }
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let v: Option<String> = Option::deserialize(d)?;
        v.map_or(Ok(Vec::new()), |s| base64::engine::general_purpose::STANDARD.decode(s).map_err(serde::de::Error::custom))
    }
}

/// Go `LockMeta`, the JSON content of a lock file.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockMeta {
    /// Go `LockedAt`, as Go's JSON time (RFC 3339).
    #[serde(default)]
    pub locked_at: String,
    /// Go `LockerHost`.
    #[serde(default)]
    pub locker_host: String,
    /// Go `LockerPID`.
    #[serde(default)]
    pub locker_pid: i64,
    /// Go `TxnID`.
    #[serde(default, with = "b64")]
    pub txn_id: Vec<u8>,
    /// Go `OwnerID`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner_id: String,
    /// Go `LockType`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lock_type: String,
    /// Go `Hint`.
    #[serde(default)]
    pub hint: String,
}

impl LockMeta {
    fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

impl std::fmt::Display for LockMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fields = vec![
            format!("at: {}", self.locked_at),
            format!("host: {}", self.locker_host),
            format!("pid: {}", self.locker_pid),
            format!("hint: {}", self.hint),
        ];
        if !self.owner_id.is_empty() {
            fields.push(format!("owner_id: {}", self.owner_id));
        }
        if !self.lock_type.is_empty() {
            fields.push(format!("lock_type: {}", self.lock_type));
        }
        write!(f, "Locked({})", fields.join(", "))
    }
}

/// Go `os.Hostname`.
fn hostname() -> String {
    let uname = rustix::system::uname();
    uname.nodename().to_string_lossy().into_owned()
}

/// Go `MakeLockMeta`: the lock's metadata from this process.
#[must_use]
pub fn make_lock_meta(input: &LockMetaInput) -> LockMeta {
    LockMeta {
        locked_at: chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        locker_host: hostname(),
        locker_pid: i64::from(std::process::id()),
        txn_id: Vec::new(),
        owner_id: input.owner_id.clone(),
        lock_type: input.lock_type.clone(),
        hint: input.hint.clone(),
    }
}

/// Go `getLockMeta`.
fn get_lock_meta(ctx: &Context, storage: &dyn Storage, path: &str) -> Result<LockMeta> {
    let file = storage
        .read_file(ctx, path)
        .map_err(|e| e.annotate(format!("failed to read existed lock file {path}")))?;
    serde_json::from_slice(&file).map_err(|e| Error::other(format!("failed to parse lock file {path}: {e}")))
}

/// Go `LockBlocker`.
#[derive(Clone, Debug)]
struct LockBlocker {
    path: String,
    meta: LockMeta,
    err: Option<String>,
}

/// Go `lockBlockerFromPath`.
fn lock_blocker_from_path(ctx: &Context, storage: &dyn Storage, path: &str) -> LockBlocker {
    match get_lock_meta(ctx, storage, path) {
        Ok(meta) => LockBlocker { path: path.to_owned(), meta, err: None },
        Err(e) => LockBlocker { path: path.to_owned(), meta: LockMeta::default(), err: Some(e.to_string()) },
    }
}

/// Go `ErrLocked`.
#[derive(Clone, Debug, Default)]
struct ErrLocked {
    path: String,
    meta: LockMeta,
    local: LockMetaInput,
    blocker_count: usize,
    blockers: Vec<LockBlocker>,
}

impl std::fmt::Display for ErrLocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fields = vec!["locked".to_owned()];
        if !self.path.is_empty() {
            fields.push(format!("path = {}", self.path));
        }
        if !self.meta.is_zero() {
            fields.push(format!("meta = {}", self.meta));
        }
        if !self.local.is_zero() {
            fields.push(format!("local = {}", self.local));
        }
        let sampled = self.blockers.len().min(LOCK_BLOCKER_ERROR_LIMIT);
        for blocker in &self.blockers[..sampled] {
            fields.push(format!("conflict file {}", blocker.path));
            match &blocker.err {
                Some(err) => fields.push(format!("blocker_error = {err}")),
                None => fields.push(format!("blocker_meta = {}", blocker.meta)),
            }
        }
        let omitted = self.blocker_count.max(self.blockers.len()) - sampled;
        if omitted > 0 {
            fields.push(format!("omitted_conflict_files = {omitted}"));
        }
        f.write_str(&fields.join(", "))
    }
}

/// A lock attempt's failure: [`ErrLocked`] or another error.
enum LockError {
    Locked(Box<ErrLocked>),
    Other(Error),
}

impl From<Error> for LockError {
    fn from(e: Error) -> Self {
        Self::Other(e)
    }
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Locked(l) => l.fmt(f),
            Self::Other(e) => e.fmt(f),
        }
    }
}

/// Go `VerifyWriteContext`.
struct VerifyWriteContext<'a> {
    ctx: &'a Context,
    target: String,
    storage: &'a dyn Storage,
    txn_id: uuid::Uuid,
}

impl VerifyWriteContext<'_> {
    /// Go `IntentFileName`.
    fn intent_file_name(&self) -> String {
        format!("{}.INTENT.{}", self.target, hex::encode(self.txn_id.as_bytes()))
    }

    /// Go `conflictingObjectsOfPrefixExpect`: the objects starting with
    /// `pfx` other than `expect`, deleted ones too.
    fn conflicting_objects_of_prefix_expect(&self, pfx: &str, expect: &str) -> Result<(Vec<LockBlocker>, usize)> {
        let file_name = gopath::base(pfx);
        let mut dir_name = gopath::dir(pfx);
        // Some object stores reject "." as a list prefix component.
        if dir_name == "." {
            dir_name = String::new();
        }
        let mut found = Vec::new();
        let mut blocker_count = 0;
        self.storage.walk_dir(
            self.ctx,
            Some(&WalkOption { sub_dir: dir_name, obj_prefix: file_name, include_tombstone: true, ..WalkOption::default() }),
            &mut |object_path, _| {
                if object_path != expect {
                    blocker_count += 1;
                    if found.len() < LOCK_BLOCKER_META_LIMIT {
                        found.push(object_path.to_owned());
                    }
                }
                Ok(())
            },
        )?;
        let blockers = found.iter().map(|p| lock_blocker_from_path(self.ctx, self.storage, p)).collect();
        Ok((blockers, blocker_count))
    }

    /// Go `assertOnlyMyIntent`.
    fn assert_only_my_intent(&self) -> std::result::Result<(), LockError> {
        let (blockers, blocker_count) = self.conflicting_objects_of_prefix_expect(&self.target, &self.intent_file_name())?;
        if blocker_count > 0 {
            return Err(LockError::Locked(Box::new(ErrLocked {
                path: self.target.clone(),
                blocker_count,
                blockers,
                ..ErrLocked::default()
            })));
        }
        Ok(())
    }
}

/// Go `withLockContext`.
fn with_lock_context(err: LockError, path: &str, local: &LockMetaInput) -> LockError {
    match err {
        LockError::Locked(mut locked) => {
            if locked.path.is_empty() {
                path.clone_into(&mut locked.path);
            }
            if locked.local.is_zero() {
                locked.local = local.clone();
            }
            LockError::Locked(locked)
        }
        other => other,
    }
}

/// Which conflicting objects block a lock.
enum Verify {
    /// Go `TryLockRemote`: only the intentions of the target itself.
    Nothing,
    /// Go `TryLockRemoteWrite`: anything under the lock path.
    NoOtherUnder(String),
    /// Go `TryLockRemoteRead`: a write lock.
    NoWriteLock(String),
}

/// Go `conditionalPut`.
struct ConditionalPut<'a> {
    target: String,
    verify: Verify,
    local: &'a LockMetaInput,
}

impl ConditionalPut<'_> {
    fn check_conflict(&self, cx: &VerifyWriteContext<'_>) -> std::result::Result<(), LockError> {
        // Go appends both errors; the lock conflict is reported first.
        let verified = match &self.verify {
            Verify::Nothing => Ok((Vec::new(), 0)),
            Verify::NoOtherUnder(path) => cx.conflicting_objects_of_prefix_expect(path, &cx.intent_file_name()),
            Verify::NoWriteLock(write_lock) => cx.conflicting_objects_of_prefix_expect(write_lock, ""),
        };
        let (blockers, blocker_count) = verified?;
        if blocker_count > 0 {
            return Err(LockError::Locked(Box::new(ErrLocked {
                path: self.target.clone(),
                local: self.local.clone(),
                blocker_count,
                blockers,
                ..ErrLocked::default()
            })));
        }
        cx.assert_only_my_intent().map_err(|e| with_lock_context(e, &self.target, self.local))
    }

    /// Go `CommitTo`: writes an intention, verifies, then writes the lock.
    fn commit_to(&self, ctx: &Context, s: &dyn Storage) -> std::result::Result<uuid::Uuid, LockError> {
        // Go warns when the storage lacks strong consistency; this crate has
        // no logger and the warning changes nothing.
        let txn_id = uuid::Uuid::new_v4();
        let cx = VerifyWriteContext { ctx, target: self.target.clone(), storage: s, txn_id };
        let intent_file_name = cx.intent_file_name();
        self.check_conflict(&cx).map_err(|e| annotate_lock(e, "during initial check"))?;
        s.write_file(ctx, &intent_file_name, &[]).map_err(|e| e.annotate("during writing intention file"))?;
        let res = (|| {
            self.check_conflict(&cx)
                .map_err(|e| annotate_lock(e, "during checking whether there are other intentions"))?;
            let mut meta = make_lock_meta(self.local);
            meta.txn_id = txn_id.as_bytes().to_vec();
            let content = serde_json::to_vec(&meta).expect("a plain object marshals to JSON");
            s.write_file(ctx, &self.target, &content)?;
            Ok(txn_id)
        })();
        // Go deletes the intention in a defer; a failure is only logged.
        let _ = s.delete_file(ctx, &intent_file_name);
        res
    }
}

fn annotate_lock(e: LockError, msg: &str) -> LockError {
    match e {
        LockError::Other(e) => LockError::Other(e.annotate(msg)),
        locked => locked,
    }
}

/// Go `annotateLockAttemptError`.
fn annotate_lock_attempt_error(ctx: &Context, storage: &dyn Storage, path: &str, local: &LockMetaInput, err: LockError, message: &str) -> Error {
    match err {
        LockError::Locked(mut locked) => {
            if locked.path.is_empty() {
                path.clone_into(&mut locked.path);
            }
            if locked.local.is_zero() {
                locked.local = local.clone();
            }
            if locked.meta.is_zero() {
                if let Some(first) = locked.blockers.first() {
                    locked.meta = first.meta.clone();
                } else {
                    let blocker = lock_blocker_from_path(ctx, storage, path);
                    locked.meta = blocker.meta.clone();
                    if blocker.err.is_some() {
                        locked.blockers = vec![blocker];
                    }
                }
            }
            Error::other(format!("{message}: {locked}"))
        }
        LockError::Other(err) => match get_lock_meta(ctx, storage, path) {
            Ok(meta) => err.annotate(format!("{message}; remote_lock_meta = {meta}")),
            Err(_) => err.annotate(message),
        },
    }
}

/// Go `RemoteLock`.
#[derive(Clone)]
pub struct RemoteLock {
    txn_id: uuid::Uuid,
    storage: std::sync::Arc<dyn Storage>,
    path: String,
}

impl std::fmt::Display for RemoteLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{path={},uuid={},storage_uri={}}}", self.path, self.txn_id, self.storage.uri())
    }
}

impl RemoteLock {
    /// The lock file's path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Go `Unlock`: deletes the lock file once it is still ours.
    ///
    /// # Errors
    /// The lock file is unreadable, someone else's, or cannot be deleted.
    pub fn unlock(&self, ctx: &Context) -> Result<()> {
        let meta = get_lock_meta(ctx, self.storage.as_ref(), &self.path)?;
        if self.txn_id.as_bytes().as_slice() != meta.txn_id.as_slice() {
            return Err(Error::other(format!(
                "Txn ID mismatch: remote is {:?}, our is {}",
                meta.txn_id, self.txn_id
            )));
        }
        self.storage
            .delete_file(ctx, &self.path)
            .map_err(|e| e.annotate(format!("failed to delete lock file {}", self.path)))
    }

    /// Go `UnlockOnCleanUp`: unlocks under a fresh 30s deadline when `ctx`
    /// is done; returns the failure for the caller to log.
    pub fn unlock_on_clean_up(&self, ctx: &Context) -> Option<Error> {
        let res = if ctx.err().is_some() {
            let (cx, cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(30));
            let res = self.unlock(&cx);
            cancel.cancel();
            res
        } else {
            self.unlock(ctx)
        };
        res.err().map(|e| e.annotate(format!("Failed to unlock a lock {self}, you may need to manually delete it")))
    }
}

/// Go `writeLockName`.
fn write_lock_name(path: &str) -> String {
    format!("{path}.WRIT")
}

/// Go `newReadLockName`.
fn new_read_lock_name(path: &str) -> String {
    let read_id = u64::from_le_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap()) >> 1;
    format!("{path}.READ.{read_id:016x}")
}

/// Go `Locker`.
pub type Locker = fn(&Context, &std::sync::Arc<dyn Storage>, &str, &LockMetaInput) -> Result<RemoteLock>;

/// Go `TryLockRemote`: an exclusive lock file at `path`.
///
/// # Errors
/// Another process holds the lock, or the storage failed.
pub fn try_lock_remote(ctx: &Context, storage: &std::sync::Arc<dyn Storage>, path: &str, input: &LockMetaInput) -> Result<RemoteLock> {
    let writer = ConditionalPut { target: path.to_owned(), verify: Verify::Nothing, local: input };
    match writer.commit_to(ctx, storage.as_ref()) {
        Ok(txn_id) => Ok(RemoteLock { txn_id, storage: std::sync::Arc::clone(storage), path: path.to_owned() }),
        Err(e) => Err(annotate_lock_attempt_error(ctx, storage.as_ref(), path, input, e, &format!("failed to acquire lock on '{path}'"))),
    }
}

/// Go `TryLockRemoteWrite`: the write lock `<path>.WRIT`, refused while any
/// lock under `path` exists.
///
/// # Errors
/// The lock is held, or the storage failed.
pub fn try_lock_remote_write(ctx: &Context, storage: &std::sync::Arc<dyn Storage>, path: &str, input: &LockMetaInput) -> Result<RemoteLock> {
    let target = write_lock_name(path);
    let writer = ConditionalPut { target: target.clone(), verify: Verify::NoOtherUnder(path.to_owned()), local: input };
    match writer.commit_to(ctx, storage.as_ref()) {
        Ok(txn_id) => Ok(RemoteLock { txn_id, storage: std::sync::Arc::clone(storage), path: target }),
        Err(e) => Err(annotate_lock_attempt_error(ctx, storage.as_ref(), &target, input, e, "something wrong about the lock")),
    }
}

/// Go `TryLockRemoteRead`: a read lock `<path>.READ.<id>`, refused while
/// the write lock exists.
///
/// # Errors
/// The write lock is held, or the storage failed.
pub fn try_lock_remote_read(ctx: &Context, storage: &std::sync::Arc<dyn Storage>, path: &str, input: &LockMetaInput) -> Result<RemoteLock> {
    let target = new_read_lock_name(path);
    let write_lock = write_lock_name(path);
    let writer = ConditionalPut { target: target.clone(), verify: Verify::NoWriteLock(write_lock.clone()), local: input };
    match writer.commit_to(ctx, storage.as_ref()) {
        Ok(txn_id) => Ok(RemoteLock { txn_id, storage: std::sync::Arc::clone(storage), path: target }),
        Err(e) => Err(annotate_lock_attempt_error(
            ctx,
            storage.as_ref(),
            &write_lock,
            input,
            e,
            "failed to commit the lock due to existing lock: something wrong about the lock",
        )),
    }
}

/// Go `LockWithRetry`: 60 attempts, backing off from 1s to 60s plus a
/// fixed 2.5-7.5s jitter.
///
/// # Errors
/// The last attempt's error, or `ctx` ended.
pub fn lock_with_retry(
    ctx: &Context,
    locker: Locker,
    storage: &std::sync::Arc<dyn Storage>,
    path: &str,
    input: &LockMetaInput,
) -> Result<RemoteLock> {
    const JITTER_MS: u64 = 5000;
    let nanos = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    let jitter = Duration::from_millis(u64::from(nanos) % JITTER_MS + JITTER_MS / 2);
    let (mut retry_times, mut next_backoff) = (0, Duration::from_secs(1));
    loop {
        let err = match locker(ctx, storage, path, input) {
            Ok(lock) => return Ok(lock),
            Err(err) => err,
        };
        if retry_times >= LOCK_RETRY_TIMES {
            return Err(err.annotate(format!("failed to acquire lock after {LOCK_RETRY_TIMES} retries")));
        }
        retry_times += 1;
        let retry_after = next_backoff + jitter;
        next_backoff = (next_backoff * 2).min(Duration::from_secs(60));
        if ctx.sleep(retry_after) {
            let ctx_err = ctx.err().map(|e| e.to_string()).unwrap_or_default();
            return Err(Error::new(crate::ErrorKind::Canceled, format!("lock retry stopped: {ctx_err}: {err}")));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::*;

    fn create_mock_storage() -> (Arc<dyn Storage>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(crate::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
        (storage, dir)
    }

    fn input(hint: &str) -> LockMetaInput {
        LockMetaInput { hint: hint.to_owned(), ..LockMetaInput::default() }
    }

    /// Go `TestTryLockRemote`.
    #[test]
    fn test_try_lock_remote() {
        let ctx = Context::background();
        let (strg, dir) = create_mock_storage();
        let lock = try_lock_remote(&ctx, &strg, "test.lock", &input("This file is mine!")).unwrap();
        assert!(dir.path().join("test.lock").exists());
        lock.unlock(&ctx).unwrap();
        assert!(!dir.path().join("test.lock").exists());
    }

    /// Go `TestConflictLock`.
    #[test]
    fn test_conflict_lock() {
        let ctx = Context::background();
        let (strg, dir) = create_mock_storage();
        let lock = try_lock_remote(&ctx, &strg, "test.lock", &input("This file is mine!")).unwrap();
        let err = try_lock_remote(&ctx, &strg, "test.lock", &input("This file is mine!")).err().unwrap();
        assert!(err.to_string().contains("conflict file test.lock"), "{err}");
        assert!(dir.path().join("test.lock").exists());
        lock.unlock(&ctx).unwrap();
        assert!(!dir.path().join("test.lock").exists());
    }

    /// Go `TestRWLock`.
    #[test]
    fn test_rw_lock() {
        let ctx = Context::background();
        let (strg, dir) = create_mock_storage();
        let lock = try_lock_remote_read(&ctx, &strg, "test.lock", &input("I wanna read it!")).unwrap();
        let lock2 = try_lock_remote_read(&ctx, &strg, "test.lock", &input("I wanna read it too!")).unwrap();
        assert!(try_lock_remote_write(&ctx, &strg, "test.lock", &input("I wanna write it, you get out!")).is_err());
        lock.unlock(&ctx).unwrap();
        lock2.unlock(&ctx).unwrap();
        let l = try_lock_remote_write(&ctx, &strg, "test.lock", &input("Can I have a write lock?")).unwrap();
        assert!(dir.path().join("test.lock.WRIT").exists());
        l.unlock(&ctx).unwrap();
        assert!(!Path::new(&dir.path().join("test.lock.WRIT")).exists());
    }

    /// Go `TestConflictLockReportsRemotePathAndMeta`.
    #[test]
    fn test_conflict_lock_reports_remote_path_and_meta() {
        let ctx = Context::background();
        let (strg, _dir) = create_mock_storage();
        let remote = LockMetaInput { owner_id: "remote-op".into(), lock_type: "migration-read".into(), hint: "remote".into() };
        let lock = try_lock_remote(&ctx, &strg, "test.lock", &remote).unwrap();
        let local = LockMetaInput { owner_id: "local-op".into(), lock_type: "migration-write".into(), hint: "local".into() };
        let err = try_lock_remote(&ctx, &strg, "test.lock", &local).err().unwrap().to_string();
        for want in ["test.lock", "remote-op", "migration-read", "local-op"] {
            assert!(err.contains(want), "{want} not in {err}");
        }
        lock.unlock(&ctx).unwrap();
    }

    /// Go `TestWriteLockConflictReportsReadLockBlocker`.
    #[test]
    fn test_write_lock_conflict_reports_read_lock_blocker() {
        let ctx = Context::background();
        let (strg, _dir) = create_mock_storage();
        let read = LockMetaInput { owner_id: "read-op".into(), lock_type: "migration-read".into(), hint: "reader".into() };
        let lock = try_lock_remote_read(&ctx, &strg, "test.lock", &read).unwrap();
        let local = LockMetaInput { owner_id: "write-op".into(), lock_type: "migration-write".into(), hint: "writer".into() };
        let err = try_lock_remote_write(&ctx, &strg, "test.lock", &local).err().unwrap().to_string();
        assert!(err.contains("path = test.lock.WRIT"), "{err}");
        assert!(err.contains("test.lock.READ."), "{err}");
        assert!(err.contains("read-op"), "{err}");
        lock.unlock(&ctx).unwrap();
    }

    /// Go `TestUnlockOnCleanUp`: a done context still unlocks.
    #[test]
    fn test_unlock_on_clean_up() {
        let (strg, dir) = create_mock_storage();
        let lock = try_lock_remote(&Context::background(), &strg, "test.lock", &input("This file is mine!")).unwrap();
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        cancel.cancel();
        assert!(lock.unlock_on_clean_up(&ctx).is_none());
        assert!(!dir.path().join("test.lock").exists());
    }

    /// Go `TestLockMetaOldJSONCompatibility`.
    #[test]
    fn test_lock_meta_old_json_compatibility() {
        let meta: LockMeta = serde_json::from_str(
            r#"{"locked_at":"2026-06-15T01:02:03Z","locker_host":"host-a","locker_pid":123,"txn_id":"dHhu","hint":"old"}"#,
        )
        .unwrap();
        assert_eq!(meta.locker_host, "host-a");
        assert_eq!(meta.locker_pid, 123);
        assert_eq!(meta.hint, "old");
        assert!(meta.owner_id.is_empty());
        assert!(meta.lock_type.is_empty());
    }
}
