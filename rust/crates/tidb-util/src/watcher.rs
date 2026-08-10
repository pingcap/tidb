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

//! Non-recursive polling watcher for files and directories.
//!
//! Event and error delivery is lossless and back-pressured. One mutex covers
//! each complete poll cycle, so `add` and `remove` return only after pending
//! observations are delivered. Paths and file names retain native OS bytes;
//! rename and move detection uses Unix device/inode identity. Only one [`Op`]
//! is reported per file per poll.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use crossbeam_channel::{bounded, select, Receiver, Sender};

/// A single file operation type (Go `watcher.Op`, a bit flag).
pub mod op {
    use super::Op;

    /// A file was created.
    pub const CREATE: Op = Op(1 << 0);
    /// A file was removed.
    pub const REMOVE: Op = Op(1 << 1);
    /// A file's contents changed.
    pub const MODIFY: Op = Op(1 << 2);
    /// A file was renamed within its directory.
    pub const RENAME: Op = Op(1 << 3);
    /// A file's mode changed.
    pub const CHMOD: Op = Op(1 << 4);
    /// A file was moved across directories.
    pub const MOVE: Op = Op(1 << 5);
}

/// A set of file operation types (Go `watcher.Op`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Op(pub u32);

impl Op {
    /// Whether this set contains any of `ops` (Go `Event.HasOps`).
    #[must_use]
    pub fn has_op(self, op: Op) -> bool {
        self.0 & op.0 != 0
    }
}

impl std::ops::BitOr for Op {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Matches Go's Op.String(): '|'-joined names, leading pipe stripped.
        let mut buffer = String::new();
        for (bit, name) in [
            (op::CREATE, "CREATE"),
            (op::REMOVE, "REMOVE"),
            (op::MODIFY, "MODIFY"),
            (op::RENAME, "RENAME"),
            (op::CHMOD, "CHMOD"),
            (op::MOVE, "MOVE"),
        ] {
            if self.0 & bit.0 == bit.0 {
                buffer.push('|');
                buffer.push_str(name);
            }
        }
        f.write_str(buffer.strip_prefix('|').unwrap_or(""))
    }
}

/// Rust-native snapshot of the Go `os.FileInfo` fields exposed by an event.
#[derive(Clone, Debug)]
pub struct FileMeta {
    name: OsString,
    is_dir: bool,
    mod_time: SystemTime,
    size: u64,
    mode: u32,
    dev: u64,
    ino: u64,
}

impl FileMeta {
    fn from_metadata(name: OsString, m: &std::fs::Metadata) -> Self {
        FileMeta {
            name,
            is_dir: m.is_dir(),
            // ModTime falls back to UNIX_EPOCH if unavailable, matching Go's
            // zero-time comparison semantics closely enough for change checks.
            mod_time: m.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            size: m.len(),
            mode: m.mode(),
            dev: m.dev(),
            ino: m.ino(),
        }
    }

    /// The base name of the file (Go `FileInfo.Name`).
    #[must_use]
    pub fn name(&self) -> &OsStr {
        &self.name
    }

    /// Whether this path is a directory (Go `FileInfo.IsDir`).
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.is_dir
    }

    /// File size in bytes (Go `FileInfo.Size`).
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Last modification time (Go `FileInfo.ModTime`).
    #[must_use]
    pub fn modified(&self) -> SystemTime {
        self.mod_time
    }

    /// Native Unix mode bits used by the poller (Go `FileInfo.Mode`).
    #[must_use]
    pub fn mode(&self) -> u32 {
        self.mode
    }

    /// Native device identifier from Go `FileInfo.Sys` on Unix.
    #[must_use]
    pub fn device(&self) -> u64 {
        self.dev
    }

    /// Native inode identifier from Go `FileInfo.Sys` on Unix.
    #[must_use]
    pub fn inode(&self) -> u64 {
        self.ino
    }

    /// Go `os.SameFile`: same device and inode.
    fn same_file(&self, other: &FileMeta) -> bool {
        self.dev == other.dev && self.ino == other.ino
    }
}

/// A single file operation event (Go `watcher.Event`).
#[derive(Clone, Debug)]
pub struct Event {
    /// The metadata of the file the event concerns.
    pub file_info: FileMeta,
    /// The path of the file.
    pub path: PathBuf,
    /// The operation.
    pub op: Op,
}

impl Event {
    /// Go `Event.IsDirEvent`: whether the event concerns a directory.
    #[must_use]
    pub fn is_dir_event(&self) -> bool {
        self.file_info.is_dir()
    }

    /// Go `Event.HasOps`: whether the event's op is any of `ops`.
    #[must_use]
    pub fn has_ops(&self, ops: &[Op]) -> bool {
        ops.iter().any(|&op| self.op.has_op(op))
    }
}

/// A watcher error (Go's `ErrWatcherStarted`/`ErrWatcherClosed` and I/O).
#[derive(Debug)]
pub enum WatchError {
    /// `Start` was called while already running.
    AlreadyStarted,
    /// The watcher has been closed.
    Closed,
    /// An I/O error while listing a watched name. `not_found` records
    /// whether the cause was a missing path (Go `os.IsNotExist`).
    Io {
        /// The formatted error message.
        msg: String,
        /// Whether the underlying error was "not found".
        not_found: bool,
    },
}

impl std::fmt::Display for WatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatchError::AlreadyStarted => f.write_str("watcher already started"),
            WatchError::Closed => f.write_str("watcher already closed"),
            WatchError::Io { msg, .. } => write!(f, "{msg}"),
        }
    }
}

impl WatchError {
    fn io(context: &str, e: &std::io::Error) -> Self {
        WatchError::Io {
            msg: format!("{context}: {e}"),
            not_found: e.kind() == std::io::ErrorKind::NotFound,
        }
    }
}

impl std::error::Error for WatchError {}

/// The mutable, mutex-protected watch list shared with the poll thread.
#[derive(Default)]
struct State {
    /// The original names added for watching.
    names: HashMap<PathBuf, ()>,
    /// The latest metadata of every watched file.
    files: HashMap<PathBuf, FileMeta>,
}

impl State {
    /// Go `doRemove`: drops `name` and, if it was a directory, its children.
    fn do_remove(&mut self, name: &Path) {
        self.names.remove(name);
        let Some(fi) = self.files.remove(name) else {
            return;
        };
        if !fi.is_dir {
            return;
        }
        self.files.retain(|fp, _| fp.parent() != Some(name));
    }
}

/// Go `watcher.Watcher`: watches files/directories by polling.
///
pub struct Watcher {
    /// Receiver of file events (Go's public `Events` channel).
    pub events: Receiver<Event>,
    /// Receiver of watch errors (Go's public `Errors` channel).
    pub errors: Receiver<WatchError>,
    state: Arc<Mutex<State>>,
    running: Arc<AtomicI32>,
    // Held senders; dropping `closed_tx` broadcasts the stop signal, and the
    // event/error senders are moved into the poll thread on Start.
    closed_tx: Option<Sender<()>>,
    closed_rx: Receiver<()>,
    events_tx: Option<Sender<Event>>,
    errors_tx: Option<Sender<WatchError>>,
    handle: Option<JoinHandle<()>>,
}

impl Watcher {
    /// Go `NewWatcher`.
    #[must_use]
    pub fn new() -> Self {
        let (events_tx, events) = bounded(0);
        let (errors_tx, errors) = bounded(0);
        let (closed_tx, closed_rx) = bounded::<()>(0);
        Watcher {
            events,
            errors,
            state: Arc::new(Mutex::new(State::default())),
            running: Arc::new(AtomicI32::new(0)),
            closed_tx: Some(closed_tx),
            closed_rx,
            events_tx: Some(events_tx),
            errors_tx: Some(errors_tx),
            handle: None,
        }
    }

    /// Go `Add`: adds a file or directory (non-recursively) to the list.
    pub fn add(&self, name: impl AsRef<Path>) -> Result<(), WatchError> {
        let name = name.as_ref();
        let mut state = self.state.lock().unwrap();
        if self.is_closed() {
            return Err(WatchError::Closed);
        }
        let file_list = list_for_name(name)?;
        state.names.insert(name.to_path_buf(), ());
        state.files.extend(file_list);
        Ok(())
    }

    /// Go `Remove`: removes a file or directory from the list.
    pub fn remove(&self, name: impl AsRef<Path>) -> Result<(), WatchError> {
        let name = name.as_ref();
        let mut state = self.state.lock().unwrap();
        if self.is_closed() {
            return Err(WatchError::Closed);
        }
        state.do_remove(name);
        Ok(())
    }

    /// Whether the stop signal has been broadcast (the sender was dropped).
    fn is_closed(&self) -> bool {
        self.closed_tx.is_none()
    }

    /// Go `Start`: begins polling every `d`.
    pub fn start(&mut self, d: Duration) -> Result<(), WatchError> {
        self.start_with_ticker(crossbeam_channel::tick(d))
    }

    /// [`Watcher::start`] with an explicit tick source instead of a clock.
    ///
    /// Go's `doWatch` already selects on `ticker.C`, so the poll trigger is a
    /// channel either way; naming it lets a caller decide *when* a poll
    /// observes the filesystem. Tests use this to take a snapshot only after
    /// their filesystem mutation has fully landed, which makes every emitted
    /// event a function of the final state rather than of where the poll
    /// happened to interleave with a half-finished `write`/`rename`.
    fn start_with_ticker(&mut self, ticker: Receiver<Instant>) -> Result<(), WatchError> {
        if self
            .running
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            != Ok(0)
        {
            return Err(WatchError::AlreadyStarted);
        }
        if self.is_closed() {
            return Err(WatchError::Closed);
        }

        let state = Arc::clone(&self.state);
        let closed_rx = self.closed_rx.clone();
        let events_tx = self.events_tx.take().expect("events sender present");
        let errors_tx = self.errors_tx.take().expect("errors sender present");
        self.handle = Some(std::thread::spawn(move || {
            do_watch(&ticker, &state, &closed_rx, &events_tx, &errors_tx);
        }));
        Ok(())
    }

    /// Go `Close`: stops polling, joins the thread, and clears the list.
    pub fn close(&mut self) {
        if self
            .running
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
            != Ok(1)
        {
            return;
        }
        // Broadcast the stop signal, then wait for the poll thread. Dropping
        // the event/error senders (owned by the thread) closes those channels
        // once it exits, mirroring Go's close(Events)/close(Errors).
        self.closed_tx = None;
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let mut state = self.state.lock().unwrap();
        *state = State::default();
    }
}

impl Default for Watcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.close();
    }
}

/// Go `doWatch`: the poll loop, running one poll per tick.
fn do_watch(
    ticker: &Receiver<Instant>,
    state: &Arc<Mutex<State>>,
    closed_rx: &Receiver<()>,
    events_tx: &Sender<Event>,
    errors_tx: &Sender<WatchError>,
) {
    loop {
        select! {
            recv(closed_rx) -> _ => return,
            // A disconnected tick source means no further poll can ever be
            // requested, so it stops the loop. Go's `time.Ticker` channel is
            // never closed, so this arm is unreachable via `start`.
            recv(ticker) -> tick => {
                if tick.is_err() {
                    return;
                }
                // One lock covers listing, event delivery, and snapshot
                // replacement. Add/Remove therefore cannot return between a
                // poll's observation and its rendezvous sends.
                let mut state = state.lock().unwrap();
                let Some(curr) = list_for_all(&mut state, closed_rx, errors_tx) else {
                    return;
                };
                if poll_events(&state.files, &curr, closed_rx, events_tx).is_break() {
                    return;
                }
                state.files = curr;
            }
        }
    }
}

/// A send helper that respects the stop signal: returns `Break` if the
/// watcher closed while waiting to hand off an event (Go's
/// `select { <-closed: return; Events <- ev: }`).
fn send_event(
    events_tx: &Sender<Event>,
    closed_rx: &Receiver<()>,
    ev: Event,
) -> std::ops::ControlFlow<()> {
    select! {
        recv(closed_rx) -> _ => std::ops::ControlFlow::Break(()),
        send(events_tx, ev) -> _ => std::ops::ControlFlow::Continue(()),
    }
}

/// Go `pollEvents`: diffs the last list against `curr` and emits events.
fn poll_events(
    prev: &HashMap<PathBuf, FileMeta>,
    curr: &HashMap<PathBuf, FileMeta>,
    closed_rx: &Receiver<()>,
    events_tx: &Sender<Event>,
) -> std::ops::ControlFlow<()> {
    use std::ops::ControlFlow::{Break, Continue};

    let mut creates: HashMap<PathBuf, FileMeta> = HashMap::new();
    let mut removes: HashMap<PathBuf, FileMeta> = HashMap::new();

    // Removals: present before, absent now.
    for (fp, fi) in prev {
        if !curr.contains_key(fp) {
            removes.insert(fp.clone(), fi.clone());
        }
    }

    // Creates / modifies / chmods.
    for (fp, curr_fi) in curr {
        let Some(prev_fi) = prev.get(fp) else {
            creates.insert(fp.clone(), curr_fi.clone());
            continue;
        };
        // ModTime resolution can be coarse, so also compare size.
        if prev_fi.mod_time != curr_fi.mod_time || prev_fi.size != curr_fi.size {
            if let Break(()) = send_event(
                events_tx,
                closed_rx,
                Event {
                    path: fp.clone(),
                    op: op::MODIFY,
                    file_info: curr_fi.clone(),
                },
            ) {
                return Break(());
            }
        }
        if prev_fi.mode != curr_fi.mode {
            if let Break(()) = send_event(
                events_tx,
                closed_rx,
                Event {
                    path: fp.clone(),
                    op: op::CHMOD,
                    file_info: curr_fi.clone(),
                },
            ) {
                return Break(());
            }
        }
    }

    // Renames / moves: a remove and a create that are the same inode.
    let remove_keys: Vec<PathBuf> = removes.keys().cloned().collect();
    for remove_fp in remove_keys {
        let remove_fi = removes[&remove_fp].clone();
        let matched = creates
            .iter()
            .find(|(_, create_fi)| remove_fi.same_file(create_fi))
            .map(|(k, _)| k.clone());
        if let Some(create_fp) = matched {
            let same_dir = remove_fp.parent() == create_fp.parent();
            let op = if same_dir { op::RENAME } else { op::MOVE };
            removes.remove(&remove_fp);
            creates.remove(&create_fp);
            if let Break(()) = send_event(
                events_tx,
                closed_rx,
                Event {
                    path: remove_fp, // for Move, use the from-path
                    op,
                    file_info: remove_fi,
                },
            ) {
                return Break(());
            }
        }
    }

    for (fp, fi) in creates {
        if let Break(()) = send_event(
            events_tx,
            closed_rx,
            Event {
                path: fp,
                op: op::CREATE,
                file_info: fi,
            },
        ) {
            return Break(());
        }
    }
    for (fp, fi) in removes {
        if let Break(()) = send_event(
            events_tx,
            closed_rx,
            Event {
                path: fp,
                op: op::REMOVE,
                file_info: fi,
            },
        ) {
            return Break(());
        }
    }
    Continue(())
}

/// Go `listForAll`: lists every watched name, reporting and pruning names
/// that have disappeared. Returns `None` if the watcher closed mid-list.
fn list_for_all(
    state: &mut State,
    closed_rx: &Receiver<()>,
    errors_tx: &Sender<WatchError>,
) -> Option<HashMap<PathBuf, FileMeta>> {
    let names: Vec<PathBuf> = state.names.keys().cloned().collect();
    let mut file_list = HashMap::new();
    for name in names {
        match list_for_name(&name) {
            Ok(fl) => file_list.extend(fl),
            Err(e) => {
                if is_not_found(&e) {
                    state.do_remove(&name);
                }
                select! {
                    recv(closed_rx) -> _ => return None,
                    send(errors_tx, e) -> _ => {},
                }
            }
        }
    }
    Some(file_list)
}

/// Whether a listing error was a "not found" (Go `os.IsNotExist`).
fn is_not_found(e: &WatchError) -> bool {
    matches!(
        e,
        WatchError::Io {
            not_found: true,
            ..
        }
    )
}

/// Go `listForName`: metadata for a file, or for a directory's direct
/// children (non-recursive). Directory entries use symlink-free metadata,
/// matching Go's `entry.Info()`.
fn list_for_name(name: &Path) -> Result<HashMap<PathBuf, FileMeta>, WatchError> {
    let stat = std::fs::metadata(name)
        .map_err(|e| WatchError::io(&format!("name {}", name.display()), &e))?;
    let mut list = HashMap::new();
    let is_dir = stat.is_dir();
    let base_name = name.file_name().unwrap_or(name.as_os_str()).to_os_string();
    list.insert(
        name.to_path_buf(),
        FileMeta::from_metadata(base_name, &stat),
    );
    if !is_dir {
        return Ok(list);
    }
    let entries = std::fs::read_dir(name)
        .map_err(|e| WatchError::io(&format!("directory {}", name.display()), &e))?;
    for entry in entries {
        let entry =
            entry.map_err(|e| WatchError::io(&format!("directory {}", name.display()), &e))?;
        let fi = entry
            .metadata()
            .map_err(|e| WatchError::io(&format!("directory {}", name.display()), &e))?;
        let entry_name = entry.file_name();
        let fp = name.join(&entry_name);
        list.insert(fp, FileMeta::from_metadata(entry_name, &fi));
    }
    Ok(list)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn assert_event(w: &Watcher, path: &Path, op: Op) {
        loop {
            select! {
                recv(w.events) -> ev => {
                    let ev = ev.expect("events channel closed");
                    if ev.is_dir_event() {
                        continue; // skip directory events
                    }
                    assert!(ev.has_ops(&[op]), "expected op {op}, got {}", ev.op);
                    assert_eq!(ev.path, path);
                    return;
                }
                recv(w.errors) -> err => {
                    panic!("watch error: {}", err.expect("errors channel closed"));
                }
            }
        }
    }

    // Go TestWatcher: create/modify/chmod/rename/remove/create/move over a
    // temp directory.
    //
    // Go drives this with a 10ms ticker, which makes each assertion a bet that
    // the mutation lands wholly between two polls. It does not always: a poll
    // that lists while `Rename` is in flight legitimately sees the file in
    // both directories (Create) or in neither (Remove), and one that lists
    // between a truncating open and its write legitimately reports two
    // Modifies. Those are honest observations of a poller, not watcher bugs,
    // so the fix is to stop racing the poller rather than to widen a timeout:
    // this test supplies the ticks itself, one per mutation, after the
    // mutation has returned. Every poll then sees a settled filesystem and
    // every event below is determined, not probable.
    #[test]
    fn watcher_lifecycle() {
        let dir = std::env::temp_dir().join(format!("tidb_watcher_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let old_name = "mysql-bin.000001";
        let new_name = "mysql-bin.000002";
        let old_path = dir.join(old_name);
        let new_path = dir.join(new_name);

        let mut w = Watcher::new();
        w.add(&dir).unwrap();
        // Rendezvous: `poll()` returns only once the watch thread has taken
        // the tick, and the thread lists only after that.
        let (tick_tx, tick_rx) = bounded::<Instant>(0);
        w.start_with_ticker(tick_rx).unwrap();
        let poll = || tick_tx.send(Instant::now()).unwrap();

        // create
        std::fs::write(&old_path, b"").unwrap();
        poll();
        assert_event(&w, &old_path, op::CREATE);

        // modify: Go opens O_WRONLY and writes once. `fs::write` would open
        // O_TRUNC first, a second metadata change the Go test never makes.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&old_path)
                .unwrap();
            f.write_all(b"meaningless content").unwrap();
        }
        poll();
        assert_event(&w, &old_path, op::MODIFY);

        // chmod
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&old_path, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
        poll();
        assert_event(&w, &old_path, op::CHMOD);

        // rename (within the same directory)
        std::fs::rename(&old_path, &new_path).unwrap();
        poll();
        assert_event(&w, &old_path, op::RENAME);

        // remove
        std::fs::remove_file(&new_path).unwrap();
        poll();
        assert_event(&w, &new_path, op::REMOVE);

        // create again
        std::fs::write(&old_path, b"").unwrap();
        poll();
        assert_event(&w, &old_path, op::CREATE);

        // move to another (independent) directory, like Go's second TempDir
        let dir2 = std::env::temp_dir().join(format!("tidb_watcher_test2_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir2);
        std::fs::create_dir_all(&dir2).unwrap();
        w.add(&dir2).unwrap();
        let old_path2 = dir2.join(old_name);
        std::fs::rename(&old_path, &old_path2).unwrap();
        poll();
        assert_event(&w, &old_path, op::MOVE);

        w.close();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[test]
    fn op_string_and_errors() {
        assert_eq!(op::CREATE.to_string(), "CREATE");
        assert_eq!((op::MODIFY | op::CHMOD).to_string(), "MODIFY|CHMOD");
        assert_eq!(Op(0).to_string(), "");

        let mut w = Watcher::new();
        w.start(Duration::from_millis(10)).unwrap();
        // A second Start while running is rejected.
        assert!(matches!(
            w.start(Duration::from_millis(10)),
            Err(WatchError::AlreadyStarted)
        ));
        w.close();
    }

    #[test]
    fn close_unblocks_pending_delivery_and_seals_the_watcher() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending.bin");
        let mut watcher = Watcher::new();
        watcher.add(dir.path()).unwrap();
        let (tick_tx, tick_rx) = bounded::<Instant>(0);
        watcher.start_with_ticker(tick_rx).unwrap();

        std::fs::write(&path, b"new").unwrap();
        tick_tx.send(Instant::now()).unwrap();
        watcher.close();

        assert!(matches!(
            watcher.events.try_recv(),
            Err(crossbeam_channel::TryRecvError::Disconnected)
        ));
        assert!(matches!(
            watcher.errors.try_recv(),
            Err(crossbeam_channel::TryRecvError::Disconnected)
        ));
        assert!(matches!(watcher.add(&path), Err(WatchError::Closed)));
        assert!(matches!(watcher.remove(&path), Err(WatchError::Closed)));
        assert!(matches!(
            watcher.start(Duration::from_millis(1)),
            Err(WatchError::Closed)
        ));
    }

    #[test]
    fn file_meta_exposes_the_file_info_contract() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.bin");
        std::fs::write(&path, b"abc").unwrap();

        let native = std::fs::metadata(&path).unwrap();
        let listed = list_for_name(&path).unwrap();
        let meta = &listed[&path];
        assert_eq!(meta.name(), path.file_name().unwrap());
        assert_eq!(meta.size(), native.len());
        assert_eq!(meta.modified(), native.modified().unwrap());
        assert_eq!(meta.mode(), native.mode());
        assert_eq!(meta.device(), native.dev());
        assert_eq!(meta.inode(), native.ino());
        assert!(!meta.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_paths_round_trip_without_loss() {
        use std::ffi::OsString;
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let path = PathBuf::from(OsString::from_vec(b"file-\xff".to_vec()));
        let watcher = Watcher::new();
        watcher.remove(&path).unwrap();
        let name = path.file_name().unwrap().to_os_string();

        let event = Event {
            file_info: FileMeta {
                name,
                is_dir: false,
                mod_time: SystemTime::UNIX_EPOCH,
                size: 0,
                mode: 0,
                dev: 0,
                ino: 0,
            },
            path,
            op: op::REMOVE,
        };
        assert_eq!(event.path.as_os_str().as_bytes(), b"file-\xff");
        assert_eq!(event.file_info.name().as_bytes(), b"file-\xff");
    }
}
