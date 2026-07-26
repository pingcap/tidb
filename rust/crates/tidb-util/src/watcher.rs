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

//! `pkg/util/watcher`: a polling file/directory watcher.
//!
//! Faithful adaptations:
//! - Go's unbuffered `Events`/`Errors` channels become crossbeam
//!   rendezvous channels (`bounded(0)`), so the poll thread blocks on each
//!   send until the consumer receives -- preserving Go's back-pressure and
//!   making delivery lossless.
//! - Go's `close(closed)` broadcast becomes a `bounded::<()>(0)` channel
//!   whose sender is dropped on [`Watcher::close`]; every `select!` arm
//!   watching `closed_rx` then observes disconnection, exactly like a
//!   received zero value on a closed Go channel.
//! - `os.SameFile` becomes a `(dev, ino)` comparison (Unix `MetadataExt`),
//!   used to reclassify a remove+create pair as a rename/move.
//!
//! Like Go, only one `Op` is reported per file per poll; the priority is
//! Modify, Chmod, Rename/Move, Create/Remove.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use crossbeam_channel::{bounded, select, Receiver, Sender};

/// A single file operation type (Go `watcher.Op`, a bit flag).
pub mod op {
    /// A file was created.
    pub const CREATE: u32 = 1 << 0;
    /// A file was removed.
    pub const REMOVE: u32 = 1 << 1;
    /// A file's contents changed.
    pub const MODIFY: u32 = 1 << 2;
    /// A file was renamed within its directory.
    pub const RENAME: u32 = 1 << 3;
    /// A file's mode changed.
    pub const CHMOD: u32 = 1 << 4;
    /// A file was moved across directories.
    pub const MOVE: u32 = 1 << 5;
}

/// A set of file operation types (Go `watcher.Op`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Op(pub u32);

impl Op {
    /// Whether this set contains any of `ops` (Go `Event.HasOps`).
    #[must_use]
    pub fn has_op(self, op: u32) -> bool {
        self.0 & op != 0
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
            if self.0 & bit == bit {
                buffer.push('|');
                buffer.push_str(name);
            }
        }
        f.write_str(buffer.strip_prefix('|').unwrap_or(""))
    }
}

/// The snapshot of a path's metadata the watcher compares between polls
/// (Go's `os.FileInfo`, reduced to the fields the poller uses).
#[derive(Clone, Debug)]
pub struct FileMeta {
    is_dir: bool,
    mod_time: SystemTime,
    size: u64,
    mode: u32,
    dev: u64,
    ino: u64,
}

impl FileMeta {
    fn from_metadata(m: &std::fs::Metadata) -> Self {
        FileMeta {
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

    /// Whether this path is a directory (Go `FileInfo.IsDir`).
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.is_dir
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
    pub path: String,
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
    pub fn has_ops(&self, ops: &[u32]) -> bool {
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
    names: HashMap<String, ()>,
    /// The latest metadata of every watched file.
    files: HashMap<String, FileMeta>,
}

impl State {
    /// Go `doRemove`: drops `name` and, if it was a directory, its children.
    fn do_remove(&mut self, name: &str) {
        self.names.remove(name);
        let Some(fi) = self.files.remove(name) else {
            return;
        };
        if !fi.is_dir {
            return;
        }
        self.files
            .retain(|fp, _| Path::new(fp).parent() != Some(Path::new(name)));
    }
}

/// Go `watcher.Watcher`: watches files/directories by polling.
///
/// Divergence from Go: the watch list is guarded by a `Mutex`, but the port
/// is not otherwise engineered for concurrent `Add`/`Remove`/`close` from
/// many threads at once (Go relies on the same single mutex, so this matches
/// realistic usage).
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
    pub fn add(&self, name: &str) -> Result<(), WatchError> {
        let mut state = self.state.lock().unwrap();
        if self.is_closed() {
            return Err(WatchError::Closed);
        }
        let file_list = list_for_name(name)?;
        state.names.insert(name.to_owned(), ());
        state.files.extend(file_list);
        Ok(())
    }

    /// Go `Remove`: removes a file or directory from the list.
    pub fn remove(&self, name: &str) -> Result<(), WatchError> {
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
            do_watch(d, &state, &closed_rx, &events_tx, &errors_tx);
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

/// Go `doWatch`: the poll loop, ticking every `d`.
fn do_watch(
    d: Duration,
    state: &Arc<Mutex<State>>,
    closed_rx: &Receiver<()>,
    events_tx: &Sender<Event>,
    errors_tx: &Sender<WatchError>,
) {
    let ticker = crossbeam_channel::tick(d);
    loop {
        select! {
            recv(closed_rx) -> _ => return,
            recv(ticker) -> _ => {
                let Some(curr) = list_for_all(state, closed_rx, errors_tx) else {
                    return;
                };
                if poll_events(state, &curr, closed_rx, events_tx).is_break() {
                    return;
                }
                let mut s = state.lock().unwrap();
                s.files = curr;
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
    state: &Arc<Mutex<State>>,
    curr: &HashMap<String, FileMeta>,
    closed_rx: &Receiver<()>,
    events_tx: &Sender<Event>,
) -> std::ops::ControlFlow<()> {
    use std::ops::ControlFlow::{Break, Continue};

    // Snapshot the previous file list under the lock, then release it: Go
    // holds w.mu across the whole poll, but the sends here are rendezvous and
    // would deadlock add()/remove() callers; the poll thread is the only
    // writer of `files`, so a snapshot is equivalent.
    let prev = state.lock().unwrap().files.clone();

    let mut creates: HashMap<String, FileMeta> = HashMap::new();
    let mut removes: HashMap<String, FileMeta> = HashMap::new();

    // Removals: present before, absent now.
    for (fp, fi) in &prev {
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
                    op: Op(op::MODIFY),
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
                    op: Op(op::CHMOD),
                    file_info: curr_fi.clone(),
                },
            ) {
                return Break(());
            }
        }
    }

    // Renames / moves: a remove and a create that are the same inode.
    let remove_keys: Vec<String> = removes.keys().cloned().collect();
    for remove_fp in remove_keys {
        let remove_fi = removes[&remove_fp].clone();
        let matched = creates
            .iter()
            .find(|(_, create_fi)| remove_fi.same_file(create_fi))
            .map(|(k, _)| k.clone());
        if let Some(create_fp) = matched {
            let same_dir = Path::new(&remove_fp).parent() == Path::new(&create_fp).parent();
            let op = if same_dir { op::RENAME } else { op::MOVE };
            removes.remove(&remove_fp);
            creates.remove(&create_fp);
            if let Break(()) = send_event(
                events_tx,
                closed_rx,
                Event {
                    path: remove_fp, // for Move, use the from-path
                    op: Op(op),
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
                op: Op(op::CREATE),
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
                op: Op(op::REMOVE),
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
    state: &Arc<Mutex<State>>,
    closed_rx: &Receiver<()>,
    errors_tx: &Sender<WatchError>,
) -> Option<HashMap<String, FileMeta>> {
    let names: Vec<String> = state.lock().unwrap().names.keys().cloned().collect();
    let mut file_list = HashMap::new();
    for name in names {
        match list_for_name(&name) {
            Ok(fl) => file_list.extend(fl),
            Err(e) => {
                if is_not_found(&e) {
                    state.lock().unwrap().do_remove(&name);
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
fn list_for_name(name: &str) -> Result<HashMap<String, FileMeta>, WatchError> {
    let stat = std::fs::metadata(name).map_err(|e| WatchError::io(&format!("name {name}"), &e))?;
    let mut list = HashMap::new();
    let is_dir = stat.is_dir();
    list.insert(name.to_owned(), FileMeta::from_metadata(&stat));
    if !is_dir {
        return Ok(list);
    }
    let entries =
        std::fs::read_dir(name).map_err(|e| WatchError::io(&format!("directory {name}"), &e))?;
    for entry in entries {
        let entry = entry.map_err(|e| WatchError::io(&format!("directory {name}"), &e))?;
        let fi = entry
            .metadata()
            .map_err(|e| WatchError::io(&format!("directory {name}"), &e))?;
        let fp = Path::new(name)
            .join(entry.file_name())
            .to_string_lossy()
            .into_owned();
        list.insert(fp, FileMeta::from_metadata(&fi));
    }
    Ok(list)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn assert_event(w: &Watcher, path: &str, op: u32) {
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
    // temp directory, polling every 10ms.
    #[test]
    fn watcher_lifecycle() {
        let dir = std::env::temp_dir().join(format!("tidb_watcher_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let old_name = "mysql-bin.000001";
        let new_name = "mysql-bin.000002";
        let old_path = dir.join(old_name).to_string_lossy().into_owned();
        let new_path = dir.join(new_name).to_string_lossy().into_owned();

        let mut w = Watcher::new();
        w.add(&dir.to_string_lossy()).unwrap();
        w.start(Duration::from_millis(10)).unwrap();

        // create
        std::fs::write(&old_path, b"").unwrap();
        assert_event(&w, &old_path, op::CREATE);

        // modify
        std::fs::write(&old_path, b"meaningless content").unwrap();
        assert_event(&w, &old_path, op::MODIFY);

        // chmod
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&old_path, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
        assert_event(&w, &old_path, op::CHMOD);

        // rename (within the same directory)
        std::fs::rename(&old_path, &new_path).unwrap();
        assert_event(&w, &old_path, op::RENAME);

        // remove
        std::fs::remove_file(&new_path).unwrap();
        assert_event(&w, &new_path, op::REMOVE);

        // create again
        std::fs::write(&old_path, b"").unwrap();
        assert_event(&w, &old_path, op::CREATE);

        // move to another (independent) directory, like Go's second TempDir
        let dir2 = std::env::temp_dir().join(format!("tidb_watcher_test2_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir2);
        std::fs::create_dir_all(&dir2).unwrap();
        w.add(&dir2.to_string_lossy()).unwrap();
        let old_path2 = dir2.join(old_name).to_string_lossy().into_owned();
        std::fs::rename(&old_path, &old_path2).unwrap();
        assert_event(&w, &old_path, op::MOVE);

        w.close();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[test]
    fn op_string_and_errors() {
        assert_eq!(Op(op::CREATE).to_string(), "CREATE");
        assert_eq!(Op(op::MODIFY | op::CHMOD).to_string(), "MODIFY|CHMOD");
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
}
