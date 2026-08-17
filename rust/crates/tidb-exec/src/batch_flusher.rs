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

//! Go `pkg/resourcegroup/runaway/flusher.go`'s generic size/time-triggered
//! batching primitive: `batchFlusher[K, V]`.
//!
//! SEED, not a complete package: `pkg/resourcegroup/runaway` is a much larger
//! package (runaway-query watch, quarantine, and queueing) and this file
//! ports only `flusher.go`'s 140 production lines -- the struct, its
//! constructor, and its four methods (`tickerCh`, `stop`, `add`, `flush`).
//! Every symbol in that one file is here:
//!
//! - `batchFlusher[K, V]` -> [`BatchFlusher`].
//! - `newBatchFlusher` -> [`BatchFlusher::new`], narrowed as described below.
//! - `(*batchFlusher).tickerCh` -> [`BatchFlusher::tick_channel`].
//! - `(*batchFlusher).stop` -> [`BatchFlusher::stop`].
//! - `(*batchFlusher).add` -> [`BatchFlusher::add`].
//! - `(*batchFlusher).flush` -> [`BatchFlusher::flush`].
//!
//! The trigger conditions the source pins are preserved exactly: `add`
//! flushes once the buffer reaches `threshold` entries, and `flush` is a
//! no-op on an empty buffer (Go: `batchSize == 0`, resetting `lastFlushTime`
//! only when it was already non-zero). `mergeFn` is called for every `add`
//! before the size check, exactly as Go orders it.
//!
//! Narrowings, all named:
//!
//! - `// boundary:` Go `pkg/metrics` -- the six Prometheus fields
//!   (`batchSizeObserver`, `durationObserver`, `intervalObserver`,
//!   `flushSuccessCounter`, `flushErrorCounter`, `addCounter`) are dropped.
//!   They are pure telemetry read by no code in this file; nothing here
//!   branches on them.
//! - `// boundary:` `github.com/pingcap/failpoint`'s `FastRunawayGC` (forces
//!   `add` to flush regardless of `threshold`) and `skipFlush` (makes
//!   `flush` a no-op) hooks are dropped. Both are chaos-test-only injection
//!   points with no effect outside a running failpoint build, and
//!   `failpoint` is not a dependency of this crate.
//! - `// boundary:` `util/logutil`'s `BgLogger()` calls in `stop` and
//!   `flush`'s error path are dropped; they are side-effecting observability
//!   with no bearing on the struct's state.
//! - `// boundary:` Go `newBatchFlusher`'s `genSQL func(map[K]V) (string,
//!   []any)` and `pool util.SessionPool` parameters, and the
//!   `ExecRCRestrictedSQL` call they compose into `flushFn`, belong to the
//!   `runaway` package's SQL-writing concern, not to the generic batching
//!   primitive itself -- Go's own test helper (`newTestBatchFlusher` in
//!   `flusher_test.go`) already constructs a `batchFlusher` by supplying
//!   `flushFn` directly, confirming the flush function is meant to be
//!   injected as a unit. [`BatchFlusher::new`] takes the already-composed
//!   [`FlushFn`] as a parameter; a caller that wants `runaway`'s exact
//!   SQL-batch-write behavior builds that closure itself over its own
//!   session pool.
//! - `time.Ticker` becomes [`IntervalTicker`]: a background thread that
//!   sends one tick per `interval` on a capacity-1 channel, mirroring Go's
//!   buffered ticker channel (an unread tick is dropped, not queued).
//!   `Stop` only requests the next loop iteration exit early, exactly as
//!   Go's `Ticker.Stop` does not block on the runtime timer goroutine.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, TrySendError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Go `mergeFn func(map[K]V, K, V)`: absorbs `(key, value)` into `buffer`.
pub type MergeFn<K, V> = Box<dyn FnMut(&mut HashMap<K, V>, K, V)>;

/// Go `flushFn func(map[K]V) error`: persists the buffered batch.
pub type FlushFn<K, V> = Box<dyn FnMut(&HashMap<K, V>) -> Result<(), FlushError>>;

/// boundary: Go's `flushFn` returns the stdlib `error` interface. Nothing in
/// `batchFlusher` inspects the error beyond its presence (it only feeds a
/// success/error metric counter, which is itself dropped -- see the module
/// docs), so this narrows to a message-carrying error rather than porting a
/// dynamic error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushError(pub String);

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for FlushError {}

/// Go `time.Ticker`, narrowed to the one capability this file uses: a
/// channel that receives one tick per `interval`. See the module-level
/// narrowing note for the drop-if-unread and non-blocking-stop semantics
/// this preserves.
struct IntervalTicker {
    receiver: Receiver<Instant>,
    stop: Arc<AtomicBool>,
}

impl IntervalTicker {
    fn new(interval: Duration) -> Self {
        let (sender, receiver) = mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        thread::spawn(move || loop {
            thread::sleep(interval);
            if worker_stop.load(Ordering::Relaxed) {
                return;
            }
            // A buffered channel of capacity 1: a tick nobody has read yet
            // is dropped rather than queued, matching Go's `time.Ticker`.
            match sender.try_send(Instant::now()) {
                Ok(()) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => return,
            }
        });
        Self { receiver, stop }
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Go `batchFlusher[K, V]` (`flusher.go:28`): a size/time-triggered batch
/// buffer. `K` must be usable as a `HashMap` key, matching Go's `comparable`
/// constraint.
pub struct BatchFlusher<K, V> {
    name: String,
    buffer: HashMap<K, V>,
    threshold: usize,
    last_flush_time: Option<Instant>,
    merge_fn: MergeFn<K, V>,
    flush_fn: FlushFn<K, V>,
    ticker: IntervalTicker,
}

impl<K, V> BatchFlusher<K, V>
where
    K: Eq + Hash,
{
    /// Go `newBatchFlusher` (`flusher.go:45-82`), narrowed as described in
    /// the module docs: `flush_fn` is the already-composed closure a caller
    /// would otherwise build from `genSQL` and a session pool.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        interval: Duration,
        threshold: usize,
        merge_fn: MergeFn<K, V>,
        flush_fn: FlushFn<K, V>,
    ) -> Self {
        Self {
            name: name.into(),
            buffer: HashMap::with_capacity(threshold),
            threshold,
            last_flush_time: None,
            merge_fn,
            flush_fn,
            ticker: IntervalTicker::new(interval),
        }
    }

    /// Go `(*batchFlusher).tickerCh` (`flusher.go:84-86`).
    #[must_use]
    pub fn tick_channel(&self) -> &Receiver<Instant> {
        &self.ticker.receiver
    }

    /// Go `(*batchFlusher).stop` (`flusher.go:88-93`): flushes whatever is
    /// buffered, then stops the ticker.
    pub fn stop(&mut self) -> Option<FlushError> {
        let result = self.flush();
        self.ticker.stop();
        result
    }

    /// Go `(*batchFlusher).add` (`flusher.go:95-105`): merges `(key, value)`
    /// into the buffer, then flushes once the buffer reaches `threshold`.
    pub fn add(&mut self, key: K, value: V) {
        (self.merge_fn)(&mut self.buffer, key, value);
        if self.buffer.len() >= self.threshold {
            self.flush();
        }
    }

    /// Go `(*batchFlusher).flush` (`flusher.go:107-140`). A no-op on an
    /// empty buffer; otherwise runs `flush_fn` over the buffer and replaces
    /// it with a fresh, empty one, exactly as Go always reallocates
    /// `f.buffer` regardless of `flushFn`'s outcome.
    pub fn flush(&mut self) -> Option<FlushError> {
        let batch_size = self.buffer.len();
        if batch_size == 0 {
            // Reset `last_flush_time` so the next real flush after an idle
            // period does not record the idle gap as a flush interval.
            self.last_flush_time = None;
            return None;
        }

        let now = Instant::now();
        let outcome = (self.flush_fn)(&self.buffer);

        self.last_flush_time = Some(now);
        self.buffer = HashMap::with_capacity(self.threshold);
        outcome.err()
    }

    /// The flusher's configured name, matching the source's metric label.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The buffered, not-yet-flushed entries.
    #[must_use]
    pub fn buffer(&self) -> &HashMap<K, V> {
        &self.buffer
    }

    /// The number of buffered, not-yet-flushed entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Whether the buffer currently holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
