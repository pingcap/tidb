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

//! Task scheduling metadata from `pkg/resourcemanager/poolmanager`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

const SHARD_COUNT: usize = 8;

/// A task submitted to a managed pool.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// A shared, explicitly closable channel corresponding to a Go channel.
pub struct Channel<T> {
    inner: Arc<ChannelInner<T>>,
}

struct ChannelInner<T> {
    sender: Mutex<Option<crossbeam_channel::Sender<T>>>,
    receiver: crossbeam_channel::Receiver<T>,
    close_sender: Mutex<Option<crossbeam_channel::Sender<()>>>,
    closed: crossbeam_channel::Receiver<()>,
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Channel<T> {
    /// Creates a channel with the source buffer capacity.
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(capacity);
        Self::from_parts(sender, receiver)
    }

    /// Wraps an existing channel.
    pub fn from_parts(
        sender: crossbeam_channel::Sender<T>,
        receiver: crossbeam_channel::Receiver<T>,
    ) -> Self {
        let (close_sender, closed) = crossbeam_channel::bounded(0);
        Self {
            inner: Arc::new(ChannelInner {
                sender: Mutex::new(Some(sender)),
                receiver,
                close_sender: Mutex::new(Some(close_sender)),
                closed,
            }),
        }
    }

    /// Sends a value, blocking until a receiver or buffer slot is available.
    pub fn send(&self, value: T) {
        self.send_result(value)
            .unwrap_or_else(|_| panic!("send on closed channel"));
    }

    /// Sends a value and returns it when the channel is closed.
    pub fn send_result(&self, value: T) -> Result<(), crossbeam_channel::SendError<T>> {
        let sender = self
            .inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            return Err(crossbeam_channel::SendError(value));
        };
        let mut selected = crossbeam_channel::Select::new();
        let send_index = selected.send(&sender);
        let closed_index = selected.recv(&self.inner.closed);
        let operation = selected.select();
        if operation.index() == send_index {
            operation.send(&sender, value)
        } else {
            debug_assert_eq!(operation.index(), closed_index);
            let _ = operation.recv(&self.inner.closed);
            Err(crossbeam_channel::SendError(value))
        }
    }

    /// Attempts a nonblocking send.
    pub fn try_send(&self, value: T) -> Result<(), crossbeam_channel::TrySendError<T>> {
        if !matches!(
            self.inner.closed.try_recv(),
            Err(crossbeam_channel::TryRecvError::Empty)
        ) {
            return Err(crossbeam_channel::TrySendError::Disconnected(value));
        }
        let sender = self
            .inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            return Err(crossbeam_channel::TrySendError::Disconnected(value));
        };
        sender.try_send(value)
    }

    /// Returns a receiver for this channel.
    pub fn receiver(&self) -> crossbeam_channel::Receiver<T> {
        self.inner.receiver.clone()
    }

    pub(crate) fn sender(&self) -> Option<crossbeam_channel::Sender<T>> {
        self.inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Closes the channel. Buffered values remain receivable.
    pub fn close(&self) {
        let sender = self
            .inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        assert!(sender.is_some(), "close of closed channel");
        self.inner
            .close_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    /// Closes the channel if it is still open.
    #[doc(hidden)]
    pub fn close_if_open(&self) {
        let sender = self
            .inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if sender.is_some() {
            self.inner
                .close_sender
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
    }

    /// Sends unless a cancellation channel becomes ready.
    #[doc(hidden)]
    pub fn send_or_cancel(&self, value: T, cancelled: &crossbeam_channel::Receiver<()>) -> bool {
        let Some(sender) = self.sender() else {
            return false;
        };
        let mut selected = crossbeam_channel::Select::new();
        let send_index = selected.send(&sender);
        let closed_index = selected.recv(&self.inner.closed);
        let cancelled_index = selected.recv(cancelled);
        let operation = selected.select();
        if operation.index() == send_index {
            operation.send(&sender, value).is_ok()
        } else if operation.index() == closed_index {
            let _ = operation.recv(&self.inner.closed);
            false
        } else {
            debug_assert_eq!(operation.index(), cancelled_index);
            let _ = operation.recv(cancelled);
            false
        }
    }
}

/// A source task channel.
pub type TaskChannel = Channel<Task>;

/// A source worker-exit channel.
pub type ExitChannel = Channel<()>;

/// Metadata used to control or observe one pool task.
pub struct Meta {
    create_ts: SystemTime,
    exit_ch: Option<ExitChannel>,
    task_ch: Option<TaskChannel>,
    task_id: u64,
    running: AtomicI32,
    initial_concurrency: i32,
}

impl Meta {
    /// Creates task metadata.
    pub fn new(
        task_id: u64,
        exit_ch: Option<ExitChannel>,
        task_ch: Option<TaskChannel>,
        concurrency: i32,
    ) -> Self {
        Self {
            create_ts: SystemTime::now(),
            exit_ch,
            task_ch,
            task_id,
            running: AtomicI32::new(0),
            initial_concurrency: concurrency,
        }
    }

    /// Returns the task identifier.
    pub fn task_id(&self) -> u64 {
        self.task_id
    }

    /// Increments the running worker count.
    pub fn increment_task(&self) {
        self.running.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrements the running worker count.
    pub fn decrement_task(&self) {
        self.running.fetch_sub(1, Ordering::SeqCst);
    }

    /// Returns the task channel.
    pub fn task_channel(&self) -> Option<TaskChannel> {
        self.task_ch.clone()
    }

    /// Returns the exit channel.
    pub fn exit_channel(&self) -> Option<ExitChannel> {
        self.exit_ch.clone()
    }
}

struct TaskStatusContainer {
    stats: RwLock<HashMap<u64, Arc<Meta>>>,
}

/// Controls and observes tasks registered with a pool.
pub struct TaskManager {
    tasks: [TaskStatusContainer; SHARD_COUNT],
    concurrency: i32,
}

impl TaskManager {
    /// Creates a task manager with the pool's initial concurrency.
    pub fn new(concurrency: i32) -> Self {
        Self {
            tasks: std::array::from_fn(|_| TaskStatusContainer {
                stats: RwLock::new(HashMap::new()),
            }),
            concurrency,
        }
    }

    /// Registers task metadata, replacing an existing task with the same ID.
    pub fn register_task(&self, task: Arc<Meta>) {
        self.tasks[shard_id(task.task_id)]
            .stats
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task.task_id, task);
    }

    /// Deletes task metadata.
    pub fn delete_task(&self, task_id: u64) {
        self.tasks[shard_id(task_id)]
            .stats
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&task_id);
    }

    /// Returns the pool's initial concurrency.
    pub fn origin_concurrency(&self) -> i32 {
        self.concurrency
    }

    /// Selects a task to receive an additional worker.
    pub fn overclock(&self) -> (u64, Option<Arc<Meta>>) {
        self.iter(can_boost)
    }

    /// Requests that one selected task stop a worker.
    pub fn downclock(&self) {
        let (_, task) = self.iter(can_pause);
        if let Some(task) = task {
            if let Some(channel) = &task.exit_ch {
                let _ = channel.try_send(());
            }
        }
    }

    fn iter(&self, select: impl Fn(&Meta, SystemTime) -> (bool, bool)) -> (u64, Option<Arc<Meta>>) {
        let mut task_id = 0;
        let mut result = None;
        let mut compare_ts = SystemTime::UNIX_EPOCH;
        'shards: for task in &self.tasks {
            let stats = task
                .stats
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (id, metadata) in stats.iter() {
                if result.is_none() {
                    if metadata.running.load(Ordering::SeqCst) != 0 {
                        result = Some(Arc::clone(metadata));
                    }
                    task_id = *id;
                    compare_ts = metadata.create_ts;
                    continue;
                }
                let (found, stop) = select(metadata, compare_ts);
                if found {
                    task_id = *id;
                    result = Some(Arc::clone(metadata));
                    compare_ts = metadata.create_ts;
                }
                if stop {
                    break 'shards;
                }
            }
        }
        (task_id, result)
    }
}

fn shard_id(task_id: u64) -> usize {
    (task_id % SHARD_COUNT as u64) as usize
}

fn can_pause(metadata: &Meta, minimum: SystemTime) -> (bool, bool) {
    if metadata.initial_concurrency < metadata.running.load(Ordering::SeqCst)
        && metadata.running.load(Ordering::SeqCst) != 0
    {
        return (true, true);
    }
    if metadata.create_ts < minimum && metadata.running.load(Ordering::SeqCst) != 0 {
        return (true, false);
    }
    (false, false)
}

fn can_boost(metadata: &Meta, maximum: SystemTime) -> (bool, bool) {
    if metadata.running.load(Ordering::SeqCst) < metadata.initial_concurrency {
        return (true, true);
    }
    if metadata.create_ts > maximum {
        return (true, false);
    }
    (false, false)
}
