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

//! Duplicate-key detection from Go `pkg/lightning/duplicate`.

use std::cmp::Ordering;
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TrySendError};
use tidb_log::{Field, Level, Value};

use crate::extsort::{ExternalSorter, Writer as ExternalWriter};
use crate::lightning_log::Logger;

/// Error returned by duplicate detection.
#[derive(Debug)]
pub struct Error {
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Wraps an error returned by a handler or handler constructor.
    pub fn from_source(source: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl From<crate::extsort::Error> for Error {
    fn from(error: crate::extsort::Error) -> Self {
        Self {
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }
}

/// Result returned by duplicate detection and handlers.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct InternalKey {
    key: Vec<u8>,
    key_id: Vec<u8>,
}

impl fmt::Display for InternalKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.key {
            write!(formatter, "{byte:02X}")?;
        }
        if !self.key_id.is_empty() {
            formatter.write_str("@")?;
            for byte in &self.key_id {
                write!(formatter, "{byte:02X}")?;
            }
        }
        Ok(())
    }
}

fn compare_internal_key(left: &InternalKey, right: &InternalKey) -> Ordering {
    left.key
        .cmp(&right.key)
        .then_with(|| left.key_id.cmp(&right.key_id))
}

fn encode_internal_key(destination: &mut Vec<u8>, key: &InternalKey) {
    destination.clear();
    tidb_codec::encode_bytes(destination, &key.key);
    destination.extend_from_slice(&key.key_id);
}

fn decode_internal_key(encoded: &[u8], key: &mut InternalKey) -> Result<()> {
    let (leftover, decoded) =
        tidb_codec::decode_bytes(encoded).map_err(|error| Error::message(error.to_string()))?;
    key.key = decoded;
    key.key_id.clear();
    key.key_id.extend_from_slice(leftover);
    Ok(())
}

/// Handles one duplicate-key group at a time.
///
/// Calls follow `begin`, two or more `append` calls, `end`, then `close`.
pub trait Handler: Send {
    /// Starts a duplicate group.
    fn begin(&mut self, key: &[u8]) -> Result<()>;

    /// Appends a lexicographically ordered source key identifier.
    fn append(&mut self, key_id: &[u8]) -> Result<()>;

    /// Ends the current duplicate group.
    fn end(&mut self) -> Result<()>;

    /// Closes the handler after task processing finishes.
    fn close(&mut self) -> Result<()>;
}

/// Constructs one handler for each detector worker.
pub type HandlerConstructor =
    Arc<dyn Fn(&AtomicBool) -> Result<Box<dyn Handler>> + Send + Sync + 'static>;

/// Optional arguments for [`Detector::detect`].
#[derive(Clone, Default)]
pub struct DetectOptions {
    /// Maximum concurrent workers. Non-positive values use available CPUs.
    pub concurrency: isize,
    /// Handler constructor; absence selects a no-op handler.
    pub handler_constructor: Option<HandlerConstructor>,
}

impl DetectOptions {
    fn adjusted(mut self) -> Self {
        if self.concurrency <= 0 {
            self.concurrency = std::thread::available_parallelism().map_or(1, usize::from) as isize;
        }
        if self.handler_constructor.is_none() {
            self.handler_constructor = Some(Arc::new(|_| Ok(Box::new(NopHandler))));
        }
        self
    }
}

struct NopHandler;

impl Handler for NopHandler {
    fn begin(&mut self, _key: &[u8]) -> Result<()> {
        Ok(())
    }

    fn append(&mut self, _key_id: &[u8]) -> Result<()> {
        Ok(())
    }

    fn end(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Detects duplicate keys written into an external sorter.
#[derive(Clone)]
pub struct Detector {
    sorter: Arc<dyn ExternalSorter>,
    logger: Logger,
}

impl Detector {
    /// Creates a detector.
    pub fn new(sorter: Arc<dyn ExternalSorter>, logger: Logger) -> Self {
        Self {
            sorter,
            logger: logger.with(&[Field::new(
                "component",
                Value::Str("duplicate.Detector".to_owned()),
            )]),
        }
    }

    /// Creates a writer which encodes each `(key, key_id)` pair.
    pub fn key_adder(&self, canceled: &AtomicBool) -> Result<KeyAdder> {
        Ok(KeyAdder {
            writer: self.sorter.new_writer(canceled)?,
            key_buffer: Vec::new(),
        })
    }

    /// Sorts all keys and invokes handlers for every duplicate group.
    pub fn detect(
        &self,
        canceled: &Arc<AtomicBool>,
        options: Option<DetectOptions>,
    ) -> Result<i64> {
        let canceled = Arc::clone(canceled);
        let options = options.unwrap_or_default().adjusted();
        let task = self.logger.begin(Level::Info, "sort keys");
        let sort_result = self.sorter.sort(&canceled).map_err(Error::from);
        task.end(
            Level::Error,
            sort_result
                .as_ref()
                .err()
                .map(|error| error as &dyn StdError),
            &[],
        );
        sort_result?;

        let (start_key, end_key) = self.range_bounds(&canceled)?;
        if compare_internal_key(&start_key, &end_key) != Ordering::Less {
            return Ok(0);
        }

        let (task_sender, task_receiver) = crossbeam_channel::bounded(1);
        task_sender
            .send(Task { start_key, end_key })
            .expect("new task channel is open");
        let task_counter = Arc::new(TaskCounter::new(1));
        let duplicate_count = Arc::new(AtomicI64::new(0));
        // errgroup.WithContext derives one context from the caller and cancels
        // it when any worker returns. Keep the same single cancellation view
        // for handler construction and task processing.
        let group_canceled = Arc::new(AtomicBool::new(canceled.load(AtomicOrdering::SeqCst)));
        let finished = Arc::new(AtomicBool::new(false));
        let first_error = Arc::new(Mutex::new(None));
        let drain_receiver = task_receiver.clone();

        let relay_parent = Arc::clone(&canceled);
        let relay_group = Arc::clone(&group_canceled);
        let relay_finished = Arc::clone(&finished);
        let cancellation_relay = std::thread::spawn(move || {
            while !relay_group.load(AtomicOrdering::Relaxed)
                && !relay_finished.load(AtomicOrdering::Relaxed)
            {
                if relay_parent.load(AtomicOrdering::Relaxed) {
                    relay_group.store(true, AtomicOrdering::SeqCst);
                    return;
                }
                std::thread::park_timeout(Duration::from_millis(1));
            }
        });

        let mut handles = Vec::with_capacity(options.concurrency as usize);
        for _ in 0..options.concurrency as usize {
            let constructor = Arc::clone(options.handler_constructor.as_ref().expect("adjusted"));
            let detector = self.clone();
            let group_canceled = Arc::clone(&group_canceled);
            let finished = Arc::clone(&finished);
            let first_error = Arc::clone(&first_error);
            let receiver = task_receiver.clone();
            let sender = task_sender.clone();
            let counter = Arc::clone(&task_counter);
            let duplicate_count = Arc::clone(&duplicate_count);
            handles.push(std::thread::spawn(move || {
                let result = (|| {
                    let handler = constructor(&group_canceled)?;
                    let mut worker = Worker {
                        sorter: detector.sorter,
                        task_sender: sender,
                        task_receiver: receiver,
                        task_counter: counter,
                        duplicate_count,
                        handler,
                        logger: detector.logger,
                    };
                    worker.run(&group_canceled, &finished)
                })();
                if let Err(error) = result {
                    let mut slot = first_error
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if slot.is_none() {
                        *slot = Some(error);
                    }
                    drop(slot);
                    group_canceled.store(true, AtomicOrdering::SeqCst);
                }
            }));
        }
        drop(task_receiver);

        let drain_tasks = task_counter.clone();
        let coordinator = std::thread::spawn(move || {
            let mut worker_panic = None;
            for handle in handles {
                if let Err(payload) = handle.join() {
                    if worker_panic.is_none() {
                        worker_panic = Some(payload);
                    }
                }
            }
            if worker_panic.is_some() {
                drain_tasks.finish();
                while drain_receiver.recv().is_ok() {}
            } else {
                while let Ok(_task) = drain_receiver.recv() {
                    drain_tasks.done();
                }
            }
            let _ = cancellation_relay.join();
            if let Some(payload) = worker_panic {
                std::panic::resume_unwind(payload);
            }
        });

        task_counter.wait();
        finished.store(true, AtomicOrdering::SeqCst);
        drop(task_sender);
        if let Err(payload) = coordinator.join() {
            std::panic::resume_unwind(payload);
        }
        if let Some(error) = first_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(error);
        }
        Ok(duplicate_count.load(AtomicOrdering::SeqCst))
    }

    fn range_bounds(&self, canceled: &AtomicBool) -> Result<(InternalKey, InternalKey)> {
        let mut iterator = self.sorter.new_iterator(canceled)?;
        let mut start_key = InternalKey::default();
        let mut end_key = InternalKey::default();
        if iterator.first() {
            decode_internal_key(iterator.unsafe_key(), &mut start_key)?;
        } else if let Some(error) = iterator.error() {
            return Err(Error::message(error.to_string()));
        }
        if iterator.last() {
            decode_internal_key(iterator.unsafe_key(), &mut end_key)?;
            end_key.key.push(0);
        } else if let Some(error) = iterator.error() {
            return Err(Error::message(error.to_string()));
        }
        let _ = iterator.close();
        Ok((start_key, end_key))
    }
}

/// Adds `(key, key_id)` pairs to a detector.
pub struct KeyAdder {
    writer: Box<dyn ExternalWriter>,
    key_buffer: Vec<u8>,
}

impl KeyAdder {
    /// Adds a key and source identifier.
    pub fn add(&mut self, key: &[u8], key_id: &[u8]) -> Result<()> {
        self.key_buffer.clear();
        tidb_codec::encode_bytes(&mut self.key_buffer, key);
        self.key_buffer.extend_from_slice(key_id);
        self.writer.put(&self.key_buffer, &[])?;
        Ok(())
    }

    /// Flushes buffered keys.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flushes and closes the adder.
    pub fn close(&mut self) -> Result<()> {
        self.writer.close()?;
        Ok(())
    }
}

#[derive(Clone)]
struct Task {
    start_key: InternalKey,
    end_key: InternalKey,
}

struct TaskCounter {
    count: Mutex<usize>,
    zero: Condvar,
}

impl TaskCounter {
    fn new(count: usize) -> Self {
        Self {
            count: Mutex::new(count),
            zero: Condvar::new(),
        }
    }

    fn add(&self) {
        *self
            .count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
    }

    fn done(&self) {
        let mut count = self
            .count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *count -= 1;
        if *count == 0 {
            self.zero.notify_all();
        }
    }

    fn wait(&self) {
        let mut count = self
            .count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *count != 0 {
            count = self
                .zero
                .wait(count)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn finish(&self) {
        let mut count = self
            .count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *count = 0;
        self.zero.notify_all();
    }
}

struct Worker {
    sorter: Arc<dyn ExternalSorter>,
    task_sender: Sender<Task>,
    task_receiver: Receiver<Task>,
    task_counter: Arc<TaskCounter>,
    duplicate_count: Arc<AtomicI64>,
    handler: Box<dyn Handler>,
    logger: Logger,
}

impl Worker {
    fn run(&mut self, canceled: &AtomicBool, finished: &AtomicBool) -> Result<()> {
        loop {
            if finished.load(AtomicOrdering::Relaxed) {
                return Ok(());
            }
            if canceled.load(AtomicOrdering::Relaxed) {
                return Err(Error::message("context canceled"));
            }
            match self.task_receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(task) => self.run_task(canceled, task)?,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
    }

    fn run_task(&mut self, canceled: &AtomicBool, mut task: Task) -> Result<()> {
        let log_task = self
            .logger
            .with(&[
                Field::new("startKey", Value::Str(task.start_key.to_string())),
                Field::new("initialEndKey", Value::Str(task.end_key.to_string())),
            ])
            .begin(Level::Info, "run task");
        let mut processed_keys = 0_i64;
        let result = self.process_task(canceled, &mut task, &mut processed_keys);
        self.task_counter.done();
        let close_result = self.handler.close();
        let result = result.and(close_result);
        log_task.end(
            Level::Error,
            result.as_ref().err().map(|error| error as &dyn StdError),
            &[
                Field::new("endKey", Value::Str(task.end_key.to_string())),
                Field::new("processedKeys", Value::I64(processed_keys)),
            ],
        );
        result
    }

    fn process_task(
        &mut self,
        canceled: &AtomicBool,
        task: &mut Task,
        processed_keys: &mut i64,
    ) -> Result<()> {
        let mut iterator = self.sorter.new_iterator(canceled)?;
        let mut encoded_start = Vec::new();
        encode_internal_key(&mut encoded_start, &task.start_key);
        let mut in_duplicate = false;
        let mut previous_key = InternalKey::default();
        let mut current_key = InternalKey::default();
        let mut iterations = 0_usize;
        let mut valid = iterator.seek(&encoded_start);
        while valid {
            decode_internal_key(iterator.unsafe_key(), &mut current_key)?;
            if compare_internal_key(&current_key, &task.end_key) != Ordering::Less {
                break;
            }
            *processed_keys += 1;
            if current_key.key == previous_key.key {
                if in_duplicate {
                    self.handler.append(&current_key.key_id)?;
                } else {
                    self.handler.begin(&current_key.key)?;
                    self.handler.append(&previous_key.key_id)?;
                    self.handler.append(&current_key.key_id)?;
                    in_duplicate = true;
                    self.duplicate_count.fetch_add(1, AtomicOrdering::SeqCst);
                }
            } else if in_duplicate {
                self.handler.end()?;
                in_duplicate = false;
            }

            iterations += 1;
            if iterations.is_multiple_of(1_000) {
                if canceled.load(AtomicOrdering::Relaxed) {
                    return Err(Error::message("context canceled"));
                }
                if self.task_sender.is_empty() {
                    let user_split_key = gen_split_key(&current_key.key, &task.end_key.key);
                    if user_split_key > current_key.key {
                        let split_key = InternalKey {
                            key: user_split_key,
                            key_id: Vec::new(),
                        };
                        self.task_counter.add();
                        match self.task_sender.try_send(Task {
                            start_key: split_key.clone(),
                            end_key: task.end_key.clone(),
                        }) {
                            Ok(()) => task.end_key = split_key,
                            Err(TrySendError::Full(_)) => self.task_counter.done(),
                            Err(TrySendError::Disconnected(_)) => self.task_counter.done(),
                        }
                    }
                }
            }
            std::mem::swap(&mut previous_key, &mut current_key);
            valid = iterator.next();
        }
        if let Some(error) = iterator.error() {
            return Err(Error::message(error.to_string()));
        }
        if in_duplicate {
            self.handler.end()?;
        }
        let _ = iterator.close();
        Ok(())
    }
}

fn gen_split_key(start_key: &[u8], end_key: &[u8]) -> Vec<u8> {
    if start_key == end_key {
        return start_key.to_vec();
    }
    let prefix_length = common_prefix_length(start_key, end_key);
    let mut split_key = start_key[..prefix_length].to_vec();
    if prefix_length == start_key.len() {
        split_key.push(end_key[prefix_length] / 2);
        return split_key;
    }

    let first = start_key[prefix_length];
    let second = end_key[prefix_length];
    if first.wrapping_add(1) < second {
        split_key.push(first + (second - first) / 2);
        return split_key;
    }
    split_key.push(first);
    for byte in &start_key[prefix_length + 1..] {
        split_key.push(0xff);
        if *byte != 0xff {
            return split_key;
        }
    }
    split_key.push(0xff);
    split_key
}

fn common_prefix_length(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .position(|(left, right)| left != right)
        .unwrap_or_else(|| left.len().min(right.len()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::extsort::{open_disk_sorter, DiskSorterOptions};

    #[test]
    fn internal_key() {
        let inputs = vec![
            InternalKey::default(),
            InternalKey {
                key: vec![],
                key_id: vec![1, 2, 3, 4],
            },
            InternalKey {
                key: vec![0],
                key_id: vec![2, 3, 4, 5],
            },
            InternalKey {
                key: vec![0, 1],
                key_id: vec![3, 4, 5, 6],
            },
            InternalKey {
                key: vec![0, 1, 2],
                key_id: vec![4, 5, 6, 7],
            },
            InternalKey {
                key: vec![0, 1, 2, 3],
                key_id: vec![5, 6, 7, 8],
            },
            InternalKey {
                key: vec![0, 1, 2, 3, 4],
                key_id: vec![6, 7, 8, 9],
            },
            InternalKey {
                key: vec![0, 1, 2, 3, 4, 5],
                key_id: vec![7, 8, 9, 10],
            },
            InternalKey {
                key: vec![0, 1, 2, 3, 4, 5, 6],
                key_id: vec![8, 9, 10, 11],
            },
            InternalKey {
                key: vec![0, 1, 2, 3, 4, 5, 6, 7],
                key_id: vec![9, 10, 11, 12],
            },
            InternalKey {
                key: vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
                key_id: vec![10, 11, 12, 13],
            },
        ];
        let encoded: Vec<_> = inputs
            .iter()
            .map(|input| {
                let mut output = Vec::new();
                encode_internal_key(&mut output, input);
                let mut decoded = InternalKey::default();
                decode_internal_key(&output, &mut decoded).unwrap();
                assert_eq!(&decoded, input);
                output
            })
            .collect();
        for left in 0..inputs.len() {
            for right in left + 1..inputs.len() {
                assert_eq!(
                    compare_internal_key(&inputs[left], &inputs[right]),
                    encoded[left].cmp(&encoded[right])
                );
            }
        }
    }

    #[test]
    fn gen_split_key() {
        for (start, end, expected) in [
            (&[1, 2][..], &[1, 2][..], &[1, 2][..]),
            (&[1, 2][..], &[1, 2, 3, 4, 5][..], &[1, 2, 1][..]),
            (
                &[1, 2, 3, 4, 5, 6][..],
                &[1, 2, 5, 6, 7, 8][..],
                &[1, 2, 4][..],
            ),
            (&[1, 2, 3, 4][..], &[1, 2, 4, 5][..], &[1, 2, 3, 0xff][..]),
            (
                &[1, 2, 3, 0xff, 4][..],
                &[1, 2, 4, 5][..],
                &[1, 2, 3, 0xff, 0xff][..],
            ),
            (
                &[1, 2, 3, 0xff, 0xff][..],
                &[1, 2, 4, 5][..],
                &[1, 2, 3, 0xff, 0xff, 0xff][..],
            ),
        ] {
            assert_eq!(super::gen_split_key(start, end), expected);
        }
    }

    #[derive(Debug)]
    struct Collected {
        key: Vec<u8>,
        key_ids: Vec<Vec<u8>>,
    }

    struct Collector {
        key: Vec<u8>,
        key_ids: Vec<Vec<u8>>,
        sender: Sender<Collected>,
    }

    impl Handler for Collector {
        fn begin(&mut self, key: &[u8]) -> Result<()> {
            self.key = key.to_vec();
            Ok(())
        }

        fn append(&mut self, key_id: &[u8]) -> Result<()> {
            self.key_ids.push(key_id.to_vec());
            Ok(())
        }

        fn end(&mut self) -> Result<()> {
            self.sender
                .send(Collected {
                    key: std::mem::take(&mut self.key),
                    key_ids: std::mem::take(&mut self.key_ids),
                })
                .map_err(|error| Error::message(error.to_string()))
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn detector() {
        const KEY_COUNT: usize = 100_000;
        const ADDERS: usize = 10;
        let directory = tempfile::tempdir().unwrap();
        let sorter = Arc::new(
            open_disk_sorter(
                directory.path(),
                Some(DiskSorterOptions {
                    logger: None,
                    ..Default::default()
                }),
            )
            .unwrap(),
        );
        let detector = Detector::new(sorter.clone(), crate::lightning_log::l());
        let canceled = Arc::new(AtomicBool::new(false));
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let keys: Arc<Vec<Vec<u8>>> = Arc::new(
            (0..KEY_COUNT)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state % KEY_COUNT as u64).to_be_bytes().to_vec()
                })
                .collect(),
        );
        std::thread::scope(|scope| {
            for adder_index in 0..ADDERS {
                let detector = detector.clone();
                let canceled = Arc::clone(&canceled);
                let keys = Arc::clone(&keys);
                scope.spawn(move || {
                    let mut adder = detector.key_adder(&canceled).unwrap();
                    for (index, key) in keys.iter().enumerate() {
                        if index % ADDERS == adder_index {
                            adder.add(key, &(index as u64).to_be_bytes()).unwrap();
                        }
                    }
                    adder.close().unwrap();
                });
            }
        });

        let (sender, receiver) = crossbeam_channel::bounded(KEY_COUNT);
        let constructor_sender = sender.clone();
        let duplicate_count = detector
            .detect(
                &canceled,
                Some(DetectOptions {
                    concurrency: 4,
                    handler_constructor: Some(Arc::new(move |_| {
                        Ok(Box::new(Collector {
                            key: Vec::new(),
                            key_ids: Vec::new(),
                            sender: constructor_sender.clone(),
                        }))
                    })),
                }),
            )
            .unwrap();
        drop(sender);
        let mut results: Vec<_> = receiver.try_iter().collect();
        assert_eq!(results.len(), duplicate_count as usize);
        results.sort_by(|left, right| left.key.cmp(&right.key));

        let mut expected = BTreeMap::<Vec<u8>, Vec<Vec<u8>>>::new();
        for (index, key) in keys.iter().enumerate() {
            expected
                .entry(key.clone())
                .or_default()
                .push((index as u64).to_be_bytes().to_vec());
        }
        expected.retain(|_, key_ids| key_ids.len() > 1);
        assert_eq!(results.len(), expected.len());
        for (result, (key, mut key_ids)) in results.into_iter().zip(expected) {
            key_ids.sort();
            assert_eq!(result.key, key);
            assert_eq!(result.key_ids, key_ids);
            assert!(result.key_ids.len() >= 2);
            assert!(result
                .key_ids
                .windows(2)
                .all(|window| window[0] <= window[1]));
            for key_id in result.key_ids {
                let index = u64::from_be_bytes(key_id.try_into().unwrap()) as usize;
                assert_eq!(keys[index], result.key);
            }
        }
        sorter.close().unwrap();
    }

    #[derive(Debug)]
    struct MockError;

    impl fmt::Display for MockError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mock error")
        }
    }

    impl StdError for MockError {}

    #[test]
    fn detector_fail() {
        let directory = tempfile::tempdir().unwrap();
        let sorter = Arc::new(open_disk_sorter(directory.path(), None).unwrap());
        let detector = Detector::new(sorter, crate::lightning_log::l());
        let canceled = Arc::new(AtomicBool::new(false));
        let mut adder = detector.key_adder(&canceled).unwrap();
        adder.add(b"key", b"keyID").unwrap();
        adder.close().unwrap();
        let error = detector
            .detect(
                &canceled,
                Some(DetectOptions {
                    concurrency: 4,
                    handler_constructor: Some(Arc::new(|_| Err(Error::from_source(MockError)))),
                }),
            )
            .unwrap_err();
        assert_eq!(error.to_string(), "mock error");
        assert!(error
            .source()
            .is_some_and(|source| source.is::<MockError>()));
    }
}
