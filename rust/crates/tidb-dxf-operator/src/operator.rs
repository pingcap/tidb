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

use std::any::TypeId;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::{bounded, select, Receiver, Sender};

use crate::{SimpleDataChannel, WithSink, WithSource};

pub(crate) fn display_type_name<T>() -> String {
    let components = std::any::type_name::<T>().split("::").collect::<Vec<_>>();
    components[components.len().saturating_sub(2)..].join(".")
}

/// Error returned by a Go `Operator` method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorError(pub String);

impl fmt::Display for OperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for OperatorError {}

/// Native carrier of Go workerpool `Context`.
pub struct Context {
    cancel_sender: Mutex<Option<Sender<()>>>,
    cancelled: Receiver<()>,
    first_error: Mutex<Option<OperatorError>>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    /// Go `workerpool.NewContext(context.Background())`.
    #[must_use]
    pub fn new() -> Self {
        let (cancel_sender, cancelled) = bounded(0);
        Self {
            cancel_sender: Mutex::new(Some(cancel_sender)),
            cancelled,
            first_error: Mutex::new(None),
        }
    }

    /// Go `Context.OnError`.
    pub fn on_error(&self, error: OperatorError) {
        let mut first = self
            .first_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if first.is_none() {
            *first = Some(error);
        }
        drop(first);
        self.cancel();
    }

    /// Go `Context.OperatorErr`.
    #[must_use]
    pub fn operator_error(&self) -> Option<OperatorError> {
        self.first_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Go `Context.Cancel`.
    pub fn cancel(&self) {
        self.cancel_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(crate) fn cancelled(&self) -> Receiver<()> {
        self.cancelled.clone()
    }
}

/// Go `Operator`.
pub trait Operator: Send + Sync {
    /// Go `Open`.
    fn open(&self) -> Result<(), OperatorError>;
    /// Go `Close`.
    fn close(&self) -> Result<(), OperatorError>;
    /// Go `String`.
    fn operator_string(&self) -> String;
    /// Native interface assertion used by Go `GetReaderAndWriter`.
    fn tunable(&self) -> Option<&dyn TunableOperator> {
        None
    }
}

/// Go `TunableOperator`.
pub trait TunableOperator: Send + Sync {
    /// Go `TuneWorkerPoolSize`.
    fn tune_worker_pool_size(&self, worker_num: i32, wait: bool);
    /// Go `GetWorkerPoolSize`.
    fn worker_pool_size(&self) -> i32;
}

/// Go `TaskMayPanic`.
pub trait TaskMayPanic: Send + 'static {
    /// Go `RecoverArgs`; `Some(error)` is reported for a panic.
    fn recover_args(&self) -> (String, String, Option<OperatorError>);
}

/// Native carrier of Go `workerpool.None`.
pub struct NoResult;

/// Go `workerpool.Worker`.
pub trait Worker<T, R>: Send + 'static {
    /// Go `HandleTask`.
    fn handle_task(&mut self, task: T, send: &mut dyn FnMut(R)) -> Result<(), OperatorError>;
    /// Go `Close`.
    fn close(&mut self) -> Result<(), OperatorError>;
}

type WorkerFactory<T, R> = dyn Fn() -> Option<Box<dyn Worker<T, R>>> + Send + Sync;

/// Go `AsyncOperator`.
pub struct AsyncOperator<T: TaskMayPanic, R: Send + 'static> {
    context: Arc<Context>,
    worker_factory: Arc<WorkerFactory<T, R>>,
    _name: String,
    workers: AtomicI32,
    started: AtomicBool,
    active_workers: Arc<AtomicUsize>,
    source: Mutex<Option<SimpleDataChannel<T>>>,
    sink: Mutex<Option<SimpleDataChannel<R>>>,
    quit_sender: Sender<Option<Sender<()>>>,
    quit_receiver: Receiver<Option<Sender<()>>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl<T: TaskMayPanic, R: Send + 'static> AsyncOperator<T, R> {
    /// Go `NewAsyncOperator` at Rust's worker-factory boundary.
    #[must_use]
    pub fn new(
        context: Arc<Context>,
        name: impl Into<String>,
        worker_num: i32,
        worker_factory: impl Fn() -> Option<Box<dyn Worker<T, R>>> + Send + Sync + 'static,
    ) -> Self {
        let (quit_sender, quit_receiver) = bounded(0);
        Self {
            context,
            worker_factory: Arc::new(worker_factory),
            _name: name.into(),
            workers: AtomicI32::new(worker_num.max(1)),
            started: AtomicBool::new(false),
            active_workers: Arc::new(AtomicUsize::new(0)),
            source: Mutex::new(None),
            sink: Mutex::new(None),
            quit_sender,
            quit_receiver,
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Go `NewAsyncOperatorWithTransform`.
    #[must_use]
    pub fn with_transform(
        context: Arc<Context>,
        name: impl Into<String>,
        worker_num: i32,
        transform: impl Fn(T) -> R + Send + Sync + 'static,
    ) -> Self {
        let transform: Arc<dyn Fn(T) -> R + Send + Sync> = Arc::new(transform);
        Self::new(context, name, worker_num, move || {
            Some(Box::new(TransformWorker {
                transform: Arc::clone(&transform),
            }))
        })
    }

    fn spawn_worker(&self) {
        let Some(mut worker) = (self.worker_factory)() else {
            return;
        };
        let source = self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("AsyncOperator source is initialized by open");
        let sink = self
            .sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let cancelled = self.context.cancelled();
        let context = Arc::clone(&self.context);
        let quit = self.quit_receiver.clone();
        let active = Arc::clone(&self.active_workers);
        active.fetch_add(1, Ordering::AcqRel);
        let handle = std::thread::spawn(move || {
            let receiver = source.receiver();
            loop {
                select! {
                    recv(cancelled) -> _ => break,
                    recv(quit) -> message => {
                        if let Ok(Some(ack)) = message {
                            let _ = ack.send(());
                        }
                        break;
                    }
                    recv(receiver) -> message => {
                        let Ok(task) = message else { break };
                        let (label, function, panic_error) = task.recover_args();
                        let output = catch_unwind(AssertUnwindSafe(|| {
                            let mut send = |result| {
                                if let Some(channel) = &sink {
                                    let cancelled = context.cancelled();
                                    let _ = channel.send_or_cancel(result, &cancelled);
                                }
                            };
                            worker.handle_task(task, &mut send)
                        }));
                        match output {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => context.on_error(error),
                            Err(_) => context.on_error(panic_error.unwrap_or_else(|| {
                                OperatorError(format!("task panic: {label}, func info: {function}"))
                            })),
                        }
                    }
                }
            }
            if let Err(error) = worker.close() {
                context.on_error(error);
            }
            if active.fetch_sub(1, Ordering::AcqRel) == 1 {
                if let Some(channel) = sink {
                    channel.finish();
                }
            }
        });
        self.handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(handle);
    }
}

impl<T: TaskMayPanic, R: Send + 'static> WithSource<T> for AsyncOperator<T, R> {
    fn set_source(&self, channel: SimpleDataChannel<T>) {
        *self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(channel);
    }
}

impl<T: TaskMayPanic, R: Send + 'static> WithSink<R> for AsyncOperator<T, R> {
    fn set_sink(&self, channel: SimpleDataChannel<R>) {
        *self
            .sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(channel);
    }
}

impl<T: TaskMayPanic, R: Send + 'static> Operator for AsyncOperator<T, R> {
    fn open(&self) -> Result<(), OperatorError> {
        {
            let mut source = self
                .source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if source.is_none() {
                *source = Some(SimpleDataChannel::new());
            }
        }
        if TypeId::of::<R>() != TypeId::of::<NoResult>() {
            let mut sink = self
                .sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if sink.is_none() {
                *sink = Some(SimpleDataChannel::new());
            }
        }
        for _ in 0..self.workers.load(Ordering::Acquire) {
            self.spawn_worker();
        }
        self.started.store(true, Ordering::Release);
        Ok(())
    }

    fn close(&self) -> Result<(), OperatorError> {
        let handles = std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for handle in handles {
            let _ = handle.join();
        }
        if let Some(sink) = self
            .sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            sink.try_finish();
        }
        self.started.store(false, Ordering::Release);
        Ok(())
    }

    fn operator_string(&self) -> String {
        format!(
            "AsyncOp[{}, {}]",
            display_type_name::<T>(),
            display_type_name::<R>()
        )
    }

    fn tunable(&self) -> Option<&dyn TunableOperator> {
        Some(self)
    }
}

impl<T: TaskMayPanic, R: Send + 'static> TunableOperator for AsyncOperator<T, R> {
    fn tune_worker_pool_size(&self, worker_num: i32, wait: bool) {
        let worker_num = worker_num.max(1);
        let previous = self.workers.swap(worker_num, Ordering::AcqRel);
        if !self.started.load(Ordering::Acquire) {
            return;
        }
        if worker_num > previous {
            for _ in previous..worker_num {
                self.spawn_worker();
            }
        } else {
            let mut acknowledgements = Vec::new();
            for _ in worker_num..previous {
                let acknowledgement = if wait {
                    let (sender, receiver) = bounded(0);
                    Some((sender, receiver))
                } else {
                    None
                };
                let message = acknowledgement.as_ref().map(|(sender, _)| sender.clone());
                let cancelled = self.context.cancelled();
                select! {
                    send(self.quit_sender, message) -> result => {
                        if result.is_err() { break; }
                    },
                    recv(cancelled) -> _ => break,
                }
                if let Some((_, receiver)) = acknowledgement {
                    acknowledgements.push(receiver);
                }
            }
            for acknowledgement in acknowledgements {
                let cancelled = self.context.cancelled();
                select! {
                    recv(acknowledgement) -> _ => {},
                    recv(cancelled) -> _ => break,
                }
            }
        }
    }

    fn worker_pool_size(&self) -> i32 {
        self.workers.load(Ordering::Acquire)
    }
}

struct TransformWorker<T, R> {
    transform: Arc<dyn Fn(T) -> R + Send + Sync>,
}

impl<T: TaskMayPanic, R: Send + 'static> Worker<T, R> for TransformWorker<T, R> {
    fn handle_task(&mut self, task: T, send: &mut dyn FnMut(R)) -> Result<(), OperatorError> {
        send((self.transform)(task));
        Ok(())
    }

    fn close(&mut self) -> Result<(), OperatorError> {
        Ok(())
    }
}
