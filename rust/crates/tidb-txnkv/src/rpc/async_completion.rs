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

//! Source-shaped completion boundary for client-go asynchronous requests.
//!
//! The production dispatcher is deliberately not implemented by the unary
//! transport. Pinned client-go starts `SendRequestAsync` only through one
//! BatchCommands connection; the later batch-stream owner must implement this
//! contract without taking over RegionCache or RequestSelector policy.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::mem;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};

use crate::client::{DirectUnaryRequest, DirectUnaryResponse};

use super::{DirectUnaryClientError, UnaryCallContext};

/// Failure in the local once-only completion driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionError {
    /// A second driver attempted to own the same completion queue.
    ConcurrentDriver,
    /// The caller cancelled before every queued task could run.
    Cancelled,
    /// The caller's deadline elapsed before every queued task could run.
    DeadlineExceeded,
    /// A terminal value had already been delivered.
    AlreadyCompleted,
}

impl fmt::Display for CompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConcurrentDriver => formatter.write_str("completion already has a driver"),
            Self::Cancelled => formatter.write_str("completion execution cancelled"),
            Self::DeadlineExceeded => formatter.write_str("completion execution deadline exceeded"),
            Self::AlreadyCompleted => formatter.write_str("completion already fulfilled"),
        }
    }
}

impl Error for CompletionError {}

/// Observable state of a caller-driven completion queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompletionRunLoopState {
    /// No caller currently owns the queue.
    #[default]
    Idle,
    /// The sole driver is waiting for an append or cancellation notification.
    Waiting,
    /// The sole driver is executing a detached batch of tasks.
    Running,
}

/// Result of one drive attempt, including work completed before cancellation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompletionRunOutcome {
    executed: usize,
    error: Option<CompletionError>,
}

impl CompletionRunOutcome {
    const fn completed(executed: usize) -> Self {
        Self {
            executed,
            error: None,
        }
    }

    const fn failed(executed: usize, error: CompletionError) -> Self {
        Self {
            executed,
            error: Some(error),
        }
    }

    /// Number of tasks executed before the drive returned.
    #[must_use]
    pub const fn executed(self) -> usize {
        self.executed
    }

    /// Terminal drive error, if the drive was rejected or cancelled.
    #[must_use]
    pub const fn error(self) -> Option<CompletionError> {
        self.error
    }
}

/// Exact reason a completion drive was interrupted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionCancellationReason {
    /// The caller explicitly cancelled the attempt.
    Cancelled,
    /// The caller's deadline elapsed.
    DeadlineExceeded,
}

impl CompletionCancellationReason {
    const fn as_error(self) -> CompletionError {
        match self {
            Self::Cancelled => CompletionError::Cancelled,
            Self::DeadlineExceeded => CompletionError::DeadlineExceeded,
        }
    }
}

struct CancellationState {
    reason: Mutex<Option<CompletionCancellationReason>>,
    waiters: Mutex<Vec<Weak<RunLoopSignal>>>,
}

/// Cloneable cancellation carrier for one completion-queue drive.
///
/// Cancellation is monotonic. Each waiting run loop registers its own paired
/// mutex and condition variable so one cancellation can safely wake every
/// loop without ever using a condition variable with a different mutex.
#[derive(Clone)]
pub struct CompletionCancellation {
    inner: Arc<CancellationState>,
}

impl fmt::Debug for CompletionCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionCancellation")
            .field("reason", &self.reason())
            .finish()
    }
}

impl Default for CompletionCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl CompletionCancellation {
    /// Creates an active cancellation carrier.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancellationState {
                reason: Mutex::new(None),
                waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Cancels current and future drives using this carrier.
    pub fn cancel(&self) {
        self.cancel_with(CompletionCancellationReason::Cancelled);
    }

    /// Interrupts current and future drives with the exact source reason.
    ///
    /// Cancellation is monotonic: the first reason wins.
    pub fn cancel_with(&self, reason: CompletionCancellationReason) {
        {
            let mut current = self
                .inner
                .reason
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if current.is_some() {
                return;
            }
            *current = Some(reason);
        }
        let waiters = {
            let mut registered = self
                .inner
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut waiters = Vec::with_capacity(registered.len());
            registered.retain(|waiting| {
                if let Some(waiting) = waiting.upgrade() {
                    waiters.push(waiting);
                    true
                } else {
                    false
                }
            });
            waiters
        };
        for waiting in waiters {
            let _guard = waiting
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            waiting.changed.notify_all();
        }
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    /// Returns the exact cancellation reason, if cancellation was requested.
    #[must_use]
    pub fn reason(&self) -> Option<CompletionCancellationReason> {
        *self
            .inner
            .reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

type CompletionTask = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
struct RunLoopInner {
    runnable: VecDeque<CompletionTask>,
    state: CompletionRunLoopState,
}

#[derive(Default)]
struct RunLoopSignal {
    inner: Mutex<RunLoopInner>,
    changed: Condvar,
}

/// Configurable source-shaped worker delegation used by [`CompletionRunLoop::go`].
pub trait CompletionSpawner: Send + Sync + 'static {
    /// Starts one independent producer task.
    fn go(&self, task: Box<dyn FnOnce() + Send + 'static>);
}

/// Caller-driven task queue used by asynchronous response completion.
///
/// Producers can start independent work through [`Self::go`], while a
/// response-pull caller becomes the sole completion driver through
/// [`Self::execute_ready`] or [`Self::execute`]. Tasks appended by a running
/// completion are drained before a successful drive returns.
#[derive(Clone)]
pub struct CompletionRunLoop {
    signal: Arc<RunLoopSignal>,
    spawner: Option<Arc<dyn CompletionSpawner>>,
}

impl Default for CompletionRunLoop {
    fn default() -> Self {
        Self {
            signal: Arc::new(RunLoopSignal::default()),
            spawner: None,
        }
    }
}

impl fmt::Debug for CompletionRunLoop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionRunLoop")
            .field("state", &self.state())
            .field("num_runnable", &self.num_runnable())
            .field("has_spawner", &self.spawner.is_some())
            .finish()
    }
}

impl CompletionRunLoop {
    /// Creates an idle, empty completion queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an idle completion queue backed by a source-shaped worker pool.
    #[must_use]
    pub fn with_spawner(spawner: Arc<dyn CompletionSpawner>) -> Self {
        Self {
            signal: Arc::new(RunLoopSignal::default()),
            spawner: Some(spawner),
        }
    }

    /// Starts producer work immediately, delegating to the configured pool.
    ///
    /// Without a pool this mirrors client-go's `go f()` path with one detached
    /// operating-system thread. Completion delivery still uses the run loop.
    pub fn go<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(spawner) = &self.spawner {
            spawner.go(Box::new(task));
        } else {
            drop(std::thread::spawn(task));
        }
    }

    /// Returns the current queue state.
    #[must_use]
    pub fn state(&self) -> CompletionRunLoopState {
        self.lock_inner().state
    }

    /// Returns the number of tasks waiting in the shared runnable queue.
    #[must_use]
    pub fn num_runnable(&self) -> usize {
        self.lock_inner().runnable.len()
    }

    /// Appends one task and wakes a waiting driver.
    pub fn append<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.append_task(Box::new(task));
    }

    /// Drives all tasks currently ready without waiting for future work.
    ///
    /// An empty idle queue completes successfully with zero tasks. A second
    /// caller is rejected while another caller is waiting or running.
    #[must_use]
    pub fn execute_ready(&self) -> CompletionRunOutcome {
        let running = {
            let mut inner = self.lock_inner();
            if inner.state != CompletionRunLoopState::Idle {
                return CompletionRunOutcome::failed(0, CompletionError::ConcurrentDriver);
            }
            if inner.runnable.is_empty() {
                return CompletionRunOutcome::completed(0);
            }
            inner.state = CompletionRunLoopState::Running;
            mem::take(&mut inner.runnable)
        };
        self.run(running, None)
    }

    /// Waits for work and drives it until drained or cancelled.
    ///
    /// When cancellation interrupts a running batch, unexecuted tasks retain
    /// their original order ahead of tasks appended during that batch.
    #[must_use]
    pub fn execute(&self, cancellation: &CompletionCancellation) -> CompletionRunOutcome {
        let running = loop {
            let mut inner = self.lock_inner();
            if inner.state != CompletionRunLoopState::Idle {
                return CompletionRunOutcome::failed(0, CompletionError::ConcurrentDriver);
            }
            if let Some(reason) = cancellation.reason() {
                return CompletionRunOutcome::failed(0, reason.as_error());
            }
            if !inner.runnable.is_empty() {
                inner.state = CompletionRunLoopState::Running;
                break mem::take(&mut inner.runnable);
            }

            inner.state = CompletionRunLoopState::Waiting;
            cancellation.register_waiter(&self.signal);
            inner = self
                .signal
                .changed
                .wait_while(inner, |current| {
                    current.state == CompletionRunLoopState::Waiting && !cancellation.is_cancelled()
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if let Some(reason) = cancellation.reason() {
                inner.state = CompletionRunLoopState::Idle;
                return CompletionRunOutcome::failed(0, reason.as_error());
            }
            // Append changes Waiting back to Idle before notifying us. Loop to
            // re-check ownership and detach the newly runnable batch.
        };
        self.run(running, Some(cancellation))
    }

    fn append_task(&self, task: CompletionTask) {
        let wake = {
            let mut inner = self.lock_inner();
            inner.runnable.push_back(task);
            if inner.state == CompletionRunLoopState::Waiting {
                inner.state = CompletionRunLoopState::Idle;
                true
            } else {
                false
            }
        };
        if wake {
            self.signal.changed.notify_one();
        }
    }

    fn run(
        &self,
        mut running: VecDeque<CompletionTask>,
        cancellation: Option<&CompletionCancellation>,
    ) -> CompletionRunOutcome {
        let mut executed = 0;
        loop {
            while !running.is_empty() {
                if let Some(reason) = cancellation.and_then(CompletionCancellation::reason) {
                    let mut inner = self.lock_inner();
                    running.append(&mut inner.runnable);
                    inner.runnable = running;
                    inner.state = CompletionRunLoopState::Idle;
                    return CompletionRunOutcome::failed(executed, reason.as_error());
                }
                let task = running
                    .pop_front()
                    .expect("non-empty completion batch must have a front task");
                task();
                executed += 1;
            }

            let mut inner = self.lock_inner();
            if inner.runnable.is_empty() {
                inner.state = CompletionRunLoopState::Idle;
                return CompletionRunOutcome::completed(executed);
            }
            running = mem::take(&mut inner.runnable);
        }
    }

    fn lock_inner(&self) -> MutexGuard<'_, RunLoopInner> {
        self.signal
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl CompletionCancellation {
    fn register_waiter(&self, run_loop: &Arc<RunLoopSignal>) {
        let waiting = Arc::downgrade(run_loop);
        let mut waiters = self
            .inner
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !waiters
            .iter()
            .any(|registered| Weak::ptr_eq(registered, &waiting))
        {
            waiters.push(waiting);
        }
    }
}

type CompletionTransform<T, E> = Box<dyn FnOnce(Result<T, E>) -> Result<T, E> + Send + 'static>;
type CompletionTerminal<T, E> = Box<dyn FnOnce(Result<T, E>) + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallbackPhase {
    Open,
    Executing,
    Complete,
}

struct CallbackInner<T, E> {
    phase: CallbackPhase,
    transforms: Vec<CompletionTransform<T, E>>,
    terminal: Option<CompletionTerminal<T, E>>,
}

struct CallbackState<T, E> {
    inner: Mutex<CallbackInner<T, E>>,
    complete: Condvar,
}

struct OnceBodyGuard<T, E> {
    state: Arc<CallbackState<T, E>>,
}

impl<T, E> Drop for OnceBodyGuard<T, E> {
    fn drop(&mut self) {
        {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            debug_assert_eq!(inner.phase, CallbackPhase::Executing);
            inner.phase = CallbackPhase::Complete;
        }
        self.state.complete.notify_all();
    }
}

/// Once-only callback whose scheduled form is driven by a completion queue.
pub struct CompletionCallback<T, E> {
    run_loop: CompletionRunLoop,
    state: Arc<CallbackState<T, E>>,
}

impl<T, E> Clone for CompletionCallback<T, E> {
    fn clone(&self) -> Self {
        Self {
            run_loop: self.run_loop.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<T, E> CompletionCallback<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Creates a callback bound to `run_loop` and one terminal action.
    pub fn new<F>(run_loop: CompletionRunLoop, terminal: F) -> Self
    where
        F: FnOnce(Result<T, E>) + Send + 'static,
    {
        Self {
            run_loop,
            state: Arc::new(CallbackState {
                inner: Mutex::new(CallbackInner {
                    phase: CallbackPhase::Open,
                    transforms: Vec::new(),
                    terminal: Some(Box::new(terminal)),
                }),
                complete: Condvar::new(),
            }),
        }
    }

    /// Returns the shared caller-driven executor.
    #[must_use]
    pub fn run_loop(&self) -> CompletionRunLoop {
        self.run_loop.clone()
    }

    /// Adds a deferred transform executed before the terminal action.
    ///
    /// Transforms execute in reverse injection order, matching nested callback
    /// wrappers in the pinned client-go implementation.
    pub fn inject<F>(&self, transform: F) -> Result<(), CompletionError>
    where
        F: FnOnce(Result<T, E>) -> Result<T, E> + Send + 'static,
    {
        let mut inner = self
            .state
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.phase != CallbackPhase::Open {
            return Err(CompletionError::AlreadyCompleted);
        }
        inner.transforms.push(Box::new(transform));
        Ok(())
    }

    /// Claims and invokes the callback immediately on the current thread.
    /// Concurrent duplicate attempts wait for this full delivery to finish and
    /// then return silently, matching Go's `sync.Once.Do` ordering guarantee.
    pub fn invoke(&self, result: Result<T, E>) {
        let Some((transforms, terminal, _once_body)) = self.begin_once_body() else {
            return;
        };
        Self::deliver(result, transforms, terminal);
    }

    /// Claims the callback and queues its terminal delivery for later driving.
    /// Concurrent duplicate attempts wait until the winning task is appended
    /// and then return silently, matching Go's `sync.Once.Do` ordering guarantee.
    pub fn schedule(&self, result: Result<T, E>) {
        let Some((transforms, terminal, _once_body)) = self.begin_once_body() else {
            return;
        };
        self.run_loop.append_task(Box::new(move || {
            Self::deliver(result, transforms, terminal);
        }));
    }

    fn begin_once_body(
        &self,
    ) -> Option<(
        Vec<CompletionTransform<T, E>>,
        CompletionTerminal<T, E>,
        OnceBodyGuard<T, E>,
    )> {
        let mut inner = self
            .state
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            match inner.phase {
                CallbackPhase::Open => break,
                CallbackPhase::Executing => {
                    inner = self
                        .state
                        .complete
                        .wait(inner)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                CallbackPhase::Complete => return None,
            }
        }
        inner.phase = CallbackPhase::Executing;
        let transforms = mem::take(&mut inner.transforms);
        let terminal = inner
            .terminal
            .take()
            .expect("unfulfilled callback must retain its terminal action");
        Some((
            transforms,
            terminal,
            OnceBodyGuard {
                state: Arc::clone(&self.state),
            },
        ))
    }

    fn deliver(
        mut result: Result<T, E>,
        transforms: Vec<CompletionTransform<T, E>>,
        terminal: CompletionTerminal<T, E>,
    ) {
        for transform in transforms.into_iter().rev() {
            result = transform(result);
        }
        terminal(result);
    }
}

struct PullState<T, E> {
    cancelled: bool,
    result: Option<Result<T, E>>,
}

/// Cloneable source-side authority for one asynchronous request.
///
/// The request shares the pull side's cancellation state and the callback's
/// once-only terminal gate. Scheduler adapters can therefore observe caller
/// cancellation and schedule a typed failure without introducing parallel
/// cancellation or completion state.
pub struct CompletionRequest<T, E> {
    callback: CompletionCallback<T, E>,
    inner: Arc<Mutex<PullState<T, E>>>,
}

impl<T, E> Clone for CompletionRequest<T, E> {
    fn clone(&self) -> Self {
        Self {
            callback: self.callback.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, E> fmt::Debug for CompletionRequest<T, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cancelled = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cancelled;
        formatter
            .debug_struct("CompletionRequest")
            .field("cancelled", &cancelled)
            .field("run_loop_state", &self.callback.run_loop.state())
            .finish_non_exhaustive()
    }
}

impl<T, E> CompletionRequest<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Returns the shared caller-driven executor.
    #[must_use]
    pub fn run_loop(&self) -> CompletionRunLoop {
        self.callback.run_loop()
    }

    /// Adds a deferred transform before this request is completed.
    pub fn inject<F>(&self, transform: F) -> Result<(), CompletionError>
    where
        F: FnOnce(Result<T, E>) -> Result<T, E> + Send + 'static,
    {
        self.callback.inject(transform)
    }

    /// Completes this request immediately on the current thread.
    pub fn invoke(&self, result: Result<T, E>) {
        self.callback.invoke(result);
    }

    /// Schedules this request's completion on its shared run loop.
    pub fn schedule(&self, result: Result<T, E>) {
        self.callback.schedule(result);
    }

    /// Schedules one typed terminal failure through the same once-only gate.
    pub fn schedule_error(&self, error: E) {
        self.callback.schedule(Err(error));
    }

    /// Whether the pull-side caller cancelled this exact request.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cancelled
    }
}

/// Pull-side owner of one scheduled completion result.
///
/// Multiple pulls are allowed, but each pull consumes at most one terminal
/// result. Cancellation invokes the exact source-attempt hook and suppresses a
/// terminal action that has not already acquired this pull state; it does not
/// fabricate an error response.
pub struct CompletionPull<T, E> {
    run_loop: CompletionRunLoop,
    inner: Arc<Mutex<PullState<T, E>>>,
    cancel_hook: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<T, E> CompletionPull<T, E> {
    /// Drives ready callbacks and returns this request's terminal result.
    pub fn try_complete(&mut self) -> Result<Option<Result<T, E>>, CompletionError> {
        let outcome = self.run_loop.execute_ready();
        if let Some(error) = outcome.error() {
            return Err(error);
        }
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(inner.result.take())
    }

    /// Cancels this exact source attempt and suppresses later publication.
    pub fn cancel(&mut self) {
        {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.cancelled = true;
        }
        if let Some(cancel_hook) = self.cancel_hook.take() {
            cancel_hook();
        }
    }

    /// Whether this request has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cancelled
    }
}

/// Creates both sides of one once-only completion with its exact cancel hook.
pub fn completion_pair<T, E, F>(
    run_loop: CompletionRunLoop,
    cancel_hook: F,
) -> (CompletionRequest<T, E>, CompletionPull<T, E>)
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnOnce() + Send + 'static,
{
    let inner = Arc::new(Mutex::new(PullState {
        cancelled: false,
        result: None,
    }));
    let terminal_inner = Arc::clone(&inner);
    let callback = CompletionCallback::new(run_loop.clone(), move |result| {
        let mut inner = terminal_inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !inner.cancelled {
            inner.result = Some(result);
        }
    });
    let request = CompletionRequest {
        callback,
        inner: Arc::clone(&inner),
    };
    (
        request,
        CompletionPull {
            run_loop,
            inner,
            cancel_hook: Some(Box::new(cancel_hook)),
        },
    )
}

impl PendingRequest for CompletionPull<DirectUnaryResponse, DirectUnaryClientError> {
    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError> {
        CompletionPull::try_complete(self)
    }

    fn cancel(&mut self) {
        CompletionPull::cancel(self);
    }
}

/// One in-flight source request returned by [`AsyncRequestDispatcher::begin`].
pub trait PendingRequest {
    /// Polls without blocking. `None` means the exact attempt is still pending.
    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError>;

    /// Cancels this exact attempt without inventing a terminal response.
    fn cancel(&mut self);
}

/// Address-directed BatchCommands attempt boundary used by async policy.
pub trait AsyncRequestDispatcher {
    /// Concrete once-only pending handle.
    type Pending: PendingRequest;

    /// Begins exactly one attempt using an already selected target/proxy route.
    fn begin(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError>;

    /// Executes a retry synchronously after async policy advances its state.
    fn send_retry_sync(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError>;
}
