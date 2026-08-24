// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Callback and run-loop primitives used by asynchronous client paths.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use thiserror::Error;
use tokio::sync::Notify;

/// A synchronous task owned by an executor.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// A pool that can run a task asynchronously.
pub trait Pool: Send + Sync {
    /// Submits a task to the pool.
    fn spawn(&self, task: Task);
}

/// An executor that can queue one or more tasks for later execution.
pub trait Executor: Pool {
    /// Appends tasks in execution order. Implementations must support concurrent calls.
    fn append(&self, tasks: Vec<Task>);
}

type CallbackFn<T, E> = Box<dyn FnOnce(T, Option<E>) + Send + 'static>;
type Injector<T, E> = Box<dyn FnOnce(T, Option<E>) -> (T, Option<E>) + Send + 'static>;

struct CallbackInner<T, E> {
    claimed: AtomicBool,
    executor: Option<Arc<dyn Executor>>,
    callback: Mutex<Option<CallbackFn<T, E>>>,
    injectors: Mutex<Vec<Injector<T, E>>>,
}

/// A callback that can be fulfilled exactly once, immediately or through an executor.
pub struct Callback<T, E> {
    inner: Arc<CallbackInner<T, E>>,
}

impl<T, E> Clone for Callback<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, E> Callback<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Creates a callback associated with an optional executor.
    pub fn new(
        executor: Option<Arc<dyn Executor>>,
        callback: impl FnOnce(T, Option<E>) + Send + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(CallbackInner {
                claimed: AtomicBool::new(false),
                executor,
                callback: Mutex::new(Some(Box::new(callback))),
                injectors: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns the callback's executor.
    pub fn executor(&self) -> Option<Arc<dyn Executor>> {
        self.inner.executor.clone()
    }

    /// Adds an action that runs before the callback.
    ///
    /// Injected actions run in reverse registration order.
    pub fn inject(&self, injector: impl FnOnce(T, Option<E>) -> (T, Option<E>) + Send + 'static) {
        self.inner
            .injectors
            .lock()
            .expect("callback injector mutex poisoned")
            .push(Box::new(injector));
    }

    /// Fulfills the callback immediately in the current thread.
    pub fn invoke(&self, value: T, error: Option<E>) {
        if self.claim() {
            Self::call(self.inner.clone(), value, error);
        }
    }

    /// Schedules callback fulfillment through its executor.
    ///
    /// # Panics
    ///
    /// Panics when the callback has no executor, matching client-go's nil-executor behavior.
    pub fn schedule(&self, value: T, error: Option<E>) {
        if !self.claim() {
            return;
        }
        let executor = self
            .inner
            .executor
            .clone()
            .expect("cannot schedule a callback without an executor");
        let inner = self.inner.clone();
        executor.append(vec![Box::new(move || Self::call(inner, value, error))]);
    }

    fn claim(&self) -> bool {
        self.inner
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn call(inner: Arc<CallbackInner<T, E>>, mut value: T, mut error: Option<E>) {
        let injectors = std::mem::take(
            &mut *inner
                .injectors
                .lock()
                .expect("callback injector mutex poisoned"),
        );
        for injector in injectors.into_iter().rev() {
            (value, error) = injector(value, error);
        }
        let callback = inner
            .callback
            .lock()
            .expect("callback mutex poisoned")
            .take()
            .expect("claimed callback must still have its function");
        callback(value, error);
    }
}

/// Current run-loop execution state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum State {
    /// Not executing and not waiting.
    #[default]
    Idle,
    /// Waiting for a task or cancellation.
    Waiting,
    /// Executing queued tasks.
    Running,
}

/// Error returned by [`RunLoop::execute`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RunLoopError {
    /// Another caller is already executing this run loop.
    #[error("runloop: already executing")]
    AlreadyExecuting,
    /// Execution was cancelled before all runnable tasks finished.
    #[error("runloop: cancelled")]
    Cancelled,
}

#[derive(Clone)]
pub struct Cancellation {
    inner: Arc<CancellationInner>,
    parent: Option<Arc<Cancellation>>,
}

#[derive(Default)]
struct CancellationInner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Cancellation {
    /// Creates a child cancellation handle. Cancelling the parent interrupts
    /// the child, while cancelling the child leaves the parent unaffected.
    pub fn child(&self) -> Self {
        Self {
            inner: Arc::new(CancellationInner::default()),
            parent: Some(Arc::new(self.clone())),
        }
    }

    /// Cancels current and future waits.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.notify.notify_one();
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.is_cancelled())
    }

    pub(crate) async fn cancelled(&self) {
        while !self.is_cancelled() {
            if let Some(parent) = &self.parent {
                let parent_cancelled = Box::pin(parent.cancelled());
                tokio::select! {
                    _ = self.inner.notify.notified() => {}
                    _ = parent_cancelled => {}
                }
            } else {
                self.inner.notify.notified().await;
            }
        }
    }
}

impl Default for Cancellation {
    fn default() -> Self {
        Self {
            inner: Arc::new(CancellationInner::default()),
            parent: None,
        }
    }
}

#[derive(Default)]
struct RunLoopInner {
    runnable: VecDeque<Task>,
    state: State,
}

/// A single-consumer run loop with concurrently appendable tasks.
pub struct RunLoop {
    pool: RwLock<Option<Arc<dyn Pool>>>,
    inner: Mutex<RunLoopInner>,
    ready: Notify,
}

impl Default for RunLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl RunLoop {
    /// Creates an idle run loop with no custom task pool.
    pub fn new() -> Self {
        Self {
            pool: RwLock::new(None),
            inner: Mutex::new(RunLoopInner::default()),
            ready: Notify::new(),
        }
    }

    /// Replaces the optional pool used by [`Pool::spawn`].
    pub fn set_pool(&self, pool: Option<Arc<dyn Pool>>) {
        *self.pool.write().expect("runloop pool lock poisoned") = pool;
    }

    /// Returns the current run-loop state.
    pub fn state(&self) -> State {
        self.inner.lock().expect("runloop mutex poisoned").state
    }

    /// Returns the number of queued tasks not in the currently executing batch.
    pub fn num_runnable(&self) -> usize {
        self.inner
            .lock()
            .expect("runloop mutex poisoned")
            .runnable
            .len()
    }

    /// Executes all runnable work, waiting when initially empty.
    ///
    /// Tasks appended by a running task are included in the same call. If cancellation occurs,
    /// unexecuted tasks are returned to the front of the runnable queue in their original order.
    pub async fn execute(&self, cancellation: &Cancellation) -> (usize, Result<(), RunLoopError>) {
        let mut running = loop {
            let runnable = {
                let mut inner = self.inner.lock().expect("runloop mutex poisoned");
                if inner.state != State::Idle {
                    return (0, Err(RunLoopError::AlreadyExecuting));
                }
                if inner.runnable.is_empty() {
                    inner.state = State::Waiting;
                    None
                } else {
                    inner.state = State::Running;
                    Some(std::mem::take(&mut inner.runnable))
                }
            };
            if let Some(runnable) = runnable {
                break runnable;
            } else {
                tokio::select! {
                    _ = self.ready.notified() => continue,
                    _ = cancellation.cancelled() => {
                        self.inner.lock().expect("runloop mutex poisoned").state = State::Idle;
                        return (0, Err(RunLoopError::Cancelled));
                    }
                }
            }
        };

        let mut count = 0;
        loop {
            while let Some(task) = running.pop_front() {
                if cancellation.is_cancelled() {
                    running.push_front(task);
                    let mut inner = self.inner.lock().expect("runloop mutex poisoned");
                    running.append(&mut inner.runnable);
                    inner.runnable = running;
                    inner.state = State::Idle;
                    return (count, Err(RunLoopError::Cancelled));
                }
                task();
                count += 1;
            }

            let mut inner = self.inner.lock().expect("runloop mutex poisoned");
            if inner.runnable.is_empty() {
                inner.state = State::Idle;
                return (count, Ok(()));
            }
            running = std::mem::take(&mut inner.runnable);
        }
    }
}

impl Pool for RunLoop {
    fn spawn(&self, task: Task) {
        if let Some(pool) = self
            .pool
            .read()
            .expect("runloop pool lock poisoned")
            .clone()
        {
            pool.spawn(task);
        } else {
            std::thread::spawn(task);
        }
    }
}

impl Executor for RunLoop {
    fn append(&self, tasks: Vec<Task>) {
        if tasks.is_empty() {
            return;
        }
        let notify = {
            let mut inner = self.inner.lock().expect("runloop mutex poisoned");
            inner.runnable.extend(tasks);
            if inner.state == State::Waiting {
                inner.state = State::Idle;
                true
            } else {
                false
            }
        };
        if notify {
            self.ready.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct MockExecutor {
        tasks: Mutex<VecDeque<Task>>,
    }

    impl MockExecutor {
        fn len(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }

        fn run_one(&self) {
            self.tasks.lock().unwrap().pop_front().unwrap()();
        }
    }

    impl Pool for MockExecutor {
        fn spawn(&self, task: Task) {
            self.append(vec![task]);
        }
    }

    impl Executor for MockExecutor {
        fn append(&self, tasks: Vec<Task>) {
            self.tasks.lock().unwrap().extend(tasks);
        }
    }

    #[test]
    fn callback_injects_in_reverse_order() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let callback = Callback::<Vec<i32>, String>::new(None, move |values, error| {
            assert!(error.is_none());
            *captured.lock().unwrap() = values;
        });
        callback.inject(|mut values, error| {
            values.push(3);
            (values, error)
        });
        callback.inject(|mut values, error| {
            values.push(2);
            (values, error)
        });
        callback.inject(|mut values, error| {
            values.push(1);
            (values, error)
        });
        callback.invoke(Vec::new(), None);
        assert_eq!(*output.lock().unwrap(), [1, 2, 3]);
    }

    #[test]
    fn callback_is_fulfilled_once_for_every_call_order() {
        let executor = Arc::new(MockExecutor::default());

        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let callback = Callback::<i32, String>::new(Some(executor.clone()), move |value, _| {
            captured.lock().unwrap().push(value);
        });
        callback.invoke(1, None);
        callback.invoke(2, None);
        assert_eq!(*output.lock().unwrap(), [1]);

        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let callback = Callback::<i32, String>::new(Some(executor.clone()), move |value, _| {
            captured.lock().unwrap().push(value);
        });
        callback.schedule(1, None);
        callback.schedule(2, None);
        assert_eq!(executor.len(), 1);
        assert!(output.lock().unwrap().is_empty());
        executor.run_one();
        assert_eq!(*output.lock().unwrap(), [1]);

        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let callback = Callback::<i32, String>::new(Some(executor.clone()), move |value, _| {
            captured.lock().unwrap().push(value);
        });
        callback.invoke(1, None);
        callback.schedule(2, None);
        assert_eq!(executor.len(), 0);
        assert_eq!(*output.lock().unwrap(), [1]);

        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let callback = Callback::<i32, String>::new(Some(executor.clone()), move |value, _| {
            captured.lock().unwrap().push(value);
        });
        callback.schedule(1, None);
        callback.invoke(2, None);
        assert_eq!(executor.len(), 1);
        assert!(output.lock().unwrap().is_empty());
        executor.run_one();
        assert_eq!(*output.lock().unwrap(), [1]);
    }

    #[test]
    fn runloop_spawn_uses_default_thread_or_custom_pool() {
        let loop_ = RunLoop::new();
        let value = Arc::new(AtomicU32::new(0));
        let captured = value.clone();
        loop_.spawn(Box::new(move || captured.store(1, Ordering::Release)));
        for _ in 0..1000 {
            if value.load(Ordering::Acquire) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(value.load(Ordering::Acquire), 1);

        let pool = Arc::new(MockExecutor::default());
        loop_.set_pool(Some(pool.clone()));
        let captured = value.clone();
        loop_.spawn(Box::new(move || captured.store(2, Ordering::Release)));
        assert_eq!(pool.len(), 1);
        assert_eq!(value.load(Ordering::Acquire), 1);
        pool.run_one();
        assert_eq!(value.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn runloop_waits_and_executes_appended_tasks() {
        let loop_ = Arc::new(RunLoop::new());
        let output = Arc::new(Mutex::new(Vec::new()));
        let producer_loop = loop_.clone();
        let producer_output = output.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            producer_loop.append(vec![Box::new(move || {
                producer_output.lock().unwrap().push(1)
            })]);
        });

        let (count, result) = loop_.execute(&Cancellation::default()).await;
        result.unwrap();
        assert_eq!(count, 1);
        assert_eq!(loop_.state(), State::Idle);
        assert_eq!(loop_.num_runnable(), 0);
        assert_eq!(*output.lock().unwrap(), [1]);
    }

    #[tokio::test]
    async fn runloop_includes_tasks_appended_while_running() {
        let loop_ = Arc::new(RunLoop::new());
        let output = Arc::new(Mutex::new(Vec::new()));
        let nested_loop = loop_.clone();
        let first_output = output.clone();
        let second_output = output.clone();
        loop_.append(vec![Box::new(move || {
            nested_loop.append(vec![Box::new(move || {
                second_output.lock().unwrap().push(2)
            })]);
            first_output.lock().unwrap().push(1);
        })]);

        let (count, result) = loop_.execute(&Cancellation::default()).await;
        result.unwrap();
        assert_eq!(count, 2);
        assert_eq!(*output.lock().unwrap(), [1, 2]);
    }

    #[tokio::test]
    async fn runloop_leaves_later_tasks_for_the_next_execute() {
        let loop_ = Arc::new(RunLoop::new());
        let output = Arc::new(Mutex::new(Vec::new()));
        let producer_loop = loop_.clone();
        let first_output = output.clone();
        let second_output = output.clone();
        loop_.append(vec![Box::new(move || {
            let producer_loop = producer_loop.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(1));
                producer_loop.append(vec![Box::new(move || {
                    second_output.lock().unwrap().push(2)
                })]);
            });
            first_output.lock().unwrap().push(1);
        })]);

        assert_eq!(loop_.execute(&Cancellation::default()).await, (1, Ok(())));
        assert_eq!(*output.lock().unwrap(), [1]);
        assert_eq!(loop_.execute(&Cancellation::default()).await, (1, Ok(())));
        assert_eq!(*output.lock().unwrap(), [1, 2]);
    }

    #[tokio::test]
    async fn cancellation_preserves_unexecuted_tasks() {
        let loop_ = Arc::new(RunLoop::new());
        let cancellation = Cancellation::default();
        let output = Arc::new(Mutex::new(Vec::new()));
        let first_output = output.clone();
        let second_output = output.clone();
        let cancel_from_task = cancellation.clone();
        loop_.append(vec![
            Box::new(move || {
                cancel_from_task.cancel();
                first_output.lock().unwrap().push(1);
            }),
            Box::new(move || second_output.lock().unwrap().push(2)),
        ]);

        assert_eq!(
            loop_.execute(&cancellation).await,
            (1, Err(RunLoopError::Cancelled))
        );
        assert_eq!(loop_.state(), State::Idle);
        assert_eq!(loop_.num_runnable(), 1);
        assert_eq!(*output.lock().unwrap(), [1]);
    }

    #[tokio::test]
    async fn cancellation_wakes_a_waiting_runloop() {
        let loop_ = Arc::new(RunLoop::new());
        let cancellation = Cancellation::default();
        let cancel_from_thread = cancellation.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            cancel_from_thread.cancel();
        });

        assert_eq!(
            loop_.execute(&cancellation).await,
            (0, Err(RunLoopError::Cancelled))
        );
        assert_eq!(loop_.state(), State::Idle);
        assert_eq!(loop_.num_runnable(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_execute_is_rejected() {
        let loop_ = Arc::new(RunLoop::new());
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let release_task = release.clone();
        let entered_task = entered.clone();
        loop_.append(vec![Box::new(move || {
            entered_task.notify_one();
            futures::executor::block_on(release_task.notified());
        })]);

        let executing_loop = loop_.clone();
        let handle =
            tokio::spawn(async move { executing_loop.execute(&Cancellation::default()).await });
        entered.notified().await;
        assert_eq!(
            loop_.execute(&Cancellation::default()).await,
            (0, Err(RunLoopError::AlreadyExecuting))
        );
        release.notify_one();
        assert_eq!(handle.await.unwrap(), (1, Ok(())));
    }
}
