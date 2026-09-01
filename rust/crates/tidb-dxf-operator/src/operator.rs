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

use std::fmt;
use std::sync::Arc;

use tidb_resourcemanager::workerpool::{Context as WorkerContext, PanicError, WorkerPool};
pub use tidb_resourcemanager::workerpool::{NoResult, TaskMayPanic, Worker};

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

impl From<PanicError> for OperatorError {
    fn from(error: PanicError) -> Self {
        Self(error.to_string())
    }
}

/// DXF adapter around the shared workerpool context.
pub struct Context {
    inner: Arc<WorkerContext<OperatorError>>,
}

impl Context {
    /// Go `workerpool.NewContext(context.Background())`.
    pub fn new() -> Self {
        Self {
            inner: WorkerContext::new(Option::None),
        }
    }

    /// Go `Context.OnError`.
    pub fn on_error(&self, error: OperatorError) {
        self.inner.on_error(error);
    }

    /// Go `Context.OperatorErr`.
    pub fn operator_error(&self) -> Option<OperatorError> {
        self.inner.operator_error()
    }

    /// Go `Context.Cancel`.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    pub(crate) fn cancelled(&self) -> crossbeam_channel::Receiver<()> {
        self.inner.cancelled()
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
        Option::None
    }
}

/// Go `TunableOperator`.
pub trait TunableOperator: Send + Sync {
    /// Go `TuneWorkerPoolSize`.
    fn tune_worker_pool_size(&self, worker_num: i32, wait: bool);
    /// Go `GetWorkerPoolSize`.
    fn worker_pool_size(&self) -> i32;
}

/// Go `AsyncOperator` backed by the shared workerpool package.
pub struct AsyncOperator<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    context: Arc<Context>,
    pool: Arc<WorkerPool<T, R, OperatorError>>,
}

impl<T, R> AsyncOperator<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    /// Go `NewAsyncOperator`.
    pub fn new(context: Arc<Context>, pool: Arc<WorkerPool<T, R, OperatorError>>) -> Self {
        Self { context, pool }
    }

    /// Go `NewAsyncOperatorWithTransform`.
    pub fn with_transform(
        context: Arc<Context>,
        name: impl Into<String>,
        worker_num: i32,
        transform: impl Fn(T) -> R + Send + Sync + 'static,
    ) -> Self {
        let transform: Arc<dyn Fn(T) -> R + Send + Sync> = Arc::new(transform);
        let pool = WorkerPool::new(
            name.into(),
            tidb_resourcemanager::util::Component::DistTask,
            worker_num as isize,
            move || {
                Some(Box::new(TransformWorker {
                    transform: Arc::clone(&transform),
                }))
            },
            vec![],
        );
        Self::new(context, pool)
    }
}

impl<T, R> WithSource<T> for AsyncOperator<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    fn set_source(&self, channel: SimpleDataChannel<T>) {
        self.pool.set_task_receiver(channel.channel());
    }
}

impl<T, R> WithSink<R> for AsyncOperator<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    fn set_sink(&self, channel: SimpleDataChannel<R>) {
        self.pool.set_result_sender(channel.channel());
    }
}

impl<T, R> Operator for AsyncOperator<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    fn open(&self) -> Result<(), OperatorError> {
        self.pool.start(Arc::clone(&self.context.inner));
        Ok(())
    }

    fn close(&self) -> Result<(), OperatorError> {
        self.pool.release();
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

impl<T, R> TunableOperator for AsyncOperator<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    fn tune_worker_pool_size(&self, worker_num: i32, wait: bool) {
        self.pool.tune(worker_num, wait);
    }

    fn worker_pool_size(&self) -> i32 {
        self.pool.cap()
    }
}

struct TransformWorker<T, R> {
    transform: Arc<dyn Fn(T) -> R + Send + Sync>,
}

impl<T, R> Worker<T, R, OperatorError> for TransformWorker<T, R>
where
    T: TaskMayPanic<OperatorError>,
    R: Send + 'static,
{
    fn handle_task(&mut self, task: T, send: &mut dyn FnMut(R)) -> Result<(), OperatorError> {
        send((self.transform)(task));
        Ok(())
    }

    fn close(&mut self) -> Result<(), OperatorError> {
        Ok(())
    }
}
