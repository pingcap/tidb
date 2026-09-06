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

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::select;

use crate::{
    operator::display_type_name, AsyncOperator, Context, Operator, OperatorError,
    SimpleDataChannel, TaskMayPanic, TunableOperator, WithSink, WithSource,
};

/// Go `SimpleDataSource`.
pub struct SimpleDataSource<T: TaskMayPanic<OperatorError>> {
    context: Arc<Context>,
    inputs: Mutex<Option<Vec<T>>>,
    target: Mutex<Option<SimpleDataChannel<T>>>,
    handle: Mutex<Option<JoinHandle<Result<(), OperatorError>>>>,
}

impl<T: TaskMayPanic<OperatorError>> SimpleDataSource<T> {
    /// Go `NewSimpleDataSource`.
    pub fn new(context: Arc<Context>, inputs: Vec<T>) -> Self {
        Self {
            context,
            inputs: Mutex::new(Some(inputs)),
            target: Mutex::new(None),
            handle: Mutex::new(None),
        }
    }
}

impl<T: TaskMayPanic<OperatorError>> WithSink<T> for SimpleDataSource<T> {
    fn set_sink(&self, channel: SimpleDataChannel<T>) {
        *self
            .target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(channel);
    }
}

impl<T: TaskMayPanic<OperatorError>> Operator for SimpleDataSource<T> {
    fn open(&self) -> Result<(), OperatorError> {
        let inputs = self
            .inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_default();
        let target = self
            .target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("SimpleDataSource sink is required");
        let cancelled = self.context.cancelled();
        let handle = std::thread::spawn(move || {
            for input in inputs {
                if !target.send_or_cancel(input, &cancelled) {
                    target.try_finish();
                    return Err(OperatorError("context canceled".to_owned()));
                }
            }
            target.finish();
            Ok(())
        });
        *self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        Ok(())
    }

    fn close(&self) -> Result<(), OperatorError> {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match handle {
            Some(handle) => handle
                .join()
                .unwrap_or_else(|_| Err(OperatorError("source panic".to_owned()))),
            None => Ok(()),
        }
    }

    fn operator_string(&self) -> String {
        format!("SimpleDataSource[{}]", display_type_name::<T>())
    }
}

/// Go private `simpleSink`, exposed for native external package tests.
pub(crate) struct SimpleSink<R: Send + 'static> {
    context: Arc<Context>,
    drainer: Arc<dyn Fn(R) + Send + Sync>,
    source: Mutex<Option<SimpleDataChannel<R>>>,
    handle: Mutex<Option<JoinHandle<Result<(), OperatorError>>>>,
}

impl<R: Send + 'static> SimpleSink<R> {
    /// Go `newSimpleSink`.
    pub(crate) fn new(context: Arc<Context>, drainer: impl Fn(R) + Send + Sync + 'static) -> Self {
        Self {
            context,
            drainer: Arc::new(drainer),
            source: Mutex::new(None),
            handle: Mutex::new(None),
        }
    }
}

impl<R: Send + 'static> WithSource<R> for SimpleSink<R> {
    fn set_source(&self, channel: SimpleDataChannel<R>) {
        *self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(channel);
    }
}

impl<R: Send + 'static> Operator for SimpleSink<R> {
    fn open(&self) -> Result<(), OperatorError> {
        let source = self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("SimpleSink source is required");
        let cancelled = self.context.cancelled();
        let receiver = source.receiver();
        let drainer = Arc::clone(&self.drainer);
        let handle = std::thread::spawn(move || loop {
            select! {
                recv(cancelled) -> _ => return Err(OperatorError("context canceled".to_owned())),
                recv(receiver) -> value => match value {
                    Ok(value) => drainer(value),
                    Err(_) => return Ok(()),
                }
            }
        });
        *self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        Ok(())
    }

    fn close(&self) -> Result<(), OperatorError> {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match handle {
            Some(handle) => handle
                .join()
                .unwrap_or_else(|_| Err(OperatorError("sink panic".to_owned()))),
            None => Ok(()),
        }
    }

    fn operator_string(&self) -> String {
        "simpleSink".to_owned()
    }
}

/// Go private `simpleOperator`, exposed for native external package tests.
pub(crate) struct SimpleOperator<T: TaskMayPanic<OperatorError>, R: Send + 'static> {
    inner: AsyncOperator<T, R>,
}

impl<T: TaskMayPanic<OperatorError>, R: Send + 'static> SimpleOperator<T, R> {
    /// Go `newSimpleOperator`.
    pub(crate) fn new(
        context: Arc<Context>,
        transform: impl Fn(T) -> R + Send + Sync + 'static,
        concurrency: i32,
    ) -> Self {
        Self {
            inner: AsyncOperator::with_transform(context, "simple", concurrency, transform),
        }
    }
}

impl<T: TaskMayPanic<OperatorError>, R: Send + 'static> WithSource<T> for SimpleOperator<T, R> {
    fn set_source(&self, channel: SimpleDataChannel<T>) {
        self.inner.set_source(channel);
    }
}

impl<T: TaskMayPanic<OperatorError>, R: Send + 'static> WithSink<R> for SimpleOperator<T, R> {
    fn set_sink(&self, channel: SimpleDataChannel<R>) {
        self.inner.set_sink(channel);
    }
}

impl<T: TaskMayPanic<OperatorError>, R: Send + 'static> Operator for SimpleOperator<T, R> {
    fn open(&self) -> Result<(), OperatorError> {
        self.inner.open()
    }

    fn close(&self) -> Result<(), OperatorError> {
        self.inner.close()
    }

    fn operator_string(&self) -> String {
        format!("simpleOperator({})", self.inner.operator_string())
    }

    fn tunable(&self) -> Option<&dyn TunableOperator> {
        Some(self)
    }
}

impl<T: TaskMayPanic<OperatorError>, R: Send + 'static> TunableOperator for SimpleOperator<T, R> {
    fn tune_worker_pool_size(&self, worker_num: i32, wait: bool) {
        self.inner.tune_worker_pool_size(worker_num, wait);
    }

    fn worker_pool_size(&self) -> i32 {
        self.inner.worker_pool_size()
    }
}
