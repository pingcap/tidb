// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/testutil/operator.go`: a source and a sink for operator tests.

use std::sync::Mutex;
use std::thread::JoinHandle;

use operator::{Operator, OperatorError, SimpleDataChannel, WithSink, WithSource};

/// Go `OperatorTestSource`: sends its values, then closes the channel.
pub struct OperatorTestSource<T: Send + 'static> {
    to_be_sent: Mutex<Option<Vec<T>>>,
    ch: Mutex<Option<SimpleDataChannel<T>>>,
    err_group: Mutex<Option<JoinHandle<()>>>,
}

impl<T: Send + 'static> OperatorTestSource<T> {
    /// Go `NewOperatorTestSource`.
    #[must_use]
    pub fn new(to_be_sent: Vec<T>) -> Self {
        Self { to_be_sent: Mutex::new(Some(to_be_sent)), ch: Mutex::new(None), err_group: Mutex::new(None) }
    }
}

impl<T: Send + 'static> WithSink<T> for OperatorTestSource<T> {
    fn set_sink(&self, sink: SimpleDataChannel<T>) {
        *self.ch.lock().unwrap() = Some(sink);
    }
}

impl<T: Send + 'static> Operator for OperatorTestSource<T> {
    fn open(&self) -> Result<(), OperatorError> {
        let ch = self.ch.lock().unwrap().clone().expect("OperatorTestSource sink is required");
        let data = self.to_be_sent.lock().unwrap().take().unwrap_or_default();
        *self.err_group.lock().unwrap() = Some(std::thread::spawn(move || {
            for value in data {
                if ch.send(value).is_err() {
                    break;
                }
            }
            ch.finish();
        }));
        Ok(())
    }

    fn close(&self) -> Result<(), OperatorError> {
        if let Some(handle) = self.err_group.lock().unwrap().take() {
            handle.join().map_err(|_| OperatorError::new("test source panic"))?;
        }
        Ok(())
    }

    fn operator_string(&self) -> String {
        "testSource".to_owned()
    }
}

/// Go `OperatorTestSink`: collects every value until the channel closes.
pub struct OperatorTestSink<T: Send + 'static> {
    ch: Mutex<Option<SimpleDataChannel<T>>>,
    err_group: Mutex<Option<JoinHandle<Vec<T>>>>,
    collected: Mutex<Vec<T>>,
}

impl<T: Send + 'static> Default for OperatorTestSink<T> {
    fn default() -> Self {
        Self { ch: Mutex::new(None), err_group: Mutex::new(None), collected: Mutex::new(Vec::new()) }
    }
}

impl<T: Send + 'static> OperatorTestSink<T> {
    /// Go `NewOperatorTestSink`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `OperatorTestSink.Collect`.
    pub fn collect(&self) -> Vec<T> {
        std::mem::take(&mut self.collected.lock().unwrap())
    }
}

impl<T: Send + 'static> WithSource<T> for OperatorTestSink<T> {
    fn set_source(&self, source: SimpleDataChannel<T>) {
        *self.ch.lock().unwrap() = Some(source);
    }
}

impl<T: Send + 'static> Operator for OperatorTestSink<T> {
    fn open(&self) -> Result<(), OperatorError> {
        let receiver = self.ch.lock().unwrap().clone().expect("OperatorTestSink source is required").receiver();
        *self.err_group.lock().unwrap() = Some(std::thread::spawn(move || receiver.iter().collect()));
        Ok(())
    }

    fn close(&self) -> Result<(), OperatorError> {
        if let Some(handle) = self.err_group.lock().unwrap().take() {
            let collected = handle.join().map_err(|_| OperatorError::new("test sink panic"))?;
            self.collected.lock().unwrap().extend(collected);
        }
        Ok(())
    }

    fn operator_string(&self) -> String {
        "testSink".to_owned()
    }
}
