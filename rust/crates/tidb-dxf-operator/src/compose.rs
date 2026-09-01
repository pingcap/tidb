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

use crossbeam_channel::{bounded, Receiver, RecvError, Select, SendError, Sender};

/// Go `WithSource`.
pub trait WithSource<T> {
    /// Go `SetSource`.
    fn set_source(&self, channel: SimpleDataChannel<T>);
}

/// Go `WithSink`.
pub trait WithSink<T> {
    /// Go `SetSink`.
    fn set_sink(&self, channel: SimpleDataChannel<T>);
}

struct ChannelState<T> {
    sender: Mutex<Option<Sender<T>>>,
    receiver: Receiver<T>,
    close_sender: Mutex<Option<Sender<()>>>,
    closed: Receiver<()>,
}

/// Go `SimpleDataChannel`; a zero-capacity channel preserves Go's
/// unbuffered hand-off.
pub struct SimpleDataChannel<T> {
    state: Arc<ChannelState<T>>,
}

impl<T> Clone for SimpleDataChannel<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Default for SimpleDataChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SimpleDataChannel<T> {
    /// Go `NewSimpleDataChannel(make(chan T))`.
    #[must_use]
    pub fn new() -> Self {
        let (sender, receiver) = bounded(0);
        Self::from_channel(sender, receiver)
    }

    /// Native form of Go `NewSimpleDataChannel(ch)` for an already-created
    /// channel.
    #[must_use]
    pub fn from_channel(sender: Sender<T>, receiver: Receiver<T>) -> Self {
        let (close_sender, closed) = bounded(0);
        Self {
            state: Arc::new(ChannelState {
                sender: Mutex::new(Some(sender)),
                receiver,
                close_sender: Mutex::new(Some(close_sender)),
                closed,
            }),
        }
    }

    pub(crate) fn sender(&self) -> Option<Sender<T>> {
        self.state
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn receiver(&self) -> Receiver<T> {
        self.state.receiver.clone()
    }

    /// Sends one value through Go `Channel()`.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let Some(sender) = self.sender() else {
            return Err(SendError(value));
        };
        let mut selected = Select::new();
        let send_index = selected.send(&sender);
        let closed_index = selected.recv(&self.state.closed);
        let operation = selected.select();
        if operation.index() == send_index {
            operation.send(&sender, value)
        } else {
            debug_assert_eq!(operation.index(), closed_index);
            let _ = operation.recv(&self.state.closed);
            Err(SendError(value))
        }
    }

    /// Receives one value through Go `Channel()`.
    pub fn receive(&self) -> Result<T, RecvError> {
        self.state.receiver.recv()
    }

    /// Go `Finish`.
    pub fn finish(&self) {
        let sender = self
            .state
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("close of closed channel");
        drop(sender);
        self.state
            .close_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(crate) fn try_finish(&self) {
        let sender = self
            .state
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(sender);
        self.state
            .close_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(crate) fn send_or_cancel(&self, value: T, cancelled: &Receiver<()>) -> bool {
        let Some(sender) = self.sender() else {
            return false;
        };
        let mut selected = Select::new();
        let send_index = selected.send(&sender);
        let closed_index = selected.recv(&self.state.closed);
        let cancelled_index = selected.recv(cancelled);
        let operation = selected.select();
        if operation.index() == send_index {
            operation.send(&sender, value).is_ok()
        } else if operation.index() == closed_index {
            let _ = operation.recv(&self.state.closed);
            false
        } else {
            debug_assert_eq!(operation.index(), cancelled_index);
            let _ = operation.recv(cancelled);
            false
        }
    }
}

/// Go `Compose`.
pub fn compose<T>(upstream: &impl WithSink<T>, downstream: &impl WithSource<T>) {
    let channel = SimpleDataChannel::new();
    upstream.set_sink(channel.clone());
    downstream.set_source(channel);
}
