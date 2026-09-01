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

use crossbeam_channel::{Receiver, RecvError, SendError, Sender};
use tidb_resourcemanager::poolmanager::Channel;

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

/// Go `SimpleDataChannel`; a zero-capacity channel preserves Go's
/// unbuffered hand-off.
pub struct SimpleDataChannel<T> {
    channel: Channel<T>,
}

impl<T> Clone for SimpleDataChannel<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T> SimpleDataChannel<T> {
    /// Go `NewSimpleDataChannel(make(chan T))`.
    pub fn new() -> Self {
        Self {
            channel: Channel::bounded(0),
        }
    }

    /// Native form of Go `NewSimpleDataChannel(ch)` for an already-created
    /// channel.
    pub fn from_channel(sender: Sender<T>, receiver: Receiver<T>) -> Self {
        Self {
            channel: Channel::from_parts(sender, receiver),
        }
    }

    pub(crate) fn channel(&self) -> Channel<T> {
        self.channel.clone()
    }

    pub(crate) fn receiver(&self) -> Receiver<T> {
        self.channel.receiver()
    }

    /// Sends one value through Go `Channel()`.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.channel.send_result(value)
    }

    /// Receives one value through Go `Channel()`.
    pub fn receive(&self) -> Result<T, RecvError> {
        self.channel.receiver().recv()
    }

    /// Go `Finish`.
    pub fn finish(&self) {
        self.channel.close();
    }

    pub(crate) fn try_finish(&self) {
        self.channel.close_if_open();
    }

    pub(crate) fn send_or_cancel(&self, value: T, cancelled: &Receiver<()>) -> bool {
        self.channel.send_or_cancel(value, cancelled)
    }
}

/// Go `Compose`.
pub fn compose<T>(upstream: &impl WithSink<T>, downstream: &impl WithSource<T>) {
    let channel = SimpleDataChannel::new();
    upstream.set_sink(channel.clone());
    downstream.set_source(channel);
}
