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

#![allow(missing_docs)]

#[path = "../src/accept_loop.rs"]
mod accept_loop;

use std::collections::VecDeque;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use accept_loop::{AcceptListener, AcceptLoop, AcceptLoopError, AcceptLoopExit};

#[derive(Debug)]
struct FakeListener {
    events: VecDeque<Result<u8, &'static str>>,
}

impl FakeListener {
    fn new(events: impl IntoIterator<Item = Result<u8, &'static str>>) -> Self {
        Self {
            events: events.into_iter().collect(),
        }
    }
}

impl AcceptListener for FakeListener {
    type Connection = u8;
    type Error = &'static str;

    fn accept(&mut self) -> Result<Self::Connection, Self::Error> {
        self.events.pop_front().unwrap_or(Err("listener exhausted"))
    }
}

#[test]
fn nil_listener_is_a_clean_source_shaped_exit() {
    let mut loop_ = AcceptLoop::<FakeListener>::new(None);
    assert!(loop_.listener().is_none());
    assert!(!loop_.is_shutdown_requested());
    let result = loop_
        .run(|_| -> Result<(), &'static str> { panic!("handler must not run without a listener") });
    assert!(matches!(result, Ok(AcceptLoopExit::NoListener)));
}

#[test]
fn accepted_connections_are_owned_by_the_handler_until_shutdown() {
    let mut loop_ = AcceptLoop::new(Some(FakeListener::new([Ok(1), Ok(2), Ok(3)])));
    let shutdown = loop_.shutdown_handle();
    assert!(loop_.listener().is_some());
    assert!(!loop_.is_shutdown_requested());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_handler = Arc::clone(&seen);
    let result = loop_.run(move |connection| {
        seen_by_handler
            .lock()
            .expect("record connection")
            .push(connection);
        if seen_by_handler.lock().expect("inspect connections").len() == 3 {
            shutdown.shutdown();
        }
        Ok::<_, &'static str>(())
    });

    assert!(matches!(result, Ok(AcceptLoopExit::Shutdown)));
    assert!(loop_.is_shutdown_requested());
    assert_eq!(*seen.lock().expect("read connections"), vec![1, 2, 3]);
}

#[test]
fn unexpected_listener_error_is_propagated() {
    let mut loop_ = AcceptLoop::new(Some(FakeListener::new([Err("accept failed")])));
    let result = loop_.run(|_| Ok::<_, &'static str>(()));
    assert!(matches!(
        result,
        Err(AcceptLoopError::Listener("accept failed"))
    ));
}

#[test]
fn handler_error_is_propagated_without_accepting_another_connection() {
    let mut loop_ = AcceptLoop::new(Some(FakeListener::new([Ok(9), Ok(10)])));
    let result = loop_.run(|connection| {
        assert_eq!(connection, 9);
        Err::<(), _>("handler failed")
    });
    assert!(matches!(
        result,
        Err(AcceptLoopError::Handler("handler failed"))
    ));
}

#[test]
fn listener_error_after_shutdown_is_a_clean_exit() {
    let mut loop_ = AcceptLoop::new(Some(FakeListener::new([Err("closed")])));
    loop_.shutdown();
    let result = loop_.run(|_| Ok::<_, &'static str>(()));
    assert!(matches!(result, Ok(AcceptLoopExit::Shutdown)));
}

#[test]
fn tcp_listener_accepts_a_real_connection_and_handler_can_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP listener");
    let address = listener.local_addr().expect("listener address");
    let mut loop_ = AcceptLoop::new(Some(listener));
    let shutdown = loop_.shutdown_handle();
    let (accepted, receiver) = mpsc::channel();

    let worker = thread::spawn(move || {
        loop_.run(move |(stream, peer)| {
            accepted
                .send((peer, stream.peer_addr().expect("peer address")))
                .expect("report accepted connection");
            shutdown.shutdown();
            Ok::<_, io::Error>(())
        })
    });

    let client = TcpStream::connect(address).expect("connect TCP listener");
    let (accepted_peer, observed_peer) = receiver.recv().expect("accepted connection");
    assert_eq!(accepted_peer, observed_peer);
    drop(client);
    assert!(matches!(
        worker.join().expect("accept worker").expect("accept loop"),
        AcceptLoopExit::Shutdown
    ));
}
