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

#[path = "../src/listener.rs"]
mod listener;

use std::net::TcpStream;

use listener::{ListenerConfig, ListenerError, ListenerLifecycle, ListenerState};

#[test]
fn no_endpoint_is_an_explicit_bind_error() {
    let mut lifecycle = ListenerLifecycle::new(ListenerConfig::no_endpoint());
    assert_eq!(lifecycle.state(), ListenerState::New);
    assert!(matches!(lifecycle.bind(), Err(ListenerError::NoEndpoint)));
    assert_eq!(lifecycle.state(), ListenerState::New);
    assert!(!lifecycle.is_healthy());
}

#[test]
fn bind_is_idempotent_and_reports_the_ephemeral_address() {
    let mut lifecycle = ListenerLifecycle::new(ListenerConfig::tcp4("127.0.0.1", 0));
    let address = lifecycle.bind().expect("bind localhost listener");
    assert_ne!(address.port(), 0);
    assert_eq!(lifecycle.bound_address(), Some(address));
    assert_eq!(lifecycle.state(), ListenerState::Bound);
    assert!(lifecycle.tcp_listener().is_some());

    let second_address = lifecycle.bind().expect("idempotent bind");
    assert_eq!(second_address, address);

    // The socket is real, not an in-memory placeholder.  No accept loop is
    // owned by this leaf, so the connection is deliberately left unopened.
    TcpStream::connect(address).expect("connect to bound listener");
}

#[test]
fn activation_and_shutdown_publish_health_in_the_source_order() {
    let mut lifecycle = ListenerLifecycle::new(ListenerConfig::tcp4("127.0.0.1", 0));
    lifecycle.bind().expect("bind localhost listener");
    assert!(!lifecycle.is_healthy());

    lifecycle.activate().expect("activate listener");
    lifecycle.activate().expect("idempotent activation");
    assert_eq!(lifecycle.state(), ListenerState::Active);
    assert!(lifecycle.is_healthy());
    assert!(!lifecycle.is_in_shutdown());

    lifecycle.begin_shutdown();
    assert_eq!(lifecycle.state(), ListenerState::ShuttingDown);
    assert!(!lifecycle.is_healthy());
    assert!(lifecycle.is_in_shutdown());

    lifecycle.close();
    lifecycle.close();
    assert_eq!(lifecycle.state(), ListenerState::Closed);
    assert!(lifecycle.bound_address().is_none());
    assert!(lifecycle.tcp_listener().is_none());
    assert!(matches!(
        lifecycle.bind(),
        Err(ListenerError::InvalidTransition {
            operation: "bind",
            state: ListenerState::Closed,
        })
    ));
}

#[test]
fn shutdown_flags_are_independent_and_sticky() {
    let mut lifecycle = ListenerLifecycle::new(ListenerConfig::no_endpoint());
    assert!(!lifecycle.force_shutdown());
    assert!(!lifecycle.need_request_manager_free());

    lifecycle.set_force_shutdown();
    lifecycle.set_need_request_manager_free();
    lifecycle.set_force_shutdown();
    assert!(lifecycle.force_shutdown());
    assert!(lifecycle.need_request_manager_free());
}

#[test]
fn invalid_transitions_do_not_bind_or_report_healthy() {
    let config = ListenerConfig::tcp("127.0.0.1", 0);
    let mut lifecycle = ListenerLifecycle::new(config.clone());
    assert_eq!(lifecycle.config(), &config);
    assert!(matches!(
        lifecycle.activate(),
        Err(ListenerError::InvalidTransition {
            operation: "activate",
            state: ListenerState::New,
        })
    ));
    assert_eq!(lifecycle.state(), ListenerState::New);
    assert!(!lifecycle.is_healthy());

    lifecycle.close();
    assert!(matches!(
        lifecycle.activate(),
        Err(ListenerError::InvalidTransition {
            operation: "activate",
            state: ListenerState::Closed,
        })
    ));
}
