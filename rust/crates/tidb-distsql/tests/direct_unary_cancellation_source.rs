// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#![allow(missing_docs)]

use std::sync::Arc;

use tidb_distsql::{ExecutionState, KvRequestMetadata, TransportBinding, TransportRequest};

#[test]
fn detached_execution_fans_out_to_the_bound_request_authority() {
    let execution = ExecutionState::new();
    let detached = execution.detach();
    let request = TransportRequest::new(KvRequestMetadata::default(), Arc::clone(&detached.cancel));
    let bound = request.bind(TransportBinding::new()).unwrap();

    assert!(Arc::ptr_eq(
        request.execution_cancellation(),
        &execution.cancel
    ));
    assert!(Arc::ptr_eq(
        bound.execution_cancellation(),
        &execution.cancel
    ));
    assert!(!Arc::ptr_eq(
        bound.request_cancellation().unwrap(),
        &execution.cancel
    ));
    assert!(!bound.request_cancellation().unwrap().is_cancelled());
    execution.cancel.cancel();
    assert!(detached.cancel.is_cancelled());
    assert!(bound.request_cancellation().unwrap().is_cancelled());
}

#[test]
fn already_cancelled_execution_stays_cancelled_across_request_binding() {
    let execution = ExecutionState::new();
    execution.cancel.cancel();
    let request =
        TransportRequest::new(KvRequestMetadata::default(), Arc::clone(&execution.cancel));

    assert!(request
        .bind(TransportBinding::new())
        .unwrap()
        .request_cancellation()
        .unwrap()
        .is_cancelled());
}

#[test]
fn closing_one_bound_request_does_not_cancel_its_execution_or_sibling() {
    let execution = ExecutionState::new();
    let request =
        TransportRequest::new(KvRequestMetadata::default(), Arc::clone(&execution.cancel));
    let first = request.bind(TransportBinding::new()).unwrap();
    let second = request.bind(TransportBinding::new()).unwrap();

    first.request_cancellation().unwrap().cancel();

    assert!(first.request_cancellation().unwrap().is_cancelled());
    assert!(!second.request_cancellation().unwrap().is_cancelled());
    assert!(!execution.cancel.is_cancelled());
}

#[test]
fn concurrent_execution_cancel_cannot_miss_a_registering_request() {
    for _ in 0..64 {
        let execution = Arc::new(tidb_distsql::CancelHandle::default());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let child_execution = Arc::clone(&execution);
        let child_barrier = Arc::clone(&barrier);
        let child = std::thread::spawn(move || {
            child_barrier.wait();
            child_execution.request_child()
        });

        barrier.wait();
        execution.cancel();
        assert!(child.join().unwrap().is_cancelled());
    }
}
