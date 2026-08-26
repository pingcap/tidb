// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use tikv_client::async_util::{Callback, Executor, Pool, RunLoop, State, Task};

#[derive(Default)]
struct ImmediateExecutor;

impl Pool for ImmediateExecutor {
    fn spawn(&self, task: Task) {
        task();
    }
}

impl Executor for ImmediateExecutor {
    fn append(&self, tasks: Vec<Task>) {
        for task in tasks {
            task();
        }
    }
}

#[test]
fn downstream_crate_can_use_the_public_async_package() {
    let executor = Arc::new(ImmediateExecutor);
    let output = Arc::new(Mutex::new(Vec::new()));
    let captured = output.clone();
    let callback =
        Callback::<Vec<i32>, String>::new(Some(executor.clone()), move |values, error| {
            assert!(error.is_none());
            *captured.lock().unwrap() = values;
        });
    callback.inject(|mut values, error| {
        values.push(2);
        (values, error)
    });
    callback.inject(|mut values, error| {
        values.push(1);
        (values, error)
    });
    callback.schedule(Vec::new(), None);
    assert_eq!(*output.lock().unwrap(), [1, 2]);

    executor.spawn(Box::new(|| {}));
    let run_loop = RunLoop::new();
    assert_eq!(run_loop.state(), State::Idle);
    assert_eq!(run_loop.num_runnable(), 0);
}
