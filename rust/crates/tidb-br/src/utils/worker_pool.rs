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

//! Go `pkg/util.WorkerPool` with `golang.org/x/sync/errgroup`: at most
//! `limit` tasks run at once; the first error cancels the group's context.

use std::sync::{Arc, Mutex};

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_gocontext::{CancelFunc, Context};

use crate::errors::{Error, Result};

/// Go `util.WorkerPool`.
#[derive(Clone)]
pub struct WorkerPool {
    name: String,
    give: Sender<()>,
    take: Receiver<()>,
}

impl WorkerPool {
    /// Go `NewWorkerPool(limit, name)`.
    #[must_use]
    pub fn new(limit: usize, name: &str) -> Self {
        let limit = limit.max(1);
        let (give, take) = bounded(limit);
        for _ in 0..limit {
            give.send(())
                .expect("the pool's channel has room for its workers");
        }
        Self {
            name: name.to_owned(),
            give,
            take,
        }
    }

    /// The pool's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Go `ApplyOnErrorGroup`: waits for a free worker, then runs `f` in the
    /// group.
    pub fn apply_on_error_group(
        &self,
        eg: &ErrGroup,
        f: impl FnOnce() -> Result<()> + Send + 'static,
    ) {
        let _ = self.take.recv();
        let give = self.give.clone();
        eg.go(move || {
            let res = f();
            let _ = give.send(());
            res
        });
    }
}

/// Go `errgroup.WithContext`.
pub struct ErrGroup {
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    first: Arc<Mutex<Option<Error>>>,
    cancel: CancelFunc,
}

impl ErrGroup {
    /// Go `errgroup.WithContext(ctx)`: the group and its context.
    #[must_use]
    pub fn with_context(ctx: &Context) -> (Self, Context) {
        let (ectx, cancel) = Context::with_cancel(ctx);
        (
            Self {
                handles: Mutex::default(),
                first: Arc::default(),
                cancel,
            },
            ectx,
        )
    }

    /// Go `Go`.
    pub fn go(&self, f: impl FnOnce() -> Result<()> + Send + 'static) {
        let first = Arc::clone(&self.first);
        let cancel = self.cancel.clone();
        let h = std::thread::spawn(move || {
            if let Err(err) = f() {
                let mut first = first.lock().unwrap();
                if first.is_none() {
                    *first = Some(err);
                    cancel.cancel();
                }
            }
        });
        self.handles.lock().unwrap().push(h);
    }

    /// Go `Wait`: the first error.
    ///
    /// # Errors
    /// The first task's error.
    pub fn wait(&self) -> Result<()> {
        loop {
            let hs: Vec<_> = std::mem::take(&mut *self.handles.lock().unwrap());
            if hs.is_empty() {
                break;
            }
            for h in hs {
                let _ = h.join();
            }
        }
        self.cancel.cancel();
        self.first.lock().unwrap().clone().map_or(Ok(()), Err)
    }
}
