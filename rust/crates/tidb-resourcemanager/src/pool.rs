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

//! Shared pool state from `pkg/resourcemanager/pool`.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;

/// Errors returned by resource-manager pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolError {
    /// A task was submitted to a closed pool.
    Closed,
    /// Pool concurrency is exhausted and blocking is disabled.
    Overload,
    /// Pool construction parameters are invalid.
    ParamsInvalid,
}

impl fmt::Display for PoolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Closed => "this pool has been closed",
            Self::Overload => {
                "the number of concurrency has reached the upper limit and Block is set"
            }
            Self::ParamsInvalid => "the pool params are invalid",
        })
    }
}

impl std::error::Error for PoolError {}

/// State shared by concrete resource-manager pools.
pub struct BasePool {
    last_tune_ts: Mutex<SystemTime>,
    name: String,
    generator: AtomicU64,
}

impl BasePool {
    /// Creates base pool state.
    pub fn new() -> Self {
        Self {
            last_tune_ts: Mutex::new(SystemTime::now()),
            name: String::new(),
            generator: AtomicU64::new(0),
        }
    }

    /// Sets the pool name.
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    /// Returns the pool name.
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Generates the next one-based task identifier.
    pub fn generate_task_id(&self) -> u64 {
        self.generator
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
    }

    /// Returns the last pool tuning time.
    pub fn last_tuner_ts(&self) -> SystemTime {
        *self
            .last_tune_ts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Sets the last pool tuning time.
    pub fn set_last_tune_ts(&self, time: SystemTime) {
        *self
            .last_tune_ts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = time;
    }
}
