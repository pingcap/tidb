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

//! Ristretto ring.go: reusable, lossy read stripes. A stripe is exclusively
//! borrowed from the pool, so readers never hold its lock while publishing.
use std::sync::Mutex;

pub(crate) struct Ring {
    pool: Mutex<Vec<Vec<u64>>>,
    capacity: usize,
}
impl Ring {
    pub fn new(capacity: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            capacity,
        }
    }
    pub fn push(&self, key: u64, consumer: impl FnOnce(Vec<u64>) -> Result<(), Vec<u64>>) {
        let mut stripe = self
            .pool
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.capacity));
        stripe.push(key);
        if stripe.len() >= self.capacity {
            stripe = match consumer(stripe) {
                Ok(()) => Vec::with_capacity(self.capacity),
                Err(mut stripe) => {
                    stripe.clear();
                    stripe
                }
            };
        }
        self.pool.lock().unwrap().push(stripe);
    }
}
