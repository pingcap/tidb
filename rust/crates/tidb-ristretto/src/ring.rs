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

//! Ristretto ring.go: reusable, lossy read stripes. A stripe is detached
//! before publishing, so the consumer runs outside the stripe lock.
use std::sync::Mutex;

use tidb_zeropool::ThreadShards;

pub(crate) struct Ring {
    stripes: ThreadShards<Mutex<Vec<u64>>>,
    capacity: usize,
}
impl Ring {
    pub fn new(capacity: usize) -> Self {
        Self {
            stripes: ThreadShards::new(Mutex::default),
            capacity,
        }
    }
    pub fn push(&self, key: u64, consumer: impl FnOnce(Vec<u64>) -> Result<(), Vec<u64>>) {
        let local = self.stripes.local();
        let mut stripe = local.lock().unwrap();
        if stripe.capacity() == 0 {
            stripe.reserve_exact(self.capacity);
        }
        stripe.push(key);
        if stripe.len() < self.capacity {
            return;
        }
        let batch = std::mem::take(&mut *stripe);
        drop(stripe);
        if let Err(mut batch) = consumer(batch) {
            batch.clear();
            let mut stripe = local.lock().unwrap();
            if stripe.capacity() == 0 {
                *stripe = batch;
            }
        }
    }
}
