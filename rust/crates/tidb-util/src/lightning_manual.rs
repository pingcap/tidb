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

//! Native allocation from Go `pkg/lightning/manual`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::membuf::Block;

/// A safe maximum slice length for the cgo-backed implementation.
pub const MAX_ARRAY_LEN: usize = (1 << 31) - 1;

/// Allocates a zeroed byte buffer of size `n`.
pub fn new(n: isize) -> Block {
    let length = usize::try_from(n).expect("negative allocation size");
    assert!(
        length <= MAX_ARRAY_LEN,
        "allocation size exceeds MaxArrayLen"
    );
    if length == 0 {
        return Block::from_vec(Vec::new());
    }
    Block::manually_managed(vec![0; length])
}

/// Frees a byte buffer allocated by [`new`].
pub fn free(bytes: Block) {
    bytes.release();
}

/// A manual allocator with an optional shared live-allocation counter.
#[derive(Clone, Default)]
pub struct Allocator {
    /// Counts allocations that have not been explicitly freed.
    pub ref_count: Option<Arc<AtomicI64>>,
}

impl Allocator {
    /// Allocates a zeroed byte buffer and increments the optional counter.
    pub fn alloc(&self, n: isize) -> Block {
        if let Some(ref_count) = &self.ref_count {
            ref_count.fetch_add(1, Ordering::SeqCst);
        }
        new(n)
    }

    /// Frees a byte buffer and decrements the optional counter.
    pub fn free(&self, bytes: Block) {
        if let Some(ref_count) = &self.ref_count {
            ref_count.fetch_sub(1, Ordering::SeqCst);
        }
        free(bytes);
    }

    /// Reports a nonzero live-allocation count.
    pub fn check_ref_count(&self) -> Result<(), String> {
        let Some(ref_count) = &self.ref_count else {
            return Ok(());
        };
        if ref_count.load(Ordering::SeqCst) != 0 {
            return Err(format!(
                "memory leak detected, refCnt: {}",
                ref_count.load(Ordering::SeqCst)
            ));
        }
        Ok(())
    }
}

impl crate::membuf::Allocator for Allocator {
    fn alloc(&self, n: isize) -> Block {
        Self::alloc(self, n)
    }

    fn free(&self, block: Block) {
        Self::free(self, block);
    }
}
