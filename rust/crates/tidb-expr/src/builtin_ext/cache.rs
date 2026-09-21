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

//! Go `pkg/expression/builtinFuncCache[T]`.
//!
//! The cache has one item for one statement context. A failed constructor is
//! deliberately not retained, and a new context replaces the old item. The
//! read lock keeps the ordinary per-row hit path cheap; the write lock is the
//! once-only construction path used by concurrent evaluators.

use std::sync::{Arc, RwLock};

#[derive(Debug)]
struct CacheItem<T> {
    context_id: u64,
    value: Arc<T>,
}

/// A single context-keyed lazy value, matching Go's `builtinFuncCache[T]`.
#[derive(Debug)]
pub(crate) struct BuiltinFuncCache<T> {
    cached: RwLock<Option<CacheItem<T>>>,
}

impl<T> Default for BuiltinFuncCache<T> {
    fn default() -> Self {
        Self {
            cached: RwLock::new(None),
        }
    }
}

impl<T> Clone for BuiltinFuncCache<T> {
    /// Go builtin signatures start with an empty cache after `Clone`.
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T> BuiltinFuncCache<T> {
    /// Returns the value for `context_id` without initializing the cache.
    pub(crate) fn get_cache(&self, context_id: u64) -> Option<Arc<T>> {
        self.cached
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|item| item.context_id == context_id)
            .map(|item| Arc::clone(&item.value))
    }

    /// Returns the existing value or constructs it once for this context.
    /// Errors are not cached, exactly as Go's constructor path behaves.
    pub(crate) fn get_or_init_cache<E>(
        &self,
        context_id: u64,
        construct: impl FnOnce() -> Result<T, E>,
    ) -> Result<Arc<T>, E> {
        if let Some(value) = self.get_cache(context_id) {
            return Ok(value);
        }

        let mut cached = self
            .cached
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(item) = cached.as_ref().filter(|item| item.context_id == context_id) {
            return Ok(Arc::clone(&item.value));
        }
        let value = Arc::new(construct()?);
        *cached = Some(CacheItem { context_id, value });
        Ok(Arc::clone(
            &cached.as_ref().expect("cache item installed").value,
        ))
    }
}
