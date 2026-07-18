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

//! Cascades hash/equality contracts from `pkg/planner/cascades/base/base.go`.
//!
//! Go's interfaces are open to every planner object. Rust keeps the same
//! dynamic boundary with a `dyn Hasher` and `dyn Any`; concrete logical and
//! memo objects remain owners of their own implementations.

use std::any::Any;

use crate::hash_equaler::Hasher;

/// Hashes an object into the shared cascades primitive hasher.
pub trait Hash64 {
    /// Appends this object's fields to `hasher` in source order.
    fn hash64(&self, hasher: &mut dyn Hasher);
}

/// Compares an object with another dynamically typed planner value.
pub trait Equals {
    /// Returns whether `other` has the same concrete type and value.
    fn equals(&self, other: &dyn Any) -> bool;
}

/// Combined hash/equality contract used by memo objects.
pub trait HashEquals: Hash64 + Equals {}

impl<T> HashEquals for T where T: Hash64 + Equals {}
