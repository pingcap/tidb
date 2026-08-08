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

//! Source-named pointer clone operations for [`FieldType`].

use super::FieldType;
use crate::go_runtime::GoSharedSlice;

impl FieldType {
    /// Nil-safe Go `CleanElemIsBinaryLit` pointer boundary.
    pub fn clean_elem_binary_literals_pointer(field_type: Option<&mut Self>) {
        if let Some(field_type) = field_type {
            field_type.clean_elem_binary_literals();
        }
    }

    /// Go `Clone`: a nil receiver panics; the result is a fresh pointer whose
    /// two slice headers share their original backing arrays.
    #[must_use]
    pub fn clone_pointer(source: Option<&Self>) -> Box<Self> {
        Box::new(source.expect("FieldType.Clone on nil receiver").clone())
    }

    /// Non-nil body of Go `DeepCopy`.
    #[must_use]
    pub fn deep_copy_like_go(&self) -> Self {
        let mut copied = self.clone();
        copied.elems = if self.elems.is_empty() {
            GoSharedSlice::default()
        } else {
            GoSharedSlice::from_vec_with_capacity(self.elems.snapshot(), self.elems.len())
        };
        copied.elems_is_binary_literal = if self.elems_is_binary_literal.is_empty() {
            GoSharedSlice::default()
        } else {
            GoSharedSlice::from_vec_with_capacity(
                self.elems_is_binary_literal.snapshot(),
                self.elems_is_binary_literal.len(),
            )
        };
        copied
    }

    /// Go `DeepCopy`: a nil receiver returns nil.
    #[must_use]
    pub fn deep_copy_pointer(source: Option<&Self>) -> Option<Box<Self>> {
        source.map(|source| Box::new(source.deep_copy_like_go()))
    }

    /// Nil-safe Go `MemoryUsage` pointer boundary.
    #[must_use]
    pub fn memory_usage_pointer(source: Option<&Self>) -> usize {
        source.map_or(0, Self::memory_usage)
    }
}
