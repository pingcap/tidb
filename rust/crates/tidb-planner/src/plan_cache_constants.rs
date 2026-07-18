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

//! Plan-cache constant cloning from
//! `pkg/planner/util/utilfuncp/func_pointer_misc.go`.
//!
//! The Go helper keeps safe constants shared, clones session-unsafe constants,
//! preserves nil entries, and reuses an optional destination slice. This leaf
//! preserves that ownership policy over `Arc` constants while leaving TiDB's
//! expression Datum/FieldType evaluation context external.

use std::sync::Arc;

/// Opaque constant payload with source plan-cache sharing metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanCacheConstant {
    value: Vec<u8>,
    safe_to_share: bool,
}

impl PlanCacheConstant {
    /// Creates a constant from opaque payload bytes and sharing metadata.
    #[must_use]
    pub fn new(value: impl Into<Vec<u8>>, safe_to_share: bool) -> Self {
        Self {
            value: value.into(),
            safe_to_share,
        }
    }

    /// Returns the opaque payload.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns whether this constant may be shared across sessions.
    #[must_use]
    pub const fn safe_to_share(&self) -> bool {
        self.safe_to_share
    }
}

/// Shared constant handle used by the plan-cache adapter.
pub type ConstantRef = Arc<PlanCacheConstant>;

/// Clones constants according to source plan-cache sharing semantics.
///
/// `None` input remains `None`; when every non-nil entry is safe, the original
/// handles are returned. Otherwise safe handles are retained, unsafe handles
/// are deep-cloned, and nil entries remain nil. An optional destination vector
/// is cleared and reused in the cloning path.
#[must_use]
pub fn clone_constants_for_plan_cache(
    constants: Option<&[Option<ConstantRef>]>,
    cloned: Option<Vec<Option<ConstantRef>>>,
) -> Option<Vec<Option<ConstantRef>>> {
    let constants = constants?;
    if constants
        .iter()
        .flatten()
        .all(|constant| constant.safe_to_share())
    {
        return Some(constants.to_vec());
    }

    let mut output = cloned.unwrap_or_else(|| Vec::with_capacity(constants.len()));
    output.clear();
    output.extend(constants.iter().map(|constant| {
        constant.as_ref().map(|constant| {
            if constant.safe_to_share() {
                Arc::clone(constant)
            } else {
                Arc::new((**constant).clone())
            }
        })
    }));
    Some(output)
}
