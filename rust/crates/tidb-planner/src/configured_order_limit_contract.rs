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

//! Sort direction used by configured prepared-read lowering.
//!
//! Go's `ByItems.Desc` bit maps directly to this enum. Expression binding and
//! execution remain with the planner and executor owners.

/// Sort direction corresponding to the source `ByItems.Desc` bit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConfiguredOrderDirection {
    /// Ascending order (`Desc == false`).
    Ascending,
    /// Descending order (`Desc == true`).
    Descending,
}

impl ConfiguredOrderDirection {
    /// Converts the source descending bit without changing its meaning.
    #[must_use]
    pub const fn from_descending(descending: bool) -> Self {
        if descending {
            Self::Descending
        } else {
            Self::Ascending
        }
    }
}
