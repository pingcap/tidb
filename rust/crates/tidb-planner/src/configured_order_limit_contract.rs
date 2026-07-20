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

//! Shared configured ORDER BY and LIMIT contract.
//!
//! Go's `LogicalSort`, `LogicalTopN`, and `ByItems` preserve an ordered list of
//! expressions and one descending bit per item. `LogicalLimit` and
//! `LogicalTopN` preserve independent unsigned offset and count values. This
//! dependency-closed contract carries the executable subset after a planner
//! has resolved each supported signed-BIGINT expression to its physical
//! FullSchema offset.
//!
//! SQL parsing, expression/type binding, aliases, ordinals, NULL/coercion and
//! collation semantics, partition TopN, property enumeration, pushdown, spill,
//! capacity policy, and row execution deliberately remain outside this module.

use std::{error::Error, fmt};

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

    /// Returns the source-shaped descending bit.
    #[must_use]
    pub const fn is_descending(self) -> bool {
        matches!(self, Self::Descending)
    }
}

/// One planner-resolved signed-BIGINT ORDER BY item.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConfiguredOrderKey {
    full_offset: usize,
    direction: ConfiguredOrderDirection,
}

impl ConfiguredOrderKey {
    /// Creates a key from a checked physical FullSchema offset and direction.
    #[must_use]
    pub const fn new(full_offset: usize, direction: ConfiguredOrderDirection) -> Self {
        Self {
            full_offset,
            direction,
        }
    }

    /// Returns the physical offset in Campaign 25's FullSchema row.
    #[must_use]
    pub const fn full_offset(&self) -> usize {
        self.full_offset
    }

    /// Returns this key's independent ordering direction.
    #[must_use]
    pub const fn direction(&self) -> ConfiguredOrderDirection {
        self.direction
    }
}

/// Failure to construct a checked LIMIT window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredLimitWindowError {
    /// `offset + count` cannot be represented by the executable index domain.
    EndOverflow {
        /// The requested number of rows to skip.
        offset: usize,
        /// The requested number of rows to emit after the skip.
        count: usize,
    },
}

impl fmt::Display for ConfiguredLimitWindowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndOverflow { offset, count } => {
                write!(
                    formatter,
                    "LIMIT offset {offset} plus count {count} overflows"
                )
            }
        }
    }
}

impl Error for ConfiguredLimitWindowError {}

/// A LIMIT/OFFSET window with its end computed exactly once.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConfiguredLimitWindow {
    offset: usize,
    count: usize,
    end_exclusive: usize,
}

impl ConfiguredLimitWindow {
    /// Constructs a window and rejects arithmetic overflow.
    pub const fn new(offset: usize, count: usize) -> Result<Self, ConfiguredLimitWindowError> {
        match offset.checked_add(count) {
            Some(end_exclusive) => Ok(Self {
                offset,
                count,
                end_exclusive,
            }),
            None => Err(ConfiguredLimitWindowError::EndOverflow { offset, count }),
        }
    }

    /// Returns the number of rows skipped before emission.
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the maximum number of emitted rows.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Returns the checked exclusive end (`offset + count`).
    #[must_use]
    pub const fn end_exclusive(&self) -> usize {
        self.end_exclusive
    }

    /// Returns whether the window can emit no rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Failure to construct an ORDER BY LIMIT specification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredOrderLimitSpecError {
    /// LIMIT-only is a separate plan shape and cannot masquerade as TopN.
    EmptyOrderKeys,
}

impl fmt::Display for ConfiguredOrderLimitSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyOrderKeys => formatter.write_str("ORDER BY LIMIT requires an order key"),
        }
    }
}

impl Error for ConfiguredOrderLimitSpecError {}

/// Immutable planner-to-executor contract for the bounded TopN shape.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConfiguredOrderLimitSpec {
    order_keys: Vec<ConfiguredOrderKey>,
    limit: ConfiguredLimitWindow,
}

impl ConfiguredOrderLimitSpec {
    /// Creates a TopN specification while keeping LIMIT-only structurally
    /// separate from ordered execution.
    pub fn new(
        order_keys: Vec<ConfiguredOrderKey>,
        limit: ConfiguredLimitWindow,
    ) -> Result<Self, ConfiguredOrderLimitSpecError> {
        if order_keys.is_empty() {
            return Err(ConfiguredOrderLimitSpecError::EmptyOrderKeys);
        }
        Ok(Self { order_keys, limit })
    }

    /// Returns source-order keys. Duplicate keys remain valid, as in Go.
    #[must_use]
    pub fn order_keys(&self) -> &[ConfiguredOrderKey] {
        &self.order_keys
    }

    /// Returns the checked LIMIT window.
    #[must_use]
    pub const fn limit(&self) -> ConfiguredLimitWindow {
        self.limit
    }
}
