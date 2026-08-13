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

use super::auto_id::{increment_and_offset, AutoIdError};
use super::{KvTable, TableAutoId};
use tidb_datatype::Datum;

/// The persisted bit layout of one `AUTO_RANDOM` handle column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoRandomSpec {
    /// The column whose value is generated.
    pub offset: usize,
    /// Bits occupied by the random shard.
    pub shard_bits: u64,
    /// Bits of the integer domain TiDB may use, including the sign bit.
    pub range_bits: u64,
    /// Whether the handle column is unsigned.
    pub unsigned: bool,
}

impl AutoRandomSpec {
    /// Bits occupied by the monotonically increasing portion.
    #[must_use]
    pub const fn incremental_bits(self) -> u64 {
        self.range_bits - self.shard_bits - (!self.unsigned as u64)
    }

    /// The mask that extracts the increasing portion from a complete ID.
    #[must_use]
    pub const fn incremental_mask(self) -> u64 {
        (1_u64 << self.incremental_bits()) - 1
    }

    fn compose(self, shard: u64, id: u64) -> u64 {
        let shard_mask = (1_u64 << self.shard_bits) - 1;
        ((shard & shard_mask) << self.incremental_bits()) | id
    }
}

/// Where an `AUTO_RANDOM` value came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutoRandom {
    /// The table has no `AUTO_RANDOM` column.
    Absent,
    /// The row supplied a non-zero value.
    Given(u64),
    /// A retry reused the value from the previous attempt.
    Reused(u64),
    /// The value was freshly allocated.
    Allocated(u64),
}

impl AutoRandom {
    /// The value written into the row, if this table has an auto-random column.
    #[must_use]
    pub const fn placed(self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Given(id) | Self::Reused(id) | Self::Allocated(id) => Some(id),
        }
    }
}

/// An `AUTO_RANDOM` value could not be produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoRandomError {
    /// A non-zero explicit value was supplied while the session gate was off.
    ExplicitInsertDisabled,
    /// The increasing ID source failed or exhausted its assigned bit range.
    AutoId(AutoIdError),
}

impl KvTable {
    /// Installs the table's persisted `AUTO_RANDOM` layout.
    pub fn set_auto_random(&mut self, spec: AutoRandomSpec) {
        self.auto_random = Some(spec);
        self.auto_random_id.set_unsigned(spec.unsigned);
    }

    /// Installs the distinct random-ID allocator owned by the storage tier.
    pub fn set_auto_random_id(&mut self, shared: TableAutoId) {
        self.auto_random_id = shared.0;
        if let Some(spec) = self.auto_random {
            self.auto_random_id.set_unsigned(spec.unsigned);
        }
    }

    /// The table's auto-random layout, if one is configured.
    #[must_use]
    pub const fn auto_random(&self) -> Option<AutoRandomSpec> {
        self.auto_random
    }

    /// Applies Go's explicit-value, rebase, retry, allocation, and composition
    /// rules to the auto-random column in `row`.
    pub fn apply_auto_random(
        &mut self,
        row: &mut [Datum],
        step: (u64, u64),
        explicit_allowed: bool,
        reuse: impl FnOnce() -> Option<u64>,
        shard: u64,
    ) -> Result<AutoRandom, AutoRandomError> {
        let Some(spec) = self.auto_random else {
            return Ok(AutoRandom::Absent);
        };
        // Go consumes a retry ID before it inspects this attempt's datum. A
        // replay must write the losing attempt's complete, already-composed
        // ID even when the expression would now produce something else.
        if let Some(id) = reuse() {
            row[spec.offset] = if spec.unsigned {
                Datum::UInt(id)
            } else {
                Datum::Int(id as i64)
            };
            return Ok(AutoRandom::Reused(id));
        }
        let current = match row.get(spec.offset) {
            Some(Datum::Int(value)) => *value as u64,
            Some(Datum::UInt(value)) => *value,
            _ => 0,
        };
        if current != 0 {
            if !explicit_allowed {
                return Err(AutoRandomError::ExplicitInsertDisabled);
            }
            let negative = matches!(row.get(spec.offset), Some(Datum::Int(value)) if *value < 0);
            if !negative {
                self.auto_random_id
                    .rebase(current & spec.incremental_mask())
                    .map_err(|error| AutoRandomError::AutoId(AutoIdError::Store(error)))?;
            }
            return Ok(AutoRandom::Given(current));
        }

        let (increment, offset) = increment_and_offset(step.0, step.1);
        let incremental = self
            .auto_random_id
            .alloc(increment, offset)
            .map_err(AutoRandomError::AutoId)?;
        if incremental > spec.incremental_mask() {
            return Err(AutoRandomError::AutoId(AutoIdError::Exhausted));
        }
        let id = spec.compose(shard, incremental);
        row[spec.offset] = if spec.unsigned {
            Datum::UInt(id)
        } else {
            Datum::Int(id as i64)
        };
        Ok(AutoRandom::Allocated(id))
    }

    /// Rebases the distinct increasing counter after an UPDATE writes the
    /// auto-random column explicitly.
    pub(in crate::kv_table) fn rebase_auto_random_from_row(
        &mut self,
        row: &[Datum],
    ) -> Result<(), super::KvTableError> {
        let Some(spec) = self.auto_random else {
            return Ok(());
        };
        let value = match row.get(spec.offset) {
            Some(Datum::Int(value)) if *value >= 0 => *value as u64,
            Some(Datum::UInt(value)) => *value,
            _ => return Ok(()),
        };
        self.auto_random_id
            .rebase(value & spec.incremental_mask())
            .map_err(|error| super::KvTableError::Storage(error.0))
    }
}
