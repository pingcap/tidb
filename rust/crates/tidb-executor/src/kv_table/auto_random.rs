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
    /// A rebase was requested for a table without AUTO_RANDOM.
    NotApplicable,
    /// The requested base does not fit the increasing portion.
    RebaseOverflow {
        /// The requested next base.
        base: i64,
        /// The greatest base the layout accepts.
        maximum: u64,
    },
    /// A schema change violates TiDB's AUTO_RANDOM definition rules.
    InvalidDefinition(String),
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

    /// The next increasing value this allocator will compose.
    #[must_use]
    pub fn next_auto_random(&self) -> Option<u64> {
        self.auto_random.map(|_| self.auto_random_id.next())
    }

    /// Go `ALTER TABLE ... AUTO_RANDOM_BASE=n`: raise the next increasing
    /// value, leaving a higher counter unchanged.
    pub fn rebase_auto_random(&mut self, next: i64) -> Result<(), AutoRandomError> {
        let next = self.checked_auto_random_base(next)?;
        self.auto_random_id
            .rebase_to_next(next)
            .map_err(|error| AutoRandomError::AutoId(AutoIdError::Store(error)))
    }

    /// Go `FORCE AUTO_RANDOM_BASE=n`: replace the next increasing value even
    /// when that moves the counter backwards.
    pub fn force_rebase_auto_random(&mut self, next: i64) -> Result<(), AutoRandomError> {
        let next = self.checked_auto_random_base(next)?;
        self.auto_random_id
            .force_rebase_to_next(next)
            .map_err(AutoRandomError::AutoId)
    }

    fn checked_auto_random_base(&self, next: i64) -> Result<u64, AutoRandomError> {
        let Some(spec) = self.auto_random else {
            return Err(AutoRandomError::NotApplicable);
        };
        let pattern = next as u64;
        if next < 0 || pattern & spec.incremental_mask() != pattern {
            return Err(AutoRandomError::RebaseOverflow {
                base: next,
                maximum: spec.incremental_mask(),
            });
        }
        Ok(pattern)
    }

    /// Applies Go's MODIFY COLUMN AUTO_RANDOM transition after the ordinary
    /// column checks have passed. The shared counter is deliberately advanced
    /// before the bit-capacity check, matching `checkNewAutoRandomBits`.
    pub(crate) fn alter_auto_random_spec(
        &mut self,
        next: Option<AutoRandomSpec>,
        column_name: &str,
    ) -> Result<(), AutoRandomError> {
        let previous = self.auto_random;
        let Some(next) = next else {
            return if previous.is_some() {
                Err(AutoRandomError::InvalidDefinition(
                    "adding/dropping/modifying auto_random is not supported".to_owned(),
                ))
            } else {
                Ok(())
            };
        };

        let converting = previous.is_none();
        if converting
            && (self.auto_increment_offset != Some(next.offset)
                || self.pk_handle_offset != Some(next.offset))
        {
            return Err(AutoRandomError::InvalidDefinition(
                "auto_random can only be converted from auto_increment clustered primary key"
                    .to_owned(),
            ));
        }
        if let Some(previous) = previous {
            if next.shard_bits < previous.shard_bits {
                return Err(AutoRandomError::InvalidDefinition(
                    "decreasing auto_random shard bits is not supported".to_owned(),
                ));
            }
            if next.range_bits != previous.range_bits {
                return Err(AutoRandomError::InvalidDefinition(
                    "alter the range bits of auto_random column is not supported".to_owned(),
                ));
            }
        }

        let current = if converting {
            self.auto_id.advance_global_one()
        } else {
            self.auto_random_id.advance_global_one()
        }
        .map_err(AutoRandomError::AutoId)?;
        let used_bits = u64::from(u64::BITS - current.leading_zeros());
        if used_bits > next.incremental_bits() {
            let overlap = used_bits - next.incremental_bits();
            let maximum = next.shard_bits.wrapping_sub(overlap);
            return Err(AutoRandomError::InvalidDefinition(format!(
                "max allowed auto_random shard bits is {maximum}, but got {} on column `{column_name}`",
                next.shard_bits
            )));
        }

        if converting {
            self.auto_random_id
                .rebase(current)
                .map_err(|error| AutoRandomError::AutoId(AutoIdError::Store(error)))?;
            self.clear_auto_increment_offset();
        }
        self.set_auto_random(next);
        Ok(())
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
