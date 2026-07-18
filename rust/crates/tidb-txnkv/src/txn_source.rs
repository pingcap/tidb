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

//! Transaction-source bitfields translated from `pkg/kv/option.go`.
//!
//! This module preserves the raw `u64` boundary and the source's OR-only
//! mutation semantics. In particular, CDC values are checked against the bit
//! count (`8`), not the byte mask (`255`), despite the source error describing
//! `[1, 15]`. Lossy-DDL presence also checks every bit above the CDC byte,
//! rather than masking only its own byte. Both behaviors are observable Go
//! contracts and are intentionally retained.

use std::fmt;

const CDC_WRITE_SOURCE_BITS: u64 = 8;
const CDC_WRITE_SOURCE_MAX: u64 = (1 << CDC_WRITE_SOURCE_BITS) - 1;

const LOSSY_DDL_REORG_SOURCE_BITS: u64 = 8;
const LOSSY_DDL_REORG_SOURCE_MAX: u64 = (1 << LOSSY_DDL_REORG_SOURCE_BITS) - 1;
const LOSSY_DDL_REORG_SOURCE_SHIFT: u64 = CDC_WRITE_SOURCE_BITS;

/// Source identity for a lossy column-reorg backfill job.
pub const LOSSY_DDL_COLUMN_REORG_SOURCE: u64 = 1;

/// Transaction-source bit used by Lightning physical import.
pub const LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE: u64 = 1 << 16;

/// A source-defined transaction-source range error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TxnSourceError {
    /// The CDC source exceeds the source's accepted maximum of eight.
    CdcWriteSourceOutOfRange(u64),
    /// The lossy-DDL source exceeds its eight-bit mask.
    LossyDdlReorgSourceOutOfRange(u64),
}

impl fmt::Display for TxnSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CdcWriteSourceOutOfRange(value) => write!(
                formatter,
                "value {value} is out of TiCDC write source range, should be in [1, 15]"
            ),
            Self::LossyDdlReorgSourceOutOfRange(value) => write!(
                formatter,
                "value {value} is out of lossy DDL reorg source range, should be in [1, {LOSSY_DDL_REORG_SOURCE_MAX}]"
            ),
        }
    }
}

impl std::error::Error for TxnSourceError {}

/// ORs one accepted TiCDC write source into a raw transaction source.
///
/// The exact Go source rejects values greater than `cdcWriteSourceBits` (`8`),
/// not values greater than the mask or the range printed in the error.
pub fn set_cdc_write_source(txn_source: &mut u64, value: u64) -> Result<(), TxnSourceError> {
    if value > CDC_WRITE_SOURCE_BITS {
        return Err(TxnSourceError::CdcWriteSourceOutOfRange(value));
    }
    *txn_source |= value;
    Ok(())
}

/// Returns the low-byte TiCDC source bits.
#[must_use]
pub const fn get_cdc_write_source(txn_source: u64) -> u64 {
    txn_source & CDC_WRITE_SOURCE_MAX
}

/// Returns whether any bit is set in the low TiCDC byte.
#[must_use]
pub const fn is_cdc_write_source_set(txn_source: u64) -> bool {
    (txn_source & CDC_WRITE_SOURCE_MAX) != 0
}

/// ORs one accepted lossy-DDL reorg source into its transaction-source byte.
pub fn set_lossy_ddl_reorg_source(txn_source: &mut u64, value: u64) -> Result<(), TxnSourceError> {
    if value > LOSSY_DDL_REORG_SOURCE_MAX {
        return Err(TxnSourceError::LossyDdlReorgSourceOutOfRange(value));
    }
    *txn_source |= value << LOSSY_DDL_REORG_SOURCE_SHIFT;
    Ok(())
}

/// Returns the masked lossy-DDL reorg source byte.
#[must_use]
pub const fn get_lossy_ddl_reorg_source(txn_source: u64) -> u64 {
    (txn_source >> LOSSY_DDL_REORG_SOURCE_SHIFT) & LOSSY_DDL_REORG_SOURCE_MAX
}

/// Returns whether the source sees any bit above the TiCDC byte.
///
/// This deliberately does not apply `LOSSY_DDL_REORG_SOURCE_MAX`, matching
/// `pkg/kv/option.go`. Lightning or reserved upper bits therefore return true.
#[must_use]
pub const fn is_lossy_ddl_reorg_source_set(txn_source: u64) -> bool {
    (txn_source >> LOSSY_DDL_REORG_SOURCE_SHIFT) != 0
}

#[cfg(test)]
mod tests {
    use super::{
        get_cdc_write_source, get_lossy_ddl_reorg_source, is_cdc_write_source_set,
        is_lossy_ddl_reorg_source_set, set_cdc_write_source, set_lossy_ddl_reorg_source,
        TxnSourceError, CDC_WRITE_SOURCE_MAX, LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE,
        LOSSY_DDL_COLUMN_REORG_SOURCE, LOSSY_DDL_REORG_SOURCE_MAX, LOSSY_DDL_REORG_SOURCE_SHIFT,
    };

    #[test]
    fn every_accepted_source_composes_without_clobbering_other_bits() {
        const RESERVED_SOURCE: u64 = (0xa5_u64 << 40) | LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE;

        for cdc in 0..=8 {
            for lossy in 0..=LOSSY_DDL_REORG_SOURCE_MAX {
                let expected = RESERVED_SOURCE | cdc | (lossy << LOSSY_DDL_REORG_SOURCE_SHIFT);

                let mut cdc_then_lossy = RESERVED_SOURCE;
                set_cdc_write_source(&mut cdc_then_lossy, cdc).expect("accepted CDC source");
                set_lossy_ddl_reorg_source(&mut cdc_then_lossy, lossy)
                    .expect("accepted lossy-DDL source");
                assert_eq!(cdc_then_lossy, expected, "cdc={cdc}, lossy={lossy}");

                let mut lossy_then_cdc = RESERVED_SOURCE;
                set_lossy_ddl_reorg_source(&mut lossy_then_cdc, lossy)
                    .expect("accepted lossy-DDL source");
                set_cdc_write_source(&mut lossy_then_cdc, cdc).expect("accepted CDC source");
                assert_eq!(lossy_then_cdc, expected, "cdc={cdc}, lossy={lossy}");

                assert_eq!(get_cdc_write_source(expected), cdc);
                assert_eq!(get_lossy_ddl_reorg_source(expected), lossy);
                assert_eq!(is_cdc_write_source_set(expected), cdc != 0);
                assert_eq!(
                    is_lossy_ddl_reorg_source_set(expected),
                    (expected >> LOSSY_DDL_REORG_SOURCE_SHIFT) != 0
                );
            }
        }
    }

    #[test]
    fn setters_are_or_only_and_invalid_values_do_not_mutate() {
        let mut source = 0;
        set_cdc_write_source(&mut source, 1).expect("first CDC source");
        set_cdc_write_source(&mut source, 2).expect("second CDC source");
        set_lossy_ddl_reorg_source(&mut source, 1).expect("first lossy source");
        set_lossy_ddl_reorg_source(&mut source, 2).expect("second lossy source");
        assert_eq!(get_cdc_write_source(source), 3);
        assert_eq!(get_lossy_ddl_reorg_source(source), 3);

        for invalid in [9, 15, 16, CDC_WRITE_SOURCE_MAX, 256, u64::MAX] {
            let before = source;
            assert_eq!(
                set_cdc_write_source(&mut source, invalid),
                Err(TxnSourceError::CdcWriteSourceOutOfRange(invalid))
            );
            assert_eq!(source, before);
        }
        for invalid in [256, 257, u64::MAX] {
            let before = source;
            assert_eq!(
                set_lossy_ddl_reorg_source(&mut source, invalid),
                Err(TxnSourceError::LossyDdlReorgSourceOutOfRange(invalid))
            );
            assert_eq!(source, before);
        }
    }

    #[test]
    fn surprising_source_boundaries_remain_observable() {
        for accepted in 0..=8 {
            let mut source = 0;
            set_cdc_write_source(&mut source, accepted).expect("source accepts zero through eight");
            assert_eq!(get_cdc_write_source(source), accepted);
        }

        let error = set_cdc_write_source(&mut 0, 9).expect_err("source rejects nine");
        assert_eq!(
            error.to_string(),
            "value 9 is out of TiCDC write source range, should be in [1, 15]"
        );
        let error = set_lossy_ddl_reorg_source(&mut 0, 256).expect_err("source rejects 256");
        assert_eq!(
            error.to_string(),
            "value 256 is out of lossy DDL reorg source range, should be in [1, 255]"
        );

        assert_eq!(LOSSY_DDL_COLUMN_REORG_SOURCE, 1);
        assert_eq!(LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE, 1 << 16);
        assert_eq!(
            get_lossy_ddl_reorg_source(LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE),
            0
        );
        assert!(is_lossy_ddl_reorg_source_set(
            LIGHTNING_PHYSICAL_IMPORT_TXN_SOURCE
        ));
        assert!(is_lossy_ddl_reorg_source_set(1 << 63));
    }
}
