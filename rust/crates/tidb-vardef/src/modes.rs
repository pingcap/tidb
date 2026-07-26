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

//! Small mode enums from `pkg/sessionctx/vardef/tidb_vars.go`:
//! `ClusteredIndexDefMode` and `ExchangeCompressionMode`.
//!
//! Go integer enums are modeled as newtypes over `i64` (Go `int`), following
//! the established rewrite pattern.

use crate::tidb_vars::{OFF, ON};

/// Go `ClusteredIndexDefMode` (an `int`): the default clustering behavior for a
/// primary key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClusteredIndexDefMode(pub i64);

impl ClusteredIndexDefMode {
    /// Only a single-column integer primary key defaults to clustered
    /// (Go `ClusteredIndexDefModeIntOnly`, the zero value).
    pub const INT_ONLY: ClusteredIndexDefMode = ClusteredIndexDefMode(0);
    /// The primary key defaults to clustered (Go `ClusteredIndexDefModeOn`).
    pub const ON: ClusteredIndexDefMode = ClusteredIndexDefMode(1);
    /// The primary key defaults to non-clustered (Go `ClusteredIndexDefModeOff`).
    pub const OFF: ClusteredIndexDefMode = ClusteredIndexDefMode(2);
}

/// Go `TiDBOptEnableClustered`: converts a `tidb_enable_clustered_index` option
/// string to a [`ClusteredIndexDefMode`]. Any value other than `"ON"`/`"OFF"`
/// (the Go `On`/`Off` constants) falls back to int-only.
#[must_use]
pub fn tidb_opt_enable_clustered(opt: &str) -> ClusteredIndexDefMode {
    match opt {
        _ if opt == ON => ClusteredIndexDefMode::ON,
        _ if opt == OFF => ClusteredIndexDefMode::OFF,
        _ => ClusteredIndexDefMode::INT_ONLY,
    }
}

/// Go `ExchangeCompressionMode` (an `int`): the MPP exchange compression mode.
///
/// The integer values match `tipb.CompressionMode` (NONE=0, FAST=1,
/// HIGH_COMPRESSION=2) so a value round-trips through the proto enum, plus a
/// TiDB-only `Unspecified` sentinel.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExchangeCompressionMode(pub i64);

impl ExchangeCompressionMode {
    /// No compression (Go `ExchangeCompressionModeNONE`, the zero value;
    /// `tipb.CompressionMode_NONE`).
    pub const NONE: ExchangeCompressionMode = ExchangeCompressionMode(0);
    /// Fast compression (Go `ExchangeCompressionModeFast`;
    /// `tipb.CompressionMode_FAST`).
    pub const FAST: ExchangeCompressionMode = ExchangeCompressionMode(1);
    /// High-compression ratio (Go `ExchangeCompressionModeHC`;
    /// `tipb.CompressionMode_HIGH_COMPRESSION`).
    pub const HC: ExchangeCompressionMode = ExchangeCompressionMode(2);
    /// Unspecified; let TiDB choose (Go `ExchangeCompressionModeUnspecified`).
    pub const UNSPECIFIED: ExchangeCompressionMode = ExchangeCompressionMode(3);

    /// Go `RecommendedExchangeCompressionMode` = `ExchangeCompressionModeFast`.
    pub const RECOMMENDED: ExchangeCompressionMode = Self::FAST;

    /// Go `unexported exchangeCompressionModeUnspecifiedName`.
    const UNSPECIFIED_NAME: &'static str = "UNSPECIFIED";

    /// Go `(ExchangeCompressionMode).Name`.
    ///
    /// For `Unspecified` returns `"UNSPECIFIED"`; otherwise returns the
    /// `tipb.CompressionMode` proto name (`NONE`/`FAST`/`HIGH_COMPRESSION`).
    /// Go delegates the non-unspecified case to `ToTipbCompressionMode().String()`,
    /// which maps any non-NONE/FAST/HC value to `NONE`; the proto names are
    /// inlined here (verified against `tipb.CompressionMode_name`) so this crate
    /// needs no dependency on the not-yet-ported proto stack.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::UNSPECIFIED => Self::UNSPECIFIED_NAME,
            Self::FAST => "FAST",
            Self::HC => "HIGH_COMPRESSION",
            _ => "NONE",
        }
    }

    /// Go `(ExchangeCompressionMode).ToTipbCompressionMode`, returning the
    /// `tipb.CompressionMode` integer value (NONE for anything but FAST/HC).
    #[must_use]
    pub fn to_tipb_compression_value(self) -> i32 {
        match self {
            Self::FAST => 1,
            Self::HC => 2,
            _ => 0,
        }
    }
}

/// Go `ToExchangeCompressionMode`: parses a mode name (case-insensitive) into an
/// [`ExchangeCompressionMode`], returning `None` when the name is unknown.
///
/// `"UNSPECIFIED"` maps to [`ExchangeCompressionMode::UNSPECIFIED`]; the proto
/// names `NONE`/`FAST`/`HIGH_COMPRESSION` map by their `tipb.CompressionMode`
/// value (which equals the corresponding `ExchangeCompressionMode` value).
#[must_use]
pub fn to_exchange_compression_mode(name: &str) -> Option<ExchangeCompressionMode> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "UNSPECIFIED" => Some(ExchangeCompressionMode::UNSPECIFIED),
        "NONE" => Some(ExchangeCompressionMode::NONE),
        "FAST" => Some(ExchangeCompressionMode::FAST),
        "HIGH_COMPRESSION" => Some(ExchangeCompressionMode::HC),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clustered_index_def_mode() {
        assert_eq!(
            ClusteredIndexDefMode::default(),
            ClusteredIndexDefMode::INT_ONLY
        );
        assert_eq!(tidb_opt_enable_clustered("ON"), ClusteredIndexDefMode::ON);
        assert_eq!(tidb_opt_enable_clustered("OFF"), ClusteredIndexDefMode::OFF);
        assert_eq!(
            tidb_opt_enable_clustered("INT_ONLY"),
            ClusteredIndexDefMode::INT_ONLY
        );
        assert_eq!(
            tidb_opt_enable_clustered(""),
            ClusteredIndexDefMode::INT_ONLY
        );
    }

    #[test]
    fn exchange_compression_mode_names() {
        assert_eq!(ExchangeCompressionMode::NONE.name(), "NONE");
        assert_eq!(ExchangeCompressionMode::FAST.name(), "FAST");
        assert_eq!(ExchangeCompressionMode::HC.name(), "HIGH_COMPRESSION");
        assert_eq!(ExchangeCompressionMode::UNSPECIFIED.name(), "UNSPECIFIED");
        assert_eq!(
            ExchangeCompressionMode::RECOMMENDED,
            ExchangeCompressionMode::FAST
        );
    }

    #[test]
    fn exchange_compression_mode_parse_roundtrip() {
        // Case-insensitive, and the proto value round-trips for NONE/FAST/HC.
        for m in [
            ExchangeCompressionMode::NONE,
            ExchangeCompressionMode::FAST,
            ExchangeCompressionMode::HC,
        ] {
            assert_eq!(i64::from(m.to_tipb_compression_value()), m.0);
        }
        assert_eq!(
            to_exchange_compression_mode("fast"),
            Some(ExchangeCompressionMode::FAST)
        );
        assert_eq!(
            to_exchange_compression_mode("high_compression"),
            Some(ExchangeCompressionMode::HC)
        );
        assert_eq!(
            to_exchange_compression_mode("unspecified"),
            Some(ExchangeCompressionMode::UNSPECIFIED)
        );
        assert_eq!(to_exchange_compression_mode("bogus"), None);
    }
}
