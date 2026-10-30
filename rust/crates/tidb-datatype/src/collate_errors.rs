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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use tidb_dbterror::catalog_support::{plain_err, std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `util/collate.ErrUnsupportedCollation` (`pkg/util/collate/collate.go:43`).
pub static ERR_UNSUPPORTED_COLLATION: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Ddl,
        errcode::ErrUnknownCollation,
        "Unsupported collation when new collation is enabled: '%-.64s'",
    )
});
/// Source `util/collate.ErrIllegalMixCollation` (`pkg/util/collate/collate.go:45`).
pub static ERR_ILLEGAL_MIX_COLLATION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrCantAggregateNcollations));
/// Source `util/collate.ErrIllegalMix2Collation` (`pkg/util/collate/collate.go:47`).
pub static ERR_ILLEGAL_MIX2_COLLATION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrCantAggregate2collations));
/// Source `util/collate.ErrIllegalMix3Collation` (`pkg/util/collate/collate.go:49`).
pub static ERR_ILLEGAL_MIX3_COLLATION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrCantAggregate3collations));

#[cfg(test)]
mod go_class_code_tests {
    use tidb_errno::errcode;
    use tidb_terror::TerrorClass;

    /// Each prototype carries the class and code of the Go definition it
    /// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
    /// `[class:code]`.
    #[test]
    fn prototypes_carry_their_go_class_and_code() {
        assert_eq!(
            super::ERR_UNSUPPORTED_COLLATION.class(),
            TerrorClass::Ddl,
            "collate::ERR_UNSUPPORTED_COLLATION"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_COLLATION.code().value(),
            errcode::ErrUnknownCollation as isize,
            "collate::ERR_UNSUPPORTED_COLLATION"
        );
        assert_eq!(
            super::ERR_ILLEGAL_MIX_COLLATION.class(),
            TerrorClass::Expression,
            "collate::ERR_ILLEGAL_MIX_COLLATION"
        );
        assert_eq!(
            super::ERR_ILLEGAL_MIX_COLLATION.code().value(),
            errcode::ErrCantAggregateNcollations as isize,
            "collate::ERR_ILLEGAL_MIX_COLLATION"
        );
        assert_eq!(
            super::ERR_ILLEGAL_MIX2_COLLATION.class(),
            TerrorClass::Expression,
            "collate::ERR_ILLEGAL_MIX2_COLLATION"
        );
        assert_eq!(
            super::ERR_ILLEGAL_MIX2_COLLATION.code().value(),
            errcode::ErrCantAggregate2collations as isize,
            "collate::ERR_ILLEGAL_MIX2_COLLATION"
        );
        assert_eq!(
            super::ERR_ILLEGAL_MIX3_COLLATION.class(),
            TerrorClass::Expression,
            "collate::ERR_ILLEGAL_MIX3_COLLATION"
        );
        assert_eq!(
            super::ERR_ILLEGAL_MIX3_COLLATION.code().value(),
            errcode::ErrCantAggregate3collations as isize,
            "collate::ERR_ILLEGAL_MIX3_COLLATION"
        );
    }
}
