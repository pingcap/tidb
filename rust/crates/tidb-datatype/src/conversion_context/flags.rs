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

//! Exact `pkg/types/context.go::Flags` bit authority.

/// Strict conversion flags: every source bit is unset.
pub const STRICT_FLAGS: ConversionFlags = ConversionFlags::from_bits(0);

/// Default statement conversion flags from `DefaultStmtFlags`.
pub const DEFAULT_STATEMENT_FLAGS: ConversionFlags = ConversionFlags::from_bits(
    ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED | ConversionFlags::IGNORE_ZERO_DATE_ERR,
);

/// Flags controlling datatype conversion behavior.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ConversionFlags(u16);

impl ConversionFlags {
    /// `FlagIgnoreTruncateErr`.
    pub const IGNORE_TRUNCATE_ERR: u16 = 1;
    /// `FlagTruncateAsWarning`.
    pub const TRUNCATE_AS_WARNING: u16 = 1 << 1;
    /// `FlagAllowNegativeToUnsigned`.
    pub const ALLOW_NEGATIVE_TO_UNSIGNED: u16 = 1 << 2;
    /// `FlagIgnoreZeroDateErr`.
    pub const IGNORE_ZERO_DATE_ERR: u16 = 1 << 3;
    /// `FlagIgnoreZeroInDateErr`.
    pub const IGNORE_ZERO_IN_DATE_ERR: u16 = 1 << 4;
    /// `FlagIgnoreInvalidDateErr`.
    pub const IGNORE_INVALID_DATE_ERR: u16 = 1 << 5;
    /// `FlagSkipASCIICheck`.
    pub const SKIP_ASCII_CHECK: u16 = 1 << 6;
    /// `FlagSkipUTF8Check`.
    pub const SKIP_UTF8_CHECK: u16 = 1 << 7;
    /// `FlagSkipUTF8MB4Check`.
    pub const SKIP_UTF8MB4_CHECK: u16 = 1 << 8;
    /// `FlagCastTimeToYearThroughConcat`.
    pub const CAST_TIME_TO_YEAR_THROUGH_CONCAT: u16 = 1 << 9;

    /// Creates flags from the exact source bit representation.
    #[must_use]
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Returns the exact source bit representation.
    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Returns whether truncation errors are ignored.
    #[must_use]
    pub const fn ignore_truncate_err(self) -> bool {
        self.has(Self::IGNORE_TRUNCATE_ERR)
    }

    /// Sets or clears `FlagIgnoreTruncateErr`.
    #[must_use]
    pub const fn with_ignore_truncate_err(self, value: bool) -> Self {
        self.with(Self::IGNORE_TRUNCATE_ERR, value)
    }

    /// Returns whether truncation errors become warnings.
    #[must_use]
    pub const fn truncate_as_warning(self) -> bool {
        self.has(Self::TRUNCATE_AS_WARNING)
    }

    /// Sets or clears `FlagTruncateAsWarning`.
    #[must_use]
    pub const fn with_truncate_as_warning(self, value: bool) -> Self {
        self.with(Self::TRUNCATE_AS_WARNING, value)
    }

    /// Returns whether negative-to-unsigned conversion is allowed.
    #[must_use]
    pub const fn allow_negative_to_unsigned(self) -> bool {
        self.has(Self::ALLOW_NEGATIVE_TO_UNSIGNED)
    }

    /// Sets or clears `FlagAllowNegativeToUnsigned`.
    #[must_use]
    pub const fn with_allow_negative_to_unsigned(self, value: bool) -> Self {
        self.with(Self::ALLOW_NEGATIVE_TO_UNSIGNED, value)
    }

    /// Returns whether zero-date errors are ignored.
    #[must_use]
    pub const fn ignore_zero_date_err(self) -> bool {
        self.has(Self::IGNORE_ZERO_DATE_ERR)
    }

    /// Sets or clears `FlagIgnoreZeroDateErr`.
    #[must_use]
    pub const fn with_ignore_zero_date_err(self, value: bool) -> Self {
        self.with(Self::IGNORE_ZERO_DATE_ERR, value)
    }

    /// Returns whether zero-in-date errors are ignored.
    #[must_use]
    pub const fn ignore_zero_in_date_err(self) -> bool {
        self.has(Self::IGNORE_ZERO_IN_DATE_ERR)
    }

    /// Sets or clears `FlagIgnoreZeroInDateErr`.
    #[must_use]
    pub const fn with_ignore_zero_in_date_err(self, value: bool) -> Self {
        self.with(Self::IGNORE_ZERO_IN_DATE_ERR, value)
    }

    /// Returns whether invalid-date errors are ignored.
    #[must_use]
    pub const fn ignore_invalid_date_err(self) -> bool {
        self.has(Self::IGNORE_INVALID_DATE_ERR)
    }

    /// Sets or clears `FlagIgnoreInvalidDateErr`.
    #[must_use]
    pub const fn with_ignore_invalid_date_err(self, value: bool) -> Self {
        self.with(Self::IGNORE_INVALID_DATE_ERR, value)
    }

    /// Returns whether ASCII validation is skipped.
    #[must_use]
    pub const fn skip_ascii_check(self) -> bool {
        self.has(Self::SKIP_ASCII_CHECK)
    }

    /// Sets or clears `FlagSkipASCIICheck`.
    #[must_use]
    pub const fn with_skip_ascii_check(self, value: bool) -> Self {
        self.with(Self::SKIP_ASCII_CHECK, value)
    }

    /// Returns whether UTF8MB3 validation is skipped.
    #[must_use]
    pub const fn skip_utf8_check(self) -> bool {
        self.has(Self::SKIP_UTF8_CHECK)
    }

    /// Sets or clears `FlagSkipUTF8Check`.
    #[must_use]
    pub const fn with_skip_utf8_check(self, value: bool) -> Self {
        self.with(Self::SKIP_UTF8_CHECK, value)
    }

    /// Returns whether UTF8MB4 validation is skipped.
    #[must_use]
    pub const fn skip_utf8mb4_check(self) -> bool {
        self.has(Self::SKIP_UTF8MB4_CHECK)
    }

    /// Sets or clears `FlagSkipUTF8MB4Check`.
    #[must_use]
    pub const fn with_skip_utf8mb4_check(self, value: bool) -> Self {
        self.with(Self::SKIP_UTF8MB4_CHECK, value)
    }

    /// Returns whether time-to-year casts concatenate their time fields.
    #[must_use]
    pub const fn cast_time_to_year_through_concat(self) -> bool {
        self.has(Self::CAST_TIME_TO_YEAR_THROUGH_CONCAT)
    }

    /// Sets or clears `FlagCastTimeToYearThroughConcat`.
    #[must_use]
    pub const fn with_cast_time_to_year_through_concat(self, value: bool) -> Self {
        self.with(Self::CAST_TIME_TO_YEAR_THROUGH_CONCAT, value)
    }

    const fn has(self, bit: u16) -> bool {
        self.0 & bit != 0
    }

    const fn with(self, bit: u16, value: bool) -> Self {
        if value {
            Self(self.0 | bit)
        } else {
            Self(self.0 & !bit)
        }
    }
}
