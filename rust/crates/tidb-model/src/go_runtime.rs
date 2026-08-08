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

//! Native representations of Go runtime states used by `pkg/meta/model`.
//!
//! Persisted codecs live in `serde_helpers`; this module owns non-wire states
//! such as a typed-nil pointer held inside an `any` interface. Shared pointer
//! and slice-backing representations are added here as model clone surfaces
//! migrate away from Rust-only deep ownership.

/// A Go `any` value at a `*T` type-assertion boundary.
///
/// `Typed(None)` is an interface containing a typed nil `*T`; `Other` covers
/// an untyped nil or any other dynamic type. Go type assertions distinguish
/// these states before comparing pointer values.
#[derive(Clone, Copy, Debug)]
pub enum GoPointerAny<'a, T> {
    /// The assertion to `*T` succeeds, with either a nil or non-nil pointer.
    Typed(Option<&'a T>),
    /// The assertion to `*T` fails.
    Other,
}

/// A Go `time.Time` produced from Unix milliseconds, retaining the full
/// `int64` millisecond domain even when Chrono cannot represent the year.
///
/// Model rules that only compare or carry a timestamp must not silently turn
/// an out-of-range Go time into the Unix epoch. Callers that specifically need
/// Chrono opt into the fallible conversion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoTime {
    unix_millis: i64,
}

impl GoTime {
    /// Go `time.UnixMilli`'s numeric instant constructor.
    #[must_use]
    pub const fn from_unix_millis(unix_millis: i64) -> Self {
        Self { unix_millis }
    }

    /// Go model `TSConvert2Time`: discard the low 18 logical TSO bits, then
    /// interpret the remaining physical component as Unix milliseconds.
    #[must_use]
    pub const fn from_tso(timestamp: u64) -> Self {
        Self::from_unix_millis((timestamp >> 18) as i64)
    }

    /// The exact Unix millisecond value accepted by Go `time.UnixMilli`.
    #[must_use]
    pub const fn unix_millis(self) -> i64 {
        self.unix_millis
    }

    /// Converts to Chrono UTC only when Chrono supports the source year.
    #[must_use]
    pub fn to_chrono_utc(self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(self.unix_millis)
    }
}

impl<'a, T> GoPointerAny<'a, T> {
    /// Constructs a source-typed pointer interface.
    #[must_use]
    pub fn typed(value: Option<&'a T>) -> Self {
        Self::Typed(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_time_retains_full_tso_physical_domain() {
        assert_eq!(GoTime::from_tso(0).unix_millis(), 0);
        assert_eq!(GoTime::from_tso((1_u64 << 18) - 1).unix_millis(), 0);
        assert_eq!(GoTime::from_tso(1_u64 << 18).unix_millis(), 1);
        assert_eq!(GoTime::from_tso(u64::MAX).unix_millis(), (1_i64 << 46) - 1);
        assert!(GoTime::from_tso(u64::MAX).to_chrono_utc().is_none());
    }
}
