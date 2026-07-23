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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Ordered KV versions translated from `pkg/kv/version.go`.

/// An ordered TiDB KV version value.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Version(u64);

/// The minimum version sentinel. Like Go `MinVersion`, this is not valid for a
/// real transaction.
pub const MIN_VERSION: Version = Version::new(0);

/// The maximum version sentinel. Like Go `MaxVersion`, this is not valid for a
/// real transaction.
pub const MAX_VERSION: Version = Version::new(u64::MAX);

/// Provider of monotonically increasing KV versions.
pub trait VersionProvider {
    /// Provider-specific error.
    type Error;

    /// Returns the current version.
    fn current_version(&self) -> Result<Version, Self::Error>;
}

impl Version {
    /// Creates a version from its unsigned integer value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wrapped unsigned integer value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Version, MAX_VERSION, MIN_VERSION};

    #[test]
    fn version_values_and_sentinels_are_source_exact() {
        assert_eq!(Version::new(42).value(), 42);
        assert_eq!(MIN_VERSION.value(), 0);
        assert_eq!(MAX_VERSION.value(), u64::MAX);
        assert!(MIN_VERSION < Version::new(1));
        assert!(Version::new(1) < MAX_VERSION);
    }
}
