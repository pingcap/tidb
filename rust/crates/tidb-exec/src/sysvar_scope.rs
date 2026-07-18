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

//! System-variable scope flags from `pkg/sessionctx/vardef/tidb_vars.go`.
//!
//! The Go source represents dynamic variable scope as a uint8 bitmask and
//! renders recognized bits in the fixed SESSION,GLOBAL,INSTANCE order. This
//! leaf ports that value/string contract only; SysVar registration, type
//! validation, SET/GET dispatch, and session/global persistence remain
//! external.

use std::fmt;
use std::ops::{BitOr, BitOrAssign};

/// Dynamic scope bitmask for a system variable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeFlag(u8);

impl ScopeFlag {
    /// A variable that cannot be changed dynamically.
    pub const NONE: Self = Self(0);
    /// A variable that can be changed globally.
    pub const GLOBAL: Self = Self(1 << 0);
    /// A variable that can be changed in the current session.
    pub const SESSION: Self = Self(1 << 1);
    /// A variable that is local to one TiDB instance.
    pub const INSTANCE: Self = Self(1 << 2);

    /// Creates a scope flag from its source bit representation.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns the source bit representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

impl BitOr for ScopeFlag {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for ScopeFlag {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl fmt::Display for ScopeFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::NONE {
            return f.write_str("NONE");
        }

        let mut first = true;
        for (flag, label) in [
            (Self::SESSION, "SESSION"),
            (Self::GLOBAL, "GLOBAL"),
            (Self::INSTANCE, "INSTANCE"),
        ] {
            if self.0 & flag.0 != 0 {
                if !first {
                    f.write_str(",")?;
                }
                f.write_str(label)?;
                first = false;
            }
        }
        Ok(())
    }
}
