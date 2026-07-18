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

//! Dependency-closed engine classifications from
//! `pkg/planner/cascades/pattern/engine.go`.
//!
//! The source represents engine locations as bit flags so a pattern can allow
//! one or more execution engines. This leaf preserves those flags, the
//! predefined sets, membership checks, and diagnostic labels without pulling
//! in the logical-plan or cascades runtime.

use std::fmt;

/// Execution engine selected for a cascades group.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum EngineType {
    /// TiDB root-layer execution, above a `Gather`.
    TiDb = 1,
    /// TiKV coprocessor execution, below a `Gather`.
    TiKv = 2,
    /// TiFlash coprocessor execution, below a `Gather`.
    TiFlash = 4,
}

impl EngineType {
    /// Returns the source bit value for this engine.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self as u8
    }

    /// Returns the source diagnostic label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TiDb => "EngineTiDB",
            Self::TiKv => "EngineTiKV",
            Self::TiFlash => "EngineTiFlash",
        }
    }
}

impl fmt::Display for EngineType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A bit set of engines accepted by a pattern.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EngineTypeSet(u8);

impl EngineTypeSet {
    /// A set containing only TiDB execution.
    pub const TIDB_ONLY: Self = Self(EngineType::TiDb.bits());
    /// A set containing only TiKV execution.
    pub const TIKV_ONLY: Self = Self(EngineType::TiKv.bits());
    /// A set containing only TiFlash execution.
    pub const TIFLASH_ONLY: Self = Self(EngineType::TiFlash.bits());
    /// A set containing TiKV or TiFlash execution.
    pub const TIKV_OR_TIFLASH: Self = Self(EngineType::TiKv.bits() | EngineType::TiFlash.bits());
    /// A set containing every source engine.
    pub const ALL: Self =
        Self(EngineType::TiDb.bits() | EngineType::TiKv.bits() | EngineType::TiFlash.bits());

    /// Creates a set from source-compatible bit flags.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns the underlying source-compatible bit flags.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Reports whether this set contains the given engine bit.
    #[must_use]
    pub const fn contains(self, engine: EngineType) -> bool {
        self.0 & engine.bits() != 0
    }
}
