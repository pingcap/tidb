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

//! Dependency-closed transaction-isolation metadata.
//!
//! The Go source splits this boundary across
//! `pkg/sessionctx/variable/variable.go`'s enum normalization,
//! `pkg/sessionctx/variable/varsutil.go`'s supported-level policy, and
//! `pkg/sessionctx/variable/session.go`'s three-state one-shot value.  This
//! module ports those value semantics without claiming storage isolation,
//! MVCC behavior, a live session, or a transaction coordinator.

/// The four isolation values accepted by TiDB's `tx_isolation` enum.
///
/// `READ-UNCOMMITTED` and `SERIALIZABLE` are retained in this metadata enum
/// even though the current Rust executor only executes the
/// `READ-COMMITTED`/`REPEATABLE-READ` storage paths.  Keeping the source enum
/// complete means validation and readback do not silently collapse values
/// before a future storage owner makes a policy decision.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IsolationLevel {
    /// The enum's ordinal `0`; source validation warns or rejects it unless
    /// `tidb_skip_isolation_level_check` is enabled.
    ReadUncommitted,
    /// The currently supported read-consistency mode.
    ReadCommitted,
    /// TiDB's default isolation mode and the currently supported snapshot
    /// mode.
    #[default]
    RepeatableRead,
    /// The enum's ordinal `3`; source validation warns or rejects it unless
    /// `tidb_skip_isolation_level_check` is enabled.
    Serializable,
}

impl IsolationLevel {
    /// Returns all source enum values in their Go `PossibleValues` order.
    pub const fn all() -> [Self; 4] {
        [
            Self::ReadUncommitted,
            Self::ReadCommitted,
            Self::RepeatableRead,
            Self::Serializable,
        ]
    }

    /// Returns the canonical TiDB readback spelling.
    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "READ-UNCOMMITTED",
            Self::ReadCommitted => "READ-COMMITTED",
            Self::RepeatableRead => "REPEATABLE-READ",
            Self::Serializable => "SERIALIZABLE",
        }
    }

    /// Parses a TypeEnum value using Go's case-insensitive name-or-ordinal
    /// rules.  Whitespace is intentionally not removed: the source enum
    /// validator compares the supplied value directly and leaves trimming to
    /// callers that own a different input grammar.
    pub fn parse(value: &str) -> Option<Self> {
        let upper = value.to_ascii_uppercase();
        match upper.as_str() {
            "0" | "READ-UNCOMMITTED" => Some(Self::ReadUncommitted),
            "1" | "READ-COMMITTED" => Some(Self::ReadCommitted),
            "2" | "REPEATABLE-READ" => Some(Self::RepeatableRead),
            "3" | "SERIALIZABLE" => Some(Self::Serializable),
            _ => None,
        }
    }

    /// Whether the current executor has a storage-level implementation for
    /// this value.  This is a capability fact, not enum validation: all four
    /// values remain parseable above.
    pub const fn storage_supported(self) -> bool {
        matches!(self, Self::ReadCommitted | Self::RepeatableRead)
    }
}

/// The source three-state lifecycle for `tx_isolation_one_shot`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OneShotState {
    /// No one-shot value is pending.
    #[default]
    Default,
    /// A value was set for the currently entering transaction.
    Set,
    /// A value survived the transaction boundary and is pending for the next
    /// transaction.
    Use,
}

/// A one-shot isolation value and its source-compatible state machine.
///
/// The value is intentionally independent of `TransactionState`: Go keeps
/// this setting in `SessionVars`, outside transaction rollback data.  A real
/// session/transaction owner can ask [`Self::level_for_new_txn`] which value
/// applies and call [`Self::advance_for_next_txn`] at its commit boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OneShotIsolation {
    state: OneShotState,
    value: Option<IsolationLevel>,
}

impl OneShotIsolation {
    /// Creates a pending one-shot value in the source `Set` state.
    pub fn set(level: IsolationLevel) -> Self {
        Self {
            state: OneShotState::Set,
            value: Some(level),
        }
    }

    /// Returns the current source state.
    pub const fn state(self) -> OneShotState {
        self.state
    }

    /// Returns the stored level, if a one-shot value is present.
    pub const fn value(self) -> Option<IsolationLevel> {
        self.value
    }

    /// Returns the source `@@tx_isolation_one_shot` readback spelling.
    pub const fn readback(self) -> &'static str {
        match self.value {
            Some(level) => level.canonical_name(),
            None => "",
        }
    }

    /// Returns the level selected by Go's `IsolationLevelForNewTxn`.
    ///
    /// While already in a transaction, `Set` wins.  Outside a transaction,
    /// only `Use` wins.  All other combinations fall back to the session
    /// default.  The distinction is what prevents a one-shot value from
    /// leaking into a second transaction.
    pub const fn level_for_new_txn(
        self,
        in_txn: bool,
        session_default: IsolationLevel,
    ) -> IsolationLevel {
        match (in_txn, self.state, self.value) {
            (true, OneShotState::Set, Some(level)) => level,
            (false, OneShotState::Use, Some(level)) => level,
            _ => session_default,
        }
    }

    /// Advances the source state at a transaction boundary.
    ///
    /// `Set -> Use` retains the value for the next transaction; `Use ->
    /// Default` clears it.  The `Default` state is already canonical and is
    /// left untouched.  The invariant that `Default` has no value is kept
    /// structural by this method.
    pub fn advance_for_next_txn(&mut self) {
        match self.state {
            OneShotState::Default => {}
            OneShotState::Set => self.state = OneShotState::Use,
            OneShotState::Use => {
                self.state = OneShotState::Default;
                self.value = None;
            }
        }
    }
}
