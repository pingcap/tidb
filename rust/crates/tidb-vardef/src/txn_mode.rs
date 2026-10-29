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

//! Go `vardef`'s `tidb_txn_mode` values (`sysvar.go`) and the way every Go
//! consumer reads the variable, resolved to the two modes a transaction can
//! run in. Which mode a transaction opens in is the session's decision
//! (`tidb_session::txn_mode`).

/// Go `vardef.PessimisticTxnMode`.
pub const PESSIMISTIC_TXN_MODE: &str = "pessimistic";
/// Go `vardef.OptimisticTxnMode`.
pub const OPTIMISTIC_TXN_MODE: &str = "optimistic";

/// The mode a transaction actually runs in, once every default is resolved.
///
/// Go carries this as a string that may also be empty; every consumer treats
/// the empty string as optimistic, so the resolved form has only two states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTxnMode {
    /// No read-time locks; conflicts are detected by the two-phase commit.
    Optimistic,
    /// Locking statements take TiKV pessimistic locks at `for_update_ts`.
    Pessimistic,
}

impl SessionTxnMode {
    /// Whether locking statements in this mode acquire pessimistic locks.
    #[must_use]
    pub const fn is_pessimistic(self) -> bool {
        matches!(self, Self::Pessimistic)
    }

    /// Go's string spelling, as `@@tidb_txn_mode` reports it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Optimistic => OPTIMISTIC_TXN_MODE,
            Self::Pessimistic => PESSIMISTIC_TXN_MODE,
        }
    }
}

/// Reads `@@tidb_txn_mode` the way every Go consumer reads it.
///
/// Go compares against `ast.Pessimistic` and sends everything else -- the
/// empty string included, which `SET tidb_txn_mode = ''` really can store
/// because the variable is `AllowEmptyAll` -- down the optimistic path.
#[must_use]
pub fn txn_mode_variable(value: &str) -> SessionTxnMode {
    if value == PESSIMISTIC_TXN_MODE {
        SessionTxnMode::Pessimistic
    } else {
        SessionTxnMode::Optimistic
    }
}

#[cfg(test)]
mod tests {
    use super::{txn_mode_variable, SessionTxnMode};

    #[test]
    fn only_the_exact_pessimistic_spelling_is_pessimistic() {
        assert_eq!(
            txn_mode_variable("pessimistic"),
            SessionTxnMode::Pessimistic
        );
        assert_eq!(txn_mode_variable("optimistic"), SessionTxnMode::Optimistic);
        // `SET tidb_txn_mode = ''` stores the empty string, which Go reads as
        // optimistic rather than as the pessimistic default.
        assert_eq!(txn_mode_variable(""), SessionTxnMode::Optimistic);
    }
}
