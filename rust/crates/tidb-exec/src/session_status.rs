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

//! Atomic session status flags from TiDB's `SessionVars`.
//!
//! This leaf owns only the status bitfield and its protocol masks. It does not
//! own transaction transitions, explicit-transaction metadata, cursor
//! lifecycle, or result-packet rendering. Those callers can compose the
//! bitfield around their own session/connection owners.

use std::sync::atomic::{AtomicU32, Ordering};

/// `ServerStatusInTrans` from `pkg/parser/mysql/const.go`.
pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
/// `ServerStatusAutocommit` from `pkg/parser/mysql/const.go`.
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
/// `ServerStatusCursorExists` from `pkg/parser/mysql/const.go`.
pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;

/// The atomic status bitfield carried by a live session.
#[derive(Debug)]
pub struct SessionStatus {
    status: AtomicU32,
}

impl Default for SessionStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStatus {
    /// Creates a status field with TiDB's default autocommit bit enabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            status: AtomicU32::new(SERVER_STATUS_AUTOCOMMIT as u32),
        }
    }

    /// Atomically sets or clears a protocol status flag.
    pub fn set_flag(&self, flag: u16, enabled: bool) {
        if enabled {
            self.status.fetch_or(u32::from(flag), Ordering::SeqCst);
        } else {
            self.status.fetch_and(!u32::from(flag), Ordering::SeqCst);
        }
    }

    /// Returns whether any bit in `flag` is currently set.
    #[must_use]
    pub fn has_flag(&self, flag: u16) -> bool {
        self.status.load(Ordering::SeqCst) & u32::from(flag) != 0
    }

    /// Returns the protocol-sized status value.
    #[must_use]
    pub fn bits(&self) -> u16 {
        self.status.load(Ordering::SeqCst) as u16
    }

    /// Returns whether the session advertises an active transaction.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.has_flag(SERVER_STATUS_IN_TRANS)
    }

    /// Returns whether the session advertises autocommit.
    #[must_use]
    pub fn autocommit(&self) -> bool {
        self.has_flag(SERVER_STATUS_AUTOCOMMIT)
    }
}
