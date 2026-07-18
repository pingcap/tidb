// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-shaped executor RecordSet lifecycle contracts.

/// Once-only lifecycle state shared by lazy record-set implementations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordSetLifecycle {
    advanced: bool,
    finished: bool,
    closed: bool,
}

impl RecordSetLifecycle {
    /// Records that `Next` was called, including an empty or failed call.
    pub fn mark_advanced(&mut self) {
        self.advanced = true;
    }

    /// Returns whether at least one `Next` call occurred.
    #[must_use]
    pub const fn has_advanced(&self) -> bool {
        self.advanced
    }

    /// Claims the single underlying `Finish` call.
    pub fn begin_finish(&mut self) -> bool {
        if self.finished {
            return false;
        }
        self.finished = true;
        true
    }

    /// Claims the single underlying `Close` call.
    pub fn begin_close(&mut self) -> bool {
        if self.closed {
            return false;
        }
        self.closed = true;
        true
    }

    /// Returns whether finish has been claimed.
    #[must_use]
    pub const fn is_finished(&self) -> bool {
        self.finished
    }

    /// Returns whether close has been claimed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }
}
