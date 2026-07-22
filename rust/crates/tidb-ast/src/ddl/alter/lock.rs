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

//! `ALTER TABLE ... LOCK [=] mode` payload.
//!
//! Go's `HandParser.parseAlterTableOptions` owns this option and always
//! restores it with spaces around the equals sign.

/// Requested ALTER TABLE metadata-lock level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTableLockMode {
    /// Let TiDB choose the default lock level.
    Default,
    /// Permit concurrent reads and writes where supported.
    None,
    /// Permit shared access only.
    Shared,
    /// Require exclusive access.
    Exclusive,
}

impl AlterTableLockMode {
    /// Go AST's canonical keyword spelling.
    pub fn sql(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::None => "NONE",
            Self::Shared => "SHARED",
            Self::Exclusive => "EXCLUSIVE",
        }
    }
}

/// One `ALTER TABLE LOCK` action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterTableLock {
    /// Requested lock level.
    pub mode: AlterTableLockMode,
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AlterTableLockMode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::None => {}
            Self::Shared => {}
            Self::Exclusive => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterTableLock {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { mode } = self;
        if !crate::Visitable::accept(mode, visitor) {
            return false;
        }
        let _ = mode;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
