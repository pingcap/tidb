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

//! `ALTER TABLE ... RENAME {KEY|INDEX} old TO new` payload.
//!
//! Go's `HandParser.parseAlterRename` stores source and target index names
//! separately and restores either input introducer as `RENAME INDEX`.

/// An existing secondary-index rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameIndex {
    /// Existing index name.
    pub from: String,
    /// Replacement index name.
    pub to: String,
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for RenameIndex {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { from, to } = self;
        let _ = from;
        let _ = to;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
