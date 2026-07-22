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

//! `ALTER TABLE ... DROP FOREIGN KEY name` payload.
//!
//! Go's `HandParser.parseAlterDrop` keeps this separate from DROP CHECK,
//! DROP INDEX, and DROP COLUMN. The hand parser never sets the broader AST's
//! MariaDB `IfExists` field for this branch, so this leaf contains only name.

/// An existing foreign-key constraint to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropForeignKey {
    /// Existing foreign-key constraint name.
    pub name: String,
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for DropForeignKey {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name } = self;
        let _ = name;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
