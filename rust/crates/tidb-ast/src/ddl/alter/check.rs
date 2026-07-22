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

//! `ALTER TABLE ... ALTER {CHECK|CONSTRAINT} name ...` payload.
//!
//! This is intentionally distinct from a CHECK declaration: Go's
//! `HandParser.parseAlterAlter` changes only the enforcement state of an
//! existing named constraint and never parses an expression here.

/// An existing CHECK constraint whose enforcement state is changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterCheck {
    /// Existing constraint name.
    pub name: String,
    /// Whether TiDB should enforce the constraint.
    pub enforced: bool,
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AlterCheck {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, enforced } = self;
        let _ = name;
        let _ = enforced;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
