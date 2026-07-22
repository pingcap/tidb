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

//! `ALTER TABLE ... ALTER INDEX ... {VISIBLE|INVISIBLE}` payload.
//!
//! This is the typed AST boundary for Go
//! `HandParser.parseAlterAlter`'s index-visibility branch.

use crate::IndexVisibility;

/// A secondary index whose optimizer visibility is changed.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterIndexVisibility {
    /// Secondary-index name.
    pub name: String,
    /// Requested optimizer visibility.
    pub visibility: IndexVisibility,
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AlterIndexVisibility {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, visibility } = self;
        if !crate::Visitable::accept(visibility, visitor) {
            return false;
        }
        let _ = name;
        let _ = visibility;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
