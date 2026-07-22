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

//! Dedicated physical payload for ALTER TABLE REMOVE TTL.

/// Go's payload-free AlterTableRemoveTTL specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterTableRemoveTtl;

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AlterTableRemoveTtl {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
