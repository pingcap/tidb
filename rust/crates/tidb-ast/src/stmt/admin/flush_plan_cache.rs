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

//! `ADMIN FLUSH ... PLAN_CACHE` scopes from Go's `AdminFlushPlanCache` AST.

/// The source-owned scoped plan-cache flush forms in this vertical slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminPlanCacheScope {
    /// `ADMIN FLUSH SESSION PLAN_CACHE`.
    Session,
    /// `ADMIN FLUSH GLOBAL PLAN_CACHE`.
    Global,
}

impl AdminPlanCacheScope {
    pub(crate) fn restore_into(self, out: &mut String) {
        out.push_str("ADMIN FLUSH ");
        out.push_str(match self {
            Self::Session => "SESSION",
            Self::Global => "GLOBAL",
        });
        out.push_str(" PLAN_CACHE");
    }
}
