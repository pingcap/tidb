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

//! The statement state a [`PlannerCatalog`] is planned under, read off the
//! statement's context.

use crate::StmtContext;
use tidb_infoschema::PlannerCatalog;

/// The statement state a [`PlannerCatalog`] is planned under, read off the
/// statement's context.
pub(crate) trait PlannerCatalogContext: Sized {
    fn with_statement_dirty_tables(self, ctx: &StmtContext) -> Self;
    fn with_statement_privilege_verifier(self, ctx: &StmtContext) -> Self;
    fn with_statement_related_tables_for_mdl(self, ctx: &StmtContext) -> Self;
    fn with_statement_hypo_state(self, ctx: &StmtContext) -> Self;
}

impl PlannerCatalogContext for PlannerCatalog {
    /// Go `tableHasDirtyContent` -> `session.HasDirtyContent`: a table the
    /// transaction wrote. An autocommit statement's own writes are dirty
    /// content only to the foreign-key cascade statements it plans after
    /// them, which read those tables through a `UnionScan`.
    fn with_statement_dirty_tables(self, ctx: &StmtContext) -> Self {
        if ctx.pessimistic_transaction() || ctx.optimistic_transaction() || ctx.plans_fk_cascade() {
            self.with_dirty_tables(ctx.staged_writes().dirty_table_ids())
        } else {
            self
        }
    }

    fn with_statement_privilege_verifier(self, ctx: &StmtContext) -> Self {
        self.with_privilege_verifier(
            ctx.view_definer_privilege().cloned(),
            ctx.session_privilege().cloned(),
        )
    }

    fn with_statement_related_tables_for_mdl(self, ctx: &StmtContext) -> Self {
        self.with_related_tables_for_mdl(ctx.mdl_related_table().cloned())
            .with_related_table_ids(ctx.related_table_ids().cloned())
    }

    fn with_statement_hypo_state(self, ctx: &StmtContext) -> Self {
        if ctx.in_explain_stmt() {
            self.with_explain_hypo_state(
                ctx.hypo_indexes_shared(),
                ctx.hypo_tiflash_replicas_shared(),
            )
        } else {
            self
        }
    }
}
