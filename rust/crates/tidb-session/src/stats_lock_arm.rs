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

//! `LOCK STATS` / `UNLOCK STATS` session wiring.
//!
//! Go deliberately executes the statistics-handle mutation in a borrowed
//! system session and commits that internal transaction independently of the
//! user's transaction. This module therefore stages the shared catalog
//! directly rather than the user's optional transaction working copy.

use crate::{Session, StmtOutput, WarningLevel};
use tidb_executor::{Catalog, DriverError};

struct SharedCatalogStage<'a> {
    catalog: &'a mut Catalog,
    before: Option<Catalog>,
}

impl Drop for SharedCatalogStage<'_> {
    fn drop(&mut self) {
        if let Some(before) = self.before.take() {
            *self.catalog = before;
        }
    }
}

impl Session {
    pub(crate) fn stats_lock_stmt(
        &mut self,
        statement: &tidb_ast::StatsLockStmt,
        lock: bool,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let current_database = self.current_db.clone();
        let context = self.statement_context(true);
        let message = {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| DriverError::CatalogPoisoned)?;
            let start_ts = catalog.allocate_tso();
            let before = catalog.clone();
            let mut stage = SharedCatalogStage {
                catalog: &mut catalog,
                before: Some(before),
            };
            let message = tidb_executor::stats_lock::execute_catalog_stats_lock(
                statement,
                lock,
                stage.catalog,
                &current_database,
                &context,
                start_ts,
            )?;
            stage.before = None;
            message
        };
        if !message.is_empty() {
            self.append_warning(WarningLevel::Warning, 1105, message);
        }
        Ok(Some(StmtOutput::Affected(0)))
    }
}
