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

//! The catalog-resolution half of `SPLIT TABLE`.
//!
//! A plain in-process session owns no region control plane, so it still
//! reports the ordinary unsupported statement. The cluster front end calls
//! this method before ordinary dispatch, receives raw split keys resolved
//! against this session's catalog, and publishes them through PD.

use tidb_executor::{DriverError, SplitRegionPlan};

use crate::Session;

impl Session {
    /// Plans a standalone `SPLIT TABLE` statement, or returns `None` for every
    /// other statement kind.
    pub fn prepare_split_region(
        &mut self,
        sql: &str,
    ) -> Result<Option<SplitRegionPlan>, DriverError> {
        let statement = self.parse(sql)?;
        let tidb_ast::Stmt::Admin(admin) = statement else {
            return Ok(None);
        };
        let tidb_ast::AdminStmt::SplitRegion(split) = admin.as_ref() else {
            return Ok(None);
        };
        let current_database = self.current_database().to_owned();
        self.with_catalog_mut(|catalog| {
            tidb_executor::split_region::prepare_split_region(split, &current_database, catalog)
                .map(Some)
        })
    }
}
