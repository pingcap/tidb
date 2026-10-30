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

//! What Go's executors reach through the session's `Domain`
//! (`domain.GetDomain(sctx)`): the DDL owner that runs a job, the privilege
//! tables and their cache, the global variables, the analyzer and the
//! statistics handle. A cluster node
//! installs them on every session it opens; an in-process session has none,
//! its own catalog being the whole world.

use std::sync::Arc;

use tidb_executor::analyze::exec::AnalyzeService;
use crate::ddl_service::DdlService;
use tidb_executor::stats_exec::StatsStatementService;
use crate::stored_state_service::{AccountService, SysvarService};
use crate::Session;

/// A service that carries the running statement's own state (its resource
/// group, its memory), built for each statement from the session it runs
/// on.
pub type StatementService<S> = Arc<dyn Fn(&Session) -> Box<S> + Send + Sync>;

/// The domain's services one session's executors use.
#[derive(Clone)]
pub struct DomainServices {
    /// Go `ddl.DDL` (`DoDDLJob`).
    pub ddl: Arc<dyn DdlService + Send + Sync>,
    /// The privilege tables, a system session to write them on, and the
    /// privilege cache's reload.
    pub accounts: Arc<dyn AccountService + Send + Sync>,
    /// `mysql.global_variables` (Go `SetGlobalSysVar`).
    pub sysvars: Arc<dyn SysvarService + Send + Sync>,
    /// The analyzer and the statistics it saves (Go `AnalyzeExec`'s handle).
    pub analyze: StatementService<dyn AnalyzeService>,
    /// The statistics handle's statement surface: LOAD STATS, FLUSH
    /// STATS_DELTA, DROP STATS and the statistics locks.
    pub stats: StatementService<dyn StatsStatementService>,
}

impl Session {
    /// Installs the domain's services (a cluster node's
    /// `attach_connection`).
    pub fn install_domain_services(&mut self, services: DomainServices) {
        self.domain_services = Some(services);
    }

    /// The domain's services; `None` for an in-process session.
    #[must_use]
    pub fn domain_services(&self) -> Option<&DomainServices> {
        self.domain_services.as_ref()
    }
}
