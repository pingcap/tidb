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

//! Transcreation of Go `pkg/config/external_workload.go`: Starter-only
//! external workload coordination config.

use serde::{Deserialize, Serialize};

/// External workload role (Go `ExternalWorkloadRole`, a string type).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct ExternalWorkloadRole(pub String);

/// Regular TiDB role delegating background work (Go `RoleMaster`).
pub const ROLE_MASTER: &str = "master";
/// Keyspace-level GC worker (Go `RoleGCV2Worker`).
pub const ROLE_GCV2_WORKER: &str = "gcv2";
/// TTL worker (Go `RoleTTLTaskWorker`).
pub const ROLE_TTL_TASK_WORKER: &str = "ttl";
/// Auto-analyze worker (Go `RoleAutoAnalyzeWorker`).
pub const ROLE_AUTO_ANALYZE_WORKER: &str = "auto-analyze";

impl ExternalWorkloadRole {
    fn normalized(&self) -> ExternalWorkloadRole {
        ExternalWorkloadRole(self.0.trim().to_lowercase())
    }
    fn valid(&self) -> bool {
        matches!(
            self.0.as_str(),
            ROLE_MASTER | ROLE_GCV2_WORKER | ROLE_TTL_TASK_WORKER | ROLE_AUTO_ANALYZE_WORKER
        )
    }
}

/// Starter-only external workload coordination (Go `ExternalWorkload`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ExternalWorkload {
    /// Whether external workload coordination is enabled.
    #[serde(rename = "enable")]
    pub enable: bool,
    /// The role of this instance.
    #[serde(rename = "role")]
    pub role: ExternalWorkloadRole,
    /// The serving pool name.
    #[serde(rename = "tidb-pool")]
    pub tidb_pool: String,
    /// The external workload controller address.
    #[serde(rename = "controller-addr")]
    pub controller_addr: String,
}

impl ExternalWorkload {
    /// Go `Valid`: normalizes in place, then validates.
    pub fn valid(&mut self) -> Result<(), String> {
        if !self.enable {
            return Ok(());
        }
        self.role = self.role.normalized();
        if self.role.0.is_empty() {
            self.role = ExternalWorkloadRole(ROLE_MASTER.to_string());
        }
        self.controller_addr = self.controller_addr.trim().to_string();
        self.tidb_pool = self.tidb_pool.trim().to_string();
        if self.controller_addr.is_empty() {
            return Err("external-workload controller-addr must not be empty when enabled".into());
        }
        if !self.role.valid() {
            return Err(format!("invalid external-workload role {:?}", self.role.0));
        }
        if self.tidb_pool.is_empty() {
            return Err("external-workload tidb-pool must not be empty when enabled".into());
        }
        Ok(())
    }

    /// Go `isConfigured`.
    pub fn is_configured(&self) -> bool {
        self.enable
            || !self.role.normalized().0.is_empty()
            || !self.controller_addr.trim().is_empty()
            || !self.tidb_pool.trim().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_and_configured() {
        let mut w = ExternalWorkload::default();
        assert!(!w.is_configured());
        assert!(w.valid().is_ok()); // disabled: always valid

        w.enable = true;
        assert!(w.is_configured());
        assert!(w
            .valid()
            .unwrap_err()
            .contains("controller-addr must not be empty"));

        w.controller_addr = " ctrl:8080 ".into();
        assert!(w
            .valid()
            .unwrap_err()
            .contains("tidb-pool must not be empty"));

        w.tidb_pool = "vip-tidb-pool".into();
        w.role = ExternalWorkloadRole(" GCV2 ".into());
        w.valid().unwrap();
        assert_eq!(w.role.0, "gcv2");
        assert_eq!(w.controller_addr, "ctrl:8080");

        // Empty role defaults to master.
        let mut w2 = ExternalWorkload {
            enable: true,
            controller_addr: "c:1".into(),
            tidb_pool: "p".into(),
            ..Default::default()
        };
        w2.valid().unwrap();
        assert_eq!(w2.role.0, "master");

        let mut w3 = ExternalWorkload {
            enable: true,
            controller_addr: "c:1".into(),
            tidb_pool: "p".into(),
            role: ExternalWorkloadRole("bogus".into()),
        };
        assert!(w3
            .valid()
            .unwrap_err()
            .contains("invalid external-workload role"));

        // role alone marks the section configured.
        let w4 = ExternalWorkload {
            role: ExternalWorkloadRole("ttl".into()),
            ..Default::default()
        };
        assert!(w4.is_configured());
    }
}
