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

//! Source tests for Go `pkg/config/config_test.go::TestExternalWorkloadValid`.

use tidb_config::config_tree::new_config;
use tidb_config::deploymode::Mode;
use tidb_config::external_workload::ROLE_GCV2_WORKER;

#[test]
fn external_workload_valid_matches_source() {
    let mut config = new_config();
    config.valid().unwrap();

    config.external_workload.enable = true;
    assert!(config
        .valid()
        .unwrap_err()
        .contains("external-workload can only be configured when deploy-mode is starter"));

    let mut config = new_config();
    assert!(config
        .load_str("tidb.toml", "[external-workload]\nenable = false\n")
        .unwrap_err()
        .to_string()
        .contains("external-workload can only be configured when deploy-mode is starter"));

    if !tidb_config::kerneltype::is_next_gen() {
        return;
    }

    let mut config = new_config();
    config.deploy_mode = Mode::Starter;
    config.external_workload.enable = true;
    assert!(config
        .valid()
        .unwrap_err()
        .contains("external-workload controller-addr must not be empty"));

    config.external_workload.controller_addr = "http://127.0.0.1:1234".to_owned();
    config.external_workload.tidb_pool.clear();
    assert!(config
        .valid()
        .unwrap_err()
        .contains("external-workload tidb-pool must not be empty"));

    config.external_workload.tidb_pool = "pool-a".to_owned();
    config.external_workload.role.0 = "unknown".to_owned();
    assert!(config
        .valid()
        .unwrap_err()
        .contains(r#"invalid external-workload role "unknown""#));

    config.external_workload.role.0 = " GCV2 ".to_owned();
    config.valid().unwrap();
    assert_eq!(config.external_workload.role.0, ROLE_GCV2_WORKER);
}
