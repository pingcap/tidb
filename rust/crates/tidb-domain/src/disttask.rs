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

//! Complete transcreation of Go `pkg/util/disttask` (`idservice.go`):
//! distributed-task executor identity helpers.
//!
//! This module lives in `tidb-domain` because the Go package imports
//! `pkg/domain/{infosync,serverinfo}`. The test-only lookup accepts the mock
//! server map explicitly in place of Go's package-global mock manager.

use std::collections::HashMap;

use crate::serverinfo::ServerInfo;
use crate::serverinfo_syncer::Syncer;

/// Go `GenerateExecID`: formats the advertised IP and SQL port with
/// `net.JoinHostPort` semantics.
#[must_use]
pub fn generate_exec_id(info: &ServerInfo) -> String {
    if info.static_info.ip.contains(':') {
        format!("[{}]:{}", info.static_info.ip, info.static_info.port)
    } else {
        format!("{}:{}", info.static_info.ip, info.static_info.port)
    }
}

/// Go `MatchServerInfo`: whether `scheduler_id` identifies a listed server.
#[must_use]
pub fn match_server_info(server_infos: &[ServerInfo], scheduler_id: &str) -> bool {
    find_server_info(server_infos, scheduler_id) >= 0
}

/// Go `FindServerInfo`: returns the first matching index, or `-1`.
#[must_use]
pub fn find_server_info(server_infos: &[ServerInfo], scheduler_id: &str) -> isize {
    server_infos
        .iter()
        .position(|server| generate_exec_id(server) == scheduler_id)
        .map_or(-1, |index| index as isize)
}

/// Go `GenerateSubtaskExecID`: resolves `id` from current infosync state.
///
/// Discovery errors, an empty server map, and a missing ID all return the
/// empty string.
#[must_use]
pub fn generate_subtask_exec_id(syncer: &Syncer, id: &str) -> String {
    let Ok(server_infos) = syncer.all_server_info() else {
        return String::new();
    };
    generate_subtask_exec_id_for_test(&server_infos, id)
}

/// Go `GenerateSubtaskExecID4Test`: resolves `id` from mock server state.
#[must_use]
pub fn generate_subtask_exec_id_for_test(
    server_infos: &HashMap<String, ServerInfo>,
    id: &str,
) -> String {
    server_infos
        .get(id)
        .map(generate_exec_id)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serverinfo::StaticInfo;

    #[test]
    fn generate_exec_id_matches_every_go_test_vector() {
        for (ip, port, expected) in [
            ("", 0, ":0"),
            ("10.124.122.25", 3456, "10.124.122.25:3456"),
            ("10.124", 3456, "10.124:3456"),
            ("", 65537, ":65537"),
            (
                "ABCD:EF01:2345:6789:ABCD:EF01:2345:6789",
                65537,
                "[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:65537",
            ),
        ] {
            let info = ServerInfo {
                static_info: StaticInfo {
                    ip: ip.to_owned(),
                    port,
                    ..StaticInfo::default()
                },
                ..ServerInfo::default()
            };
            assert_eq!(generate_exec_id(&info), expected);
        }
    }
}
