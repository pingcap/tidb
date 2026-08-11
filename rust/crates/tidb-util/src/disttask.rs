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

//! Distributed-task executor identity helpers from Go `pkg/util/disttask`.
//!
//! Go receives the full `serverinfo.ServerInfo`, but this package reads only
//! its IP address and SQL port. [`ExecServerInfo`] is that dependency
//! projection. [`ServerInfoSource`] is the native boundary for Go's global
//! `infosync.GetAllServerInfo`; it keeps discovery ownership outside this
//! dependency-leaf utility while preserving the source's error and lookup
//! behavior. The Rust workspace has no distributed-task scheduler or
//! infosync authority yet, so there is deliberately no fabricated live
//! consumer; that future owner must implement this one source trait.

use std::collections::HashMap;

/// The server-info fields used to construct a distributed-task executor ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecServerInfo {
    /// TiDB's advertised IP address or host name.
    pub ip: String,
    /// TiDB's advertised SQL port. Go stores this as `uint`, so values above
    /// the TCP port range remain printable rather than being rejected here.
    pub port: u64,
}

impl ExecServerInfo {
    /// Creates the dependency projection consumed by this package.
    #[must_use]
    pub fn new(ip: impl Into<String>, port: u64) -> Self {
        Self {
            ip: ip.into(),
            port,
        }
    }
}

/// Supplies the current infosync server map.
pub trait ServerInfoSource {
    /// Error returned while loading the current server map.
    type Error;

    /// Returns servers keyed by the infosync ID used by subtask metadata.
    fn all_server_info(&self) -> Result<HashMap<String, ExecServerInfo>, Self::Error>;
}

/// Go `GenerateExecID`: formats the advertised address with
/// `net.JoinHostPort` semantics.
#[must_use]
pub fn generate_exec_id(info: &ExecServerInfo) -> String {
    if info.ip.contains(':') {
        format!("[{}]:{}", info.ip, info.port)
    } else {
        format!("{}:{}", info.ip, info.port)
    }
}

/// Go `MatchServerInfo`: whether `scheduler_id` identifies a listed server.
#[must_use]
pub fn match_server_info(server_infos: &[ExecServerInfo], scheduler_id: &str) -> bool {
    find_server_info(server_infos, scheduler_id).is_some()
}

/// Go `FindServerInfo`: finds the first listed server matching `scheduler_id`.
///
/// Rust uses `Option<usize>` for Go's index-or-`-1` result.
#[must_use]
pub fn find_server_info(server_infos: &[ExecServerInfo], scheduler_id: &str) -> Option<usize> {
    server_infos
        .iter()
        .position(|server| generate_exec_id(server) == scheduler_id)
}

/// Go `GenerateSubtaskExecID`: loads infosync state and formats the server
/// selected by `id`.
///
/// Like Go, discovery errors, an empty server map, and a missing ID all return
/// the empty string.
#[must_use]
pub fn generate_subtask_exec_id(source: &impl ServerInfoSource, id: &str) -> String {
    let Ok(server_infos) = source.all_server_info() else {
        return String::new();
    };
    generate_subtask_exec_id_for_test(&server_infos, id)
}

/// Go `GenerateSubtaskExecID4Test`: resolves `id` from an injected server map.
#[must_use]
pub fn generate_subtask_exec_id_for_test(
    server_infos: &HashMap<String, ExecServerInfo>,
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
            assert_eq!(generate_exec_id(&ExecServerInfo::new(ip, port)), expected);
        }
    }

    #[test]
    fn matching_and_finding_use_the_first_formatted_server_id() {
        let infos = [
            ExecServerInfo::new("10.0.0.1", 4000),
            ExecServerInfo::new("::1", 4000),
            ExecServerInfo::new("10.0.0.1", 4000),
        ];

        assert_eq!(find_server_info(&infos, "10.0.0.1:4000"), Some(0));
        assert_eq!(find_server_info(&infos, "[::1]:4000"), Some(1));
        assert_eq!(find_server_info(&infos, "missing:4000"), None);
        assert!(match_server_info(&infos, "[::1]:4000"));
        assert!(!match_server_info(&infos, "::1:4000"));
    }

    #[derive(Clone)]
    struct Source {
        servers: Option<HashMap<String, ExecServerInfo>>,
    }

    impl ServerInfoSource for Source {
        type Error = ();

        fn all_server_info(&self) -> Result<HashMap<String, ExecServerInfo>, Self::Error> {
            self.servers.clone().ok_or(())
        }
    }

    #[test]
    fn subtask_lookup_preserves_go_error_empty_and_missing_behavior() {
        let error = Source { servers: None };
        assert_eq!(generate_subtask_exec_id(&error, "node-1"), "");

        let empty = Source {
            servers: Some(HashMap::new()),
        };
        assert_eq!(generate_subtask_exec_id(&empty, "node-1"), "");

        let servers = HashMap::from([(
            "node-1".to_owned(),
            ExecServerInfo::new("2001:db8::1", 4000),
        )]);
        let source = Source {
            servers: Some(servers.clone()),
        };
        assert_eq!(
            generate_subtask_exec_id(&source, "node-1"),
            "[2001:db8::1]:4000"
        );
        assert_eq!(generate_subtask_exec_id(&source, "node-2"), "");
        assert_eq!(
            generate_subtask_exec_id_for_test(&servers, "node-1"),
            "[2001:db8::1]:4000"
        );
        assert_eq!(generate_subtask_exec_id_for_test(&servers, "node-2"), "");
    }
}
