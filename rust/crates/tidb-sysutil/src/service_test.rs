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

//! Go `service_test.go`.

use std::time::Duration;

use tidb_proto::diagnosticspb as pb;

use crate::search_log_test::serve;
use crate::DiagnosticsServer;

#[test]
fn test_rpc_server_info() {
    let s = serve(tempfile::tempdir().unwrap(), DiagnosticsServer::default());
    let address = format!("http://{}", s.address);
    s.runtime.block_on(async move {
        // Set up a connection to the server.
        let mut client = pb::diagnostics_client::DiagnosticsClient::connect(address)
            .await
            .unwrap();
        // Contact the server and print out its response.
        for tp in [
            // Test for load info.
            pb::ServerInfoType::LoadInfo,
            // Test for hardware info.
            pb::ServerInfoType::HardwareInfo,
            // Test for system info.
            pb::ServerInfoType::SystemInfo,
        ] {
            let r = tokio::time::timeout(
                Duration::from_secs(5),
                client.server_info(pb::ServerInfoRequest { tp: tp as i32 }),
            )
            .await
            .unwrap()
            .unwrap()
            .into_inner();
            assert_ne!(r.items.len(), 0, "{tp:?}");
        }
    });
}

/// Go `ServerInfo` orders items by type, then name, and `All` is the three
/// kinds together.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn server_info_items_are_sorted_and_all_unions_the_kinds() {
    let hardware = crate::server_info_items(pb::ServerInfoType::HardwareInfo as i32);
    let keys: Vec<(String, String)> = hardware
        .iter()
        .map(|item| (item.tp.clone(), item.name.clone()))
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    let cpu = hardware.iter().find(|item| item.tp == "cpu").unwrap();
    let pair_keys: Vec<&str> = cpu.pairs.iter().map(|pair| pair.key.as_str()).collect();
    assert_eq!(
        pair_keys,
        [
            "cpu-arch",
            "cpu-logical-cores",
            "cpu-physical-cores",
            "cpu-frequency",
            "cache"
        ]
    );
    assert!(hardware
        .iter()
        .any(|item| item.tp == "memory" && item.name == "memory"));
    let all = crate::server_info_items(pb::ServerInfoType::All as i32);
    for kind in ["cpu", "memory", "net", "system"] {
        assert!(all.iter().any(|item| item.tp == kind), "{kind}");
    }
    assert!(all
        .iter()
        .any(|item| item.tp == "cpu" && item.name == "usage"));
    assert!(crate::server_info_items(99).is_empty());
}

/// Go `GetMemoryCapacity` answers the registered reader once one is set.
#[test]
fn registered_memory_capacity_replaces_the_host_reading() {
    if let Ok(host) = crate::get_memory_capacity() {
        assert!(host > 0);
    }
    fn fixed() -> Result<u64, String> {
        Ok(42)
    }
    crate::register_get_memory_capacity(fixed);
    assert_eq!(crate::get_memory_capacity(), Ok(42));
    let hardware = crate::server_info_items(pb::ServerInfoType::HardwareInfo as i32);
    let memory = hardware
        .iter()
        .find(|item| item.tp == "memory" && item.name == "memory")
        .unwrap();
    assert_eq!(memory.pairs[0].value, "42");
}
