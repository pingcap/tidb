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

use super::*;
use crate::test_util::{go_records, MockPdClient};

/// Go `util.HandleTiKVAddress` (then `JoinPath("config")`), against Go.
#[test]
fn handle_tikv_address_matches_go() {
    let stores = [
        metapb::Store { id: 1, address: "127.0.0.1:20160".into(), status_address: "127.0.0.1:20180".into(), ..Default::default() },
        metapb::Store { id: 2, address: "tikv-0:20160".into(), status_address: "0.0.0.0:20180".into(), ..Default::default() },
        metapb::Store { id: 3, address: "[::1]:20160".into(), status_address: "[::2]:20180".into(), ..Default::default() },
        metapb::Store { id: 4, address: "127.0.0.1:20160".into(), ..Default::default() },
        metapb::Store { id: 5, address: "https://h1:20160".into(), status_address: "https://h2:20180".into(), ..Default::default() },
    ];
    for (s, r) in stores.iter().zip(go_records("tikv_addr")) {
        let (out, err) = match handle_tikv_address(s, "http://") {
            Ok(a) => (format!("{}/config", a.trim_end_matches('/')), String::new()),
            Err(e) => (String::new(), e.to_string()),
        };
        assert_eq!(out, r["out"].as_str().unwrap(), "{r}");
        assert_eq!(err, r["err"].as_str().unwrap(), "{r}");
    }
}

/// Go `util.GetAllTiKVStores` under each store behavior.
#[test]
fn get_all_tikv_stores_matches_go() {
    let label = |v: &str| vec![metapb::StoreLabel { key: "engine".into(), value: v.into() }];
    let stores = vec![
        metapb::Store { id: 1, ..Default::default() },
        metapb::Store { id: 2, address: "a".into(), labels: label("tiflash"), ..Default::default() },
        metapb::Store { id: 3, labels: label("tiflash_compute"), ..Default::default() },
    ];
    let pd = MockPdClient::with_stores(stores);
    let ctx = Context::background();
    for (b, r) in [StoreBehavior::ErrorOnTiFlash, StoreBehavior::SkipTiFlash, StoreBehavior::TiFlashOnly]
        .into_iter()
        .zip(go_records("all_stores"))
    {
        let (ids, err) = match get_all_tikv_stores(&ctx, &pd, b) {
            Ok(s) => (s.iter().map(|s| s.id).collect::<Vec<_>>(), String::new()),
            Err(e) => (Vec::new(), e.to_string()),
        };
        let want: Vec<u64> = r["ids"].as_array().unwrap().iter().map(|v| v.as_u64().unwrap()).collect();
        assert_eq!(ids, want, "{r}");
        assert_eq!(err, r["err"].as_str().unwrap(), "{r}");
    }
}

/// Go `config.Parse*FromConfig`.
#[test]
fn kv_config_parsers_match_go() {
    for r in go_records("kvconfig") {
        let input = r["in"].as_str().unwrap().as_bytes();
        let threads = crate::config::parse_import_threads_from_config(input);
        assert_eq!(threads.clone().unwrap_or(0), r["threads"].as_u64().unwrap(), "{r}");
        assert_eq!(threads.is_err(), r["e1"].as_bool().unwrap(), "{r}");
        let merge = crate::config::parse_merge_region_size_from_config(input);
        assert_eq!(merge.clone().unwrap_or((0, 0)), (r["size"].as_u64().unwrap(), r["keys"].as_u64().unwrap()), "{r}");
        assert_eq!(merge.is_err(), !r["e2"].as_str().unwrap().is_empty(), "{r}");
        let log = crate::config::parse_log_backup_enable_from_config(input);
        assert_eq!(log.clone().unwrap_or(false), r["log"].as_bool().unwrap(), "{r}");
        assert_eq!(log.is_err(), r["e3"].as_bool().unwrap(), "{r}");
    }
}
