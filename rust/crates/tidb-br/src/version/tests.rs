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

//! Go `version_test.go`'s cases, against a Go master capture
//! (`zzprobe/e07conn`, `testdata/conn_go.jsonl`) that widens
//! `TestCheckClusterVersion` to every release, store version and checker
//! pairing and records Go's exact error texts.

use std::sync::Mutex;

use serde_json::Value as Json;
use tidb_proto::metapb;

use super::*;
use crate::test_util::{go_records, MockPdClient};

/// The release version and checkpoint error are process-wide, like Go's.
static SERIAL: Mutex<()> = Mutex::new(());

type Checker = Box<dyn Fn(&metapb::Store, &Version) -> Result<()>>;

fn checker(name: &str) -> Checker {
    match name {
        "br" => Box::new(check_version_for_br),
        "pitr" => Box::new(check_version_for_br_pitr),
        "ddl" => Box::new(check_version_for_ddl),
        "keyspace" => Box::new(check_version_for_keyspace_br),
        "backup4.0.12" => Box::new(check_version_for_backup(tidb_go_semver::new("4.0.12"))),
        "backup5.0.0-rc" => Box::new(check_version_for_backup(tidb_go_semver::new("5.0.0-rc"))),
        "backup6.0.0" => Box::new(check_version_for_backup(tidb_go_semver::new("6.0.0"))),
        other => panic!("{other}"),
    }
}

fn err_text(r: Result<()>) -> String {
    r.err().map(|e| e.to_string()).unwrap_or_default()
}

/// Go `TestCheckClusterVersion`, every captured pairing.
#[test]
fn test_check_cluster_version() {
    let _g = SERIAL.lock().unwrap();
    let ctx = tidb_gocontext::Context::background();
    for r in go_records("cluster") {
        let s = |k: &str| r[k].as_str().unwrap().to_owned();
        build::set_release_version(Some(&s("release")));
        let store = if r["tiflash"].as_bool().unwrap() {
            metapb::Store {
                id: 2,
                peer_address: "127.0.0.1:3930".into(),
                version: s("store"),
                labels: vec![metapb::StoreLabel { key: "engine".into(), value: "tiflash".into() }],
                ..Default::default()
            }
        } else {
            metapb::Store { id: 1, address: "127.0.0.1:20160".into(), version: s("store"), ..Default::default() }
        };
        let pd = MockPdClient::with_stores(vec![store]);
        let ck = checker(&s("checker"));
        let got = err_text(check_cluster_version(&ctx, &pd, ck.as_ref()));
        assert_eq!(got, s("err"), "{r}");
        let tiflash = r["tiflash"].as_bool().unwrap();
        if let Some(cp) = r.get("checkpoint").filter(|_| !tiflash) {
            assert_eq!(err_text(check_checkpoint_support()), cp.as_str().unwrap(), "{r}");
        }
        if let Some(b) = r.get("pitr_batch").filter(|_| !tiflash) {
            assert_eq!(check_pitr_support_batch_kv_files(), b.as_bool().unwrap(), "{r}");
        }
    }
    build::set_release_version(None);
}

/// Go `TestNextMajorVersion`.
#[test]
fn test_next_major_version() {
    let _g = SERIAL.lock().unwrap();
    for r in go_records("next_major") {
        build::set_release_version(Some(r["release"].as_str().unwrap()));
        let v = next_major_version();
        assert_eq!(v.to_string(), r["out"].as_str().unwrap(), "{r}");
        assert_eq!(Json::from(v.major), r["major"], "{r}");
    }
    build::set_release_version(None);
}

/// Go `TestCompareVersion`.
#[test]
fn test_compare_version() {
    let n = tidb_go_semver::new;
    assert_eq!(n("4.0.0-rc").compare(&n("4.0.0-rc.2")), -1);
    assert_eq!(n("4.0.0-beta.3").compare(&n("4.0.0-rc.2")), -1);
    assert_eq!(n("4.0.0-rc.1").compare(&n("4.0.0")), -1);
    assert_eq!(n("4.0.0-beta.1").compare(&n("4.0.0")), -1);
    assert_eq!(n(&remove_v_and_hash("4.0.0-rc-35-g31dae220")).compare(&n("4.0.0-rc.2")), -1);
    assert_eq!(n(&remove_v_and_hash("4.0.0-9-g30f0b014")).compare(&n("4.0.0-rc.1")), 1);
    assert_eq!(n(&remove_v_and_hash("v3.0.0-beta-211-g09beefbe0-dirty")).compare(&n("3.0.0-beta")), 0);
    assert_eq!(n(&remove_v_and_hash("v3.0.5-dirty")).compare(&n("3.0.5")), 0);
    assert_eq!(n(&remove_v_and_hash("v3.0.5-beta.12-dirty")).compare(&n("3.0.5-beta.12")), 0);
    assert_eq!(n(&remove_v_and_hash("v2.1.0-rc.1-7-g38c939f-dirty")).compare(&n("2.1.0-rc.1")), 0);
}

/// Go `TestExtractTiDBVersion`.
#[test]
fn test_extract_tidb_version() {
    for r in go_records("extract") {
        let res = extract_tidb_version(r["in"].as_str().unwrap());
        let (out, err) = match res {
            Ok(v) => (v.to_string(), String::new()),
            Err(e) => (String::new(), e.to_string()),
        };
        assert_eq!(out, r["out"].as_str().unwrap(), "{r}");
        assert_eq!(err, r["err"].as_str().unwrap(), "{r}");
    }
}

/// Go `TestCheckVersion`.
#[test]
fn test_check_version() {
    for r in go_records("check_version") {
        let v: Vec<Version> = r["in"].as_array().unwrap().iter().map(|s| tidb_go_semver::new(s.as_str().unwrap())).collect();
        assert_eq!(err_text(check_version("TiNB", &v[0], &v[1], &v[2])), r["err"].as_str().unwrap(), "{r}");
    }
}

/// Go `TestNormalizeBackupVersion`.
#[test]
fn test_normalize_backup_version() {
    for r in go_records("normalize") {
        let out = normalize_backup_version(r["in"].as_str().unwrap()).map_or("<nil>".to_owned(), |v| v.to_string());
        assert_eq!(out, r["out"].as_str().unwrap(), "{r}");
    }
}

/// Go `TestDetectServerInfo` and `TestParseServerInfoNextGen`.
#[test]
fn test_parse_server_info() {
    for r in go_records("server_info") {
        let info = parse_server_info(r["in"].as_str().unwrap());
        assert_eq!(info.server_type.to_string(), r["type"].as_str().unwrap(), "{r}");
        assert_eq!(info.server_version.to_string(), r["version"].as_str().unwrap(), "{r}");
        assert_eq!(info.next_gen, r["next_gen"].as_bool().unwrap(), "{r}");
    }
}
