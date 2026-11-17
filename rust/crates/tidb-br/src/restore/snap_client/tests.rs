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

//! Go `br/pkg/restore/snap_client/{import,tikv_sender,placement_rule_manager}_test.go`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value as Json;
use tidb_ast::CiString;
use tidb_gocontext::Context;
use tidb_model::{GoShared, IndexInfo, PartitionDefinition, PartitionInfo, TableInfo};
use tidb_proto::backup::File;
use tidb_proto::{import_sstpb, kvrpcpb, metapb, pdpb};
use tidb_tablecodec::table_key::encode_table_prefix;

use super::import::get_sst_meta_from_file;
use super::placement_rule_manager::new_placement_rule_manager;
use super::*;
use crate::errors::{self, Result};
use crate::metautil;
use crate::restore::import_client::ImporterClient;
use crate::restore::restorer::{BackupFileSet, BalancedFileImporter, FileImporter};
use crate::restore::split::{RegionInfo, SplitClient};
use crate::restore_utils::{get_rewrite_rules, CreatedTable, RewriteRules, WRITE_CF_NAME};

fn enc(k: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    tidb_codec::encode_bytes(&mut b, k);
    b
}

fn bg() -> Context {
    Context::background()
}

/// Go `TestGetSSTMetaFromFile`.
#[test]
fn get_sst_meta_from_file_clips_to_region() {
    let file = File {
        name: "file_write.sst".into(),
        start_key: b"t1a".to_vec(),
        end_key: b"t1ccc".to_vec(),
        ..Default::default()
    };
    let rule = import_sstpb::RewriteRule {
        old_key_prefix: b"t1".to_vec(),
        new_key_prefix: b"t2".to_vec(),
        ..Default::default()
    };
    let region = metapb::Region {
        start_key: b"t2abc".to_vec(),
        end_key: b"t3a".to_vec(),
        ..Default::default()
    };
    let meta = get_sst_meta_from_file(&file, &region, &rule, RewriteMode::Legacy).unwrap();
    let range = meta.range.unwrap();
    assert_eq!(range.start, b"t2abc");
    assert_eq!(range.end, b"t2\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");
    assert_eq!(meta.cf_name, "write");
}

/// Go `FakeSplitClient`.
#[derive(Default)]
struct FakeSplitClient {
    regions: Vec<RegionInfo>,
    /// Go `flowControlSplitClient`: the scans in flight and their maximum.
    in_flight: AtomicI32,
    max_flight: Option<i32>,
}

impl SplitClient for FakeSplitClient {
    fn get_store(&self, _: &Context, id: u64) -> Result<metapb::Store> {
        Ok(metapb::Store {
            id,
            ..Default::default()
        })
    }
    fn get_region(&self, _: &Context, _: &[u8]) -> Result<Option<RegionInfo>> {
        unimplemented!()
    }
    fn get_region_by_id(&self, _: &Context, _: u64) -> Result<Option<RegionInfo>> {
        unimplemented!()
    }
    fn split_keys_and_scatter(&self, _: &Context, _: &[Vec<u8>]) -> Result<Vec<RegionInfo>> {
        Ok(Vec::new())
    }
    fn split_keys(&self, _: &Context, _: &[Vec<u8>]) -> Result<Vec<RegionInfo>> {
        Ok(Vec::new())
    }
    fn split_wait_and_scatter(
        &self,
        _: &Context,
        _: &RegionInfo,
        _: &[Vec<u8>],
    ) -> Result<Vec<RegionInfo>> {
        unimplemented!()
    }
    fn get_operator(&self, _: &Context, _: u64) -> Result<pdpb::GetOperatorResponse> {
        unimplemented!()
    }
    fn scan_regions(
        &self,
        _: &Context,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<RegionInfo>> {
        if let Some(max) = self.max_flight {
            let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            std::thread::sleep(std::time::Duration::from_millis(1));
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            assert!(cur <= max, "{cur} scans in flight");
            let leader = Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            });
            let region = metapb::Region {
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                ..Default::default()
            };
            return Ok(vec![RegionInfo {
                region,
                leader,
                ..Default::default()
            }]);
        }
        let mut out = Vec::new();
        for r in &self.regions {
            if r.region.start_key.as_slice() <= end && r.region.end_key.as_slice() > start {
                out.push(r.clone());
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
    fn get_placement_rule(
        &self,
        _: &Context,
        _: &str,
        _: &str,
    ) -> Result<tikv_client::pd_http::Rule> {
        unimplemented!()
    }
    fn set_placement_rule(&self, _: &Context, _: &tikv_client::pd_http::Rule) -> Result<()> {
        unimplemented!()
    }
    fn delete_placement_rule(&self, _: &Context, _: &str, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn set_stores_label(&self, _: &Context, _: &[u64], _: &str, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn wait_regions_scattered(&self, _: &Context, _: &[RegionInfo]) -> (usize, Result<()>) {
        (0, Ok(()))
    }
}

/// Go `fakeImporterClient`, with `retryDownloadImporterClient`'s knobs.
#[derive(Default)]
struct FakeImporterClient {
    speed_limit_req: Mutex<HashMap<u64, import_sstpb::SetDownloadSpeedLimitRequest>>,
    support_latest_mvcc: Option<bool>,
    probe_err: Option<errors::Error>,
    first_err: Option<errors::Error>,
    download_calls: AtomicUsize,
    uuids: Mutex<Vec<Vec<u8>>>,
    ingests: AtomicUsize,
}

impl ImporterClient for FakeImporterClient {
    fn download_sst(
        &self,
        _: &Context,
        _: u64,
        req: import_sstpb::DownloadRequest,
    ) -> Result<import_sstpb::DownloadResponse> {
        let sst = req.sst.unwrap_or_default();
        self.uuids.lock().unwrap().push(sst.uuid.clone());
        if self.download_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(err) = &self.first_err {
                return Err(Arc::clone(err));
            }
        }
        Ok(import_sstpb::DownloadResponse {
            range: sst.range,
            ..Default::default()
        })
    }
    fn multi_ingest(
        &self,
        _: &Context,
        _: u64,
        _: import_sstpb::MultiIngestRequest,
    ) -> Result<import_sstpb::IngestResponse> {
        self.ingests.fetch_add(1, Ordering::SeqCst);
        Ok(import_sstpb::IngestResponse::default())
    }
    fn set_download_speed_limit(
        &self,
        _: &Context,
        store_id: u64,
        req: import_sstpb::SetDownloadSpeedLimitRequest,
    ) -> Result<import_sstpb::SetDownloadSpeedLimitResponse> {
        self.speed_limit_req.lock().unwrap().insert(store_id, req);
        Ok(import_sstpb::SetDownloadSpeedLimitResponse::default())
    }
    fn batch_download_latest_mvcc(
        &self,
        _: &Context,
        _: u64,
        _: import_sstpb::DownloadRequest,
    ) -> Result<import_sstpb::DownloadResponse> {
        unimplemented!()
    }
    fn add_force_partition_range(
        &self,
        _: &Context,
        _: u64,
        _: import_sstpb::AddPartitionRangeRequest,
    ) -> Result<()> {
        Ok(())
    }
    fn remove_force_partition_range(
        &self,
        _: &Context,
        _: u64,
        _: import_sstpb::RemovePartitionRangeRequest,
    ) -> Result<()> {
        Ok(())
    }
    fn close_grpc_client(&self) -> Result<()> {
        Ok(())
    }
    fn is_batch_download_latest_mvcc_supported(&self, _: &Context, _: &[u64]) -> Result<bool> {
        if let Some(err) = &self.probe_err {
            return Err(Arc::clone(err));
        }
        Ok(self.support_latest_mvcc.unwrap_or(true))
    }
    fn check_multi_ingest_support(&self, _: &Context, _: &[u64]) -> Result<()> {
        Ok(())
    }
}

/// Go `generateRegions`.
fn generate_regions() -> Vec<RegionInfo> {
    let bounds = [
        vec![],
        enc(&encode_table_prefix(0)),
        enc(&encode_table_prefix(1)),
        enc(&encode_table_prefix(2)),
        enc(&encode_table_prefix(200)),
        vec![],
    ];
    bounds
        .windows(2)
        .enumerate()
        .map(|(i, w)| RegionInfo {
            region: metapb::Region {
                id: i as u64,
                start_key: w[0].clone(),
                end_key: w[1].clone(),
                peers: vec![
                    metapb::Peer {
                        store_id: 1,
                        ..Default::default()
                    },
                    metapb::Peer {
                        store_id: 2,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
            ..Default::default()
        })
        .collect()
}

/// Go `generateFiles`.
fn generate_files() -> (Vec<File>, RewriteRules) {
    let files = (0..10)
        .map(|_| File {
            start_key: encode_table_prefix(100),
            end_key: encode_table_prefix(100),
            ..Default::default()
        })
        .collect();
    let rules = RewriteRules {
        data: vec![import_sstpb::RewriteRule {
            old_key_prefix: encode_table_prefix(100),
            new_key_prefix: encode_table_prefix(1),
            ..Default::default()
        }],
        ..Default::default()
    };
    (files, rules)
}

fn store(id: u64, up: bool, labels: &[(&str, &str)]) -> metapb::Store {
    metapb::Store {
        id,
        state: if up {
            metapb::StoreState::Up
        } else {
            metapb::StoreState::Offline
        } as i32,
        labels: labels
            .iter()
            .map(|(k, v)| metapb::StoreLabel {
                key: (*k).into(),
                value: (*v).into(),
            })
            .collect(),
        ..Default::default()
    }
}

/// Go `generateStores`.
fn generate_stores() -> Vec<metapb::Store> {
    vec![
        store(1, true, &[("engine", "tiflash")]),
        store(2, true, &[("engine", "tikv"), ("exclusive", "restore")]),
        store(3, false, &[("engine", "tikv")]),
        store(4, false, &[("engine", "tikv"), ("exclusive", "restore")]),
    ]
}

/// Go `NewSnapFileImporterOptionsForTest`.
fn options(
    split: Arc<dyn SplitClient>,
    import: Arc<dyn ImporterClient>,
    stores: Vec<metapb::Store>,
    mode: RewriteMode,
    concurrency_per_store: usize,
    scan_concurrency: usize,
) -> SnapFileImporterOptions {
    SnapFileImporterOptions::new(
        None,
        split,
        import,
        None,
        mode,
        stores,
        concurrency_per_store,
        scan_concurrency,
        Vec::new(),
        Vec::new(),
    )
}

/// Go `TestUnproperConfigSnapImporter`.
#[test]
fn unproper_config_snap_importer() {
    let opt = options(
        Arc::new(FakeSplitClient::default()),
        Arc::new(FakeImporterClient::default()),
        Vec::new(),
        RewriteMode::Keyspace,
        0,
        0,
    );
    assert!(SnapFileImporter::new(kvrpcpb::ApiVersion::V1, KvMode::TiDBFull, opt).is_err());
}

/// Go `TestSnapImporter`.
#[test]
fn snap_importer() {
    let split = Arc::new(FakeSplitClient {
        regions: generate_regions(),
        ..Default::default()
    });
    let import = Arc::new(FakeImporterClient::default());
    let opt = options(
        split,
        Arc::clone(&import) as _,
        generate_stores(),
        RewriteMode::Keyspace,
        10,
        0,
    );
    let importer = SnapFileImporter::new(kvrpcpb::ApiVersion::V1, KvMode::TiDBFull, opt).unwrap();
    importer.set_download_speed_limit(&bg(), 1, 5).unwrap();
    let set = import.speed_limit_req.lock().unwrap()[&1].clone();
    assert_eq!(set.speed_limit, 5);
    assert!(!set.task_id.is_empty());
    assert_eq!(set.ttl_seconds, import::DOWNLOAD_RATE_LIMIT_TTL_SECONDS);
    importer.set_download_speed_limit(&bg(), 1, 0).unwrap();
    let reset = import.speed_limit_req.lock().unwrap()[&1].clone();
    assert_eq!(reset.speed_limit, 0);
    assert_eq!(reset.task_id, set.task_id);
    assert_eq!(reset.ttl_seconds, import::DOWNLOAD_RATE_LIMIT_TTL_SECONDS);

    let (files, rules) = generate_files();
    let rules = Arc::new(rules);
    for file in files {
        importer.pause_for_backpressure();
        let set = BackupFileSet {
            sst_files: vec![Arc::new(file)],
            rewrite_rules: Some(Arc::clone(&rules)),
            ..Default::default()
        };
        importer.import(&bg(), &[set]).unwrap();
    }
    importer.close().unwrap();
}

/// Go `TestSnapImporterPDScanRequestFlowControl`.
#[test]
fn snap_importer_pd_scan_request_flow_control() {
    let split = Arc::new(FakeSplitClient {
        max_flight: Some(1),
        ..Default::default()
    });
    let opt = options(
        split,
        Arc::new(FakeImporterClient::default()),
        generate_stores(),
        RewriteMode::Keyspace,
        10,
        1,
    );
    let importer = SnapFileImporter::new(kvrpcpb::ApiVersion::V1, KvMode::TiDBFull, opt).unwrap();
    std::thread::scope(|s| {
        for _ in 0..200 {
            s.spawn(|| {
                importer
                    .paginate_scan_region_for_test(&bg(), &[], &[])
                    .unwrap()
            });
        }
    });
}

/// Go `makeCompactedFileSets(1, 1)`.
fn one_write_file_set() -> BackupFileSet {
    let mut end = encode_table_prefix(100);
    end.push(b'z');
    let file = File {
        name: format!("file-0-0_{WRITE_CF_NAME}.sst"),
        cf: WRITE_CF_NAME.into(),
        start_key: encode_table_prefix(100),
        end_key: end,
        ..Default::default()
    };
    let rules = RewriteRules {
        data: vec![import_sstpb::RewriteRule {
            old_key_prefix: encode_table_prefix(100),
            new_key_prefix: encode_table_prefix(1),
            ..Default::default()
        }],
        ..Default::default()
    };
    BackupFileSet {
        sst_files: vec![Arc::new(file)],
        rewrite_rules: Some(Arc::new(rules)),
        ..Default::default()
    }
}

/// Go `runDownloadRetryUUIDTest`'s importer over one region on store 1.
fn retry_importer(import: Arc<FakeImporterClient>) -> (Arc<SnapFileImporter>, Vec<metapb::Store>) {
    let region = RegionInfo {
        region: metapb::Region {
            id: 1,
            start_key: enc(&encode_table_prefix(1)),
            end_key: enc(&encode_table_prefix(2)),
            peers: vec![metapb::Peer {
                store_id: 1,
                ..Default::default()
            }],
            ..Default::default()
        },
        leader: Some(metapb::Peer {
            store_id: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let split = Arc::new(FakeSplitClient {
        regions: vec![region],
        ..Default::default()
    });
    let stores = vec![metapb::Store {
        id: 1,
        state: metapb::StoreState::Up as i32,
        ..Default::default()
    }];
    let opt = options(split, import, stores.clone(), RewriteMode::Keyspace, 1, 0);
    (
        SnapFileImporter::new(kvrpcpb::ApiVersion::V1, KvMode::TiDBFull, opt).unwrap(),
        stores,
    )
}

/// Go `runDownloadRetryUUIDTest`: the SST UUIDs each download sent.
fn run_download_retry_uuid_test(
    support_peer_retry: bool,
    first_err: tonic::Status,
) -> Vec<Vec<u8>> {
    let import = Arc::new(FakeImporterClient {
        support_latest_mvcc: Some(support_peer_retry),
        first_err: Some(errors::grpc(first_err)),
        ..Default::default()
    });
    let (importer, stores) = retry_importer(Arc::clone(&import));
    importer
        .check_peer_download_retry_support(&bg(), &stores)
        .unwrap();
    importer.import(&bg(), &[one_write_file_set()]).unwrap();
    importer.close().unwrap();
    let uuids = import.uuids.lock().unwrap().clone();
    uuids
}

/// Go `TestDownloadRetriesWithinPeerWhenSupported`.
#[test]
fn download_retries_within_peer_when_supported() {
    let uuids = run_download_retry_uuid_test(true, tonic::Status::cancelled("context canceled"));
    assert_eq!(uuids.len(), 2);
    assert_eq!(uuids[0], uuids[1]);
}

/// Go `TestDownloadKeepsExistingPeerBackoffWhenPeerRetryUnsupported`.
#[test]
fn download_keeps_existing_peer_backoff_when_peer_retry_unsupported() {
    let uuids =
        run_download_retry_uuid_test(false, tonic::Status::unavailable("transport is closing"));
    assert_eq!(uuids.len(), 2);
    assert_eq!(uuids[0], uuids[1]);
}

/// Go `TestCheckPeerDownloadRetrySupportFallsBackOnProbeError`.
#[test]
fn check_peer_download_retry_support_falls_back_on_probe_error() {
    let import = Arc::new(FakeImporterClient {
        support_latest_mvcc: Some(true),
        probe_err: Some(errors::grpc(tonic::Status::unavailable(
            "probe unavailable",
        ))),
        first_err: Some(errors::grpc(tonic::Status::cancelled("context canceled"))),
        ..Default::default()
    });
    let (importer, stores) = retry_importer(Arc::clone(&import));
    importer
        .check_peer_download_retry_support(&bg(), &stores)
        .unwrap();
    assert!(importer.import(&bg(), &[one_write_file_set()]).is_err());
    assert_eq!(import.uuids.lock().unwrap().len(), 1);
    importer.close().unwrap();
}

fn index(id: i64, name: &str) -> IndexInfo {
    IndexInfo {
        id,
        name: CiString::new(name),
        ..IndexInfo::default()
    }
}

fn partitions(defs: Vec<(i64, String)>) -> Option<GoShared<PartitionInfo>> {
    let definitions: Vec<PartitionDefinition> = defs
        .into_iter()
        .map(|(id, name)| PartitionDefinition {
            id,
            name: CiString::new(&name),
            ..PartitionDefinition::default()
        })
        .collect();
    Some(GoShared::new(PartitionInfo {
        definitions: definitions.into(),
        ..PartitionInfo::default()
    }))
}

fn old_table(info: TableInfo, files_of_physicals: BTreeMap<i64, Vec<File>>) -> metautil::Table {
    metautil::Table {
        db: tidb_model::DBInfo {
            name: CiString::new("test"),
            ..Default::default()
        },
        info: Some(info),
        total_kvs: 0,
        total_bytes: 0,
        tiflash_replicas: 0,
        crc64_xor: 0,
        files_of_physicals,
        stats: None,
        stats_file_indexes: Vec::new(),
        is_merge_option_allowed: false,
        partition_merge_option_allowed: HashMap::new(),
    }
}

/// Go `newCreatedTable`.
fn new_created_table(
    old_id: i64,
    new_id: i64,
    old_parts: &[i64],
    new_parts: &[i64],
) -> CreatedTable<metautil::Table> {
    let parts = |ids: &[i64]| {
        partitions(
            ids.iter()
                .enumerate()
                .map(|(i, id)| (*id, i.to_string()))
                .collect(),
        )
    };
    CreatedTable {
        table: Some(TableInfo {
            id: new_id,
            partition: parts(new_parts),
            ..TableInfo::default()
        }),
        old_table: Some(old_table(
            TableInfo {
                id: old_id,
                partition: parts(old_parts),
                ..TableInfo::default()
            },
            BTreeMap::new(),
        )),
        rewrite_rule: None,
    }
}

/// Go `TestGetSortedPhysicalTables`, through the split keys' order: the
/// physical tables come by new ID.
#[test]
fn get_sorted_physical_tables_by_new_id() {
    let tables = vec![
        new_created_table(100, 200, &[32, 145, 324], &[900, 23, 54]),
        new_created_table(300, 400, &[322, 11245, 343224], &[9030, 22353, 5354]),
    ];
    let got = tikv_sender::get_sorted_physical_tables_for_test(&tables);
    let old: Vec<i64> = got.iter().map(|t| t.old_physical_id).collect();
    let new: Vec<i64> = got.iter().map(|t| t.new_physical_id).collect();
    assert_eq!(old, vec![145, 324, 100, 300, 32, 343224, 322, 11245]);
    assert_eq!(new, vec![23, 54, 200, 400, 900, 5354, 9030, 22353]);
}

/// Go `downstreamID`.
fn downstream_id(up: i64) -> i64 {
    up + ((999 - up) % 10 + 1) * 1000
}

fn hex_bytes(s: &Json) -> Vec<u8> {
    hex::decode(s.as_str().unwrap()).unwrap()
}

/// Go `generateCreatedTables` for one case of the capture.
fn generate_created_tables(case: &Json) -> Vec<CreatedTable<metautil::Table>> {
    let indices = || vec![index(1, "idx1"), index(2, "idx2"), index(3, "idx3")];
    let mut out = Vec::new();
    for up in case["upstream_table_ids"].as_array().unwrap() {
        let up = up.as_i64().unwrap();
        let mut files_of = BTreeMap::new();
        if let Some(m) = case["files"].get(up.to_string()) {
            for (phy, fs) in m.as_object().unwrap() {
                let files = fs
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|f| File {
                        name: f["name"].as_str().unwrap().into(),
                        start_key: hex_bytes(&f["start_key"]),
                        end_key: hex_bytes(&f["end_key"]),
                        total_kvs: f["total_kvs"].as_u64().unwrap(),
                        total_bytes: f["total_bytes"].as_u64().unwrap(),
                        cf: f["cf"].as_str().unwrap().into(),
                        ..Default::default()
                    })
                    .collect();
                files_of.insert(phy.parse::<i64>().unwrap(), files);
            }
        }
        let mut table = TableInfo {
            id: downstream_id(up),
            name: CiString::new(format!("tbl-{up}")),
            indices: indices().into(),
            ..TableInfo::default()
        };
        let mut info = TableInfo {
            id: up,
            indices: indices().into(),
            ..TableInfo::default()
        };
        if let Some(ids) = case["upstream_partition_ids"].get(up.to_string()) {
            let ids: Vec<i64> = ids
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect();
            table.partition = partitions(
                ids.iter()
                    .map(|id| (downstream_id(*id), format!("p_{id}")))
                    .collect(),
            );
            info.partition = partitions(ids.iter().map(|id| (*id, format!("p_{id}"))).collect());
        }
        let rules = get_rewrite_rules(&table, &info, 0, true);
        out.push(CreatedTable {
            table: Some(table),
            old_table: Some(old_table(info, files_of)),
            rewrite_rule: Some(rules),
        });
    }
    // Go shuffles the tables; the result must not depend on their order.
    out.reverse();
    out
}

/// Go `TestSortAndValidateFileRanges`, over its cases captured from Go
/// master (`testdata/sort_validate_go.json`).
#[test]
fn sort_and_validate_file_ranges_matches_go() {
    let _s = crate::restore::split::tests::serial();
    let cases: Vec<Json> =
        serde_json::from_str(include_str!("../../../testdata/sort_validate_go.json")).unwrap();
    assert_eq!(cases.len(), 16);
    for (i, case) in cases.iter().enumerate() {
        let tables = generate_created_tables(case);
        let mut cpt: HashMap<i64, HashSet<String>> = HashMap::new();
        if let Some(m) = case["checkpoint"].as_object() {
            for (id, keys) in m {
                cpt.insert(
                    id.parse().unwrap(),
                    keys.as_array()
                        .unwrap()
                        .iter()
                        .map(|k| k.as_str().unwrap().to_owned())
                        .collect(),
                );
            }
        }
        let (keys, groups) = sort_and_validate_file_ranges(
            &tables,
            &cpt,
            case["split_size_bytes"].as_u64().unwrap(),
            case["split_key_count"].as_u64().unwrap(),
            case["split_on_table"].as_bool().unwrap(),
        )
        .unwrap();
        let want_keys: Vec<Vec<u8>> = case["split_keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(hex_bytes)
            .collect();
        assert_eq!(keys, want_keys, "case {i}");
        let want = case["groups"].as_array().unwrap();
        assert_eq!(groups.len(), want.len(), "case {i}");
        for (g, w) in groups.iter().zip(want) {
            let w = w.as_array().unwrap();
            assert_eq!(g.len(), w.len(), "case {i}");
            for (set, ws) in g.iter().zip(w) {
                assert_eq!(set.table_id, ws["table_id"].as_i64().unwrap(), "case {i}");
                // Go compares the expected names only, as far as they go.
                for (f, name) in set.sst_files.iter().zip(ws["names"].as_array().unwrap()) {
                    assert_eq!(f.name, name.as_str().unwrap(), "case {i}");
                }
            }
        }
    }
}

/// Go `TestContextManagerOffline`.
#[test]
fn placement_rule_manager_offline() {
    let pd = Arc::new(crate::test_util::MockPdClient::default());
    let rt = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );
    let mut m = new_placement_rule_manager(&bg(), pd, None, Default::default(), rt, false).unwrap();
    let tables = vec![new_created_table(100, 200, &[1], &[2])];
    m.set_placement_rule(&bg(), &tables).unwrap();
    m.reset_placement_rules(&bg()).unwrap();
}

/// Go `TestContextManagerOnlineNoStores`: no up TiKV store has the label.
#[test]
fn placement_rule_manager_online_no_stores() {
    let pd = Arc::new(crate::test_util::MockPdClient::with_stores(vec![
        store(1, true, &[("engine", "tiflash")]),
        store(2, false, &[("engine", "tikv")]),
    ]));
    let rt = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );
    let mut m = new_placement_rule_manager(&bg(), pd, None, Default::default(), rt, true).unwrap();
    let tables = vec![new_created_table(100, 200, &[1], &[2])];
    m.set_placement_rule(&bg(), &tables).unwrap();
    m.reset_placement_rules(&bg()).unwrap();
}
