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

//! A live check of the snapshot restore's split, scatter, download and
//! ingest against a real cluster: a full backup's tables are restored into
//! unused table IDs (no schema is created; the restore task does that), and
//! every restored KV is compared with the source table's.
//!
//! Run with a cluster whose TiKV can read the backup:
//! `E07_PD=127.0.0.1:2379 E07_BACKUP=local:///path cargo test -p tidb-br
//! --test restore_live -- --ignored --nocapture`; `E07_SPLIT_ON_TABLE=0`
//! merges ranges across tables, so the regions are split at their ends.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tidb_br::glue::{Glue, GlueClient, Progress};
use tidb_br::pdclient::{PdClient, TikvPdClient};
use tidb_br::restore::import_mode_switcher::ImportModeSwitcher;
use tidb_br::restore::snap_client::{RestoreTablesContext, SnapClient};
use tidb_br::restore_utils::{get_rewrite_rules, CreatedTable};
use tidb_br::utils::store_manager::Keepalive;
use tidb_gocontext::Context;
use tidb_model::{GoShared, PartitionDefinition, PartitionInfo, TableInfo};
use tidb_tablecodec::table_key::encode_table_prefix;

struct NoProgress;

impl Progress for NoProgress {
    fn inc(&self) {}
    fn inc_by(&self, _: i64) {}
    fn get_current(&self) -> i64 {
        0
    }
    fn close(&self) {}
}

/// A glue that only reports progress.
struct ProgressGlue;

impl Glue for ProgressGlue {
    fn get_domain(&self) -> tidb_br::errors::Result<Arc<dyn tidb_br::glue::Domain>> {
        unimplemented!()
    }
    fn create_session(&self) -> tidb_br::errors::Result<Box<dyn tidb_br::glue::Session>> {
        unimplemented!()
    }
    fn open(&self) -> tidb_br::errors::Result<Arc<dyn tidb_br::glue::Storage>> {
        unimplemented!()
    }
    fn owns_storage(&self) -> bool {
        false
    }
    fn start_progress(&self, _: &Context, _: &str, _: i64, _: bool) -> Arc<dyn Progress> {
        Arc::new(NoProgress)
    }
    fn record(&self, _: &str, _: u64) {}
    fn get_version(&self) -> String {
        String::new()
    }
    fn use_one_shot_session(
        &self,
        _: bool,
        _: &mut dyn FnMut(&dyn tidb_br::glue::Session) -> tidb_br::errors::Result<()>,
    ) -> tidb_br::errors::Result<()> {
        unimplemented!()
    }
    fn get_client(&self) -> GlueClient {
        GlueClient::Sql
    }
}

/// The table with new IDs from `next`, partitions too.
fn renumber(old: &TableInfo, next: &mut i64, ids: &mut BTreeMap<i64, i64>) -> TableInfo {
    let mut t = old.clone();
    let mut bump = |old_id: i64| {
        *next += 1;
        ids.insert(old_id, *next);
        *next
    };
    t.id = bump(old.id);
    if let Some(part) = &old.partition {
        let mut p: PartitionInfo = part.read().clone();
        let defs: Vec<PartitionDefinition> = part.read().definitions.with_visible(|d| d.to_vec());
        let defs: Vec<PartitionDefinition> = defs
            .into_iter()
            .map(|mut d| {
                d.id = bump(d.id);
                d
            })
            .collect();
        p.definitions = defs.into();
        t.partition = Some(GoShared::new(p));
    }
    t
}

/// Every KV under a table prefix now, keys without the prefix.
fn scan_table(rt: &tokio::runtime::Runtime, pd: &str, table_id: i64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    rt.block_on(async {
        let client = tikv_client::TransactionClient::new(vec![pd.to_owned()])
            .await
            .unwrap();
        let ts = client.current_timestamp().await.unwrap();
        let mut snap = client.snapshot(ts, tikv_client::TransactionOptions::new_optimistic());
        let (start, end) = (
            encode_table_prefix(table_id),
            encode_table_prefix(table_id + 1),
        );
        let prefix = start.len();
        let mut out = BTreeMap::new();
        let mut from = start;
        loop {
            let batch: Vec<_> = snap
                .scan(from.clone()..end.clone(), 10_000)
                .await
                .unwrap()
                .collect();
            let n = batch.len();
            for kv in batch {
                let k: Vec<u8> = kv.0.into();
                from = k.clone();
                from.push(0);
                out.insert(k[prefix..].to_vec(), kv.1);
            }
            if n < 10_000 {
                return out;
            }
        }
    })
}

#[test]
#[ignore = "needs a live cluster: E07_PD and E07_BACKUP"]
fn restore_tables_into_new_ids_matches_source() {
    let (Ok(pd_addr), Ok(backup)) = (std::env::var("E07_PD"), std::env::var("E07_BACKUP")) else {
        panic!("set E07_PD and E07_BACKUP");
    };
    let ctx = Context::background();
    let store = tidb_txnkv::driver::tikv_store::TikvStore::connect(
        vec![pd_addr.clone()],
        tikv_client::Config::default(),
    )
    .unwrap();
    let rt = Arc::clone(store.runtime());
    let pd_http = store.pd_http_client();
    let pd: Arc<dyn PdClient> = Arc::new(TikvPdClient::new(store));

    let cipher = tidb_proto::backup::CipherInfo {
        cipher_type: tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32,
        ..Default::default()
    };
    let reader = tidb_br::task::read_backup_meta(&backup, &cipher).unwrap();
    let (backend, _) = tidb_br::task::get_storage(
        &ctx,
        &backup,
        &tidb_objstore::BackendOptions::default(),
        false,
        false,
    )
    .unwrap();
    let tables = reader
        .read_schemas_files_with_options(
            &ctx,
            tidb_br::metautil::ReadSchemaConfig {
                skip_files: false,
                skip_stats: true,
            },
        )
        .unwrap();
    let mut next = 900_000_000 + i64::from(std::process::id() % 1000) * 1000;
    let first = next + 1;
    let mut ids = BTreeMap::new();
    let mut created = Vec::new();
    for t in tables {
        let Some(info) = t.info.clone() else { continue };
        if info.is_view() || info.is_sequence() {
            continue;
        }
        let new = renumber(&info, &mut next, &mut ids);
        let rules = get_rewrite_rules(&new, &info, 0, true);
        created.push(CreatedTable {
            table: Some(new),
            old_table: Some(t),
            rewrite_rule: Some(rules),
        });
    }
    assert!(!created.is_empty(), "the backup has no table");
    eprintln!("restoring {} tables, physical IDs {ids:?}", created.len());

    let switcher = ImportModeSwitcher::new(
        Arc::clone(&pd),
        Duration::from_secs(10),
        Default::default(),
        Arc::clone(&rt),
    );
    switcher.go_switch_to_import_mode(&ctx).unwrap();
    let mut client = SnapClient::new(
        Arc::clone(&pd),
        pd_http,
        Default::default(),
        Keepalive {
            time: Duration::from_secs(10),
            timeout: Duration::from_secs(3),
        },
        Arc::clone(&rt),
    );
    client.set_rewrite_mode(&ctx);
    eprintln!("rewrite mode {:?}", client.get_rewrite_mode());
    client.set_concurrency_per_store(128);
    client
        .init_clients(&ctx, Some(backend), tidb_proto::kvrpcpb::ApiVersion::V1)
        .unwrap();
    let mut protect_start = Vec::new();
    tidb_codec::encode_bytes(&mut protect_start, &encode_table_prefix(first));
    let mut protect_end = Vec::new();
    tidb_codec::encode_bytes(&mut protect_end, &encode_table_prefix(next + 1));
    let cpt = HashMap::<i64, HashSet<String>>::new();
    let res = client.restore_tables(
        &ctx,
        &RestoreTablesContext {
            log_progress: false,
            split_size_bytes: 96 << 20,
            split_key_count: 960_000,
            split_on_table: std::env::var("E07_SPLIT_ON_TABLE").map_or(true, |v| v != "0"),
            online: false,
            created_tables: &created,
            checkpoint_set_with_table_id: &cpt,
            compact_protect_start_key: protect_start,
            compact_protect_end_key: protect_end,
            glue: &ProgressGlue,
        },
    );
    client.close();
    switcher.switch_to_normal_mode(&ctx).unwrap();
    res.unwrap();

    let check_rt = tokio::runtime::Runtime::new().unwrap();
    let mut total = 0;
    for (old_id, new_id) in &ids {
        let src = scan_table(&check_rt, &pd_addr, *old_id);
        let dst = scan_table(&check_rt, &pd_addr, *new_id);
        eprintln!(
            "physical {old_id} -> {new_id}: {} source KVs, {} restored",
            src.len(),
            dst.len()
        );
        assert_eq!(src, dst, "physical table {old_id} -> {new_id}");
        total += dst.len();
    }
    assert!(total > 0);
}
