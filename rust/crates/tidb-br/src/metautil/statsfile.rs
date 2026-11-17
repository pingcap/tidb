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

//! Go `br/pkg/metautil/statsfile.go`: a table's statistics in
//! `backupmeta.schema.stats.<physical ID>` files, or inline when small,
//! written at backup and loaded back at restore.

use prost::Message;
use sha2::{Digest, Sha256};
use tidb_gocontext::Context;
use tidb_proto::backup::{CipherInfo, StatsBlock, StatsFile, StatsFileIndex};

use super::Storage;

/// Go `maxStatsJsonTableSize`.
const MAX_STATS_JSON_TABLE_SIZE: usize = 32 * 1024 * 1024;
/// Go `inlineSize`.
const INLINE_SIZE: usize = 8 * 1024;

/// Go `getStatsFileName`.
#[must_use]
pub fn get_stats_file_name(physical_id: i64) -> String {
    format!("backupmeta.schema.stats.{physical_id:09}")
}

/// Go `StatsWriter`.
pub struct StatsWriter {
    storage: Storage,
    cipher: Option<CipherInfo>,
    stats_file_indexes: Vec<StatsFileIndex>,
    total_size: usize,
    stats_file: StatsFile,
}

impl StatsWriter {
    /// Go `newStatsWriter`.
    #[must_use]
    pub fn new(storage: Storage, cipher: Option<CipherInfo>) -> Self {
        Self {
            storage,
            cipher,
            stats_file_indexes: Vec::new(),
            total_size: 0,
            stats_file: StatsFile::default(),
        }
    }

    /// Go `writeStatsFileAndClear`: the first small file is inlined.
    fn write_stats_file_and_clear(
        &mut self,
        ctx: &Context,
        physical_id: i64,
    ) -> Result<(), String> {
        let file_name = get_stats_file_name(physical_id);
        let content = std::mem::take(&mut self.stats_file).encode_to_vec();
        self.total_size = 0;
        if self.stats_file_indexes.is_empty() && content.len() < INLINE_SIZE {
            self.stats_file_indexes.push(StatsFileIndex {
                inline_data: content,
                ..Default::default()
            });
            return Ok(());
        }
        let checksum = Sha256::digest(&content).to_vec();
        let size_ori = content.len() as u64;
        let (encrypted, iv) = super::writer::encrypt(&content, self.cipher.as_ref())?;
        self.storage
            .write_file(ctx, &file_name, &encrypted)
            .map_err(|e| e.to_string())?;
        self.stats_file_indexes.push(StatsFileIndex {
            name: file_name,
            sha256: checksum,
            size_enc: encrypted.len() as u64,
            size_ori,
            cipher_iv: iv,
            ..Default::default()
        });
        Ok(())
    }

    /// Go `BackupStats`: `json_table` is the Go `util.JSONTable` JSON;
    /// `None` (Go's nil table) is skipped.
    ///
    /// # Errors
    /// The write error.
    pub fn backup_stats(
        &mut self,
        ctx: &Context,
        json_table: Option<Vec<u8>>,
        physical_id: i64,
    ) -> Result<(), String> {
        let Some(stats_bytes) = json_table else {
            return Ok(());
        };
        self.total_size += stats_bytes.len();
        self.stats_file.blocks.push(StatsBlock {
            physical_id,
            json_table: stats_bytes,
        });
        if self.total_size > MAX_STATS_JSON_TABLE_SIZE {
            self.write_stats_file_and_clear(ctx, physical_id)?;
        }
        Ok(())
    }

    /// Go `BackupStatsDone`: the file indexes of the table's statistics.
    ///
    /// # Errors
    /// The write error.
    pub fn backup_stats_done(&mut self, ctx: &Context) -> Result<Vec<StatsFileIndex>, String> {
        if self.total_size == 0 || self.stats_file.blocks.is_empty() {
            return Ok(self.stats_file_indexes.clone());
        }
        let physical_id = self.stats_file.blocks[0].physical_id;
        self.write_stats_file_and_clear(ctx, physical_id)?;
        Ok(self.stats_file_indexes.clone())
    }
}

/// Go `RestoreStats`: each partition's statistics downloaded, its physical
/// ID rewritten, and loaded into `new_table_info`. Go downloads with four
/// workers into a channel the stats handle drains concurrently; here the
/// files are read in order and handed over in one call.
///
/// # Errors
/// A read, decrypt, checksum or decode error, an ID without a rewrite, or
/// the load error.
pub fn restore_stats(
    ctx: &Context,
    storage: &Storage,
    cipher: Option<&CipherInfo>,
    dom: &dyn crate::glue::Domain,
    new_table_info: &tidb_model::TableInfo,
    stats_file_indexes: &[StatsFileIndex],
    rewrite_id_map: &std::collections::BTreeMap<i64, i64>,
) -> crate::errors::Result<()> {
    let tasks = download_stats(ctx, storage, cipher, stats_file_indexes, rewrite_id_map)?;
    dom.load_partition_stats_from_json(ctx, new_table_info, tasks)
}

/// Go `downloadStats`.
fn download_stats(
    ctx: &Context,
    storage: &Storage,
    cipher: Option<&CipherInfo>,
    stats_file_indexes: &[StatsFileIndex],
    rewrite_id_map: &std::collections::BTreeMap<i64, i64>,
) -> crate::errors::Result<Vec<(i64, tidb_stats_util::JsonTable)>> {
    use crate::errors;
    let mut tasks = Vec::new();
    for stats_file in stats_file_indexes {
        if let Some(err) = ctx.err() {
            return Err(errors::errorf(err.to_string()));
        }
        let content = if stats_file.inline_data.is_empty() {
            let content = storage
                .read_file(ctx, &stats_file.name)
                .map_err(|e| errors::errorf(e.to_string()))?;
            let decrypted = crate::utils::decrypt(&content, cipher, &stats_file.cipher_iv)
                .map_err(errors::errorf)?;
            let checksum = Sha256::digest(&decrypted);
            if stats_file.sha256.as_slice() != checksum.as_slice() {
                return Err(std::sync::Arc::new(
                    errors::ERR_INVALID_META_FILE.gen_with_stack(format!(
                        "invalid metafile: checksum mismatch expect {}, got {}",
                        hex::encode(&stats_file.sha256),
                        hex::encode(checksum)
                    )),
                ));
            }
            decrypted
        } else {
            stats_file.inline_data.clone()
        };
        let blocks =
            StatsFile::decode(content.as_slice()).map_err(|e| errors::errorf(e.to_string()))?;
        for block in blocks.blocks {
            let Some(&physical_id) = rewrite_id_map.get(&block.physical_id) else {
                return Err(std::sync::Arc::new(errors::ERR_RESTORE_INVALID_REWRITE.gen_with_stack(format!(
                    // Go's message has no verb for the argument.
                    "invalid rewrite rule%!(EXTRA string=not rewrite rule matched, old physical id: {})",
                    block.physical_id
                ))));
            };
            let json_table: tidb_stats_util::JsonTable = serde_json::from_slice(&block.json_table)
                .map_err(|e| errors::errorf(e.to_string()))?;
            tasks.push((physical_id, json_table));
        }
    }
    Ok(tasks)
}
