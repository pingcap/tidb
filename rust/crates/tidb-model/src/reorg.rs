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

//! `pkg/meta/model/reorg.go`: backfill/reorganization state, persisted reorg
//! metadata, and the backfill JSON codec.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use serde::{Deserialize, Serialize};

use crate::job::{JobMeta, TimeZoneLocation};

/// Go `BackfillState` (a `byte`): the state of the backfill-merge process.
/// A newtype over `u8` so unknown values round-trip; [`Display`] falls
/// through to `"backfill state unknown"` like Go's `switch` default.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct BackfillState(pub u8);

impl BackfillState {
    /// The backfill-merge process is not used (Go `BackfillStateInapplicable`,
    /// the zero value).
    pub const INAPPLICABLE: BackfillState = BackfillState(0);
    /// The backfill process is running (Go `BackfillStateRunning`).
    pub const RUNNING: BackfillState = BackfillState(1);
    /// The temporary index is ready to merge back (Go
    /// `BackfillStateReadyToMerge`).
    pub const READY_TO_MERGE: BackfillState = BackfillState(2);
    /// The temporary index is merging back (Go `BackfillStateMerging`).
    pub const MERGING: BackfillState = BackfillState(3);
}

impl std::fmt::Display for BackfillState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            BackfillState::RUNNING => "backfill state running",
            BackfillState::READY_TO_MERGE => "backfill state ready to merge",
            BackfillState::MERGING => "backfill state merging",
            BackfillState::INAPPLICABLE => "backfill state inapplicable",
            _ => "backfill state unknown",
        })
    }
}

/// Go `ReorgStage` (a `byte`): the stage of a reorganization, persisted to
/// reorg meta to avoid repeating completed work.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ReorgStage(pub u8);

static DDL_REORG_WORKER_COUNT: AtomicI64 = AtomicI64::new(4);
static DDL_REORG_BATCH_SIZE: AtomicI64 = AtomicI64::new(256);

/// Updates the process defaults used for old persisted metadata whose dynamic
/// fields are zero. This is the Rust boundary corresponding to Go vardef's
/// runtime atomics.
pub fn set_ddl_reorg_process_defaults(worker_count: i64, batch_size: i64) {
    DDL_REORG_WORKER_COUNT.store(worker_count, Ordering::SeqCst);
    DDL_REORG_BATCH_SIZE.store(batch_size, Ordering::SeqCst);
}

/// Go `DDLReorgMeta`. Opaque TiDB errors are retained as raw JSON values so
/// their persisted representation is not normalized into a Rust-only error
/// hierarchy.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DDLReorgMeta {
    #[serde(rename = "sql_mode", default)]
    pub sql_mode: u64,
    #[serde(rename = "warnings", default)]
    pub warnings: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "warnings_count", default)]
    pub warnings_count: Option<BTreeMap<String, i64>>,
    #[serde(rename = "location", default)]
    pub location: Option<TimeZoneLocation>,
    #[serde(rename = "reorg_tp", default)]
    pub reorg_type: ReorgType,
    #[serde(rename = "is_fast_reorg", default)]
    pub is_fast_reorg: bool,
    #[serde(rename = "is_dist_reorg", default)]
    pub is_dist_reorg: bool,
    #[serde(rename = "use_cloud_storage", default)]
    pub use_cloud_storage: bool,
    #[serde(rename = "resource_group_name", default)]
    pub resource_group_name: String,
    #[serde(rename = "version", default)]
    pub version: i64,
    #[serde(rename = "target_scope", default)]
    pub target_scope: String,
    #[serde(rename = "max_node_count", default)]
    pub max_node_count: i64,
    #[serde(rename = "analyze_state", default)]
    pub analyze_state: i8,
    #[serde(rename = "stage", default)]
    pub stage: ReorgStage,
    #[serde(
        rename = "use_new_collate",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub use_new_collate: Option<bool>,
    #[serde(rename = "concurrency", default)]
    concurrency: i64,
    #[serde(rename = "batch_size", default)]
    batch_size: i64,
    #[serde(rename = "max_write_speed", default)]
    max_write_speed: i64,
}

impl DDLReorgMeta {
    /// Go `ShallowCopy`. Rust's owned warning maps are copied rather than
    /// aliased; all persisted values and mutations remain independent.
    #[must_use]
    pub fn shallow_copy(&self) -> Self {
        self.clone()
    }

    #[must_use]
    pub fn get_concurrency(&self) -> i64 {
        if self.concurrency == 0 {
            DDL_REORG_WORKER_COUNT.load(Ordering::SeqCst)
        } else {
            self.concurrency
        }
    }

    pub fn set_concurrency(&mut self, concurrency: i64) {
        self.concurrency = concurrency;
    }

    #[must_use]
    pub fn get_batch_size(&self) -> i64 {
        if self.batch_size == 0 {
            DDL_REORG_BATCH_SIZE.load(Ordering::SeqCst)
        } else {
            self.batch_size
        }
    }

    pub fn set_batch_size(&mut self, batch_size: i64) {
        self.batch_size = batch_size;
    }

    #[must_use]
    pub fn get_max_write_speed(&self) -> i64 {
        self.max_write_speed
    }

    pub fn set_max_write_speed(&mut self, max_write_speed: i64) {
        self.max_write_speed = max_write_speed;
    }

    #[must_use]
    pub fn get_use_new_collate_or_default(&self, default_value: bool) -> bool {
        self.use_new_collate.unwrap_or(default_value)
    }

    pub fn set_use_new_collate(&mut self, use_new_collate: bool) {
        self.use_new_collate = Some(use_new_collate);
    }
}

impl ReorgStage {
    /// Not started (Go `ReorgStageNone`).
    pub const NONE: ReorgStage = ReorgStage(0);
    /// The column is being updated (Go `ReorgStageModifyColumnUpdateColumn`).
    pub const MODIFY_COLUMN_UPDATE_COLUMN: ReorgStage = ReorgStage(1);
    /// The index is being recreated (Go
    /// `ReorgStageModifyColumnRecreateIndex`).
    pub const MODIFY_COLUMN_RECREATE_INDEX: ReorgStage = ReorgStage(2);
    /// The reorganization is complete (Go `ReorgStageModifyColumnCompleted`).
    pub const MODIFY_COLUMN_COMPLETED: ReorgStage = ReorgStage(3);
}

/// Go `ReorgType` (an `int8`): the reorganization backend/strategy.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ReorgType(pub i8);

impl ReorgType {
    /// No reorganization (Go `ReorgTypeNone`, the zero value).
    pub const NONE: ReorgType = ReorgType(0);
    /// Transactional reorganization (Go `ReorgTypeTxn`).
    pub const TXN: ReorgType = ReorgType(1);
    /// Ingest (lightning) reorganization (Go `ReorgTypeIngest`).
    pub const INGEST: ReorgType = ReorgType(2);
    /// Transactional reorganization with a merge phase (Go
    /// `ReorgTypeTxnMerge`).
    pub const TXN_MERGE: ReorgType = ReorgType(3);

    /// Go `NeedMergeProcess`: whether this strategy has a temp-index merge.
    #[must_use]
    pub fn need_merge_process(self) -> bool {
        self == ReorgType::INGEST || self == ReorgType::TXN_MERGE
    }
}

impl std::fmt::Display for ReorgType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            ReorgType::TXN => "txn",
            ReorgType::INGEST => "ingest",
            ReorgType::TXN_MERGE => "txn-merge",
            // ReorgTypeNone and any unknown value.
            _ => "",
        })
    }
}

/// Go `ReorgMetaVersion0`: the minimum `DDLReorgMeta` version.
pub const REORG_META_VERSION0: i64 = 0;
/// Go `CurrentReorgMetaVersion`: the current `DDLReorgMeta` version.
pub const CURRENT_REORG_META_VERSION: i64 = 1;

/// The analyze-state values stored in `DDLReorgMeta.AnalyzeState`
/// (Go's `AnalyzeState*` constants).
pub mod analyze_state {
    /// Not started.
    pub const NONE: i8 = 0;
    /// Running.
    pub const RUNNING: i8 = 1;
    /// Skipped.
    pub const SKIPPED: i8 = 2;
    /// Done.
    pub const DONE: i8 = 3;
    /// Timed out.
    pub const TIMEOUT: i8 = 4;
    /// Failed.
    pub const FAILED: i8 = 5;
}

mod go_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    fn encode(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let second = *chunk.get(1).unwrap_or(&0);
            let third = *chunk.get(2).unwrap_or(&0);
            let value = (u32::from(chunk[0]) << 16) | (u32::from(second) << 8) | u32::from(third);
            for position in 0..4 {
                if position <= chunk.len() {
                    output.push(ALPHABET[((value >> (18 - 6 * position)) & 0x3f) as usize] as char);
                } else {
                    output.push('=');
                }
            }
        }
        output
    }

    fn decode<E: serde::de::Error>(text: &str) -> Result<Vec<u8>, E> {
        let mut output = Vec::with_capacity(text.len() / 4 * 3);
        let mut accumulator = 0_u32;
        let mut bits = 0_u32;
        for byte in text.bytes().take_while(|byte| *byte != b'=') {
            let Some(index) = ALPHABET.iter().position(|candidate| *candidate == byte) else {
                return Err(E::custom("illegal base64 data"));
            };
            accumulator = (accumulator << 6) | index as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                output.push(((accumulator >> bits) & 0xff) as u8);
            }
        }
        Ok(output)
    }

    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_none(),
            Some(bytes) => serializer.serialize_str(&encode(bytes)),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        match Option::<String>::deserialize(deserializer)? {
            None => Ok(None),
            Some(text) => decode(&text).map(Some),
        }
    }
}

/// Go `BackfillMeta` and its JSON codec.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BackfillMeta {
    #[serde(rename = "is_unique", default)]
    pub is_unique: bool,
    #[serde(rename = "end_include", default)]
    pub end_include: bool,
    #[serde(rename = "err", default)]
    pub error: Option<serde_json::Value>,
    #[serde(rename = "sql_mode", default)]
    pub sql_mode: u64,
    #[serde(rename = "warnings", default)]
    pub warnings: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "warnings_count", default)]
    pub warnings_count: Option<BTreeMap<String, i64>>,
    #[serde(rename = "location", default)]
    pub location: Option<TimeZoneLocation>,
    #[serde(rename = "reorg_tp", default)]
    pub reorg_type: ReorgType,
    #[serde(rename = "row_count", default)]
    pub row_count: i64,
    #[serde(rename = "start_key", default, with = "go_bytes")]
    pub start_key: Option<Vec<u8>>,
    #[serde(rename = "end_key", default, with = "go_bytes")]
    pub end_key: Option<Vec<u8>>,
    #[serde(rename = "curr_key", default, with = "go_bytes")]
    pub current_key: Option<Vec<u8>>,
    #[serde(rename = "job_meta", default)]
    pub job_meta: Option<JobMeta>,
}

impl BackfillMeta {
    /// Go `Encode`.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Go `Decode`.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.is_null() {
            return Ok(());
        }
        *self = serde_json::from_value(value)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_state_strings() {
        assert_eq!(BackfillState::RUNNING.to_string(), "backfill state running");
        assert_eq!(
            BackfillState::READY_TO_MERGE.to_string(),
            "backfill state ready to merge"
        );
        assert_eq!(BackfillState::MERGING.to_string(), "backfill state merging");
        assert_eq!(
            BackfillState::INAPPLICABLE.to_string(),
            "backfill state inapplicable"
        );
        assert_eq!(BackfillState(9).to_string(), "backfill state unknown");
        assert_eq!(BackfillState::default(), BackfillState::INAPPLICABLE);
    }

    #[test]
    fn reorg_type_string_and_merge() {
        assert_eq!(ReorgType::NONE.to_string(), "");
        assert_eq!(ReorgType::TXN.to_string(), "txn");
        assert_eq!(ReorgType::INGEST.to_string(), "ingest");
        assert_eq!(ReorgType::TXN_MERGE.to_string(), "txn-merge");

        assert!(!ReorgType::NONE.need_merge_process());
        assert!(!ReorgType::TXN.need_merge_process());
        assert!(ReorgType::INGEST.need_merge_process());
        assert!(ReorgType::TXN_MERGE.need_merge_process());
    }

    #[test]
    fn versions_and_stage() {
        assert_eq!(REORG_META_VERSION0, 0);
        assert_eq!(CURRENT_REORG_META_VERSION, 1);
        assert_eq!(ReorgStage::default(), ReorgStage::NONE);
        assert_eq!(analyze_state::FAILED, 5);
    }

    #[test]
    fn ddl_reorg_meta_dynamic_defaults_and_collation_boundaries() {
        set_ddl_reorg_process_defaults(7, 512);
        let mut meta = DDLReorgMeta::default();
        assert_eq!(meta.get_concurrency(), 7);
        assert_eq!(meta.get_batch_size(), 512);
        assert_eq!(meta.get_max_write_speed(), 0);
        meta.set_concurrency(1);
        meta.set_batch_size(2);
        meta.set_max_write_speed(3);
        assert_eq!(meta.get_concurrency(), 1);
        assert_eq!(meta.get_batch_size(), 2);
        assert_eq!(meta.get_max_write_speed(), 3);
        assert!(meta.get_use_new_collate_or_default(true));
        assert!(!meta.get_use_new_collate_or_default(false));
        meta.set_use_new_collate(false);
        assert!(!meta.get_use_new_collate_or_default(true));
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains(r#""use_new_collate":false"#));
    }

    #[test]
    fn backfill_meta_codec_preserves_byte_boundaries() {
        let original = BackfillMeta {
            end_include: true,
            start_key: Some(vec![0, 1, 255]),
            end_key: Some(Vec::new()),
            job_meta: Some(JobMeta {
                schema_id: 1,
                table_id: 2,
                query: "alter table t add index idx(a)".to_owned(),
                priority: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = original.encode().unwrap();
        let json = std::str::from_utf8(&bytes).unwrap();
        assert!(json.contains(r#""start_key":"AAH/""#));
        assert!(json.contains(r#""end_key":"""#));
        assert!(json.contains(r#""curr_key":null"#));
        let mut decoded = BackfillMeta::default();
        decoded.decode(&bytes).unwrap();
        assert_eq!(decoded.start_key, original.start_key);
        assert_eq!(decoded.end_key, original.end_key);
        assert_eq!(decoded.job_meta, original.job_meta);
        decoded.decode(b"null").unwrap();
        assert_eq!(decoded.start_key, original.start_key);
        assert_eq!(decoded.end_key, original.end_key);
    }
}
