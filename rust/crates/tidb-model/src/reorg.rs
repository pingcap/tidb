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

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::job::{JobMeta, TimeZoneLocation};
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, GoJsonMerge, NullNoopSeed, OptionBytesSeed,
    OptionMergeSeed, OptionStringMapMergeSeed,
};

/// Go `BackfillState` (a `byte`): the state of the backfill-merge process.
/// A newtype over `u8` so unknown values round-trip; [`Display`] falls
/// through to `"backfill state unknown"` like Go's `switch` default.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct BackfillState(
    /// Persisted state ordinal.
    pub u8,
);

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
pub struct ReorgStage(
    /// Persisted stage ordinal.
    pub u8,
);

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
    /// SQL mode captured for reorganization expression evaluation.
    #[serde(rename = "sql_mode", default)]
    pub sql_mode: u64,
    /// Warning payloads keyed by TiDB error identifier.
    #[serde(rename = "warnings", default)]
    pub warnings: Option<BTreeMap<String, serde_json::Value>>,
    /// Warning occurrence counts keyed by TiDB error identifier.
    #[serde(rename = "warnings_count", default)]
    pub warnings_count: Option<BTreeMap<String, i64>>,
    /// Time zone captured for reorganization expression evaluation.
    #[serde(rename = "location", default)]
    pub location: Option<TimeZoneLocation>,
    /// Reorganization strategy.
    #[serde(rename = "reorg_tp", default)]
    pub reorg_type: ReorgType,
    /// Whether fast ingest reorganization is enabled.
    #[serde(rename = "is_fast_reorg", default)]
    pub is_fast_reorg: bool,
    /// Whether distributed reorganization is enabled.
    #[serde(rename = "is_dist_reorg", default)]
    pub is_dist_reorg: bool,
    /// Whether reorganization uses cloud storage.
    #[serde(rename = "use_cloud_storage", default)]
    pub use_cloud_storage: bool,
    /// Resource group assigned to the reorganization.
    #[serde(rename = "resource_group_name", default)]
    pub resource_group_name: String,
    /// Persisted reorganization metadata version.
    #[serde(rename = "version", default)]
    pub version: i64,
    /// Store-label scope targeted by the job.
    #[serde(rename = "target_scope", default)]
    pub target_scope: String,
    /// Maximum number of nodes used by distributed reorganization.
    #[serde(rename = "max_node_count", default)]
    pub max_node_count: i64,
    /// Analyze phase state stored with modify-column work.
    #[serde(rename = "analyze_state", default)]
    pub analyze_state: i8,
    /// Current reorganization stage.
    #[serde(rename = "stage", default)]
    pub stage: ReorgStage,
    #[serde(
        rename = "use_new_collate",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Captured collation mode; `None` requests the caller-provided fallback.
    pub use_new_collate: Option<bool>,
    #[serde(rename = "concurrency", default)]
    concurrency: i64,
    #[serde(rename = "batch_size", default)]
    batch_size: i64,
    #[serde(rename = "max_write_speed", default)]
    max_write_speed: i64,
}

impl DDLReorgMeta {
    /// Go `ShallowCopy`'s scalar value result.
    ///
    /// Rust's owned warning maps are copied rather than aliased. The source
    /// map-backing-store identity is recorded as a measured representation
    /// boundary in the package ledger instead of being hidden by this method.
    #[must_use]
    pub fn shallow_copy(&self) -> Self {
        self.clone()
    }

    /// Returns persisted concurrency, or the current process default when zero.
    #[must_use]
    pub fn get_concurrency(&self) -> i64 {
        if self.concurrency == 0 {
            DDL_REORG_WORKER_COUNT.load(Ordering::SeqCst)
        } else {
            self.concurrency
        }
    }

    /// Stores dynamic reorganization concurrency.
    pub fn set_concurrency(&mut self, concurrency: i64) {
        self.concurrency = concurrency;
    }

    /// Returns persisted batch size, or the current process default when zero.
    #[must_use]
    pub fn get_batch_size(&self) -> i64 {
        if self.batch_size == 0 {
            DDL_REORG_BATCH_SIZE.load(Ordering::SeqCst)
        } else {
            self.batch_size
        }
    }

    /// Stores dynamic reorganization batch size.
    pub fn set_batch_size(&mut self, batch_size: i64) {
        self.batch_size = batch_size;
    }

    /// Returns the maximum write speed, where zero means unlimited.
    #[must_use]
    pub fn get_max_write_speed(&self) -> i64 {
        self.max_write_speed
    }

    /// Stores the maximum reorganization write speed.
    pub fn set_max_write_speed(&mut self, max_write_speed: i64) {
        self.max_write_speed = max_write_speed;
    }

    /// Returns the captured collation mode or `default_value` for old metadata.
    #[must_use]
    pub fn get_use_new_collate_or_default(&self, default_value: bool) -> bool {
        self.use_new_collate.unwrap_or(default_value)
    }

    /// Captures the collation mode for persisted reorganization work.
    pub fn set_use_new_collate(&mut self, use_new_collate: bool) {
        self.use_new_collate = Some(use_new_collate);
    }
}

impl GoJsonMerge for DDLReorgMeta {
    fn go_json_merge<'de, D>(&mut self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MergeVisitor<'a>(&'a mut DDLReorgMeta);

        impl<'de> Visitor<'de> for MergeVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let destination = self.0;
                while let Some(key) = map.next_key::<String>()? {
                    if go_json_field_matches(&key, "sql_mode") {
                        map.next_value_seed(NullNoopSeed(&mut destination.sql_mode))?;
                    } else if go_json_field_matches(&key, "warnings") {
                        map.next_value_seed(OptionStringMapMergeSeed(&mut destination.warnings))?;
                    } else if go_json_field_matches(&key, "warnings_count") {
                        map.next_value_seed(OptionStringMapMergeSeed(
                            &mut destination.warnings_count,
                        ))?;
                    } else if go_json_field_matches(&key, "location") {
                        map.next_value_seed(OptionMergeSeed(&mut destination.location))?;
                    } else if go_json_field_matches(&key, "reorg_tp") {
                        map.next_value_seed(NullNoopSeed(&mut destination.reorg_type))?;
                    } else if go_json_field_matches(&key, "is_fast_reorg") {
                        map.next_value_seed(NullNoopSeed(&mut destination.is_fast_reorg))?;
                    } else if go_json_field_matches(&key, "is_dist_reorg") {
                        map.next_value_seed(NullNoopSeed(&mut destination.is_dist_reorg))?;
                    } else if go_json_field_matches(&key, "use_cloud_storage") {
                        map.next_value_seed(NullNoopSeed(&mut destination.use_cloud_storage))?;
                    } else if go_json_field_matches(&key, "resource_group_name") {
                        map.next_value_seed(NullNoopSeed(&mut destination.resource_group_name))?;
                    } else if go_json_field_matches(&key, "version") {
                        map.next_value_seed(NullNoopSeed(&mut destination.version))?;
                    } else if go_json_field_matches(&key, "target_scope") {
                        map.next_value_seed(NullNoopSeed(&mut destination.target_scope))?;
                    } else if go_json_field_matches(&key, "max_node_count") {
                        map.next_value_seed(NullNoopSeed(&mut destination.max_node_count))?;
                    } else if go_json_field_matches(&key, "analyze_state") {
                        map.next_value_seed(NullNoopSeed(&mut destination.analyze_state))?;
                    } else if go_json_field_matches(&key, "stage") {
                        map.next_value_seed(NullNoopSeed(&mut destination.stage))?;
                    } else if go_json_field_matches(&key, "use_new_collate") {
                        destination.use_new_collate = map.next_value()?;
                    } else if go_json_field_matches(&key, "concurrency") {
                        map.next_value_seed(NullNoopSeed(&mut destination.concurrency))?;
                    } else if go_json_field_matches(&key, "batch_size") {
                        map.next_value_seed(NullNoopSeed(&mut destination.batch_size))?;
                    } else if go_json_field_matches(&key, "max_write_speed") {
                        map.next_value_seed(NullNoopSeed(&mut destination.max_write_speed))?;
                    } else {
                        ignore_unknown(&mut map)?;
                    }
                }
                Ok(())
            }
        }

        deserializer.deserialize_map(MergeVisitor(self))
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
pub struct ReorgType(
    /// Persisted strategy ordinal.
    pub i8,
);

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

/// Go `BackfillMeta` and its JSON codec.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BackfillMeta {
    /// Whether the backfilled index enforces uniqueness.
    #[serde(rename = "is_unique", default)]
    pub is_unique: bool,
    /// Whether the end key belongs to the backfill range.
    #[serde(rename = "end_include", default)]
    pub end_include: bool,
    /// Persisted backfill error payload.
    #[serde(rename = "err", default)]
    pub error: Option<serde_json::Value>,
    /// SQL mode captured for backfill evaluation.
    #[serde(rename = "sql_mode", default)]
    pub sql_mode: u64,
    /// Warning payloads keyed by TiDB error identifier.
    #[serde(rename = "warnings", default)]
    pub warnings: Option<BTreeMap<String, serde_json::Value>>,
    /// Warning occurrence counts keyed by TiDB error identifier.
    #[serde(rename = "warnings_count", default)]
    pub warnings_count: Option<BTreeMap<String, i64>>,
    /// Time zone captured for backfill evaluation.
    #[serde(rename = "location", default)]
    pub location: Option<TimeZoneLocation>,
    /// Backfill reorganization strategy.
    #[serde(rename = "reorg_tp", default)]
    pub reorg_type: ReorgType,
    /// Rows processed by the backfill task.
    #[serde(rename = "row_count", default)]
    pub row_count: i64,
    #[serde(rename = "start_key", default, with = "crate::serde_helpers::go_bytes")]
    /// Inclusive start key, preserving nil versus allocated-empty bytes.
    pub start_key: Option<Vec<u8>>,
    #[serde(rename = "end_key", default, with = "crate::serde_helpers::go_bytes")]
    /// End key, preserving nil versus allocated-empty bytes.
    pub end_key: Option<Vec<u8>>,
    #[serde(rename = "curr_key", default, with = "crate::serde_helpers::go_bytes")]
    /// Current progress key, preserving nil versus allocated-empty bytes.
    pub current_key: Option<Vec<u8>>,
    /// Embedded subset of the owning DDL job metadata.
    #[serde(rename = "job_meta", default)]
    pub job_meta: Option<JobMeta>,
}

impl BackfillMeta {
    /// Go `Encode`.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        crate::serde_helpers::to_go_json(self)
    }

    /// Go `Decode`.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        // Validate the whole document before mutating, as Go's scanner does;
        // then stream the original bytes so duplicate keys remain observable.
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.is_null() {
            return Ok(());
        }
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        self.go_json_merge(&mut deserializer)?;
        deserializer.end()
    }
}

impl GoJsonMerge for BackfillMeta {
    fn go_json_merge<'de, D>(&mut self, deserializer: D) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MergeVisitor<'a>(&'a mut BackfillMeta);

        impl<'de> Visitor<'de> for MergeVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let destination = self.0;
                while let Some(key) = map.next_key::<String>()? {
                    if go_json_field_matches(&key, "is_unique") {
                        map.next_value_seed(NullNoopSeed(&mut destination.is_unique))?;
                    } else if go_json_field_matches(&key, "end_include") {
                        map.next_value_seed(NullNoopSeed(&mut destination.end_include))?;
                    } else if go_json_field_matches(&key, "err") {
                        destination.error = map.next_value()?;
                    } else if go_json_field_matches(&key, "sql_mode") {
                        map.next_value_seed(NullNoopSeed(&mut destination.sql_mode))?;
                    } else if go_json_field_matches(&key, "warnings") {
                        map.next_value_seed(OptionStringMapMergeSeed(&mut destination.warnings))?;
                    } else if go_json_field_matches(&key, "warnings_count") {
                        map.next_value_seed(OptionStringMapMergeSeed(
                            &mut destination.warnings_count,
                        ))?;
                    } else if go_json_field_matches(&key, "location") {
                        map.next_value_seed(OptionMergeSeed(&mut destination.location))?;
                    } else if go_json_field_matches(&key, "reorg_tp") {
                        map.next_value_seed(NullNoopSeed(&mut destination.reorg_type))?;
                    } else if go_json_field_matches(&key, "row_count") {
                        map.next_value_seed(NullNoopSeed(&mut destination.row_count))?;
                    } else if go_json_field_matches(&key, "start_key") {
                        map.next_value_seed(OptionBytesSeed(&mut destination.start_key))?;
                    } else if go_json_field_matches(&key, "end_key") {
                        map.next_value_seed(OptionBytesSeed(&mut destination.end_key))?;
                    } else if go_json_field_matches(&key, "curr_key") {
                        map.next_value_seed(OptionBytesSeed(&mut destination.current_key))?;
                    } else if go_json_field_matches(&key, "job_meta") {
                        map.next_value_seed(OptionMergeSeed(&mut destination.job_meta))?;
                    } else {
                        ignore_unknown(&mut map)?;
                    }
                }
                Ok(())
            }
        }

        deserializer.deserialize_map(MergeVisitor(self))
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

        decoded.decode(br#"{"row_count":9}"#).unwrap();
        assert_eq!(decoded.row_count, 9);
        assert_eq!(decoded.start_key, original.start_key);

        let error = decoded
            .decode(br#"{"row_count":10,"sql_mode":"bad"}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(decoded.row_count, 10);
        assert_eq!(decoded.end_key, original.end_key);

        // Go's padded StdEncoding rejects incomplete quanta and padding in a
        // non-final quartet, while accepting CR/LF inside valid input.
        for invalid in [
            r#"{"start_key":"A"}"#,
            r#"{"start_key":"AA"}"#,
            r#"{"start_key":"AAA"}"#,
            r#"{"start_key":"AA=A"}"#,
            r#"{"start_key":"AA==AAAA"}"#,
            r#"{"start_key":"AA$="}"#,
        ] {
            assert!(serde_json::from_str::<BackfillMeta>(invalid).is_err());
        }
        let with_newline: BackfillMeta = serde_json::from_str(r#"{"start_key":"AA\nH/"}"#).unwrap();
        assert_eq!(with_newline.start_key, Some(vec![0, 1, 255]));

        let escaped = BackfillMeta {
            error: Some(serde_json::json!({"message": "<>&\u{2028}\u{2029}", "ratio": 1.0})),
            ..Default::default()
        }
        .encode()
        .unwrap();
        let escaped = std::str::from_utf8(&escaped).unwrap();
        assert!(escaped.contains(r#"\u003c\u003e\u0026\u2028\u2029"#));
        assert!(escaped.contains(r#""ratio":1"#));
    }

    #[test]
    fn backfill_decode_matches_go_object_stream_boundaries() {
        let mut location = TimeZoneLocation::default();
        location.name = "UTC".to_owned();
        let mut meta = BackfillMeta {
            row_count: 1,
            start_key: Some(vec![0, 1, 255]),
            end_key: Some(vec![2]),
            warnings: Some(BTreeMap::from([(
                "old".to_owned(),
                serde_json::json!({"message": "old"}),
            )])),
            warnings_count: Some(BTreeMap::from([("old".to_owned(), 1)])),
            location: Some(location),
            job_meta: Some(JobMeta {
                schema_id: 10,
                table_id: 20,
                query: "old query".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };

        meta.decode(
            br#"{
                "ROW_COUNT":2,
                "row_count":3,
                "sql_mode":null,
                "WARNINGS":{"new":{"message":"new"}},
                "warnings_count":{"new":2},
                "LOCATION":{"offset":3600},
                "job_meta":{"TABLE_ID":21},
                "unknown":{"ignored":true}
            }"#,
        )
        .unwrap();
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.sql_mode, 0);
        assert_eq!(meta.start_key, Some(vec![0, 1, 255]));
        assert_eq!(meta.end_key, Some(vec![2]));
        assert!(meta.warnings.as_ref().unwrap().contains_key("old"));
        assert!(meta.warnings.as_ref().unwrap().contains_key("new"));
        assert_eq!(meta.warnings_count.as_ref().unwrap()["old"], 1);
        assert_eq!(meta.warnings_count.as_ref().unwrap()["new"], 2);
        let location = meta.location.as_ref().unwrap();
        assert_eq!(location.name, "UTC");
        assert_eq!(location.offset, 3600);
        let job_meta = meta.job_meta.as_ref().unwrap();
        assert_eq!(job_meta.schema_id, 10);
        assert_eq!(job_meta.table_id, 21);
        assert_eq!(job_meta.query, "old query");

        let malformed_row_count = meta.row_count;
        assert!(meta.decode(br#"{"row_count":99,"sql_mode":}"#).is_err());
        assert_eq!(meta.row_count, malformed_row_count);

        let error = meta
            .decode(br#"{"row_count":4,"start_key":"AA$="}"#)
            .unwrap_err();
        assert!(error.to_string().contains("illegal base64 data"));
        assert_eq!(meta.row_count, 4);
        assert_eq!(meta.start_key, Some(vec![0, 1, 255]));

        assert!(meta
            .decode(br#"{"end_include":true,"row_count":1.5}"#)
            .is_err());
        assert!(meta.end_include);
        assert_eq!(meta.row_count, 4);

        assert!(meta
            .decode(br#"{"is_unique":true,"row_count":9223372036854775808}"#)
            .is_err());
        assert!(meta.is_unique);
        assert_eq!(meta.row_count, 4);

        meta.decode(
            br#"{
                "row_count":null,
                "warnings":null,
                "warnings_count":null,
                "location":null,
                "start_key":null,
                "end_key":null,
                "curr_key":null,
                "job_meta":null
            }"#,
        )
        .unwrap();
        assert_eq!(meta.row_count, 4);
        assert!(meta.warnings.is_none());
        assert!(meta.warnings_count.is_none());
        assert!(meta.location.is_none());
        assert!(meta.start_key.is_none());
        assert!(meta.end_key.is_none());
        assert!(meta.current_key.is_none());
        assert!(meta.job_meta.is_none());
    }
}
