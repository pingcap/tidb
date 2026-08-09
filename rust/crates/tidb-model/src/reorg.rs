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

use serde::de::{DeserializeSeed, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use tidb_datatype::GoString;
use tidb_error::terror::{TerrorCode, TerrorError};

use crate::go_runtime::{GoShared, GoSharedSlice};
use crate::job::{JobMeta, TimeZoneLocation};
use crate::serde_helpers::{
    deserialize_go_object, go_json_field_matches, ignore_unknown, impl_go_json_deserialize,
    is_fatal_json_error, FatalSeed, FatalValueSeed, GoJsonMerge, NullDefaultSeed, NullNoopSeed,
    OptionSharedAtomicReplaceSeed, OptionSharedMergeSeed, OptionSharedScalarSeed,
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

/// Live process-default readers used by zero-valued persisted reorg metadata.
///
/// The model cannot own these values: Go reads vardef's runtime atomics on
/// every getter call. A higher layer supplies callbacks into its authoritative
/// runtime variables, keeping this dependency-leaf crate free of session
/// dependencies without turning a live value into a stale snapshot.
#[derive(Clone, Copy)]
pub struct DDLReorgProcessDefaults {
    worker_count: fn() -> i64,
    batch_size: fn() -> i64,
}

impl DDLReorgProcessDefaults {
    /// Connects the model getters to authoritative live runtime readers.
    #[must_use]
    pub const fn new(worker_count: fn() -> i64, batch_size: fn() -> i64) -> Self {
        Self {
            worker_count,
            batch_size,
        }
    }

    fn worker_count(self) -> i64 {
        (self.worker_count)()
    }

    fn batch_size(self) -> i64 {
        (self.batch_size)()
    }
}

/// Go warning map type. The map allocation and each non-nil error pointer are
/// independently shared by structural copies.
pub type DDLWarningMap = BTreeMap<GoString, Option<GoShared<TerrorError>>>;

/// Go warning-count map type. Structural copies retain the map allocation.
pub type DDLWarningCountMap = BTreeMap<GoString, i64>;

/// Go `DDLReorgMeta`. Warning values use the shared `tidb-error` envelope so
/// class/code/message/RFC JSON stays compatible without a Rust-only hierarchy.
#[derive(Debug, Default)]
pub struct DDLReorgMeta {
    /// SQL mode captured for reorganization expression evaluation.
    pub sql_mode: u64,
    /// Warning payloads keyed by TiDB error identifier.
    pub warnings: Option<GoShared<DDLWarningMap>>,
    /// Warning occurrence counts keyed by TiDB error identifier.
    pub warnings_count: Option<GoShared<DDLWarningCountMap>>,
    /// Time zone captured for reorganization expression evaluation.
    pub location: Option<GoShared<TimeZoneLocation>>,
    /// Reorganization strategy.
    pub reorg_type: ReorgType,
    /// Whether fast ingest reorganization is enabled.
    pub is_fast_reorg: bool,
    /// Whether distributed reorganization is enabled.
    pub is_dist_reorg: bool,
    /// Whether reorganization uses cloud storage.
    pub use_cloud_storage: bool,
    /// Resource group assigned to the reorganization.
    pub resource_group_name: GoString,
    /// Persisted reorganization metadata version.
    pub version: i64,
    /// Store-label scope targeted by the job.
    pub target_scope: GoString,
    /// Maximum number of nodes used by distributed reorganization.
    pub max_node_count: i64,
    /// Analyze phase state stored with modify-column work.
    pub analyze_state: i8,
    /// Current reorganization stage.
    pub stage: ReorgStage,
    /// Captured collation mode; `None` requests the caller-provided fallback.
    pub use_new_collate: Option<GoShared<bool>>,
    /// Dynamically adjustable worker count. Go stores this in `atomic.Int64`.
    concurrency: AtomicI64,
    /// Dynamically adjustable batch size. Go stores this in `atomic.Int64`.
    batch_size: AtomicI64,
    /// Dynamically adjustable write-rate limit. Go stores this in
    /// `atomic.Int64`.
    max_write_speed: AtomicI64,
}

impl Clone for DDLReorgMeta {
    fn clone(&self) -> Self {
        Self {
            sql_mode: self.sql_mode,
            warnings: self.warnings.clone(),
            warnings_count: self.warnings_count.clone(),
            location: self.location.clone(),
            reorg_type: self.reorg_type,
            is_fast_reorg: self.is_fast_reorg,
            is_dist_reorg: self.is_dist_reorg,
            use_cloud_storage: self.use_cloud_storage,
            resource_group_name: self.resource_group_name.clone(),
            version: self.version,
            target_scope: self.target_scope.clone(),
            max_node_count: self.max_node_count,
            analyze_state: self.analyze_state,
            stage: self.stage,
            use_new_collate: self.use_new_collate.clone(),
            concurrency: AtomicI64::new(self.concurrency.load(Ordering::SeqCst)),
            batch_size: AtomicI64::new(self.batch_size.load(Ordering::SeqCst)),
            max_write_speed: AtomicI64::new(self.max_write_speed.load(Ordering::SeqCst)),
        }
    }
}

struct SharedGoStringMap<'a, V>(&'a Option<GoShared<BTreeMap<GoString, V>>>);

impl<V: Serialize> Serialize for SharedGoStringMap<'_, V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(pointer) = self.0 else {
            return serializer.serialize_none();
        };
        let values = pointer.read();
        let mut encoded = Vec::new();
        encoded.push(b'{');
        for (key, value) in values.iter() {
            if encoded.len() != 1 {
                encoded.push(b',');
            }
            encoded.extend_from_slice(key.to_go_json_literal().as_bytes());
            encoded.push(b':');
            encoded.extend_from_slice(
                &crate::serde_helpers::to_go_json(value).map_err(serde::ser::Error::custom)?,
            );
        }
        encoded.push(b'}');
        let encoded = String::from_utf8(encoded).expect("Go JSON map encoding is UTF-8");
        let raw =
            serde_json::value::RawValue::from_string(encoded).map_err(serde::ser::Error::custom)?;
        raw.serialize(serializer)
    }
}

struct SharedGoBytes<'a>(&'a GoSharedSlice<u8>);

impl Serialize for SharedGoBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_allocated() {
            crate::serde_helpers::go_bytes::serialize(&Some(self.0.snapshot()), serializer)
        } else {
            crate::serde_helpers::go_bytes::serialize(&None, serializer)
        }
    }
}

impl Serialize for DDLReorgMeta {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Go `encoding/json` preserves declaration order. `UseNewCollate` is
        // the only `omitempty` field; atomic integers are always present.
        let field_count = if self.use_new_collate.is_some() {
            18
        } else {
            17
        };
        let mut value = serializer.serialize_struct("DDLReorgMeta", field_count)?;
        value.serialize_field("sql_mode", &self.sql_mode)?;
        value.serialize_field("warnings", &SharedGoStringMap(&self.warnings))?;
        value.serialize_field("warnings_count", &SharedGoStringMap(&self.warnings_count))?;
        value.serialize_field("location", &self.location)?;
        value.serialize_field("reorg_tp", &self.reorg_type)?;
        value.serialize_field("is_fast_reorg", &self.is_fast_reorg)?;
        value.serialize_field("is_dist_reorg", &self.is_dist_reorg)?;
        value.serialize_field("use_cloud_storage", &self.use_cloud_storage)?;
        value.serialize_field("resource_group_name", &self.resource_group_name)?;
        value.serialize_field("version", &self.version)?;
        value.serialize_field("target_scope", &self.target_scope)?;
        value.serialize_field("max_node_count", &self.max_node_count)?;
        value.serialize_field("analyze_state", &self.analyze_state)?;
        value.serialize_field("stage", &self.stage)?;
        if let Some(use_new_collate) = &self.use_new_collate {
            value.serialize_field("use_new_collate", &*use_new_collate.read())?;
        }
        value.serialize_field("concurrency", &self.concurrency.load(Ordering::SeqCst))?;
        value.serialize_field("batch_size", &self.batch_size.load(Ordering::SeqCst))?;
        value.serialize_field(
            "max_write_speed",
            &self.max_write_speed.load(Ordering::SeqCst),
        )?;
        value.end()
    }
}

/// Go `atomic.Int64.UnmarshalJSON`: decode into a temporary integer and store
/// only after successful decoding. JSON null decodes as the integer zero.
struct AtomicI64Seed<'a>(&'a AtomicI64);

impl<'de> DeserializeSeed<'de> for AtomicI64Seed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Option::<i64>::deserialize(deserializer)?.unwrap_or_default();
        self.0.store(value, Ordering::SeqCst);
        Ok(())
    }
}

struct OptionSharedWarningMapSeed<'a>(&'a mut Option<GoShared<DDLWarningMap>>);

impl<'de> DeserializeSeed<'de> for OptionSharedWarningMapSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct WarningMapVisitor<'a>(&'a mut Option<GoShared<DDLWarningMap>>);

        impl<'de> Visitor<'de> for WarningMapVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a warning map")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.0 = None;
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.0 = None;
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserialize_go_object(deserializer, self)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let pointer = self.0.get_or_insert_with(|| GoShared::new(BTreeMap::new()));
                while let Some(key) = map.next_key::<GoString>()? {
                    // A map element is decoded from a fresh zero value. The
                    // error pointer is therefore newly allocated after every
                    // successful non-null duplicate member.
                    let value = map.next_value_seed(FatalValueSeed::new())?;
                    pointer.write().insert(key, value);
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(WarningMapVisitor(self.0))
    }
}

struct OptionSharedWarningCountMapSeed<'a>(&'a mut Option<GoShared<DDLWarningCountMap>>);

impl<'de> DeserializeSeed<'de> for OptionSharedWarningCountMapSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct WarningCountMapVisitor<'a>(&'a mut Option<GoShared<DDLWarningCountMap>>);

        impl<'de> Visitor<'de> for WarningCountMapVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("null or a warning-count map")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                *self.0 = None;
                Ok(())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                *self.0 = None;
                Ok(())
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserialize_go_object(deserializer, self)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let pointer = self.0.get_or_insert_with(|| GoShared::new(BTreeMap::new()));
                let mut first_error = None;
                while let Some(key) = map.next_key::<GoString>()? {
                    let mut value = 0_i64;
                    if let Err(error) = map.next_value_seed(NullDefaultSeed(&mut value)) {
                        first_error.get_or_insert(error);
                    }
                    pointer.write().insert(key, value);
                }
                if let Some(error) = first_error {
                    return Err(error);
                }
                Ok(())
            }
        }

        deserializer.deserialize_option(WarningCountMapVisitor(self.0))
    }
}

struct SharedBytesSeed<'a>(&'a mut GoSharedSlice<u8>);

impl<'de> DeserializeSeed<'de> for SharedBytesSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Option::<String>::deserialize(deserializer)? {
            None => *self.0 = GoSharedSlice::default(),
            Some(text) => {
                let (bytes, capacity) =
                    crate::serde_helpers::go_bytes::decode_with_capacity(&text)?;
                *self.0 = GoSharedSlice::from_vec_with_capacity(bytes, capacity);
            }
        }
        Ok(())
    }
}

fn zero_terror_error() -> TerrorError {
    TerrorError::compatible(TerrorCode::new(0), "")
}

impl DDLReorgMeta {
    /// Go `ShallowCopy`: allocates a new outer pointer, shares every map and
    /// pointer field, and copies each atomic value into an independent cell.
    #[must_use]
    pub fn shallow_copy(&self) -> GoShared<Self> {
        GoShared::new(self.clone())
    }

    /// Explicit nullable receiver boundary for Go `(*DDLReorgMeta).ShallowCopy`.
    /// A nil receiver panics when the source dereferences it.
    #[must_use]
    pub fn shallow_copy_pointer(receiver: Option<&Self>) -> GoShared<Self> {
        receiver.expect("nil *DDLReorgMeta receiver").shallow_copy()
    }

    /// Returns persisted concurrency, or the current process default when zero.
    #[must_use]
    pub fn get_concurrency(&self, defaults: DDLReorgProcessDefaults) -> i64 {
        let concurrency = self.concurrency.load(Ordering::SeqCst);
        if concurrency == 0 {
            defaults.worker_count()
        } else {
            concurrency
        }
    }

    /// Stores dynamic reorganization concurrency.
    pub fn set_concurrency(&self, concurrency: i64) {
        self.concurrency.store(concurrency, Ordering::SeqCst);
    }

    /// Returns persisted batch size, or the current process default when zero.
    #[must_use]
    pub fn get_batch_size(&self, defaults: DDLReorgProcessDefaults) -> i64 {
        let batch_size = self.batch_size.load(Ordering::SeqCst);
        if batch_size == 0 {
            defaults.batch_size()
        } else {
            batch_size
        }
    }

    /// Stores dynamic reorganization batch size.
    pub fn set_batch_size(&self, batch_size: i64) {
        self.batch_size.store(batch_size, Ordering::SeqCst);
    }

    /// Returns the maximum write speed, where zero means unlimited.
    #[must_use]
    pub fn get_max_write_speed(&self) -> i64 {
        self.max_write_speed.load(Ordering::SeqCst)
    }

    /// Stores the maximum reorganization write speed.
    pub fn set_max_write_speed(&self, max_write_speed: i64) {
        self.max_write_speed
            .store(max_write_speed, Ordering::SeqCst);
    }

    /// Returns the captured collation mode or `default_value` for old metadata.
    #[must_use]
    pub fn get_use_new_collate_or_default(&self, default_value: bool) -> bool {
        self.use_new_collate
            .as_ref()
            .map_or(default_value, |value| *value.read())
    }

    /// Captures the collation mode in a freshly allocated Go `*bool`.
    pub fn set_use_new_collate(&mut self, use_new_collate: bool) {
        self.use_new_collate = Some(GoShared::new(use_new_collate));
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
                let mut first_error = None;
                while let Some(key) = map.next_key::<String>()? {
                    let field_result = (|| -> Result<(), A::Error> {
                        if go_json_field_matches(&key, "sql_mode") {
                            map.next_value_seed(NullNoopSeed(&mut destination.sql_mode))?;
                        } else if go_json_field_matches(&key, "warnings") {
                            map.next_value_seed(OptionSharedWarningMapSeed(
                                &mut destination.warnings,
                            ))?;
                        } else if go_json_field_matches(&key, "warnings_count") {
                            map.next_value_seed(OptionSharedWarningCountMapSeed(
                                &mut destination.warnings_count,
                            ))?;
                        } else if go_json_field_matches(&key, "location") {
                            map.next_value_seed(OptionSharedMergeSeed(&mut destination.location))?;
                        } else if go_json_field_matches(&key, "reorg_tp") {
                            map.next_value_seed(NullNoopSeed(&mut destination.reorg_type))?;
                        } else if go_json_field_matches(&key, "is_fast_reorg") {
                            map.next_value_seed(NullNoopSeed(&mut destination.is_fast_reorg))?;
                        } else if go_json_field_matches(&key, "is_dist_reorg") {
                            map.next_value_seed(NullNoopSeed(&mut destination.is_dist_reorg))?;
                        } else if go_json_field_matches(&key, "use_cloud_storage") {
                            map.next_value_seed(NullNoopSeed(&mut destination.use_cloud_storage))?;
                        } else if go_json_field_matches(&key, "resource_group_name") {
                            map.next_value_seed(NullNoopSeed(
                                &mut destination.resource_group_name,
                            ))?;
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
                            map.next_value_seed(OptionSharedScalarSeed(
                                &mut destination.use_new_collate,
                            ))?;
                        } else if go_json_field_matches(&key, "concurrency") {
                            map.next_value_seed(FatalSeed(AtomicI64Seed(
                                &destination.concurrency,
                            )))?;
                        } else if go_json_field_matches(&key, "batch_size") {
                            map.next_value_seed(FatalSeed(AtomicI64Seed(&destination.batch_size)))?;
                        } else if go_json_field_matches(&key, "max_write_speed") {
                            map.next_value_seed(FatalSeed(AtomicI64Seed(
                                &destination.max_write_speed,
                            )))?;
                        } else {
                            ignore_unknown(&mut map)?;
                        }
                        Ok(())
                    })();
                    if let Err(error) = field_result {
                        if is_fatal_json_error(&error) {
                            return Err(error);
                        }
                        first_error.get_or_insert(error);
                    }
                }
                if let Some(error) = first_error {
                    return Err(error);
                }
                Ok(())
            }
        }

        deserialize_go_object(deserializer, MergeVisitor(self))
    }
}

impl_go_json_deserialize!(DDLReorgMeta);

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
#[derive(Clone, Debug, Default)]
pub struct BackfillMeta {
    /// Whether the backfilled index enforces uniqueness.
    pub is_unique: bool,
    /// Whether the end key belongs to the backfill range.
    pub end_include: bool,
    /// Persisted backfill error payload.
    pub error: Option<GoShared<TerrorError>>,
    /// SQL mode captured for backfill evaluation.
    pub sql_mode: u64,
    /// Warning payloads keyed by TiDB error identifier.
    pub warnings: Option<GoShared<DDLWarningMap>>,
    /// Warning occurrence counts keyed by TiDB error identifier.
    pub warnings_count: Option<GoShared<DDLWarningCountMap>>,
    /// Time zone captured for backfill evaluation.
    pub location: Option<GoShared<TimeZoneLocation>>,
    /// Backfill reorganization strategy.
    pub reorg_type: ReorgType,
    /// Rows processed by the backfill task.
    pub row_count: i64,
    /// Inclusive start key, preserving nil versus allocated-empty bytes.
    pub start_key: GoSharedSlice<u8>,
    /// End key, preserving nil versus allocated-empty bytes.
    pub end_key: GoSharedSlice<u8>,
    /// Current progress key, preserving nil versus allocated-empty bytes.
    pub current_key: GoSharedSlice<u8>,
    /// Embedded subset of the owning DDL job metadata.
    pub job_meta: Option<GoShared<JobMeta>>,
}

impl Serialize for BackfillMeta {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serializer.serialize_struct("BackfillMeta", 13)?;
        value.serialize_field("is_unique", &self.is_unique)?;
        value.serialize_field("end_include", &self.end_include)?;
        value.serialize_field("err", &self.error)?;
        value.serialize_field("sql_mode", &self.sql_mode)?;
        value.serialize_field("warnings", &SharedGoStringMap(&self.warnings))?;
        value.serialize_field("warnings_count", &SharedGoStringMap(&self.warnings_count))?;
        value.serialize_field("location", &self.location)?;
        value.serialize_field("reorg_tp", &self.reorg_type)?;
        value.serialize_field("row_count", &self.row_count)?;
        value.serialize_field("start_key", &SharedGoBytes(&self.start_key))?;
        value.serialize_field("end_key", &SharedGoBytes(&self.end_key))?;
        value.serialize_field("curr_key", &SharedGoBytes(&self.current_key))?;
        value.serialize_field("job_meta", &self.job_meta)?;
        value.end()
    }
}

impl BackfillMeta {
    /// Go `Encode`.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        crate::serde_helpers::to_go_json(self)
    }

    /// Explicit nullable receiver boundary for Go `(*BackfillMeta).Encode`.
    /// A nil receiver is marshaled as JSON `null`.
    pub fn encode_pointer(receiver: Option<&Self>) -> Result<Vec<u8>, serde_json::Error> {
        crate::serde_helpers::to_go_json(&receiver)
    }

    /// Go `Decode`.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        // Validate the whole document before mutating, as Go's scanner does;
        // then retain each raw member so duplicates and post-error members
        // remain independently observable.
        let raw: &serde_json::value::RawValue = serde_json::from_slice(bytes)?;
        if raw.get() == "null" {
            return Ok(());
        }
        let mut deserializer = serde_json::Deserializer::from_str(raw.get());
        self.go_json_merge(&mut deserializer)
            .map_err(crate::serde_helpers::normalize_fatal_json_error)?;
        deserializer.end()
    }

    /// Explicit nullable receiver boundary for Go `(*BackfillMeta).Decode`.
    pub fn decode_pointer(
        receiver: Option<&mut Self>,
        bytes: &[u8],
    ) -> Result<(), serde_json::Error> {
        // `json.Unmarshal` validates the document before checking whether its
        // destination pointer is usable.
        let _: &serde_json::value::RawValue = serde_json::from_slice(bytes)?;
        let Some(receiver) = receiver else {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "json: Unmarshal(nil *model.BackfillMeta)",
            ));
        };
        receiver.decode(bytes)
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
                let mut first_error = None;
                while let Some(key) = map.next_key::<String>()? {
                    let field_result = (|| -> Result<(), A::Error> {
                        if go_json_field_matches(&key, "is_unique") {
                            map.next_value_seed(NullNoopSeed(&mut destination.is_unique))?;
                        } else if go_json_field_matches(&key, "end_include") {
                            map.next_value_seed(NullNoopSeed(&mut destination.end_include))?;
                        } else if go_json_field_matches(&key, "err") {
                            map.next_value_seed(FatalSeed(OptionSharedAtomicReplaceSeed::new(
                                &mut destination.error,
                                zero_terror_error,
                            )))?;
                        } else if go_json_field_matches(&key, "sql_mode") {
                            map.next_value_seed(NullNoopSeed(&mut destination.sql_mode))?;
                        } else if go_json_field_matches(&key, "warnings") {
                            map.next_value_seed(OptionSharedWarningMapSeed(
                                &mut destination.warnings,
                            ))?;
                        } else if go_json_field_matches(&key, "warnings_count") {
                            map.next_value_seed(OptionSharedWarningCountMapSeed(
                                &mut destination.warnings_count,
                            ))?;
                        } else if go_json_field_matches(&key, "location") {
                            map.next_value_seed(OptionSharedMergeSeed(&mut destination.location))?;
                        } else if go_json_field_matches(&key, "reorg_tp") {
                            map.next_value_seed(NullNoopSeed(&mut destination.reorg_type))?;
                        } else if go_json_field_matches(&key, "row_count") {
                            map.next_value_seed(NullNoopSeed(&mut destination.row_count))?;
                        } else if go_json_field_matches(&key, "start_key") {
                            map.next_value_seed(SharedBytesSeed(&mut destination.start_key))?;
                        } else if go_json_field_matches(&key, "end_key") {
                            map.next_value_seed(SharedBytesSeed(&mut destination.end_key))?;
                        } else if go_json_field_matches(&key, "curr_key") {
                            map.next_value_seed(SharedBytesSeed(&mut destination.current_key))?;
                        } else if go_json_field_matches(&key, "job_meta") {
                            map.next_value_seed(OptionSharedMergeSeed(&mut destination.job_meta))?;
                        } else {
                            ignore_unknown(&mut map)?;
                        }
                        Ok(())
                    })();
                    if let Err(error) = field_result {
                        if is_fatal_json_error(&error) {
                            return Err(error);
                        }
                        first_error.get_or_insert(error);
                    }
                }
                if let Some(error) = first_error {
                    return Err(error);
                }
                Ok(())
            }
        }

        deserialize_go_object(deserializer, MergeVisitor(self))
    }
}

impl_go_json_deserialize!(BackfillMeta);

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_WORKER_COUNT: AtomicI64 = AtomicI64::new(4);
    static TEST_BATCH_SIZE: AtomicI64 = AtomicI64::new(256);

    fn test_worker_count() -> i64 {
        TEST_WORKER_COUNT.load(Ordering::SeqCst)
    }

    fn test_batch_size() -> i64 {
        TEST_BATCH_SIZE.load(Ordering::SeqCst)
    }

    const TEST_DEFAULTS: DDLReorgProcessDefaults =
        DDLReorgProcessDefaults::new(test_worker_count, test_batch_size);

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
        TEST_WORKER_COUNT.store(7, Ordering::SeqCst);
        TEST_BATCH_SIZE.store(512, Ordering::SeqCst);
        let mut meta = DDLReorgMeta::default();
        assert_eq!(meta.get_concurrency(TEST_DEFAULTS), 7);
        assert_eq!(meta.get_batch_size(TEST_DEFAULTS), 512);
        TEST_WORKER_COUNT.store(8, Ordering::SeqCst);
        TEST_BATCH_SIZE.store(1024, Ordering::SeqCst);
        assert_eq!(meta.get_concurrency(TEST_DEFAULTS), 8);
        assert_eq!(meta.get_batch_size(TEST_DEFAULTS), 1024);
        assert_eq!(meta.get_max_write_speed(), 0);
        meta.set_concurrency(1);
        meta.set_batch_size(2);
        meta.set_max_write_speed(3);
        assert_eq!(meta.get_concurrency(TEST_DEFAULTS), 1);
        assert_eq!(meta.get_batch_size(TEST_DEFAULTS), 2);
        assert_eq!(meta.get_max_write_speed(), 3);
        meta.set_concurrency(-1);
        meta.set_batch_size(-2);
        assert_eq!(meta.get_concurrency(TEST_DEFAULTS), -1);
        assert_eq!(meta.get_batch_size(TEST_DEFAULTS), -2);
        assert!(meta.get_use_new_collate_or_default(true));
        assert!(!meta.get_use_new_collate_or_default(false));
        meta.set_use_new_collate(false);
        let first_collate = meta.use_new_collate.as_ref().unwrap().clone();
        assert!(!meta.get_use_new_collate_or_default(true));
        meta.set_use_new_collate(true);
        assert!(!first_collate.ptr_eq(meta.use_new_collate.as_ref().unwrap()));
        assert!(meta.get_use_new_collate_or_default(false));
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains(r#""use_new_collate":true"#));

        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"concurrency":null,"batch_size":null,"max_write_speed":null}"#,
        );
        meta.go_json_merge(&mut decoder).unwrap();
        assert_eq!(meta.concurrency.load(Ordering::SeqCst), 0);
        assert_eq!(meta.batch_size.load(Ordering::SeqCst), 0);
        assert_eq!(meta.max_write_speed.load(Ordering::SeqCst), 0);

        meta.set_batch_size(12);
        let mut decoder =
            serde_json::Deserializer::from_str(r#"{"concurrency":"bad","batch_size":13}"#);
        let error = meta.go_json_merge(&mut decoder).unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(meta.batch_size.load(Ordering::SeqCst), 12);

        meta.use_new_collate = None;
        let mut decoder =
            serde_json::Deserializer::from_str(r#"{"use_new_collate":"bad","version":7}"#);
        let error = meta.go_json_merge(&mut decoder).unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        let allocated = meta.use_new_collate.as_ref().unwrap().clone();
        assert!(!*allocated.read());
        assert_eq!(meta.version, 7);

        let mut decoder = serde_json::Deserializer::from_str(r#"{"use_new_collate":true}"#);
        meta.go_json_merge(&mut decoder).unwrap();
        assert!(allocated.ptr_eq(meta.use_new_collate.as_ref().unwrap()));
        assert!(*allocated.read());

        let mut decoder = serde_json::Deserializer::from_str(r#"{"use_new_collate":null}"#);
        meta.go_json_merge(&mut decoder).unwrap();
        assert!(meta.use_new_collate.is_none());
    }

    #[test]
    fn ddl_reorg_meta_atomic_fields_are_shared_for_concurrent_get_and_set() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<DDLReorgMeta>();

        let meta = std::sync::Arc::new(DDLReorgMeta::default());
        let mut writers = Vec::new();
        for value in 1..=8 {
            let meta = std::sync::Arc::clone(&meta);
            writers.push(std::thread::spawn(move || {
                meta.set_concurrency(value);
                meta.set_batch_size(value * 10);
                meta.set_max_write_speed(value * 100);
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }

        assert!((1..=8).contains(&meta.get_concurrency(TEST_DEFAULTS)));
        assert!((10..=80)
            .step_by(10)
            .any(|value| value == meta.get_batch_size(TEST_DEFAULTS)));
        assert!((100..=800)
            .step_by(100)
            .any(|value| value == meta.get_max_write_speed()));
    }

    #[test]
    fn ddl_reorg_meta_atomic_json_is_numeric_ordered_and_last_member_wins() {
        let meta = DDLReorgMeta::default();
        meta.set_concurrency(i64::MIN);
        meta.set_batch_size(i64::MAX);
        meta.set_max_write_speed(-1);
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.ends_with(&format!(
            r#","concurrency":{},"batch_size":{},"max_write_speed":-1}}"#,
            i64::MIN,
            i64::MAX
        )));

        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"CONCURRENCY":1,"concurrency":2,"batch_size":3,"batch_size":null}"#,
        );
        let mut decoded = DDLReorgMeta::default();
        decoded.go_json_merge(&mut decoder).unwrap();
        assert_eq!(decoded.concurrency.load(Ordering::SeqCst), 2);
        assert_eq!(decoded.batch_size.load(Ordering::SeqCst), 0);

        decoded.set_max_write_speed(9);
        let mut decoder =
            serde_json::Deserializer::from_str(r#"{"max_write_speed":"bad","concurrency":4}"#);
        assert!(decoded.go_json_merge(&mut decoder).is_err());
        assert_eq!(decoded.max_write_speed.load(Ordering::SeqCst), 9);
        assert_eq!(decoded.concurrency.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn ddl_reorg_shallow_copy_shares_handles_and_copies_atomics() {
        let warnings = GoShared::new(BTreeMap::from([
            ("warn".into(), None),
            (GoString::from_bytes(vec![b'k', 0xff]), None),
        ]));
        let counts = GoShared::new(BTreeMap::from([("warn".into(), 1)]));
        let location = GoShared::new(TimeZoneLocation {
            name: "UTC".into(),
            ..Default::default()
        });
        let collate = GoShared::new(false);
        let source = DDLReorgMeta {
            warnings: Some(warnings.clone()),
            warnings_count: Some(counts.clone()),
            location: Some(location.clone()),
            use_new_collate: Some(collate.clone()),
            resource_group_name: GoString::from_bytes(vec![b'r', 0xff]),
            target_scope: GoString::from_bytes(vec![b't', 0xfe]),
            ..Default::default()
        };
        source.set_concurrency(7);

        let copied = source.shallow_copy();
        let copied_value = copied.read();
        assert!(warnings.ptr_eq(copied_value.warnings.as_ref().unwrap()));
        assert!(counts.ptr_eq(copied_value.warnings_count.as_ref().unwrap()));
        assert!(location.ptr_eq(copied_value.location.as_ref().unwrap()));
        assert!(collate.ptr_eq(copied_value.use_new_collate.as_ref().unwrap()));
        assert_eq!(copied_value.get_concurrency(TEST_DEFAULTS), 7);
        copied_value.set_concurrency(8);
        drop(copied_value);
        assert_eq!(source.get_concurrency(TEST_DEFAULTS), 7);
        assert_eq!(copied.read().get_concurrency(TEST_DEFAULTS), 8);

        warnings.write().insert("later".into(), None);
        assert!(copied
            .read()
            .warnings
            .as_ref()
            .unwrap()
            .read()
            .contains_key(&GoString::from("later")));
        assert!(std::panic::catch_unwind(|| DDLReorgMeta::shallow_copy_pointer(None)).is_err());
        let json = String::from_utf8(crate::serde_helpers::to_go_json(&source).unwrap()).unwrap();
        assert!(json.contains(r#""resource_group_name":"r\ufffd""#));
        assert!(json.contains(r#""target_scope":"t\ufffd""#));
        assert!(json.contains(r#""k\ufffd":null"#));
    }

    #[test]
    fn backfill_meta_codec_preserves_byte_boundaries() {
        let original = BackfillMeta {
            end_include: true,
            start_key: GoSharedSlice::from_vec(vec![0, 1, 255]),
            end_key: GoSharedSlice::from_vec(Vec::new()),
            job_meta: Some(GoShared::new(JobMeta {
                schema_id: 1,
                table_id: 2,
                query: "alter table t add index idx(a)".into(),
                priority: 1,
                ..Default::default()
            })),
            ..Default::default()
        };
        let bytes = original.encode().unwrap();
        let json = std::str::from_utf8(&bytes).unwrap();
        assert!(json.contains(r#""start_key":"AAH/""#));
        assert!(json.contains(r#""end_key":"""#));
        assert!(json.contains(r#""curr_key":null"#));
        let mut decoded = BackfillMeta::default();
        decoded.decode(&bytes).unwrap();
        assert_eq!(decoded.start_key.snapshot(), original.start_key.snapshot());
        assert_eq!(decoded.end_key.snapshot(), original.end_key.snapshot());
        assert!(decoded.end_key.is_allocated());
        assert_eq!(
            decoded.job_meta.as_ref().unwrap().read().query,
            original.job_meta.as_ref().unwrap().read().query
        );
        decoded.decode(b"null").unwrap();
        assert_eq!(decoded.start_key.snapshot(), original.start_key.snapshot());

        decoded.decode(br#"{"row_count":9}"#).unwrap();
        assert_eq!(decoded.row_count, 9);
        assert_eq!(decoded.start_key.snapshot(), original.start_key.snapshot());

        let error = decoded
            .decode(br#"{"row_count":10,"sql_mode":"bad"}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(decoded.row_count, 10);
        assert_eq!(decoded.end_key.snapshot(), original.end_key.snapshot());

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
        assert_eq!(with_newline.start_key.snapshot(), vec![0, 1, 255]);
        assert_eq!(with_newline.start_key.capacity(), 3);

        let escaped = BackfillMeta {
            error: Some(GoShared::new(tidb_error::terror::TerrorError::compatible(
                tidb_error::terror::TerrorCode::new(2),
                "<>&\u{2028}\u{2029}",
            ))),
            ..Default::default()
        }
        .encode()
        .unwrap();
        let escaped = std::str::from_utf8(&escaped).unwrap();
        assert!(escaped.contains(r#"\u003c\u003e\u0026\u2028\u2029"#));

        assert_eq!(BackfillMeta::encode_pointer(None).unwrap(), b"null");
        let error = BackfillMeta::decode_pointer(None, br#"{}"#).unwrap_err();
        assert!(error
            .to_string()
            .contains("json: Unmarshal(nil *model.BackfillMeta)"));
        let syntax = BackfillMeta::decode_pointer(None, b"{").unwrap_err();
        assert!(!syntax
            .to_string()
            .contains("json: Unmarshal(nil *model.BackfillMeta)"));
    }

    #[test]
    fn backfill_structural_clone_shares_source_handles_and_slice_backing() {
        let error = GoShared::new(TerrorError::compatible(TerrorCode::new(2), "error"));
        let warnings = GoShared::new(BTreeMap::from([("warn".into(), None)]));
        let counts = GoShared::new(BTreeMap::from([("warn".into(), 1)]));
        let location = GoShared::new(TimeZoneLocation {
            name: "UTC".into(),
            ..Default::default()
        });
        let job_meta = GoShared::new(JobMeta {
            query: GoString::from_bytes(vec![b'q', 0xff]),
            ..Default::default()
        });
        let start_key = GoSharedSlice::from_vec_with_capacity(vec![1, 2], 8);
        let source = BackfillMeta {
            error: Some(error.clone()),
            warnings: Some(warnings.clone()),
            warnings_count: Some(counts.clone()),
            location: Some(location.clone()),
            start_key: start_key.clone(),
            job_meta: Some(job_meta.clone()),
            ..Default::default()
        };

        let copied = source.clone();
        assert!(error.ptr_eq(copied.error.as_ref().unwrap()));
        assert!(warnings.ptr_eq(copied.warnings.as_ref().unwrap()));
        assert!(counts.ptr_eq(copied.warnings_count.as_ref().unwrap()));
        assert!(location.ptr_eq(copied.location.as_ref().unwrap()));
        assert!(job_meta.ptr_eq(copied.job_meta.as_ref().unwrap()));
        assert!(start_key.backing_ptr_eq(&copied.start_key));
        assert_eq!(copied.start_key.capacity(), 8);
        copied.start_key.set(0, 9);
        assert_eq!(source.start_key.snapshot(), vec![9, 2]);
        let json = String::from_utf8(source.encode().unwrap()).unwrap();
        assert!(json.contains(r#""query":"q\ufffd""#));
    }

    #[test]
    fn backfill_decode_matches_go_object_stream_boundaries() {
        let location = TimeZoneLocation {
            name: "UTC".into(),
            ..TimeZoneLocation::default()
        };
        let mut meta = BackfillMeta {
            row_count: 1,
            sql_mode: 9,
            start_key: GoSharedSlice::from_vec_with_capacity(vec![0, 1, 255], 8),
            end_key: GoSharedSlice::from_vec(vec![2]),
            warnings: Some(GoShared::new(BTreeMap::from([("old".into(), None)]))),
            warnings_count: Some(GoShared::new(BTreeMap::from([("old".into(), 1)]))),
            location: Some(GoShared::new(location)),
            job_meta: Some(GoShared::new(JobMeta {
                schema_id: 10,
                table_id: 20,
                query: "old query".into(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let old_error = GoShared::new(TerrorError::compatible(TerrorCode::new(9), "old"));
        meta.error = Some(old_error.clone());
        let warnings_pointer = meta.warnings.as_ref().unwrap().clone();
        let warning_counts_pointer = meta.warnings_count.as_ref().unwrap().clone();
        let location_pointer = meta.location.as_ref().unwrap().clone();
        let job_meta_pointer = meta.job_meta.as_ref().unwrap().clone();
        let old_start_key = meta.start_key.clone();

        meta.decode(
            br#"{
                "ROW_COUNT":2,
                "row_count":3,
                "sql_mode":null,
                "err":{"class":21,"code":2,"message":"backfill failed","rfccode":"global:2"},
                "WARNINGS":{"new":{"class":21,"code":2,"message":"new","rfccode":"global:2"}},
                "warnings_count":{"new":2},
                "LOCATION":{"offset":3600},
                "job_meta":{"TABLE_ID":21},
                "unknown":{"ignored":true}
            }"#,
        )
        .unwrap();
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.sql_mode, 9);
        assert!(old_error.ptr_eq(meta.error.as_ref().unwrap()));
        assert_eq!(
            meta.error.as_ref().unwrap().read().message(),
            "backfill failed"
        );
        assert_eq!(meta.start_key.snapshot(), vec![0, 1, 255]);
        assert_eq!(meta.end_key.snapshot(), vec![2]);
        assert!(warnings_pointer.ptr_eq(meta.warnings.as_ref().unwrap()));
        assert!(warning_counts_pointer.ptr_eq(meta.warnings_count.as_ref().unwrap()));
        assert!(location_pointer.ptr_eq(meta.location.as_ref().unwrap()));
        assert!(job_meta_pointer.ptr_eq(meta.job_meta.as_ref().unwrap()));
        let warnings = meta.warnings.as_ref().unwrap().read();
        assert!(warnings.contains_key(&GoString::from("old")));
        assert!(warnings.contains_key(&GoString::from("new")));
        assert_eq!(
            warnings[&GoString::from("new")]
                .as_ref()
                .unwrap()
                .read()
                .message(),
            "new"
        );
        drop(warnings);
        assert_eq!(
            meta.warnings_count.as_ref().unwrap().read()[&GoString::from("old")],
            1
        );
        assert_eq!(
            meta.warnings_count.as_ref().unwrap().read()[&GoString::from("new")],
            2
        );
        meta.decode(br#"{"warnings_count":{"duplicate":1,"duplicate":2}}"#)
            .unwrap();
        assert_eq!(
            meta.warnings_count.as_ref().unwrap().read()[&GoString::from("duplicate")],
            2
        );
        let location = meta.location.as_ref().unwrap().read();
        assert_eq!(location.name, "UTC");
        assert_eq!(location.offset, 3600);
        drop(location);
        let job_meta = meta.job_meta.as_ref().unwrap().read();
        assert_eq!(job_meta.schema_id, 10);
        assert_eq!(job_meta.table_id, 21);
        assert_eq!(job_meta.query, "old query");
        drop(job_meta);

        let malformed_row_count = meta.row_count;
        assert!(meta.decode(br#"{"row_count":99,"sql_mode":}"#).is_err());
        assert_eq!(meta.row_count, malformed_row_count);

        let error = meta
            .decode(br#"{"row_count":4,"start_key":"AA$=","sql_mode":"bad","end_include":true}"#)
            .unwrap_err();
        assert!(error.to_string().contains("illegal base64 data"));
        assert_eq!(meta.row_count, 4);
        assert_eq!(meta.start_key.snapshot(), vec![0, 1, 255]);
        assert!(meta.start_key.backing_ptr_eq(&old_start_key));
        assert!(meta.end_include);
        assert_eq!(meta.sql_mode, 9);

        let was_unique = meta.is_unique;
        let error = meta.decode(br#"{"err":[],"is_unique":true}"#).unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(meta.is_unique, was_unique);
        assert!(old_error.ptr_eq(meta.error.as_ref().unwrap()));
        assert_eq!(old_error.read().message(), "backfill failed");

        assert!(meta
            .decode(br#"{"end_include":true,"row_count":1.5,"sql_mode":10}"#)
            .is_err());
        assert!(meta.end_include);
        assert_eq!(meta.row_count, 4);
        assert_eq!(meta.sql_mode, 10);

        assert!(meta
            .decode(br#"{"is_unique":true,"row_count":9223372036854775808,"sql_mode":11}"#)
            .is_err());
        assert!(meta.is_unique);
        assert_eq!(meta.row_count, 4);
        assert_eq!(meta.sql_mode, 11);

        let error = meta
            .decode(br#"{"warnings_count":{"bad":"bad","later":3},"row_count":5}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert_eq!(
            meta.warnings_count.as_ref().unwrap().read()[&GoString::from("bad")],
            0
        );
        assert_eq!(
            meta.warnings_count.as_ref().unwrap().read()[&GoString::from("later")],
            3
        );
        assert_eq!(meta.row_count, 5);

        meta.decode(br#"{"warnings_count":{"null_value":null},"row_count":6}"#)
            .unwrap();
        assert_eq!(
            meta.warnings_count.as_ref().unwrap().read()[&GoString::from("null_value")],
            0
        );
        assert_eq!(meta.row_count, 6);

        let error = meta
            .decode(br#"{"warnings":{"bad":7,"later":null},"row_count":7}"#)
            .unwrap_err();
        assert!(error.to_string().contains("invalid type"));
        assert!(!meta
            .warnings
            .as_ref()
            .unwrap()
            .read()
            .contains_key(&GoString::from("bad")));
        assert!(!meta
            .warnings
            .as_ref()
            .unwrap()
            .read()
            .contains_key(&GoString::from("later")));
        assert_eq!(meta.row_count, 6);

        meta.decode("{\"start_Key\":\"AAH/\",\"row_count\":8}".as_bytes())
            .unwrap();
        assert_eq!(meta.start_key.snapshot(), vec![0, 1, 255]);
        assert!(!meta.start_key.backing_ptr_eq(&old_start_key));
        assert_eq!(meta.start_key.capacity(), 3);
        assert_eq!(meta.row_count, 8);

        assert!(meta
            .decode(br#"{"row_count":18446744073709551616,"sql_mode":12}"#)
            .is_err());
        assert_eq!(meta.sql_mode, 12);

        meta.decode(
            br#"{
                "row_count":null,
                "err":null,
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
        assert_eq!(meta.row_count, 8);
        assert!(meta.error.is_none());
        assert!(meta.warnings.is_none());
        assert!(meta.warnings_count.is_none());
        assert!(meta.location.is_none());
        assert!(!meta.start_key.is_allocated());
        assert!(!meta.end_key.is_allocated());
        assert!(!meta.current_key.is_allocated());
        assert!(meta.job_meta.is_none());
    }
}
