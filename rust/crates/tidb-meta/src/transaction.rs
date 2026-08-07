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

//! Transaction-backed operations owned by `pkg/meta/meta.go`.
//!
//! The Go owner builds its `Mutator` over `structure.TxStructure`; this module
//! keeps the same division. [`RawTransaction`] owns raw encoded bytes while
//! [`MetaStructure`] implements TiDB's string/hash data model and [`Mutator`]
//! implements catalog semantics. The in-memory transaction is deterministic
//! test infrastructure, not a second catalog implementation.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use tidb_ast::{CiString, MEDIUM_PRIORITY_VALUE};
use tidb_metadef::system::MAX_USER_GLOBAL_ID;
use tidb_model::db::DBInfo;
use tidb_model::masking_policy::MaskingPolicyInfo;
use tidb_model::placement::PolicyInfo;
use tidb_model::resource_group::{ResourceGroupInfo, ResourceGroupSettings};
use tidb_model::schema_diff::SchemaDiff;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_util::partialjson::extract_top_level_members;

use crate::error::{MetaError, Result};
use crate::{key, structure, value};

// Go's three package-global ID mutexes serialize ID allocation across every
// Mutator, not merely clones of one transaction handle.
static GLOBAL_ID_MUTEX: Mutex<()> = Mutex::new(());
static POLICY_ID_MUTEX: Mutex<()> = Mutex::new(());
static MASKING_POLICY_ID_MUTEX: Mutex<()> = Mutex::new(());

/// Go `defaultGroupID`.
pub const DEFAULT_RESOURCE_GROUP_ID: i64 = 1;

static DEFAULT_RESOURCE_GROUP: OnceLock<Arc<ResourceGroupInfo>> = OnceLock::new();

/// Go `NextGenBootTableVersion`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NextGenBootTableVersion(pub i64);

impl NextGenBootTableVersion {
    /// Fresh cluster.
    pub const INIT: Self = Self(0);
    /// First next-generation bootstrap.
    pub const BASE: Self = Self(1);
    /// Adds `mysql.tidb_masking_policy`.
    pub const MASKING_POLICY: Self = Self(2);
}

/// Go `DDLTableVersion`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DdlTableVersion(pub i64);

impl DdlTableVersion {
    /// Original version.
    pub const INIT: Self = Self(0);
    /// Concurrent-DDL tables.
    pub const BASE: Self = Self(1);
    /// Metadata-lock tables.
    pub const MDL: Self = Self(2);
    /// Distributed reorganization tables.
    pub const BACKFILL: Self = Self(3);
    /// DDL notifier table.
    pub const DDL_NOTIFIER: Self = Self(4);
}

/// Streaming raw iterator used by Go's `structure.ReverseHashIterator`.
///
/// `next` is fallible and must not be folded into iterator construction: Go
/// can return an error after filtering or decoding an earlier history entry.
pub trait RawKvIterator {
    /// Whether the iterator currently points at an entry.
    fn valid(&self) -> bool;

    /// Value at the current iterator position.
    fn value(&self) -> &[u8];

    /// Advances to the next entry.
    fn next(&mut self) -> Result<()>;
}

/// Callback shape for one raw half-open range entry.
pub type RawRangeVisitor<'a> = dyn FnMut(&[u8], &[u8]) -> Result<()> + 'a;

/// Go `GetAllNameToIDAndTheMustLoadedTableInfo`'s paired result.
pub type NameToIdAndMustLoadedTableInfo = (BTreeMap<Vec<u8>, i64>, Vec<TableInfo>);

struct OwnedRawKvIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
    fail_next_at: Option<(usize, String)>,
}

fn owned_reverse_iterator(
    mut entries: Vec<(Vec<u8>, Vec<u8>)>,
    upper_inclusive: Option<&[u8]>,
    fail_next_at: Option<(usize, String)>,
) -> Box<dyn RawKvIterator> {
    if let Some(upper) = upper_inclusive {
        entries.retain(|(key, _)| key.as_slice() <= upper);
    }
    entries.reverse();
    Box::new(OwnedRawKvIterator {
        entries,
        position: 0,
        fail_next_at,
    })
}

impl RawKvIterator for OwnedRawKvIterator {
    fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.position].1
    }

    fn next(&mut self) -> Result<()> {
        if let Some((position, message)) = &self.fail_next_at {
            if *position == self.position {
                return Err(MetaError::Storage(message.clone()));
            }
        }
        self.position += 1;
        Ok(())
    }
}

/// The raw transactional capabilities used by Go's `structure.TxStructure`.
pub trait RawTransaction {
    /// Go `kv.Transaction.StartTS`.
    fn start_ts(&self) -> u64;

    /// Applies `PriorityHigh` and `DiskFullOpt_AllowedOnAlmostFull`, the two
    /// transaction mutations performed by Go `NewMutator`.
    fn configure_meta_mutator(&mut self);

    /// Reads one encoded key. Missing keys are `None`.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Writes one encoded key and value.
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Deletes one encoded key. Deleting a missing key succeeds.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Returns every encoded key/value pair beginning with `prefix`, in byte
    /// order and isolated from later mutation of the transaction.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Constructs a reverse iterator over `prefix`, optionally stopping at an
    /// inclusive encoded upper key. Adapters should override this when their
    /// storage iterator can fail during `next`; the materializing default is
    /// exact for stores whose prefix scan is the only fallible operation.
    fn reverse_scan_prefix(
        &mut self,
        prefix: &[u8],
        upper_inclusive: Option<&[u8]>,
    ) -> Result<Box<dyn RawKvIterator>> {
        Ok(owned_reverse_iterator(
            self.scan_prefix(prefix)?,
            upper_inclusive,
            None,
        ))
    }

    /// Iterates the half-open encoded key range `[start, end)` in byte order.
    /// Implementations must stream rather than materialize the range: Go's
    /// `IterAllTables` exists specifically to avoid loading all metadata at
    /// once.
    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_>,
    ) -> Result<()>;
}

/// Snapshot capabilities used by Go `IterAllTables`.
pub trait MetaSnapshot: RawTransaction + Send {
    /// Applies Go's internal metadata request-source options. A storage
    /// adapter may use this to tag requests; the in-memory source probe has no
    /// transport and therefore implements it as a no-op.
    fn mark_internal_meta_request(&mut self);
}

/// Go `kv.Storage.GetSnapshot` boundary used by `IterAllTables`.
pub trait MetaSnapshotStore: Sync {
    /// Independent snapshot type. Each worker receives its own snapshot at the
    /// same timestamp.
    type Snapshot: MetaSnapshot;

    /// Returns a snapshot at `start_ts`. Go's `GetSnapshot` cannot fail.
    fn snapshot(&self, start_ts: u64) -> Self::Snapshot;
}

/// A deterministic raw transaction for source-derived unit tests.
#[derive(Clone, Debug, Default)]
pub struct MemoryTransaction {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    start_ts: u64,
    configured_for_meta: bool,
    iteration_error: Option<String>,
    reverse_next_error: Option<(usize, String)>,
    get_error: Option<(usize, String)>,
    get_calls: usize,
    set_error: Option<(usize, String)>,
    set_calls: usize,
    delete_error: Option<(usize, String)>,
    delete_calls: usize,
    scan_error: Option<(usize, String)>,
    scan_calls: usize,
    internal_meta_marks: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl MemoryTransaction {
    /// Creates a source transaction with the supplied Go `StartTS`.
    #[must_use]
    pub fn at_start_ts(start_ts: u64) -> Self {
        Self {
            start_ts,
            ..Self::default()
        }
    }

    /// Returns the raw encoded contents for byte-level assertions.
    #[must_use]
    pub fn entries(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.data
    }

    /// Whether Go `NewMutator`'s transaction settings were applied.
    #[must_use]
    pub fn configured_for_meta(&self) -> bool {
        self.configured_for_meta
    }

    /// Injects a range-iteration failure into this test transaction.
    #[must_use]
    pub fn with_iteration_error(mut self, message: impl Into<String>) -> Self {
        self.iteration_error = Some(message.into());
        self
    }

    /// Injects a `ReverseHashIterator.Next` failure at the current zero-based
    /// reverse position. The failed advance leaves the iterator in place.
    #[must_use]
    pub fn with_reverse_next_error(mut self, position: usize, message: impl Into<String>) -> Self {
        self.reverse_next_error = Some((position, message.into()));
        self
    }

    /// Injects a persistent raw point-read error at a zero-based call.
    #[must_use]
    pub fn with_get_error(mut self, call: usize, message: impl Into<String>) -> Self {
        self.get_error = Some((call, message.into()));
        self.get_calls = 0;
        self
    }

    /// Injects a persistent raw write error at a zero-based call.
    #[must_use]
    pub fn with_set_error(mut self, call: usize, message: impl Into<String>) -> Self {
        self.set_error = Some((call, message.into()));
        self.set_calls = 0;
        self
    }

    /// Injects a persistent raw delete error at a zero-based call.
    #[must_use]
    pub fn with_delete_error(mut self, call: usize, message: impl Into<String>) -> Self {
        self.delete_error = Some((call, message.into()));
        self.delete_calls = 0;
        self
    }

    /// Injects a persistent prefix-scan error at a zero-based call.
    #[must_use]
    pub fn with_scan_error(mut self, call: usize, message: impl Into<String>) -> Self {
        self.scan_error = Some((call, message.into()));
        self.scan_calls = 0;
        self
    }

    /// Records each Go `RequestSourceInternal`/`InternalTxnMeta` snapshot mark.
    #[must_use]
    pub fn with_internal_meta_mark_counter(
        mut self,
        counter: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        self.internal_meta_marks = Some(counter);
        self
    }
}

impl RawTransaction for MemoryTransaction {
    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn configure_meta_mutator(&mut self) {
        self.configured_for_meta = true;
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((call, message)) = &self.get_error {
            if *call == self.get_calls {
                return Err(MetaError::Storage(message.clone()));
            }
        }
        self.get_calls += 1;
        Ok(self.data.get(key).cloned())
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if let Some((call, message)) = &self.set_error {
            if *call == self.set_calls {
                return Err(MetaError::Storage(message.clone()));
            }
        }
        self.set_calls += 1;
        self.data.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some((call, message)) = &self.delete_error {
            if *call == self.delete_calls {
                return Err(MetaError::Storage(message.clone()));
            }
        }
        self.delete_calls += 1;
        self.data.remove(key);
        Ok(())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some((call, message)) = &self.scan_error {
            if *call == self.scan_calls {
                return Err(MetaError::Storage(message.clone()));
            }
        }
        self.scan_calls += 1;
        Ok(self
            .data
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn reverse_scan_prefix(
        &mut self,
        prefix: &[u8],
        upper_inclusive: Option<&[u8]>,
    ) -> Result<Box<dyn RawKvIterator>> {
        if let Some(message) = &self.iteration_error {
            return Err(MetaError::Storage(message.clone()));
        }
        Ok(owned_reverse_iterator(
            self.scan_prefix(prefix)?,
            upper_inclusive,
            self.reverse_next_error.clone(),
        ))
    }

    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_>,
    ) -> Result<()> {
        if let Some(message) = &self.iteration_error {
            return Err(MetaError::Storage(message.clone()));
        }
        for (key, value) in self.data.range(start.to_vec()..end.to_vec()) {
            visit(key, value)?;
        }
        Ok(())
    }
}

impl MetaSnapshot for MemoryTransaction {
    fn mark_internal_meta_request(&mut self) {
        if let Some(counter) = &self.internal_meta_marks {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Go `structure.HashPair`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashPair {
    /// Decoded hash field.
    pub field: Vec<u8>,
    /// Stored value.
    pub value: Vec<u8>,
}

/// The `model.Job.Encode`/`Decode` boundary consumed by `meta.go`.
///
/// The complete DDL job representation belongs to `pkg/meta/model/job.go`;
/// keeping that codec behind this trait lets the metadata transaction preserve
/// exact call order and stored bytes without inventing a second job model.
pub trait DdlJobCodec: Sized {
    /// Job ID used as the big-endian history field.
    fn id(&self) -> i64;

    /// Go `Job.Encode(updateRawArgs)`.
    fn encode(&mut self, update_raw_args: bool) -> Result<Vec<u8>>;

    /// Go `Job.Decode`.
    fn decode(encoded: &[u8]) -> Result<Self>;
}

/// Go `LastJobIterator`.
pub trait LastJobIterator<J> {
    /// Go `GetLastJobs`.
    fn get_last_jobs(&mut self, count: i32, jobs: &mut Vec<J>) -> Result<()>;
}

/// Reverse DDL-history iterator returned by Go's three iterator constructors.
pub struct HistoryDdlJobIterator<J> {
    iterator: Box<dyn RawKvIterator>,
    schema_names: BTreeSet<String>,
    table_names: BTreeSet<String>,
    marker: std::marker::PhantomData<J>,
}

impl<J: DdlJobCodec> HistoryDdlJobIterator<J> {
    /// Go `HLastJobIterator.GetLastJobs`.
    pub fn get_last_jobs(&mut self, count: i32, jobs: &mut Vec<J>) -> Result<()> {
        if count <= 0 {
            jobs.clear();
            return Ok(());
        }
        // Go checks length, not capacity, before deciding whether to allocate
        // a replacement slice.
        if jobs.len() < count as usize {
            *jobs = Vec::with_capacity(count as usize);
        } else {
            jobs.clear();
        }
        while self.iterator.valid() && jobs.len() < count as usize {
            let matches =
                match job_matches(self.iterator.value(), &self.schema_names, &self.table_names) {
                    Ok(matches) => matches,
                    Err(error) => {
                        jobs.clear();
                        return Err(error);
                    }
                };
            if !matches {
                if let Err(error) = self.iterator.next() {
                    jobs.clear();
                    return Err(error);
                }
                continue;
            }
            let job = match J::decode(self.iterator.value()) {
                Ok(job) => job,
                Err(error) => {
                    jobs.clear();
                    return Err(error);
                }
            };
            jobs.push(job);
            if let Err(error) = self.iterator.next() {
                jobs.clear();
                return Err(error);
            }
        }
        Ok(())
    }
}

impl<J: DdlJobCodec> LastJobIterator<J> for HistoryDdlJobIterator<J> {
    fn get_last_jobs(&mut self, count: i32, jobs: &mut Vec<J>) -> Result<()> {
        HistoryDdlJobIterator::get_last_jobs(self, count, jobs)
    }
}

/// Go `model.AutoIDGroup`, kept here because `meta.go` only consumes its three
/// scalar allocator values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AutoIdGroup {
    /// `_tidb_rowid` allocator value.
    pub row_id: i64,
    /// Separate `AUTO_INCREMENT` allocator value.
    pub increment_id: i64,
    /// `AUTO_RANDOM` allocator value.
    pub random_id: i64,
}

/// Go `model.TableNameInfo` returned by the fast table-list path.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableNameInfo {
    /// Table ID.
    pub id: i64,
    /// Case-insensitive table name.
    pub name: CiString,
}

/// One Go `MustLoadFilterAttr` rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MustLoadFilterAttr<'a> {
    /// Exact serialized marker.
    pub attr: &'a [u8],
    /// Load when the marker is absent instead of present.
    pub load_if_missing: bool,
}

/// Source-default markers, in the exact order used by Go.
pub const TABLE_INFO_MUST_LOAD_FILTERS: &[MustLoadFilterAttr<'static>] = &[
    MustLoadFilterAttr {
        attr: br#""partition":null"#,
        load_if_missing: true,
    },
    MustLoadFilterAttr {
        attr: br#""Lock":null"#,
        load_if_missing: true,
    },
    MustLoadFilterAttr {
        attr: br#""tiflash_replica":null"#,
        load_if_missing: true,
    },
    MustLoadFilterAttr {
        attr: br#""temp_table_type":0"#,
        load_if_missing: true,
    },
    MustLoadFilterAttr {
        attr: br#""policy_ref_info":null"#,
        load_if_missing: true,
    },
    MustLoadFilterAttr {
        attr: br#""ttl_info":null"#,
        load_if_missing: true,
    },
    MustLoadFilterAttr {
        attr: br#""affinity":{"#,
        load_if_missing: false,
    },
];

/// Go exported `NameExtractRegexp`.
pub const NAME_EXTRACT_REGEXP: &str = r#""O":"([^"\\]*(?:\\.[^"\\]*)*)","#;
/// Go `tableNameInfoFields`.
pub const TABLE_NAME_INFO_FIELDS: &[&str] = &["id", "name"];
/// Go `jobExtractFields`.
pub const JOB_EXTRACT_FIELDS: &[&str] = &["schema_name", "table_name"];
/// Go `checkForeignKeyAttributesNil`.
pub const FOREIGN_KEY_ATTRIBUTES_NIL: &[u8] = br#""fk_info":null"#;
/// Go `checkForeignKeyAttributesZero`.
pub const FOREIGN_KEY_ATTRIBUTES_ZERO: &[u8] = br#""fk_info":[]"#;

/// Go `schstatus.TTLTuneFactors`, the exact stored shape consumed by
/// `SetDXFScheduleTuneFactors`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TtlTuneFactors {
    /// Go `time.Duration`, encoded as integer nanoseconds and omitted at zero.
    #[serde(rename = "ttl", default, skip_serializing_if = "is_zero_i64")]
    pub ttl_nanoseconds: i64,
    /// Go's embedded `time.Time` is serialized even when zero despite its
    /// `omitempty` tag.
    #[serde(
        rename = "expire_time",
        default = "go_zero_time",
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub expire_time: DateTime<Utc>,
    /// Resource amplification, omitted at zero.
    #[serde(
        rename = "amplify_factor",
        default,
        skip_serializing_if = "is_zero_f64"
    )]
    pub amplify_factor: f64,
}

impl Default for TtlTuneFactors {
    fn default() -> Self {
        Self {
            ttl_nanoseconds: 0,
            expire_time: go_zero_time(),
            amplify_factor: 0.0,
        }
    }
}

/// Go protobuf `resource_manager.Consumption` as seen by `encoding/json`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RuConsumption {
    /// Read request units.
    #[serde(rename = "r_r_u", default, skip_serializing_if = "is_zero_f64")]
    pub read_request_units: f64,
    /// Write request units.
    #[serde(rename = "w_r_u", default, skip_serializing_if = "is_zero_f64")]
    pub write_request_units: f64,
    /// Read bytes.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub read_bytes: f64,
    /// Written bytes.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub write_bytes: f64,
    /// Total CPU milliseconds.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub total_cpu_time_ms: f64,
    /// SQL-layer CPU milliseconds.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub sql_layer_cpu_time_ms: f64,
    /// KV read RPC count.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub kv_read_rpc_count: f64,
    /// KV write RPC count.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub kv_write_rpc_count: f64,
}

/// Go `GroupRUStats`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct GroupRuStats {
    /// Resource-group ID.
    pub id: i64,
    /// Resource-group name.
    pub name: String,
    /// Optional protobuf consumption record.
    pub ru_consumption: Option<RuConsumption>,
}

/// Go `DailyRUStats`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DailyRuStats {
    /// End of the aggregation period.
    #[serde(
        rename = "date",
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub end_time: DateTime<Utc>,
    /// `None` preserves a nil Go slice (`null`) separately from an empty slice.
    pub stats: Option<Vec<GroupRuStats>>,
}

/// Go `RUStats`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct RuStats {
    /// Latest daily record.
    pub latest: Option<Box<DailyRuStats>>,
    /// Previous daily record.
    pub previous: Option<Box<DailyRuStats>>,
}

/// One MVCC write record consumed by Go `GetOldestSchemaVersion`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvccWrite {
    /// Inline short value of the write.
    pub short_value: Vec<u8>,
}

/// The MVCC information needed by `GetOldestSchemaVersion`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvccInfo {
    /// Writes in the order returned by TiKV; Go reads the last one.
    pub writes: Vec<MvccWrite>,
}

/// Go helper boundary used by `GetOldestSchemaVersion`.
pub trait MvccReader {
    /// Reads MVCC info for one encoded key at the requested timestamp.
    fn mvcc_by_encoded_key(&mut self, key: &[u8], timestamp: u64) -> Result<Option<MvccInfo>>;
}

/// Go `structure.TxStructure`, specialized to the `m` metadata namespace.
struct MetaStructure<'a, T> {
    transaction: &'a mut T,
}

impl<'a, T: RawTransaction> MetaStructure<'a, T> {
    fn new(transaction: &'a mut T) -> Self {
        Self { transaction }
    }

    fn get(&mut self, logical_key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.transaction
            .get(&structure::encode_string_data_key(logical_key))
    }

    fn set(&mut self, logical_key: &[u8], stored: &[u8]) -> Result<()> {
        self.transaction.set(
            structure::encode_string_data_key(logical_key),
            stored.to_vec(),
        )
    }

    fn clear(&mut self, logical_key: &[u8]) -> Result<()> {
        self.transaction
            .delete(&structure::encode_string_data_key(logical_key))
    }

    fn get_i64(&mut self, logical_key: &[u8]) -> Result<i64> {
        self.get(logical_key)?
            .map_or(Ok(0), |stored| value::parse_int_value(&stored))
    }

    fn increment(&mut self, logical_key: &[u8], step: i64) -> Result<i64> {
        let encoded = structure::encode_string_data_key(logical_key);
        let current = self
            .transaction
            .get(&encoded)?
            .map_or(Ok(0), |stored| value::parse_int_value(&stored))?;
        let next = current.wrapping_add(step);
        self.transaction
            .set(encoded, value::encode_int_value(next))?;
        Ok(next)
    }

    fn hget(&mut self, hash: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
        self.transaction
            .get(&structure::encode_hash_data_key(hash, field))
    }

    fn hset(&mut self, hash: &[u8], field: &[u8], stored: &[u8]) -> Result<()> {
        let encoded = structure::encode_hash_data_key(hash, field);
        if self.transaction.get(&encoded)?.as_deref() != Some(stored) {
            self.transaction.set(encoded, stored.to_vec())?;
        }
        Ok(())
    }

    fn hdelete(&mut self, hash: &[u8], field: &[u8]) -> Result<()> {
        let encoded = structure::encode_hash_data_key(hash, field);
        if self.transaction.get(&encoded)?.is_some() {
            self.transaction.delete(&encoded)?;
        }
        Ok(())
    }

    fn hincrement(&mut self, hash: &[u8], field: &[u8], step: i64) -> Result<i64> {
        let current = self
            .hget(hash, field)?
            .map_or(Ok(0), |stored| value::parse_int_value(&stored))?;
        let next = current.wrapping_add(step);
        self.hset(hash, field, &value::encode_int_value(next))?;
        Ok(next)
    }

    fn hget_i64(&mut self, hash: &[u8], field: &[u8]) -> Result<i64> {
        self.hget(hash, field)?
            .map_or(Ok(0), |stored| value::parse_int_value(&stored))
    }

    fn hget_all(&mut self, hash: &[u8]) -> Result<Vec<HashPair>> {
        self.transaction
            .scan_prefix(&structure::encode_hash_data_key_prefix(hash))?
            .into_iter()
            .map(|(encoded, stored)| {
                let (_, field) = structure::decode_hash_data_key(&encoded)
                    .map_err(|_| MetaError::MalformedKey)?;
                Ok(HashPair {
                    field,
                    value: stored,
                })
            })
            .collect()
    }

    fn hget_iter(
        &mut self,
        hash: &[u8],
        visit: &mut dyn FnMut(HashPair) -> Result<()>,
    ) -> Result<()> {
        let prefix = structure::encode_hash_data_key_prefix(hash);
        // This prefix always ends in the encoded HashData flag (`0x68`), so
        // its lexicographic successor exists without carry.
        let mut end = prefix.clone();
        *end.last_mut().expect("a structure prefix is non-empty") += 1;
        self.transaction
            .iterate_range(&prefix, &end, &mut |encoded, stored| {
                let (_, field) = structure::decode_hash_data_key(encoded)
                    .map_err(|_| MetaError::MalformedKey)?;
                visit(HashPair {
                    field,
                    value: stored.to_vec(),
                })
            })
    }

    fn hclear(&mut self, hash: &[u8]) -> Result<()> {
        let keys = self
            .transaction
            .scan_prefix(&structure::encode_hash_data_key_prefix(hash))?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        for key in keys {
            self.transaction.delete(&key)?;
        }
        Ok(())
    }
}

/// Go `meta.Mutator`, sharing one transaction across clonable handles.
pub struct Mutator<T> {
    transaction: Arc<Mutex<T>>,
    start_ts: u64,
}

/// Go `meta.Option`, an option applied to a newly constructed mutator.
pub type MutatorOption<T> = Box<dyn FnMut(&mut Mutator<T>)>;

impl<T> Clone for Mutator<T> {
    fn clone(&self) -> Self {
        Self {
            transaction: Arc::clone(&self.transaction),
            start_ts: self.start_ts,
        }
    }
}

impl<T: RawTransaction> Mutator<T> {
    /// Go `NewMutator` without options.
    #[must_use]
    pub fn new(transaction: T) -> Self {
        let mut options: [MutatorOption<T>; 0] = [];
        Self::new_with_options(transaction, &mut options)
    }

    /// Go `NewMutator(txn, options...)`, including transaction configuration
    /// and source-order option execution.
    #[must_use]
    pub fn new_with_options(mut transaction: T, options: &mut [MutatorOption<T>]) -> Self {
        transaction.configure_meta_mutator();
        let start_ts = transaction.start_ts();
        let mut meta = Self {
            transaction: Arc::new(Mutex::new(transaction)),
            start_ts,
        };
        for option in options {
            option(&mut meta);
        }
        meta
    }

    /// Go `Mutator.StartTS`.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn lock(&self) -> Result<MutexGuard<'_, T>> {
        self.transaction
            .lock()
            .map_err(|_| MetaError::Storage("metadata transaction lock poisoned".to_owned()))
    }

    /// Applies a read-only assertion directly to the raw transaction.
    pub fn inspect<R>(&self, inspect: impl FnOnce(&T) -> R) -> Result<R> {
        self.lock().map(|transaction| inspect(&transaction))
    }

    /// Go `Mutator.GenGlobalID`.
    pub fn gen_global_id(&self) -> Result<i64> {
        let _guard = GLOBAL_ID_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut transaction = self.lock()?;
        let new_id = MetaStructure::new(&mut *transaction).increment(key::NEXT_GLOBAL_ID, 1)?;
        check_global_id(new_id)?;
        Ok(new_id)
    }

    /// Go `Mutator.AdvanceGlobalIDs`; returns the old global ID.
    pub fn advance_global_ids(&self, count: i32) -> Result<i64> {
        let _guard = GLOBAL_ID_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut transaction = self.lock()?;
        let new_id = MetaStructure::new(&mut *transaction)
            .increment(key::NEXT_GLOBAL_ID, i64::from(count))?;
        check_global_id(new_id)?;
        Ok(new_id.wrapping_sub(i64::from(count)))
    }

    /// Go `Mutator.GenGlobalIDs`.
    pub fn gen_global_ids(&self, count: i32) -> Result<Vec<i64>> {
        let _guard = GLOBAL_ID_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut transaction = self.lock()?;
        let new_id = MetaStructure::new(&mut *transaction)
            .increment(key::NEXT_GLOBAL_ID, i64::from(count))?;
        check_global_id(new_id)?;
        let old_id = new_id.wrapping_sub(i64::from(count));
        // Go's transaction object is not a poisonable mutex and its package
        // ID mutex is released by defer. Do not carry the transaction guard
        // through the deliberate negative-capacity panic below.
        drop(transaction);
        // Go increments first, then panics while allocating a slice with a
        // negative capacity. Keep that observable partial mutation order.
        assert!(count >= 0, "negative GenGlobalIDs count");
        Ok((1..=i64::from(count))
            .map(|offset| old_id.wrapping_add(offset))
            .collect())
    }

    /// Go `Mutator.GlobalIDKey`.
    #[must_use]
    pub fn global_id_key(&self) -> Vec<u8> {
        key::next_global_id_kv_key()
    }

    /// Go `Mutator.GetGlobalID`.
    pub fn global_id(&self) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).get_i64(key::NEXT_GLOBAL_ID)
    }

    /// Go `Mutator.GenPlacementPolicyID`.
    pub fn gen_placement_policy_id(&self) -> Result<i64> {
        let _guard = POLICY_ID_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).increment(key::POLICY_GLOBAL_ID, 1)
    }

    /// Go `Mutator.GenMaskingPolicyID`.
    pub fn gen_masking_policy_id(&self) -> Result<i64> {
        let _guard = MASKING_POLICY_ID_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).increment(key::MASKING_POLICY_GLOBAL_ID, 1)
    }

    /// Go `Mutator.GetPolicyID`.
    pub fn policy_id(&self) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).get_i64(key::POLICY_GLOBAL_ID)
    }

    /// Go `Mutator.GetMaskingPolicyID`.
    pub fn masking_policy_id(&self) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).get_i64(key::MASKING_POLICY_GLOBAL_ID)
    }

    /// Go `Mutator.GetSchemaVersion`.
    pub fn schema_version(&self) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).get_i64(key::SCHEMA_VERSION)
    }

    /// Go `Mutator.GenSchemaVersion`.
    pub fn gen_schema_version(&self) -> Result<i64> {
        self.gen_schema_versions(1)
    }

    /// Go `Mutator.GenSchemaVersions`.
    pub fn gen_schema_versions(&self, count: i64) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).increment(key::SCHEMA_VERSION, count)
    }

    /// Go `Mutator.GetSchemaVersionWithNonEmptyDiff`.
    pub fn schema_version_with_non_empty_diff(&self) -> Result<i64> {
        let version = self.schema_version()?;
        if version > 0 && self.schema_diff(version)?.is_none() {
            return Ok(version - 1);
        }
        Ok(version)
    }

    /// Go `Mutator.EncodeSchemaDiffKey`.
    #[must_use]
    pub fn encoded_schema_diff_key(&self, schema_version: i64) -> Vec<u8> {
        key::schema_diff_kv_key(schema_version)
    }

    /// Go `Mutator.CreateDatabase`.
    pub fn create_database(&self, database: &DBInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::db_key(database.id);
        if structure.hget(key::DBS, &field)?.is_some() {
            return Err(MetaError::DatabaseExists);
        }
        structure.hset(key::DBS, &field, &value::serialize_db_info(database)?)
    }

    /// Go `Mutator.IsDatabaseExist`.
    pub fn database_exists(&self, database_id: i64) -> Result<bool> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget(key::DBS, &key::db_key(database_id))
            .map(|stored| stored.is_some())
    }

    /// Go `Mutator.UpdateDatabase`.
    pub fn update_database(&self, database: &DBInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::db_key(database.id);
        if structure.hget(key::DBS, &field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        structure.hset(key::DBS, &field, &value::serialize_db_info(database)?)
    }

    /// Go `Mutator.GetDatabase`.
    pub fn database(&self, database_id: i64) -> Result<Option<DBInfo>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget(key::DBS, &key::db_key(database_id))?
            .map(|stored| value::parse_db_info(&stored))
            .transpose()
    }

    /// Go `Mutator.ListDatabases`, preserving encoded field order.
    pub fn databases(&self) -> Result<Vec<DBInfo>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget_all(key::DBS)?
            .into_iter()
            .map(|pair| value::parse_db_info(&pair.value))
            .collect()
    }

    /// Go `Mutator.IterDatabases`, stopping at the first callback error.
    pub fn iter_databases(&self, mut visit: impl FnMut(&DBInfo) -> Result<()>) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).hget_iter(key::DBS, &mut |pair| {
            let database = value::parse_db_info(&pair.value)?;
            visit(&database)
        })
    }

    /// Go `Mutator.CreateMySQLDatabaseIfNotExists`.
    pub fn create_mysql_database_if_not_exists(&self) -> Result<i64> {
        if tidb_config::kerneltype::is_next_gen() {
            let id = tidb_metadef::system::SYSTEM_DATABASE_ID;
            self.create_sys_database_by_id_if_not_exists("mysql", id)?;
            return Ok(id);
        }
        let existing = self.system_database_id()?;
        if existing != 0 {
            return Ok(existing);
        }
        let id = self.gen_global_id()?;
        self.create_sys_database_by_id("mysql", id)?;
        Ok(id)
    }

    /// Go `Mutator.CreateSysDatabaseByIDIfNotExists`.
    pub fn create_sys_database_by_id_if_not_exists(&self, name: &str, id: i64) -> Result<()> {
        if self.database_exists(id)? {
            return Ok(());
        }
        self.create_sys_database_by_id(name, id)
    }

    /// Go `Mutator.CreateSysDatabaseByID`.
    pub fn create_sys_database_by_id(&self, name: &str, id: i64) -> Result<()> {
        self.create_database(&DBInfo {
            id,
            name: CiString::new(name),
            charset: tidb_mysql::UTF8MB4Charset.to_owned(),
            collate: tidb_mysql::UTF8MB4DefaultCollation.to_owned(),
            state: SchemaState::PUBLIC,
            ..Default::default()
        })
    }

    /// Go `Mutator.GetSystemDBID`.
    pub fn system_database_id(&self) -> Result<i64> {
        Ok(self
            .databases()?
            .into_iter()
            .find(|database| database.name.lowercase() == "mysql")
            .map_or(0, |database| database.id))
    }

    /// Go `Mutator.CreatePolicy`.
    pub fn create_policy(&self, policy: &PolicyInfo) -> Result<()> {
        if policy.id == 0 {
            return Err(MetaError::InvalidObjectId("policy"));
        }
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::policy_key(policy.id);
        if structure.hget(key::POLICIES, &field)?.is_some() {
            return Err(MetaError::PolicyExists);
        }
        structure.hset(
            key::POLICIES,
            &field,
            &value::serialize_policy_info(policy)?,
        )
    }

    /// Go `Mutator.UpdatePolicy`.
    pub fn update_policy(&self, policy: &PolicyInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::policy_key(policy.id);
        if structure.hget(key::POLICIES, &field)?.is_none() {
            return Err(MetaError::PolicyNotExists);
        }
        structure.hset(
            key::POLICIES,
            &field,
            &value::serialize_policy_info(policy)?,
        )
    }

    /// Go `Mutator.GetPolicy`.
    pub fn policy(&self, policy_id: i64) -> Result<PolicyInfo> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .hget(key::POLICIES, &key::policy_key(policy_id))?
            .ok_or(MetaError::PolicyIdNotExists(policy_id))?;
        value::parse_policy_info(&stored)
    }

    /// Go `Mutator.ListPolicies` in encoded field order.
    pub fn policies(&self) -> Result<Vec<PolicyInfo>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget_all(key::POLICIES)?
            .into_iter()
            .map(|pair| value::parse_policy_info(&pair.value))
            .collect()
    }

    /// Go `Mutator.DropPolicy`.
    pub fn drop_policy(&self, policy_id: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::policy_key(policy_id);
        structure.hclear(&field)?;
        structure.hdelete(key::POLICIES, &field)
    }

    /// Go `Mutator.CreateMaskingPolicy`.
    pub fn create_masking_policy(&self, policy: &MaskingPolicyInfo) -> Result<()> {
        if policy.id == 0 {
            return Err(MetaError::InvalidObjectId("masking policy"));
        }
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::masking_policy_key(policy.id);
        if structure.hget(key::MASKING_POLICIES, &field)?.is_some() {
            return Err(MetaError::MaskingPolicyIdExists(policy.id));
        }
        let encoded = tidb_model::serde_helpers::to_go_json(policy)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        structure.hset(
            key::MASKING_POLICIES,
            &field,
            &value::attach_magic_byte(&encoded),
        )
    }

    /// Go `Mutator.UpdateMaskingPolicy`.
    pub fn update_masking_policy(&self, policy: &MaskingPolicyInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::masking_policy_key(policy.id);
        if structure.hget(key::MASKING_POLICIES, &field)?.is_none() {
            return Err(MetaError::MaskingPolicyIdNotExists(policy.id));
        }
        let encoded = tidb_model::serde_helpers::to_go_json(policy)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        structure.hset(
            key::MASKING_POLICIES,
            &field,
            &value::attach_magic_byte(&encoded),
        )
    }

    /// Go `Mutator.GetMaskingPolicy`.
    pub fn masking_policy(&self, policy_id: i64) -> Result<MaskingPolicyInfo> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .hget(key::MASKING_POLICIES, &key::masking_policy_key(policy_id))?
            .ok_or(MetaError::MaskingPolicyIdNotExists(policy_id))?;
        serde_json::from_slice(value::detach_magic_byte(&stored)?)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))
    }

    /// Go `Mutator.ListMaskingPolicies` in encoded field order.
    pub fn masking_policies(&self) -> Result<Vec<MaskingPolicyInfo>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget_all(key::MASKING_POLICIES)?
            .into_iter()
            .map(|pair| {
                serde_json::from_slice(value::detach_magic_byte(&pair.value)?)
                    .map_err(|error| MetaError::InvalidJson(error.to_string()))
            })
            .collect()
    }

    /// Go `Mutator.DropMaskingPolicy`.
    pub fn drop_masking_policy(&self, policy_id: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::masking_policy_key(policy_id);
        structure.hclear(&field)?;
        structure.hdelete(key::MASKING_POLICIES, &field)
    }

    /// Go `Mutator.AddResourceGroup`.
    pub fn add_resource_group(&self, group: &ResourceGroupInfo) -> Result<()> {
        if group.id == 0 {
            return Err(MetaError::InvalidObjectId("group"));
        }
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::resource_group_key(group.id);
        if structure.hget(key::RESOURCE_GROUPS, &field)?.is_some() {
            return Err(MetaError::ResourceGroupExists);
        }
        structure.hset(
            key::RESOURCE_GROUPS,
            &field,
            &value::attach_magic_byte(&encode_resource_group(group)?),
        )
    }

    /// Go `Mutator.UpdateResourceGroup`; ID 1 bypasses the existence check
    /// because Go's default group may not be persisted yet.
    pub fn update_resource_group(&self, group: &ResourceGroupInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let field = key::resource_group_key(group.id);
        if group.id != DEFAULT_RESOURCE_GROUP_ID
            && structure.hget(key::RESOURCE_GROUPS, &field)?.is_none()
        {
            return Err(MetaError::ResourceGroupNotExists);
        }
        structure.hset(
            key::RESOURCE_GROUPS,
            &field,
            &value::attach_magic_byte(&encode_resource_group(group)?),
        )
    }

    /// Go `Mutator.DropResourceGroup`.
    pub fn drop_resource_group(&self, group_id: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hdelete(key::RESOURCE_GROUPS, &key::resource_group_key(group_id))
    }

    /// Go `Mutator.GetResourceGroup`.
    pub fn resource_group(&self, group_id: i64) -> Result<Arc<ResourceGroupInfo>> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .hget(key::RESOURCE_GROUPS, &key::resource_group_key(group_id))?;
        match stored {
            Some(stored) => decode_resource_group(value::detach_magic_byte(&stored)?).map(Arc::new),
            None if group_id == DEFAULT_RESOURCE_GROUP_ID => Ok(default_resource_group_for_test()),
            None => Err(MetaError::ResourceGroupIdNotExists(group_id)),
        }
    }

    /// Go `Mutator.ListResourceGroups`, adding the implicit default exactly
    /// when no stored group has lower-case name `default`.
    pub fn resource_groups(&self) -> Result<Vec<Arc<ResourceGroupInfo>>> {
        let mut transaction = self.lock()?;
        let mut groups = MetaStructure::new(&mut *transaction)
            .hget_all(key::RESOURCE_GROUPS)?
            .into_iter()
            .map(|pair| decode_resource_group(value::detach_magic_byte(&pair.value)?).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        if !groups
            .iter()
            .any(|group| group.name.lowercase() == "default")
        {
            groups.push(default_resource_group_for_test());
        }
        Ok(groups)
    }

    /// Go `Mutator.CreateTableOrView`.
    pub fn create_table_or_view(&self, database_id: i64, table: &TableInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        let table_field = key::table_key(table.id);
        if structure.hget(&database_field, &table_field)?.is_some() {
            return Err(MetaError::TableExists);
        }
        structure.hset(
            &database_field,
            &table_field,
            &value::serialize_table_info(table)?,
        )
    }

    /// Go `Mutator.GenAutoTableIDKeyValue`.
    #[must_use]
    pub fn auto_table_id_key_value(
        &self,
        database_id: i64,
        table_id: i64,
        auto_id: i64,
    ) -> (Vec<u8>, Vec<u8>) {
        (
            key::auto_table_id_kv_key(database_id, table_id),
            value::encode_int_value(auto_id),
        )
    }

    /// Go `Mutator.GetAutoIDAccessors`.
    #[must_use]
    pub fn auto_ids(&self, database_id: i64, table_id: i64) -> AutoIdAccessors<T> {
        AutoIdAccessors {
            meta: self.clone(),
            database_id,
            table_id,
        }
    }

    /// Go `Mutator.CreateTableAndSetAutoID` with source mutation order.
    pub fn create_table_and_set_auto_id(
        &self,
        database_id: i64,
        table: &TableInfo,
        auto_ids: AutoIdGroup,
    ) -> Result<()> {
        self.create_table_or_view(database_id, table)?;
        self.auto_ids(database_id, table.id)
            .row_id()
            .increment(auto_ids.row_id)?;
        if table.auto_random_bits > 0 {
            self.auto_ids(database_id, table.id)
                .random_id()
                .increment(auto_ids.random_id)?;
        }
        if table.sep_auto_inc() && table.get_auto_increment_col_info().is_some() {
            self.auto_ids(database_id, table.id)
                .increment_id(tidb_model::table_info::TABLE_INFO_VERSION5)
                .increment(auto_ids.increment_id)?;
        }
        Ok(())
    }

    /// Go `Mutator.CreateSequenceAndSetSeqValue`.
    pub fn create_sequence_and_set_value(
        &self,
        database_id: i64,
        table: &TableInfo,
        sequence_value: i64,
    ) -> Result<()> {
        self.create_table_or_view(database_id, table)?;
        self.auto_ids(database_id, table.id)
            .sequence_value()
            .increment(sequence_value)?;
        Ok(())
    }

    /// Go `Mutator.RestartSequenceValue`.
    pub fn restart_sequence_value(
        &self,
        database_id: i64,
        table: &TableInfo,
        sequence_value: i64,
    ) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        if structure
            .hget(&database_field, &key::table_key(table.id))?
            .is_none()
        {
            return Err(MetaError::TableNotExists);
        }
        structure.hset(
            &database_field,
            &key::sequence_key(table.id),
            &value::encode_int_value(sequence_value),
        )
    }

    /// Go `Mutator.GetTable`.
    pub fn table(&self, database_id: i64, table_id: i64) -> Result<Option<TableInfo>> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        structure
            .hget(&database_field, &key::table_key(table_id))?
            .map(|stored| value::parse_table_info(&stored, database_id))
            .transpose()
    }

    /// Go `Mutator.CheckTableExists`.
    pub fn table_exists(&self, database_id: i64, table_id: i64) -> Result<bool> {
        self.table(database_id, table_id)
            .map(|table| table.is_some())
    }

    /// Go `Mutator.UpdateTable`; Go mutates the caller's revision before
    /// serializing, so the Rust caller passes a mutable table too.
    pub fn update_table(&self, database_id: i64, table: &mut TableInfo) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        let table_field = key::table_key(table.id);
        if structure.hget(&database_field, &table_field)?.is_none() {
            return Err(MetaError::TableNotExists);
        }
        table.revision = table.revision.wrapping_add(1);
        structure.hset(
            &database_field,
            &table_field,
            &value::serialize_table_info(table)?,
        )
    }

    /// Go `Mutator.ListTables`, filtering non-table fields from the database
    /// hash and preserving encoded field order.
    pub fn tables(&self, database_id: i64) -> Result<Vec<TableInfo>> {
        self.tables_with_cancel(database_id, || false)
    }

    /// Go `Mutator.ListTables`, with an explicit context-cancellation probe.
    pub fn tables_with_cancel(
        &self,
        database_id: i64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<Vec<TableInfo>> {
        let pairs = self.metas_by_database_id(database_id)?;
        let mut tables = Vec::with_capacity(pairs.len() / 2);
        for pair in pairs {
            if !pair.field.starts_with(key::TABLE_PREFIX.as_bytes()) {
                continue;
            }
            if cancelled() {
                return Err(MetaError::Cancelled);
            }
            tables.push(value::parse_table_info(&pair.value, database_id)?);
        }
        Ok(tables)
    }

    /// Go `Mutator.IterTables`, stopping at the first callback error.
    pub fn iter_tables(
        &self,
        database_id: i64,
        mut visit: impl FnMut(&TableInfo) -> Result<()>,
    ) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        structure.hget_iter(&database_field, &mut |pair| {
            if !pair.field.starts_with(key::TABLE_PREFIX.as_bytes()) {
                return Ok(());
            }
            let table = value::parse_table_info(&pair.value, database_id)?;
            visit(&table)
        })
    }

    /// Go `Mutator.GetMetasByDBID`.
    pub fn metas_by_database_id(&self, database_id: i64) -> Result<Vec<HashPair>> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        structure.hget_all(&database_field)
    }

    /// Go `Mutator.ListSimpleTables`.
    pub fn simple_tables(&self, database_id: i64) -> Result<Vec<TableNameInfo>> {
        self.metas_by_database_id(database_id)?
            .into_iter()
            .filter(|pair| pair.field.starts_with(key::TABLE_PREFIX.as_bytes()))
            .map(|pair| fast_unmarshal_table_name_info(&pair.value))
            .collect()
    }

    /// Go `GetTableInfoWithAttributes`.
    pub fn table_info_with_attributes(
        &self,
        database_id: i64,
        filters: &[MustLoadFilterAttr<'_>],
    ) -> Result<Vec<TableInfo>> {
        self.metas_by_database_id(database_id)?
            .into_iter()
            .filter(|pair| pair.field.starts_with(b"Table"))
            .filter(|pair| table_info_must_load_with_filters(&pair.value, false, filters))
            .map(|pair| value::parse_table_info(&pair.value, database_id))
            .collect()
    }

    /// Go `Mutator.GetAllNameToIDAndTheMustLoadedTableInfo`.
    pub fn all_name_to_id_and_must_loaded_table_info(
        &self,
        database_id: i64,
    ) -> Result<NameToIdAndMustLoadedTableInfo> {
        let id_regex = regex::bytes::Regex::new(r#""id":(\d+)"#)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        let name_regex = regex::bytes::Regex::new(NAME_EXTRACT_REGEXP)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        let mut names = BTreeMap::new();
        let mut must_load = Vec::new();
        for pair in self.metas_by_database_id(database_id)? {
            if !pair.field.starts_with(b"Table") {
                continue;
            }
            // Go indexes captures without checking their length; malformed
            // stored table JSON therefore panics at this fast path.
            let id_capture = id_regex.captures(&pair.value).unwrap();
            let name_capture = name_regex.captures(&pair.value).unwrap();
            let id = std::str::from_utf8(&id_capture[1])
                .expect("the source regex only captures ASCII digits")
                .parse::<i64>()
                .map_err(|_| MetaError::InvalidIntValue)?;
            names.insert(unescape_name_bytes(&name_capture[1]), id);
            if table_info_must_load(&pair.value) {
                must_load.push(value::parse_table_info(&pair.value, database_id)?);
            }
        }
        Ok((names, must_load))
    }

    /// Go `Mutator.DropTableOrView`.
    pub fn drop_table_or_view(&self, database_id: i64, table_id: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        if structure.hget(key::DBS, &database_field)?.is_none() {
            return Err(MetaError::DatabaseNotExists);
        }
        let table_field = key::table_key(table_id);
        if structure.hget(&database_field, &table_field)?.is_none() {
            return Err(MetaError::TableNotExists);
        }
        structure.hdelete(&database_field, &table_field)
    }

    /// Go `Mutator.DropSequence`.
    pub fn drop_sequence(&self, database_id: i64, table_id: i64) -> Result<()> {
        self.drop_table_or_view(database_id, table_id)?;
        self.auto_ids(database_id, table_id).delete()?;
        self.auto_ids(database_id, table_id)
            .sequence_value()
            .delete()
    }

    /// Go `Mutator.DropDatabase`; Go deliberately does not require existence.
    pub fn drop_database(&self, database_id: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        let mut structure = MetaStructure::new(&mut *transaction);
        let database_field = key::db_key(database_id);
        structure.hclear(&database_field)?;
        structure.hdelete(key::DBS, &database_field)
    }

    /// Go `Mutator.SetBDRRole`.
    pub fn set_bdr_role(&self, role: &[u8]) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(key::BDR_ROLE, role)
    }

    /// Go `Mutator.GetBDRRole` as raw Go-string bytes.
    pub fn bdr_role(&self) -> Result<Vec<u8>> {
        let mut transaction = self.lock()?;
        Ok(MetaStructure::new(&mut *transaction)
            .get(key::BDR_ROLE)?
            .unwrap_or_default())
    }

    /// Go `Mutator.ClearBDRRole`.
    pub fn clear_bdr_role(&self) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).clear(key::BDR_ROLE)
    }

    /// Go `Mutator.SetDDLTableVersion`.
    pub fn set_ddl_table_version(&self, version: DdlTableVersion) -> Result<()> {
        self.set_table_version(key::DDL_TABLE_VERSION, version.0)
    }

    /// Go `Mutator.SetNextGenBootTableVersion`.
    pub fn set_next_gen_boot_table_version(&self, version: NextGenBootTableVersion) -> Result<()> {
        self.set_table_version(key::BOOT_TABLE_VERSION, version.0)
    }

    fn set_table_version(&self, logical_key: &[u8], version: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(logical_key, &value::encode_int_value(version))
    }

    /// Go `Mutator.GetDDLTableVersion`.
    pub fn ddl_table_version(&self) -> Result<DdlTableVersion> {
        self.table_version(key::DDL_TABLE_VERSION)
            .map(DdlTableVersion)
    }

    /// Go `Mutator.GetNextGenBootTableVersion`.
    pub fn next_gen_boot_table_version(&self) -> Result<NextGenBootTableVersion> {
        self.table_version(key::BOOT_TABLE_VERSION)
            .map(NextGenBootTableVersion)
    }

    fn table_version(&self, logical_key: &[u8]) -> Result<i64> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .get(logical_key)?
            .unwrap_or_default();
        if stored.is_empty() {
            return Ok(0);
        }
        value::parse_int_value(&stored)
    }

    /// Go `Mutator.SetMetadataLock`.
    pub fn set_metadata_lock(&self, enabled: bool) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .set(key::METADATA_LOCK, if enabled { b"1" } else { b"0" })
    }

    /// Go `Mutator.GetMetadataLock`; `None` is Go's `isNull=true` result.
    pub fn metadata_lock(&self) -> Result<Option<bool>> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .get(key::METADATA_LOCK)?
            .unwrap_or_default();
        if stored.is_empty() {
            return Ok(None);
        }
        Ok(Some(stored == b"1"))
    }

    /// Go `Mutator.SetSchemaCacheSize`.
    pub fn set_schema_cache_size(&self, size: u64) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .set(key::SCHEMA_CACHE_SIZE, size.to_string().as_bytes())
    }

    /// Go `Mutator.GetSchemaCacheSize`; `None` is Go's `isNull=true` result.
    pub fn schema_cache_size(&self) -> Result<Option<u64>> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .get(key::SCHEMA_CACHE_SIZE)?
            .unwrap_or_default();
        if stored.is_empty() {
            return Ok(None);
        }
        std::str::from_utf8(&stored)
            .map_err(|_| MetaError::InvalidUnsignedIntValue)?
            .parse()
            .map(Some)
            .map_err(|_| MetaError::InvalidUnsignedIntValue)
    }

    /// Go `Mutator.SetIngestMaxBatchSplitRanges`.
    pub fn set_ingest_max_batch_split_ranges(&self, setting: i64) -> Result<()> {
        self.set_decimal_setting(key::INGEST_MAX_BATCH_SPLIT_RANGES, setting)
    }

    /// Go `Mutator.GetIngestMaxBatchSplitRanges`.
    pub fn ingest_max_batch_split_ranges(&self) -> Result<Option<i64>> {
        self.decimal_setting(key::INGEST_MAX_BATCH_SPLIT_RANGES)
    }

    /// Go `Mutator.SetIngestMaxInflight`.
    pub fn set_ingest_max_inflight(&self, setting: i64) -> Result<()> {
        self.set_decimal_setting(key::INGEST_MAX_INFLIGHT, setting)
    }

    /// Go `Mutator.GetIngestMaxInflight`.
    pub fn ingest_max_inflight(&self) -> Result<Option<i64>> {
        self.decimal_setting(key::INGEST_MAX_INFLIGHT)
    }

    fn set_decimal_setting(&self, logical_key: &[u8], setting: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(logical_key, &value::encode_int_value(setting))
    }

    fn decimal_setting(&self, logical_key: &[u8]) -> Result<Option<i64>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .get(logical_key)?
            .map(|stored| value::parse_int_value(&stored))
            .transpose()
    }

    /// Go `Mutator.SetIngestMaxSplitRangesPerSec`.
    pub fn set_ingest_max_split_ranges_per_sec(&self, setting: f64) -> Result<()> {
        self.set_float_setting(key::INGEST_MAX_SPLIT_RANGES_PER_SEC, setting)
    }

    /// Go `Mutator.GetIngestMaxSplitRangesPerSec`.
    pub fn ingest_max_split_ranges_per_sec(&self) -> Result<Option<f64>> {
        self.float_setting(key::INGEST_MAX_SPLIT_RANGES_PER_SEC)
    }

    /// Go `Mutator.SetIngestMaxPerSec`.
    pub fn set_ingest_max_per_sec(&self, setting: f64) -> Result<()> {
        self.set_float_setting(key::INGEST_MAX_PER_SEC, setting)
    }

    /// Go `Mutator.GetIngestMaxPerSec`.
    pub fn ingest_max_per_sec(&self) -> Result<Option<f64>> {
        self.float_setting(key::INGEST_MAX_PER_SEC)
    }

    fn set_float_setting(&self, logical_key: &[u8], setting: f64) -> Result<()> {
        let stored = go_fixed_two(setting);
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(logical_key, stored.as_bytes())
    }

    fn float_setting(&self, logical_key: &[u8]) -> Result<Option<f64>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .get(logical_key)?
            .map(|stored| {
                std::str::from_utf8(&stored)
                    .map_err(|_| MetaError::InvalidFloatValue)?
                    .parse()
                    .map_err(|_| MetaError::InvalidFloatValue)
            })
            .transpose()
    }

    /// Go `Mutator.SetDXFScheduleTuneFactors`.
    pub fn set_dxf_schedule_tune_factors(
        &self,
        keyspace: &str,
        factors: &TtlTuneFactors,
    ) -> Result<()> {
        let encoded = tidb_model::serde_helpers::to_go_json(factors)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).hset(
            key::DXF_SCHEDULE_TUNE,
            keyspace.as_bytes(),
            &encoded,
        )
    }

    /// Go `Mutator.GetDXFScheduleTuneFactors`.
    pub fn dxf_schedule_tune_factors(&self, keyspace: &str) -> Result<Option<TtlTuneFactors>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget(key::DXF_SCHEDULE_TUNE, keyspace.as_bytes())?
            .map(|encoded| {
                serde_json::from_slice(&encoded)
                    .map_err(|error| MetaError::InvalidJson(error.to_string()))
            })
            .transpose()
    }

    /// Go `Mutator.GetRUStats`.
    pub fn ru_stats(&self) -> Result<Option<RuStats>> {
        let mut transaction = self.lock()?;
        let Some(encoded) = MetaStructure::new(&mut *transaction).get(key::REQUEST_UNIT_STATS)?
        else {
            return Ok(None);
        };
        serde_json::from_slice::<Option<RuStats>>(&encoded)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))
    }

    /// Go `Mutator.SetRUStats`; `None` stores JSON `null` exactly as Go does
    /// for a nil `*RUStats`.
    pub fn set_ru_stats(&self, stats: Option<&RuStats>) -> Result<()> {
        let encoded = tidb_model::serde_helpers::to_go_json(&stats)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(key::REQUEST_UNIT_STATS, &encoded)
    }

    /// Go `Mutator.GetBootstrapVersion`.
    pub fn bootstrap_version(&self) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).get_i64(key::BOOTSTRAP)
    }

    /// Go `Mutator.FinishBootstrap`.
    pub fn finish_bootstrap(&self, version: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(key::BOOTSTRAP, &value::encode_int_value(version))
    }

    /// Go `Mutator.GetSchemaDiff`.
    pub fn schema_diff(&self, schema_version: i64) -> Result<Option<SchemaDiff>> {
        let mut transaction = self.lock()?;
        let stored = MetaStructure::new(&mut *transaction)
            .get(&key::schema_diff_key(schema_version))?
            .unwrap_or_default();
        value::parse_schema_diff(&stored)
    }

    /// Go `Mutator.SetSchemaDiff`.
    pub fn set_schema_diff(&self, diff: &SchemaDiff) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).set(
            &key::schema_diff_key(diff.version),
            &value::serialize_schema_diff(diff)?,
        )
    }

    /// Go test-only `DDLJobHistoryKey`.
    #[must_use]
    pub fn ddl_job_history_key(&self, job_id: i64) -> Vec<u8> {
        key::ddl_job_history_kv_key(job_id)
    }

    /// Go `Mutator.addHistoryDDLJob` / `AddHistoryDDLJob`.
    pub fn add_history_ddl_job<J: DdlJobCodec>(
        &self,
        job: &mut J,
        update_raw_args: bool,
    ) -> Result<()> {
        let encoded = job.encode(update_raw_args)?;
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).hset(
            key::DDL_JOB_HISTORY,
            &key::ddl_job_id_key(job.id()),
            &encoded,
        )
    }

    /// Go `Mutator.getHistoryDDLJob` / `GetHistoryDDLJob`.
    pub fn history_ddl_job<J: DdlJobCodec>(&self, job_id: i64) -> Result<Option<J>> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget(key::DDL_JOB_HISTORY, &key::ddl_job_id_key(job_id))?
            .map(|encoded| J::decode(&encoded))
            .transpose()
    }

    /// Go `Mutator.GetHistoryDDLCount`.
    pub fn history_ddl_count(&self) -> Result<u64> {
        let mut transaction = self.lock()?;
        let count = MetaStructure::new(&mut *transaction)
            .hget_all(key::DDL_JOB_HISTORY)?
            .len();
        u64::try_from(count).map_err(|_| MetaError::Storage("history count overflow".to_owned()))
    }

    /// Go `Mutator.GetLastHistoryDDLJobsIterator`.
    pub fn last_history_ddl_jobs<J: DdlJobCodec>(&self) -> Result<HistoryDdlJobIterator<J>> {
        self.history_ddl_jobs_from_field(None, BTreeSet::new(), BTreeSet::new())
    }

    /// Go `Mutator.GetLastHistoryDDLJobsIteratorWithFilter`.
    pub fn last_history_ddl_jobs_with_filter<J: DdlJobCodec>(
        &self,
        schema_names: BTreeSet<String>,
        table_names: BTreeSet<String>,
    ) -> Result<HistoryDdlJobIterator<J>> {
        self.history_ddl_jobs_from_field(None, schema_names, table_names)
    }

    /// Go `Mutator.GetHistoryDDLJobsIterator`, inclusive of `start_job_id`.
    pub fn history_ddl_jobs<J: DdlJobCodec>(
        &self,
        start_job_id: i64,
    ) -> Result<HistoryDdlJobIterator<J>> {
        self.history_ddl_jobs_from_field(
            Some(key::ddl_job_id_key(start_job_id)),
            BTreeSet::new(),
            BTreeSet::new(),
        )
    }

    fn history_ddl_jobs_from_field<J: DdlJobCodec>(
        &self,
        start_field: Option<[u8; 8]>,
        schema_names: BTreeSet<String>,
        table_names: BTreeSet<String>,
    ) -> Result<HistoryDdlJobIterator<J>> {
        let mut transaction = self.lock()?;
        let prefix = structure::encode_hash_data_key_prefix(key::DDL_JOB_HISTORY);
        let upper = start_field
            .as_ref()
            .map(|field| structure::encode_hash_data_key(key::DDL_JOB_HISTORY, field));
        let iterator = transaction.reverse_scan_prefix(&prefix, upper.as_deref())?;
        Ok(HistoryDdlJobIterator {
            iterator,
            schema_names,
            table_names,
            marker: std::marker::PhantomData,
        })
    }

    /// Increments one row-ID allocator field. This is the source operation
    /// reached through Go `GetAutoIDAccessors(...).RowID().Inc(...)`.
    pub fn increment_row_id(&self, database_id: i64, table_id: i64, step: i64) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction).hincrement(
            &key::db_key(database_id),
            &key::auto_table_id_key(table_id),
            step,
        )
    }

    /// Reads one row-ID allocator field; missing fields are zero.
    pub fn row_id(&self, database_id: i64, table_id: i64) -> Result<i64> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget_i64(&key::db_key(database_id), &key::auto_table_id_key(table_id))
    }

    /// Deletes one row-ID allocator field.
    pub fn delete_row_id(&self, database_id: i64, table_id: i64) -> Result<()> {
        let mut transaction = self.lock()?;
        MetaStructure::new(&mut *transaction)
            .hdelete(&key::db_key(database_id), &key::auto_table_id_key(table_id))
    }
}

/// Go `autoIDAccessors`, returned by `Mutator.GetAutoIDAccessors`.
pub struct AutoIdAccessors<T> {
    meta: Mutator<T>,
    database_id: i64,
    table_id: i64,
}

impl<T: RawTransaction> AutoIdAccessors<T> {
    /// Reads row, separate increment, and random IDs in Go's order.
    pub fn get(&self) -> Result<AutoIdGroup> {
        Ok(AutoIdGroup {
            row_id: self.row_id().get()?,
            increment_id: self
                .increment_id(tidb_model::table_info::TABLE_INFO_VERSION5)
                .get()?,
            random_id: self.random_id().get()?,
        })
    }

    /// Writes row, separate increment, and random IDs in Go's order.
    pub fn put(&self, values: AutoIdGroup) -> Result<()> {
        self.row_id().put(values.row_id)?;
        self.increment_id(tidb_model::table_info::TABLE_INFO_VERSION5)
            .put(values.increment_id)?;
        self.random_id().put(values.random_id)
    }

    /// Deletes row, separate increment, and random IDs in Go's order.
    pub fn delete(&self) -> Result<()> {
        self.row_id().delete()?;
        self.increment_id(tidb_model::table_info::TABLE_INFO_VERSION5)
            .delete()?;
        self.random_id().delete()
    }

    /// Go `RowID`.
    #[must_use]
    pub fn row_id(&self) -> AutoIdAccessor<T> {
        self.accessor(AutoIdKind::Row)
    }

    /// Go `IncrementID`; table versions before 5 share the row-ID field.
    #[must_use]
    pub fn increment_id(&self, table_version: u16) -> AutoIdAccessor<T> {
        if table_version < tidb_model::table_info::TABLE_INFO_VERSION5 {
            self.row_id()
        } else {
            self.accessor(AutoIdKind::Increment)
        }
    }

    /// Go `RandomID`.
    #[must_use]
    pub fn random_id(&self) -> AutoIdAccessor<T> {
        self.accessor(AutoIdKind::Random)
    }

    /// Go `SequenceValue`.
    #[must_use]
    pub fn sequence_value(&self) -> AutoIdAccessor<T> {
        self.accessor(AutoIdKind::SequenceValue)
    }

    /// Go `SequenceCycle`.
    #[must_use]
    pub fn sequence_cycle(&self) -> AutoIdAccessor<T> {
        self.accessor(AutoIdKind::SequenceCycle)
    }

    fn accessor(&self, kind: AutoIdKind) -> AutoIdAccessor<T> {
        AutoIdAccessor {
            meta: self.meta.clone(),
            database_id: self.database_id,
            table_id: self.table_id,
            kind,
        }
    }
}

#[derive(Clone, Copy)]
enum AutoIdKind {
    Row,
    Increment,
    Random,
    SequenceValue,
    SequenceCycle,
}

/// One Go `autoIDAccessor` selected from [`AutoIdAccessors`].
pub struct AutoIdAccessor<T> {
    meta: Mutator<T>,
    database_id: i64,
    table_id: i64,
    kind: AutoIdKind,
}

impl<T: RawTransaction> AutoIdAccessor<T> {
    fn field(&self) -> Vec<u8> {
        match self.kind {
            AutoIdKind::Row => key::auto_table_id_key(self.table_id),
            AutoIdKind::Increment => key::auto_increment_id_key(self.table_id),
            AutoIdKind::Random => key::auto_random_table_id_key(self.table_id),
            AutoIdKind::SequenceValue => key::sequence_key(self.table_id),
            AutoIdKind::SequenceCycle => key::sequence_cycle_key(self.table_id),
        }
    }

    /// Go `AutoIDAccessor.Get`.
    pub fn get(&self) -> Result<i64> {
        let mut transaction = self.meta.lock()?;
        MetaStructure::new(&mut *transaction)
            .hget_i64(&key::db_key(self.database_id), &self.field())
    }

    /// Go `AutoIDAccessor.Put`.
    pub fn put(&self, value: i64) -> Result<()> {
        let mut transaction = self.meta.lock()?;
        MetaStructure::new(&mut *transaction).hset(
            &key::db_key(self.database_id),
            &self.field(),
            &value::encode_int_value(value),
        )
    }

    /// Go `AutoIDAccessor.Inc`. Database and table existence are deliberately
    /// not checked because rename races make those checks invalid in Go.
    pub fn increment(&self, step: i64) -> Result<i64> {
        let mut transaction = self.meta.lock()?;
        MetaStructure::new(&mut *transaction).hincrement(
            &key::db_key(self.database_id),
            &self.field(),
            step,
        )
    }

    /// Go `AutoIDAccessor.CopyTo`; zero is deliberately not copied.
    pub fn copy_to(&self, database_id: i64, table_id: i64) -> Result<()> {
        let current = self.get()?;
        if current == 0 {
            return Ok(());
        }
        let field = match self.kind {
            AutoIdKind::Row => key::auto_table_id_key(table_id),
            AutoIdKind::Increment => key::auto_increment_id_key(table_id),
            AutoIdKind::Random => key::auto_random_table_id_key(table_id),
            AutoIdKind::SequenceValue => key::sequence_key(table_id),
            AutoIdKind::SequenceCycle => key::sequence_cycle_key(table_id),
        };
        let mut transaction = self.meta.lock()?;
        MetaStructure::new(&mut *transaction).hset(
            &key::db_key(database_id),
            &field,
            &value::encode_int_value(current),
        )
    }

    /// Go `AutoIDAccessor.Del`.
    pub fn delete(&self) -> Result<()> {
        let mut transaction = self.meta.lock()?;
        MetaStructure::new(&mut *transaction).hdelete(&key::db_key(self.database_id), &self.field())
    }
}

/// Go `splitRangeInt64Max`.
#[must_use]
pub fn split_range_int64_max(count: i64) -> Vec<(String, String)> {
    assert!(count >= 0, "negative split range count");
    let mut ranges = Vec::with_capacity(count as usize);
    // Go reaches this division after successfully allocating a zero-length
    // slice, so count zero intentionally panics here.
    let batch = 9_999_999_999_999_999_999_u64 / count as u64;
    for index in 0..count as u64 {
        let start = batch * index;
        let end = batch * (index + 1);
        ranges.push((
            if index == 0 {
                "0".to_owned()
            } else {
                format!("{start:019}")
            },
            format!("{end:019}"),
        ));
    }
    ranges
}

/// Go `IterAllTables`: streams every table field over bounded database-key
/// ranges using between one and fifteen independent snapshots. The callback is
/// serialized exactly as Go's `mu.Lock` region, while decoding and scanning
/// remain concurrent.
pub fn iter_all_tables<S, C, F>(
    store: &S,
    start_ts: u64,
    concurrency: i32,
    cancelled: &C,
    visit: F,
) -> Result<()>
where
    S: MetaSnapshotStore,
    C: Fn() -> bool + Sync,
    F: FnMut(&TableInfo) -> Result<()> + Send,
{
    let concurrency = concurrency.clamp(1, 15);
    let ranges = split_range_int64_max(i64::from(concurrency));
    let callback = Mutex::new(visit);
    let stop = AtomicBool::new(false);
    let first_error = Mutex::new(None::<MetaError>);

    // Go creates and tags every snapshot in the parent goroutine before
    // starting the corresponding worker.
    let snapshots: Vec<_> = (0..concurrency)
        .map(|_| {
            let mut snapshot = store.snapshot(start_ts);
            snapshot.mark_internal_meta_request();
            snapshot
        })
        .collect();

    std::thread::scope(|scope| {
        for (mut snapshot, (range_start, range_end)) in snapshots.into_iter().zip(ranges) {
            let callback = &callback;
            let stop = &stop;
            let first_error = &first_error;
            scope.spawn(move || {
                let worker = catch_unwind(AssertUnwindSafe(|| {
                    let mut logical_start = b"DB:".to_vec();
                    tidb_codec::encode_bytes(&mut logical_start, range_start.as_bytes());
                    let mut logical_end = b"DB:".to_vec();
                    tidb_codec::encode_bytes(&mut logical_end, range_end.as_bytes());
                    let encoded_start = structure::encode_hash_data_key_prefix(&logical_start);
                    let encoded_end = structure::encode_hash_data_key_prefix(&logical_end);

                    snapshot.iterate_range(
                        &encoded_start,
                        &encoded_end,
                        &mut |encoded_key, encoded_value| {
                            // An error from another worker cancels this worker;
                            // the originating error remains the one returned.
                            if stop.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            if cancelled() {
                                return Err(MetaError::Cancelled);
                            }
                            // Go deliberately skips malformed unrelated keys
                            // inside the bounded raw range.
                            let Ok((database_key, field)) =
                                structure::decode_hash_data_key(encoded_key)
                            else {
                                return Ok(());
                            };
                            if !field.starts_with(key::TABLE_PREFIX.as_bytes()) {
                                return Ok(());
                            }
                            let database_id = key::parse_db_key(&database_key)?;
                            let table = value::parse_table_info(encoded_value, database_id)?;
                            let mut callback = callback.lock().map_err(|_| {
                                MetaError::Storage("IterAllTables callback mutex poisoned".into())
                            })?;
                            callback(&table)
                        },
                    )
                }));

                let result = match worker {
                    Ok(result) => result,
                    Err(_) => Err(MetaError::Storage(
                        "panic recovered in IterAllTables worker".into(),
                    )),
                };
                if let Err(error) = result {
                    stop.store(true, Ordering::Release);
                    let mut first = first_error.lock().expect("error mutex is never exposed");
                    if first.is_none() {
                        *first = Some(error);
                    }
                }
            });
        }
    });

    first_error
        .into_inner()
        .expect("error mutex is never exposed")
        .map_or(Ok(()), Err)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Go `isTableInfoMustLoad` with an explicit filter list.
#[must_use]
pub fn table_info_must_load_with_filters(
    mut json: &[u8],
    check_foreign_keys_in_order: bool,
    filters: &[MustLoadFilterAttr<'_>],
) -> bool {
    if check_foreign_keys_in_order {
        let foreign_key = find_bytes(json, FOREIGN_KEY_ATTRIBUTES_NIL)
            .or_else(|| find_bytes(json, FOREIGN_KEY_ATTRIBUTES_ZERO));
        let Some(index) = foreign_key else {
            return true;
        };
        json = &json[index..];
    }
    for filter in filters {
        let Some(index) = find_bytes(json, filter.attr) else {
            if filter.load_if_missing {
                return true;
            }
            continue;
        };
        if !filter.load_if_missing {
            return true;
        }
        json = &json[index..];
    }
    false
}

/// Go `IsTableInfoMustLoad`.
#[must_use]
pub fn table_info_must_load(json: &[u8]) -> bool {
    table_info_must_load_with_filters(json, true, TABLE_INFO_MUST_LOAD_FILTERS)
}

/// Go `Unescape`; replacements are deliberately ordered.
#[must_use]
pub fn unescape_name(value: &str) -> String {
    value.replace(r#"\""#, r#"""#).replace(r#"\\"#, r#"\"#)
}

/// Byte-string form of Go `Unescape`. Go strings can contain invalid UTF-8,
/// so the fast metadata path returns byte keys rather than silently rejecting
/// bytes that `regexp` and `strings.ReplaceAll` accept.
#[must_use]
pub fn unescape_name_bytes(value: &[u8]) -> Vec<u8> {
    fn replace_all(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(input.len());
        let mut rest = input;
        while let Some(index) = rest.windows(from.len()).position(|window| window == from) {
            output.extend_from_slice(&rest[..index]);
            output.extend_from_slice(to);
            rest = &rest[index + from.len()..];
        }
        output.extend_from_slice(rest);
        output
    }

    let quotes = replace_all(value, br#"\""#, br#"""#);
    replace_all(&quotes, br#"\\"#, br#"\"#)
}

/// Go `FastUnmarshalTableNameInfo` over the partial-JSON extractor.
pub fn fast_unmarshal_table_name_info(data: &[u8]) -> Result<TableNameInfo> {
    let members = extract_top_level_members(data, TABLE_NAME_INFO_FIELDS)
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    let id = serde_json::from_str::<i64>(members["id"].get())
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    struct SourceName(String);
    impl<'de> Deserialize<'de> for SourceName {
        fn deserialize<D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> std::result::Result<Self, D::Error> {
            struct SourceNameVisitor;
            impl<'de> serde::de::Visitor<'de> for SourceNameVisitor {
                type Value = SourceName;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("a two-field CI string object")
                }

                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Self::Value, A::Error> {
                    let Some(_first_key) = map.next_key::<String>()? else {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    };
                    // Go takes token 2, the first value, without checking the
                    // first key's spelling.
                    let first_value = map.next_value::<String>()?;
                    let Some(_second_key) = map.next_key::<String>()? else {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    };
                    let second_value = map.next_value::<serde_json::Value>()?;
                    if second_value.is_array() || second_value.is_object() {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    }
                    if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                        return Err(serde::de::Error::custom("unexpected name field in JSON"));
                    }
                    Ok(SourceName(first_value))
                }
            }
            deserializer.deserialize_map(SourceNameVisitor)
        }
    }
    let SourceName(original) = serde_json::from_str(members["name"].get())
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    Ok(TableNameInfo {
        id,
        name: CiString::new(&original),
    })
}

/// Go `ExtractSchemaAndTableNameFromJob`.
pub fn extract_schema_and_table_name_from_job(data: &[u8]) -> Result<(String, String)> {
    let members = extract_top_level_members(data, JOB_EXTRACT_FIELDS)
        .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
    let schema = serde_json::from_str::<String>(members["schema_name"].get())
        .map_err(|_| MetaError::InvalidJson("unexpected name field in JSON".to_owned()))?;
    let table = serde_json::from_str::<String>(members["table_name"].get())
        .map_err(|_| MetaError::InvalidJson("unexpected name field in JSON".to_owned()))?;
    Ok((schema, table))
}

/// Go `IsJobMatch`, including the source expression's `&&`/`||` precedence.
pub fn job_matches(
    job: &[u8],
    schema_names: &BTreeSet<String>,
    table_names: &BTreeSet<String>,
) -> Result<bool> {
    if schema_names.is_empty() && table_names.is_empty() {
        return Ok(true);
    }
    let (schema_name, table_name) = extract_schema_and_table_name_from_job(job)?;
    Ok(
        ((schema_names.is_empty() || schema_names.contains(&schema_name))
            && table_names.is_empty())
            || table_names.contains(&table_name),
    )
}

/// Go `DefaultGroupMeta4Test`.
#[must_use]
pub fn default_resource_group_for_test() -> Arc<ResourceGroupInfo> {
    Arc::clone(DEFAULT_RESOURCE_GROUP.get_or_init(|| {
        Arc::new(ResourceGroupInfo {
            settings: Some(Box::new(ResourceGroupSettings {
                ru_rate: i32::MAX as u64,
                priority: MEDIUM_PRIORITY_VALUE,
                burst_limit: -1,
                ..ResourceGroupSettings::default()
            })),
            id: DEFAULT_RESOURCE_GROUP_ID,
            name: CiString::new("default"),
            state: SchemaState::PUBLIC,
        })
    }))
}

fn encode_resource_group(group: &ResourceGroupInfo) -> Result<Vec<u8>> {
    tidb_model::serde_helpers::to_go_json(group)
        .map_err(|error| MetaError::InvalidJson(error.to_string()))
}

fn decode_resource_group(encoded: &[u8]) -> Result<ResourceGroupInfo> {
    serde_json::from_slice(encoded).map_err(|error| MetaError::InvalidJson(error.to_string()))
}

/// Go `GetOldestSchemaVersion`.
pub fn oldest_schema_version(reader: &mut impl MvccReader) -> Result<i64> {
    let info = reader
        .mvcc_by_encoded_key(&key::schema_version_kv_key(), u64::MAX)?
        .ok_or(MetaError::NoSchemaVersionWrite)?;
    let write = info.writes.last().ok_or(MetaError::NoSchemaVersionWrite)?;
    value::parse_int_value(&write.short_value)
}

fn check_global_id(generated: i64) -> Result<()> {
    if generated > MAX_USER_GLOBAL_ID {
        return Err(MetaError::GlobalIdExceedsLimit {
            generated,
            limit: MAX_USER_GLOBAL_ID,
        });
    }
    Ok(())
}

fn go_fixed_two(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        format!("{value:.2}")
    }
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn go_zero_time() -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(1, 1, 1)
            .expect("Go zero date")
            .and_hms_nano_opt(0, 0, 0, 0)
            .expect("Go zero time"),
        Utc,
    )
}

fn serialize_go_time<S: serde::Serializer>(
    value: &DateTime<Utc>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

fn deserialize_go_time<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<DateTime<Utc>, D::Error> {
    let value = String::deserialize(deserializer)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
#[path = "meta_go_lockdown.rs"]
mod meta_go_lockdown;
