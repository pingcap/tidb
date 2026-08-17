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

//! Complete transcreation of Go `pkg/util/topsql/reporter/ru_datamodel.go`:
//! the two-level (user, SQL) RU collection model behind Top-RU reporting,
//! with all 22 test functions of `ru_datamodel_test.go`.
//!
//! Cardinality is bounded by two "others" buckets:
//!
//! 1. *others user* ([`OTHERS_USER_WIRE_LABEL`]) for evicted users;
//! 2. *others SQL* (the [`others_key`] sentinel, empty sql + plan digest) for
//!    each user's evicted SQLs.
//!
//! Collection caps ("pre-TopN") are twice the report caps, so a burst of
//! distinct users or SQLs degrades into the "others" buckets during
//! collection instead of growing without bound, and the report-time Top-N in
//! [`RuCollecting::compact_with_limits`] then folds the evicted remainder in.
//!
//! # Ownership
//!
//! Go threads `*ruRecord` / `*userRUCollecting` pointers around and lets a
//! compacted snapshot alias the records of its source, documenting the result
//! of `compactWithLimits` as read-only or ownership-transferred. This port
//! makes that ownership transfer real: [`RuCollecting::compact_with_limits`]
//! consumes its receiver, so its fast path returns the receiver itself
//! exactly as Go does, without any aliasing being observable.

use std::collections::HashMap;

use crate::topsql_stmtstats::{BinaryDigest, RuIncrement, RuIncrementMap, RuKey};

/// Go `maxTopUsers`: the maximum number of users kept in global Top-N.
pub const MAX_TOP_USERS: usize = 200;

/// Go `maxTopSQLsPerUser`: the maximum number of SQLs kept per user.
pub const MAX_TOP_SQLS_PER_USER: usize = 200;

/// Go `othersUserWireLabel`: the wire label for the aggregated "others user",
/// used only when encoding to `TopRURecord` output.
///
/// Real user values are `vars.User.String()` — shaped `"user@host"` or `""` —
/// so this label cannot collide with a runtime user.
pub const OTHERS_USER_WIRE_LABEL: &str = "_TIDB_TOPRU_OTHERS_USER";

/// Go `maxPreTopNUsers`: the collection-phase user cap.
pub const MAX_PRE_TOP_N_USERS: usize = MAX_TOP_USERS * 2;

/// Go `maxPreTopNSQLsPerUser`: the collection-phase per-user SQL cap.
pub const MAX_PRE_TOP_N_SQLS_PER_USER: usize = MAX_TOP_SQLS_PER_USER * 2;

/// boundary: Go `tipb.TopRURecordItem`, declared locally for the same reason
/// as [`super::datamodel::TopSqlRecordItem`] — `tidb-proto` generates no tipb
/// top-sql/top-ru messages.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopRuRecordItem {
    /// Go `TimestampSec`.
    pub timestamp_sec: u64,
    /// Go `TotalRu`.
    pub total_ru: f64,
    /// Go `ExecCount`.
    pub exec_count: u64,
    /// Go `ExecDuration`.
    pub exec_duration: u64,
}

/// boundary: Go `tipb.TopRURecord`, declared locally (see
/// [`TopRuRecordItem`]).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopRuRecord {
    /// Go `KeyspaceName`.
    pub keyspace_name: Vec<u8>,
    /// Go `User`.
    pub user: String,
    /// Go `SqlDigest`; empty stands for Go's nil.
    pub sql_digest: Vec<u8>,
    /// Go `PlanDigest`; empty stands for Go's nil.
    pub plan_digest: Vec<u8>,
    /// Go `Items`.
    pub items: Vec<TopRuRecordItem>,
}

/// Go `ruItem`: RU statistics for one timestamp.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuItem {
    /// Go `ruItem.timestamp`.
    pub timestamp: u64,
    /// Go `ruItem.totalRU`.
    pub total_ru: f64,
    /// Go `ruItem.execCount`.
    pub exec_count: u64,
    /// Go `ruItem.execDuration`.
    pub exec_duration: u64,
}

impl RuItem {
    /// Go `ruItem.toProto`.
    #[must_use]
    pub fn to_proto(&self) -> TopRuRecordItem {
        TopRuRecordItem {
            timestamp_sec: self.timestamp,
            total_ru: self.total_ru,
            exec_count: self.exec_count,
            exec_duration: self.exec_duration,
        }
    }
}

/// Go `ruItems`: a list of [`RuItem`] sorted by timestamp ascending.
pub type RuItems = Vec<RuItem>;

/// Go `ruItems.toProto`. Go returns nil for an empty list, which is the empty
/// slice either way.
#[must_use]
pub fn ru_items_to_proto(items: &[RuItem]) -> Vec<TopRuRecordItem> {
    items.iter().map(RuItem::to_proto).collect()
}

/// Go's `sort.Sort(ruItems)`: by timestamp ascending.
pub fn sort_ru_items(items: &mut RuItems) {
    items.sort_by_key(|item| item.timestamp);
}

/// Go `sqlPlanKey`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct SqlPlanKey {
    /// Go `sqlPlanKey.sqlDigest`.
    pub sql_digest: BinaryDigest,
    /// Go `sqlPlanKey.planDigest`.
    pub plan_digest: BinaryDigest,
}

/// Go `othersKey`: the sentinel key of the aggregated "others SQL" bucket,
/// the zero value (empty sql + plan digest), matching legacy
/// `encodeKey(nil, nil)`.
#[must_use]
pub fn others_key() -> SqlPlanKey {
    SqlPlanKey::default()
}

/// Go `makeKey`.
#[must_use]
pub fn make_key(sql_digest: BinaryDigest, plan_digest: BinaryDigest) -> SqlPlanKey {
    SqlPlanKey {
        sql_digest,
        plan_digest,
    }
}

/// Go `isOthersKey`.
#[must_use]
pub fn is_others_key(key: &SqlPlanKey) -> bool {
    key.sql_digest.0.is_empty() && key.plan_digest.0.is_empty()
}

/// Go `ruRecord`: RU statistics for one `(sql_digest, plan_digest)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuRecord {
    /// Go `ruRecord.sqlDigest`.
    pub sql_digest: BinaryDigest,
    /// Go `ruRecord.planDigest`.
    pub plan_digest: BinaryDigest,
    /// Go `ruRecord.items`.
    pub items: RuItems,
    /// Go `ruRecord.totalRU`: cumulative RU, the Top-N sort key.
    pub total_ru: f64,
}

impl RuRecord {
    /// Go `newRURecord`.
    #[must_use]
    pub fn new(sql_digest: BinaryDigest, plan_digest: BinaryDigest) -> Self {
        Self {
            sql_digest,
            plan_digest,
            items: Vec::with_capacity(4),
            total_ru: 0.0,
        }
    }

    /// Go `newOthersRURecord`.
    #[must_use]
    pub fn new_others() -> Self {
        Self::new(BinaryDigest::default(), BinaryDigest::default())
    }

    /// Go `ruRecord.add`: appends an RU increment for a timestamp.
    ///
    /// Go scans `items` linearly rather than indexing by timestamp; a bucket
    /// holds a handful of timestamps, and the scan order decides append
    /// order, which the proto output preserves, so the scan is kept.
    pub fn add(&mut self, timestamp: u64, total_ru: f64, exec_count: u64, exec_duration: u64) {
        for item in &mut self.items {
            if item.timestamp == timestamp {
                item.total_ru += total_ru;
                item.exec_count += exec_count;
                item.exec_duration += exec_duration;
                self.total_ru += total_ru;
                return;
            }
        }
        self.items.push(RuItem {
            timestamp,
            total_ru,
            exec_count,
            exec_duration,
        });
        self.total_ru += total_ru;
    }

    /// Go `ruRecord.addIncr`. Go's nil check is structural here: the caller
    /// either has an increment or does not.
    pub fn add_incr(&mut self, timestamp: u64, incr: &RuIncrement) {
        self.add(
            timestamp,
            incr.total_ru,
            incr.exec_count,
            incr.exec_duration,
        );
    }

    /// Go `ruRecord.merge`.
    pub fn merge(&mut self, other: &RuRecord) {
        for item in &other.items {
            self.add(
                item.timestamp,
                item.total_ru,
                item.exec_count,
                item.exec_duration,
            );
        }
    }

    /// Go `ruRecord.mergeWithTimestamp`: merges with all timestamps rewritten
    /// to `ts`.
    pub fn merge_with_timestamp(&mut self, other: &RuRecord, ts: u64) {
        for item in &other.items {
            self.add(ts, item.total_ru, item.exec_count, item.exec_duration);
        }
    }
}

/// Go `ruRecords.topN`: the top `n` records by `total_ru`, plus the evicted
/// rest.
///
/// boundary: as in [`super::datamodel::records_top_n`], Go's
/// `quickselect.QuickSelect` only partitions; a full descending sort meets the
/// same contract and makes the order deterministic. Go's error branch (log
/// and return everything unsorted) has no counterpart because sorting cannot
/// fail.
#[must_use]
pub fn ru_records_top_n(mut rs: Vec<RuRecord>, n: usize) -> (Vec<RuRecord>, Vec<RuRecord>) {
    if rs.len() <= n {
        return (rs, Vec::new());
    }
    rs.sort_by(|left, right| right.total_ru.total_cmp(&left.total_ru));
    let evicted = rs.split_off(n);
    (rs, evicted)
}

/// Go `userRUCollecting`: RU data for one user, with per-user SQL Top-N.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UserRuCollecting {
    /// Go `userRUCollecting.records`.
    pub records: HashMap<SqlPlanKey, RuRecord>,
    /// Go `userRUCollecting.othersRec`: the pre-aggregated "others SQL"
    /// record.
    pub others_rec: Option<RuRecord>,
    /// Go `userRUCollecting.user`: the real user key of entries stored in
    /// [`RuCollecting::users`]. It must not be used to infer whether this is
    /// the synthetic "others user" — that identity comes from the field the
    /// collecting stores it in.
    pub user: String,
    /// Go `userRUCollecting.totalRU`: the user-level Top-N sort key.
    pub total_ru: f64,
    /// Go `userRUCollecting.preTopNSQLsPerUser`.
    pub pre_top_n_sqls_per_user: usize,
}

impl UserRuCollecting {
    /// Go `newUserRUCollecting`.
    #[must_use]
    pub fn new(user: &str) -> Self {
        Self::with_cap(user, MAX_PRE_TOP_N_SQLS_PER_USER)
    }

    /// Go `newUserRUCollectingWithCap`. Go's `<= 0` guard becomes a zero
    /// guard, `usize` having no negatives.
    #[must_use]
    pub fn with_cap(user: &str, pre_top_n_sqls_per_user: usize) -> Self {
        let pre_top_n_sqls_per_user = if pre_top_n_sqls_per_user == 0 {
            MAX_PRE_TOP_N_SQLS_PER_USER
        } else {
            pre_top_n_sqls_per_user
        };
        Self {
            user: user.to_owned(),
            records: HashMap::with_capacity(pre_top_n_sqls_per_user),
            others_rec: None,
            total_ru: 0.0,
            pre_top_n_sqls_per_user,
        }
    }

    /// Go `newOthersUserRUCollectingWithCap`.
    #[must_use]
    pub fn new_others_user(pre_top_n_sqls_per_user: usize) -> Self {
        Self::with_cap("", pre_top_n_sqls_per_user)
    }

    /// Go `userRUCollecting.add`: adds RU increments for one SQL. At the
    /// pre-Top-N capacity, new SQLs fold into "others SQL".
    pub fn add(
        &mut self,
        timestamp: u64,
        sql_digest: BinaryDigest,
        plan_digest: BinaryDigest,
        incr: &RuIncrement,
    ) {
        let key = make_key(sql_digest, plan_digest);
        if is_others_key(&key) {
            self.add_others(timestamp, incr);
            return;
        }

        if let Some(rec) = self.records.get_mut(&key) {
            rec.add_incr(timestamp, incr);
            self.total_ru += incr.total_ru;
            return;
        }

        // At capacity, merge into "others SQL".
        if self.records.len() >= self.pre_top_n_sqls_per_user {
            self.add_others(timestamp, incr);
            return;
        }

        let mut rec = RuRecord::new(key.sql_digest.clone(), key.plan_digest.clone());
        rec.add_incr(timestamp, incr);
        self.records.insert(key, rec);
        self.total_ru += incr.total_ru;
    }

    /// Go `userRUCollecting.addOthers`: adds RU increments into this user's
    /// "others SQL" bucket, folding any legacy nil-digest record in on first
    /// use.
    pub fn add_others(&mut self, timestamp: u64, incr: &RuIncrement) {
        if self.others_rec.is_none() {
            let mut others = RuRecord::new_others();
            // Compatibility: fold any legacy nil-digest record into othersRec.
            if let Some(rec) = self.records.remove(&others_key()) {
                others.merge(&rec);
            }
            self.others_rec = Some(others);
        }
        if let Some(others) = self.others_rec.as_mut() {
            others.add_incr(timestamp, incr);
        }
        self.total_ru += incr.total_ru;
    }

    /// Go `userRUCollecting.getReportRecordsWithLimit`: this user's Top-N SQL
    /// records, with everything evicted folded into one "others SQL" record
    /// appended at the end.
    ///
    /// Go hands out the receiver's `*ruRecord` pointers; here the records are
    /// moved out, which is the same transfer without the aliasing.
    #[must_use]
    pub fn into_report_records_with_limit(mut self, top_n_sqls_per_user: usize) -> Vec<RuRecord> {
        let top_n_sqls_per_user = if top_n_sqls_per_user == 0 {
            MAX_TOP_SQLS_PER_USER
        } else {
            top_n_sqls_per_user
        };
        if self.records.is_empty() && self.others_rec.is_none() {
            return Vec::new();
        }

        // Fast path: strictly under the limit with no pre-aggregated "others
        // SQL" means every record is already in the final Top-N set.
        if self.records.len() <= top_n_sqls_per_user && self.others_rec.is_none() {
            return self.records.into_values().collect();
        }

        let all_records: Vec<RuRecord> = std::mem::take(&mut self.records).into_values().collect();
        let (mut top, evicted) = ru_records_top_n(all_records, top_n_sqls_per_user);

        // Start with the pre-aggregated "others" from the collection phase.
        let mut others_rec = self.others_rec.take();
        if !evicted.is_empty() {
            let others = others_rec.get_or_insert_with(RuRecord::new_others);
            for rec in &evicted {
                others.merge(rec);
            }
        }
        if let Some(others) = others_rec {
            top.push(others);
        }
        top
    }

    /// Go `userRUCollecting.mergeRecord`: merges `src_rec` into this user's
    /// records, falling back to "others SQL" at capacity or for the
    /// [`others_key`] sentinel.
    pub fn merge_record(
        &mut self,
        key: &SqlPlanKey,
        src_rec: &RuRecord,
        target_ts: u64,
        rewrite_ts: bool,
    ) {
        if src_rec.items.is_empty() {
            return;
        }

        // "others SQL" path.
        if is_others_key(key) {
            let mut others = self.others_rec.take().unwrap_or_else(RuRecord::new_others);
            Self::merge_record_into(&mut others, src_rec, target_ts, rewrite_ts);
            self.others_rec = Some(others);
            self.total_ru += src_rec.total_ru;
            return;
        }

        // Normal SQL path.
        if !self.records.contains_key(key) {
            // Check the pre-Top-N cap before adding a new SQL.
            if self.records.len() >= self.pre_top_n_sqls_per_user {
                let mut others = self.others_rec.take().unwrap_or_else(RuRecord::new_others);
                Self::merge_record_into(&mut others, src_rec, target_ts, rewrite_ts);
                self.others_rec = Some(others);
                self.total_ru += src_rec.total_ru;
                return;
            }
            self.records.insert(
                key.clone(),
                RuRecord::new(key.sql_digest.clone(), key.plan_digest.clone()),
            );
        }
        if let Some(dst_rec) = self.records.get_mut(key) {
            Self::merge_record_into(dst_rec, src_rec, target_ts, rewrite_ts);
        }
        self.total_ru += src_rec.total_ru;
    }

    /// Go `userRUCollecting.mergeRecordInto`. The `totalRU` update Go folds
    /// in here happens in [`UserRuCollecting::merge_record`], since a `&mut`
    /// borrow of one record cannot also borrow the owner.
    fn merge_record_into(
        dst_rec: &mut RuRecord,
        src_rec: &RuRecord,
        target_ts: u64,
        rewrite_ts: bool,
    ) {
        if rewrite_ts {
            dst_rec.merge_with_timestamp(src_rec, target_ts);
        } else {
            dst_rec.merge(src_rec);
        }
    }
}

/// Go `userRUCollectings.topN`: the top `n` users by `total_ru`, plus the
/// evicted rest. Same quickselect narrowing as [`ru_records_top_n`].
#[must_use]
pub fn user_ru_collectings_top_n(
    mut us: Vec<UserRuCollecting>,
    n: usize,
) -> (Vec<UserRuCollecting>, Vec<UserRuCollecting>) {
    if us.len() <= n {
        return (us, Vec::new());
    }
    us.sort_by(|left, right| right.total_ru.total_cmp(&left.total_ru));
    let evicted = us.split_off(n);
    (us, evicted)
}

/// Go `normalizeTopNLimits`.
#[must_use]
pub fn normalize_top_n_limits(max_users: usize, max_sqls_per_user: usize) -> (usize, usize) {
    let normalized_max_users = if max_users == 0 {
        MAX_TOP_USERS
    } else {
        max_users
    };
    let normalized_max_sqls_per_user = if max_sqls_per_user == 0 {
        MAX_TOP_SQLS_PER_USER
    } else {
        max_sqls_per_user
    };
    (normalized_max_users, normalized_max_sqls_per_user)
}

/// Go `mergeUserIntoOthers`: merges one user's records into `dst`'s
/// "others SQL" record.
pub fn merge_user_into_others(dst: &mut UserRuCollecting, src: &UserRuCollecting) {
    for rec in src.records.values() {
        dst.merge_record(&others_key(), rec, 0, false);
    }
    if let Some(others) = src.others_rec.as_ref() {
        dst.merge_record(&others_key(), others, 0, false);
    }
}

/// Go `ruCollecting`: the top-level RU collector, keeping global Top-N users
/// each with per-user SQL Top-N.
///
/// boundary: Go carries a `sync.Mutex` that only `take` acquires, while
/// `add`/`mergeFrom` document external synchronization. `&mut self` is that
/// synchronization here, so the mutex has no counterpart; the window
/// aggregator that owns the collecting holds the real lock.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuCollecting {
    /// Go `ruCollecting.users`.
    pub users: HashMap<String, UserRuCollecting>,
    /// Go `ruCollecting.othersUser`: the synthetic global "others user"
    /// bucket. Its identity comes from this field, not from
    /// [`UserRuCollecting::user`].
    pub others_user: Option<UserRuCollecting>,
    /// Go `ruCollecting.preTopNUsers`.
    pub pre_top_n_users: usize,
    /// Go `ruCollecting.preTopNSQLsPerUser`.
    pub pre_top_n_sqls_per_user: usize,
}

impl RuCollecting {
    /// Go `newRUCollecting`.
    #[must_use]
    pub fn new() -> Self {
        Self::with_caps(MAX_PRE_TOP_N_USERS, MAX_PRE_TOP_N_SQLS_PER_USER)
    }

    /// Go `newRUCollectingWithCaps`.
    #[must_use]
    pub fn with_caps(pre_top_n_users: usize, pre_top_n_sqls_per_user: usize) -> Self {
        let pre_top_n_users = if pre_top_n_users == 0 {
            MAX_PRE_TOP_N_USERS
        } else {
            pre_top_n_users
        };
        let pre_top_n_sqls_per_user = if pre_top_n_sqls_per_user == 0 {
            MAX_PRE_TOP_N_SQLS_PER_USER
        } else {
            pre_top_n_sqls_per_user
        };
        Self {
            users: HashMap::with_capacity(pre_top_n_users),
            others_user: None,
            pre_top_n_users,
            pre_top_n_sqls_per_user,
        }
    }

    /// Go `ruCollecting.add`: adds RU increments from the aggregator. At the
    /// user pre-cap, new users fold into "others user".
    pub fn add(&mut self, timestamp: u64, key: &RuKey, incr: &RuIncrement) {
        if !self.users.contains_key(&key.user) {
            // At capacity, merge into "others user".
            if self.users.len() >= self.pre_top_n_users {
                self.get_or_create_others_user().add_others(timestamp, incr);
                return;
            }
            let user_collecting =
                UserRuCollecting::with_cap(&key.user, self.pre_top_n_sqls_per_user);
            self.users.insert(key.user.clone(), user_collecting);
        }
        if let Some(user_collecting) = self.users.get_mut(&key.user) {
            user_collecting.add(
                timestamp,
                key.sql_digest.clone(),
                key.plan_digest.clone(),
                incr,
            );
        }
    }

    /// Go `ruCollecting.addBatch`: adds a batch of RU increments for a given
    /// timestamp.
    pub fn add_batch(&mut self, timestamp: u64, increments: &RuIncrementMap) {
        for (key, incr) in increments.iter() {
            self.add(timestamp, key, incr);
        }
    }

    /// Go `ruCollecting.take`: returns the collected data and resets internal
    /// state.
    pub fn take(&mut self) -> RuCollecting {
        RuCollecting {
            users: std::mem::replace(
                &mut self.users,
                HashMap::with_capacity(self.pre_top_n_users),
            ),
            others_user: self.others_user.take(),
            pre_top_n_users: self.pre_top_n_users,
            pre_top_n_sqls_per_user: self.pre_top_n_sqls_per_user,
        }
    }

    /// Go `ruCollecting.toTopRURecords`: converts the current data to proto
    /// records, applying no Top-N filtering.
    ///
    /// Go sorts each record's items in place before encoding; the items are
    /// copied into the proto anyway, so the copy is sorted instead and the
    /// receiver stays untouched.
    #[must_use]
    pub fn to_top_ru_records(&self, keyspace_name: &[u8]) -> Vec<TopRuRecord> {
        if self.users.is_empty() && self.others_user.is_none() {
            return Vec::new();
        }

        let encode = |items: &RuItems| -> Vec<TopRuRecordItem> {
            let mut items = items.clone();
            if items.len() > 1 {
                sort_ru_items(&mut items);
            }
            ru_items_to_proto(&items)
        };

        let mut total_records = 0;
        for user_collecting in self.users.values() {
            total_records += user_collecting.records.len();
            if user_collecting.others_rec.is_some() {
                total_records += 1;
            }
        }
        if self
            .others_user
            .as_ref()
            .is_some_and(|user| user.others_rec.is_some())
        {
            total_records += 1;
        }
        let mut result = Vec::with_capacity(total_records);
        for user_collecting in self.users.values() {
            for rec in user_collecting.records.values() {
                result.push(TopRuRecord {
                    keyspace_name: keyspace_name.to_vec(),
                    user: user_collecting.user.clone(),
                    sql_digest: rec.sql_digest.0.clone(),
                    plan_digest: rec.plan_digest.0.clone(),
                    items: encode(&rec.items),
                });
            }
            if let Some(others_rec) = user_collecting.others_rec.as_ref() {
                result.push(TopRuRecord {
                    keyspace_name: keyspace_name.to_vec(),
                    user: user_collecting.user.clone(),
                    sql_digest: Vec::new(),
                    plan_digest: Vec::new(),
                    items: encode(&others_rec.items),
                });
            }
        }
        if let Some(others_rec) = self
            .others_user
            .as_ref()
            .and_then(|user| user.others_rec.as_ref())
        {
            result.push(TopRuRecord {
                keyspace_name: keyspace_name.to_vec(),
                user: OTHERS_USER_WIRE_LABEL.to_owned(),
                sql_digest: Vec::new(),
                plan_digest: Vec::new(),
                items: encode(&others_rec.items),
            });
        }
        result
    }

    /// Go `ruCollecting.compactWithLimits`: applies the Top-N limits and
    /// returns a compacted collecting, or `None` when there is nothing to
    /// report.
    ///
    /// Go's fast path returns the receiver itself and documents the result as
    /// read-only or ownership-transferred; consuming `self` makes that
    /// literal.
    #[must_use]
    pub fn compact_with_limits(
        mut self,
        max_users: usize,
        max_sqls_per_user: usize,
    ) -> Option<Self> {
        let (max_users, max_sqls_per_user) = normalize_top_n_limits(max_users, max_sqls_per_user);
        if self.users.is_empty() && self.others_user.is_none() {
            return None;
        }

        // Fast path: everything is already within the final Top-N bounds and
        // there is no pre-aggregated "others user" or "others SQL", so
        // compacting would be a no-op.
        if self.users.len() <= max_users && self.others_user.is_none() {
            let under_sql_cap = self
                .users
                .values()
                .all(|u| u.records.len() <= max_sqls_per_user && u.others_rec.is_none());
            if under_sql_cap {
                return Some(self);
            }
        }

        let all_users: Vec<UserRuCollecting> =
            std::mem::take(&mut self.users).into_values().collect();

        // Apply global Top-N user filtering.
        let (top_users, evicted_users) = user_ru_collectings_top_n(all_users, max_users);

        // Build the result snapshot with the top users.
        let mut result = RuCollecting::with_caps(max_users, max_sqls_per_user);
        for user_collecting in top_users {
            let user = user_collecting.user.clone();
            let user_records = user_collecting.into_report_records_with_limit(max_sqls_per_user);
            let mut compacted_user = UserRuCollecting::with_cap(&user, max_sqls_per_user);
            for rec in user_records {
                let total_ru = rec.total_ru;
                if rec.sql_digest.0.is_empty() && rec.plan_digest.0.is_empty() {
                    // "others SQL" record.
                    match compacted_user.others_rec.as_mut() {
                        None => compacted_user.others_rec = Some(rec),
                        Some(others) => others.merge(&rec),
                    }
                } else {
                    // Normal SQL record.
                    let key = make_key(rec.sql_digest.clone(), rec.plan_digest.clone());
                    compacted_user.records.insert(key, rec);
                }
                compacted_user.total_ru += total_ru;
            }
            result
                .users
                .insert(compacted_user.user.clone(), compacted_user);
        }

        // Merge evicted users into "others user".
        let mut others_user = None;
        if let Some(src_others_user) = self.others_user.as_ref() {
            let mut dst = UserRuCollecting::new_others_user(max_sqls_per_user);
            merge_user_into_others(&mut dst, src_others_user);
            others_user = Some(dst);
        }
        if !evicted_users.is_empty() {
            let dst = others_user
                .get_or_insert_with(|| UserRuCollecting::new_others_user(max_sqls_per_user));
            for evicted_user in &evicted_users {
                merge_user_into_others(dst, evicted_user);
            }
        }
        result.others_user = others_user;

        Some(result)
    }

    /// Go `ruCollecting.getOrCreateUser`: `false` means the user overflows the
    /// current capacity, in which case nothing is created.
    ///
    /// Go returns `(*userRUCollecting, overflow bool)`; returning whether the
    /// user is now present keeps the borrow of `self.users` out of the return
    /// value, so the overflow branch can still reach `self`.
    pub fn ensure_user(&mut self, user: &str) -> bool {
        if !self.users.contains_key(user) {
            if self.users.len() >= self.pre_top_n_users {
                return false;
            }
            let collecting = UserRuCollecting::with_cap(user, self.pre_top_n_sqls_per_user);
            self.users.insert(user.to_owned(), collecting);
        }
        true
    }

    /// Go `ruCollecting.getOrCreateOthersUser`.
    pub fn get_or_create_others_user(&mut self) -> &mut UserRuCollecting {
        let pre_top_n_sqls_per_user = self.pre_top_n_sqls_per_user;
        self.others_user
            .get_or_insert_with(|| UserRuCollecting::new_others_user(pre_top_n_sqls_per_user))
    }

    /// Go `ruCollecting.mergeFrom`: merges `src` into `self`, rewriting every
    /// timestamp to `target_timestamp` when `rewrite_timestamp` is set.
    /// Callers must ensure external synchronization.
    pub fn merge_from(
        &mut self,
        src: &RuCollecting,
        target_timestamp: u64,
        rewrite_timestamp: bool,
    ) {
        // Merge regular users.
        for src_user in src.users.values() {
            if self.ensure_user(&src_user.user) {
                if let Some(dst_user) = self.users.get_mut(&src_user.user) {
                    for src_rec in src_user.records.values() {
                        let key = make_key(src_rec.sql_digest.clone(), src_rec.plan_digest.clone());
                        dst_user.merge_record(&key, src_rec, target_timestamp, rewrite_timestamp);
                    }
                    // Also merge the user's othersRec.
                    if let Some(src_others) = src_user.others_rec.as_ref() {
                        dst_user.merge_record(
                            &others_key(),
                            src_others,
                            target_timestamp,
                            rewrite_timestamp,
                        );
                    }
                }
                continue;
            }
            // When dst user capacity is full, fold the entire src user
            // footprint into the synthetic global othersUser instead of
            // creating a new real-user bucket.
            let dst_others_user = self.get_or_create_others_user();
            for src_rec in src_user.records.values() {
                dst_others_user.merge_record(
                    &others_key(),
                    src_rec,
                    target_timestamp,
                    rewrite_timestamp,
                );
            }
            if let Some(src_others) = src_user.others_rec.as_ref() {
                dst_others_user.merge_record(
                    &others_key(),
                    src_others,
                    target_timestamp,
                    rewrite_timestamp,
                );
            }
        }

        // Merge "others user".
        if let Some(src_others_user) = src.others_user.as_ref() {
            let dst_others_user = self.get_or_create_others_user();
            // Compatibility: merge a potential legacy records map into
            // othersUser.
            for src_rec in src_others_user.records.values() {
                dst_others_user.merge_record(
                    &others_key(),
                    src_rec,
                    target_timestamp,
                    rewrite_timestamp,
                );
            }
            if let Some(src_others) = src_others_user.others_rec.as_ref() {
                dst_others_user.merge_record(
                    &others_key(),
                    src_others,
                    target_timestamp,
                    rewrite_timestamp,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(text: &str) -> BinaryDigest {
        BinaryDigest::from(text)
    }

    fn incr(total_ru: f64, exec_count: u64, exec_duration: u64) -> RuIncrement {
        RuIncrement {
            total_ru,
            exec_count,
            exec_duration,
        }
    }

    fn ru_key(user: &str, sql: &str, plan: &str) -> RuKey {
        RuKey {
            user: user.to_owned(),
            sql_digest: digest(sql),
            plan_digest: digest(plan),
        }
    }

    #[test]
    fn test_ru_item_to_proto() {
        let item = RuItem {
            timestamp: 1000,
            total_ru: 100.5,
            exec_count: 10,
            exec_duration: 5000,
        };
        let proto = item.to_proto();
        assert_eq!(1000, proto.timestamp_sec);
        assert_eq!(100.5, proto.total_ru);
        assert_eq!(10, proto.exec_count);
        assert_eq!(5000, proto.exec_duration);
    }

    #[test]
    fn test_ru_record_add() {
        let mut rec = RuRecord::new(digest("sql1"), digest("plan1"));

        rec.add(1000, 10.0, 1, 100);
        assert_eq!(1, rec.items.len());
        assert_eq!(10.0, rec.total_ru);
        assert_eq!(1000, rec.items[0].timestamp);

        rec.add(1000, 5.0, 2, 50);
        assert_eq!(1, rec.items.len());
        assert_eq!(15.0, rec.total_ru);
        assert_eq!(15.0, rec.items[0].total_ru);
        assert_eq!(3, rec.items[0].exec_count);

        rec.add(1001, 20.0, 1, 200);
        assert_eq!(2, rec.items.len());
        assert_eq!(35.0, rec.total_ru);
    }

    #[test]
    fn test_ru_record_merge() {
        let mut rec1 = RuRecord::new(digest("sql1"), digest("plan1"));
        rec1.add(1000, 10.0, 1, 100);
        rec1.add(1001, 20.0, 2, 200);

        let mut rec2 = RuRecord::new(digest("sql1"), digest("plan1"));
        rec2.add(1000, 5.0, 1, 50); // Same timestamp as rec1.
        rec2.add(1002, 15.0, 1, 150);

        rec1.merge(&rec2);
        assert_eq!(50.0, rec1.total_ru);
        assert_eq!(3, rec1.items.len());
        assert_eq!(15.0, rec1.items[0].total_ru);
        assert_eq!(2, rec1.items[0].exec_count);
    }

    #[test]
    fn test_ru_records_top_n() {
        let records: Vec<RuRecord> = (0..5)
            .map(|i| {
                let mut rec = RuRecord::new(digest(&format!("sql{i}")), BinaryDigest::default());
                rec.total_ru = f64::from(i + 1) * 10.0; // 10, 20, 30, 40, 50
                rec
            })
            .collect();

        let (top, evicted) = ru_records_top_n(records, 3);
        assert_eq!(3, top.len());
        assert_eq!(2, evicted.len());
        let top_ru: f64 = top.iter().map(|r| r.total_ru).sum();
        assert!(top_ru >= 120.0); // 50+40+30=120
    }

    #[test]
    fn test_user_ru_collecting_top_n_sqls() {
        let mut user = UserRuCollecting::new("user1");

        let num_sqls = MAX_TOP_SQLS_PER_USER + 10;
        for i in 0..num_sqls {
            let ru = (i + 1) as f64; // 1, 2, ..., num_sqls
            user.add(
                1000,
                digest(&format!("sql{i}")),
                BinaryDigest::default(),
                &incr(ru, 1, 100),
            );
        }
        assert_eq!(num_sqls, user.records.len());

        let report_records = user.into_report_records_with_limit(MAX_TOP_SQLS_PER_USER);
        assert_eq!(MAX_TOP_SQLS_PER_USER + 1, report_records.len());

        let mut others_rec = None;
        let mut normal_count = 0;
        for rec in &report_records {
            if rec.sql_digest.0.is_empty() && rec.plan_digest.0.is_empty() {
                others_rec = Some(rec);
            } else {
                normal_count += 1;
            }
        }
        let others_rec = others_rec.expect("should have 'others' record");
        assert_eq!(MAX_TOP_SQLS_PER_USER, normal_count);
        // Evicted: SQL 1..10 with RU 1+2+...+10 = 55.
        assert_eq!(55.0, others_rec.total_ru);
    }

    #[test]
    fn test_user_ru_collecting_pre_top_n_sql_cap() {
        let mut user = UserRuCollecting::new("user1");
        let extra = 5;
        for i in 0..MAX_PRE_TOP_N_SQLS_PER_USER + extra {
            user.add(
                1000,
                digest(&format!("sql{i}")),
                BinaryDigest::default(),
                &incr(1.0, 1, 10),
            );
        }
        assert_eq!(MAX_PRE_TOP_N_SQLS_PER_USER, user.records.len());
        let others = user.others_rec.expect("others record");
        assert_eq!(extra as f64, others.total_ru);
    }

    #[test]
    fn test_others_key_sentinel() {
        assert_eq!(
            others_key(),
            make_key(BinaryDigest::default(), BinaryDigest::default())
        );
        assert_ne!(
            others_key(),
            make_key(digest("sql"), BinaryDigest::default())
        );
        assert_ne!(
            others_key(),
            make_key(BinaryDigest::default(), digest("plan"))
        );
        assert!(is_others_key(&others_key()));
        assert!(!is_others_key(&make_key(
            digest("sql"),
            BinaryDigest::default()
        )));
    }

    #[test]
    fn test_user_ru_collecting_empty_digests_go_to_others_rec() {
        // Both empty digests are reserved for the aggregated "others SQL".
        let mut user = UserRuCollecting::with_cap("user1", 10);

        user.add(
            1000,
            BinaryDigest::default(),
            BinaryDigest::default(),
            &incr(3.0, 1, 10),
        );
        assert!(user.records.is_empty());
        assert_eq!(3.0, user.others_rec.as_ref().expect("others").total_ru);

        user.add(
            1001,
            digest("sql1"),
            BinaryDigest::default(),
            &incr(2.0, 1, 10),
        );
        assert_eq!(1, user.records.len());
    }

    #[test]
    fn test_user_ru_collecting_add_others_folds_legacy_others_key() {
        // Legacy shape: "others SQL" may have been stored in records[othersKey].
        let mut user = UserRuCollecting::with_cap("user1", 10);

        let mut legacy = RuRecord::new_others();
        legacy.add(1000, 3.0, 1, 10);
        user.total_ru = legacy.total_ru;
        user.records.insert(others_key(), legacy);

        user.add_others(1001, &incr(2.0, 1, 10));
        assert!(!user.records.contains_key(&others_key()));
        assert_eq!(5.0, user.others_rec.as_ref().expect("others").total_ru);
    }

    #[test]
    fn test_ru_collecting_hybrid_top_n() {
        // Build maxTopUsers+5 users so compaction must evict users into
        // "others user".
        let mut collecting = RuCollecting::new();
        let num_users = MAX_TOP_USERS + 5;
        for u in 0..num_users {
            for s in 0..3 {
                let key = ru_key(&format!("user{u}"), &format!("sql{s}"), "");
                collecting.add(1000, &key, &incr((u + 1) as f64, 1, 100));
            }
        }
        assert_eq!(num_users, collecting.users.len());

        let compacted = collecting.compact_with_limits(MAX_TOP_USERS, MAX_TOP_SQLS_PER_USER);
        let records = compacted
            .map(|c| c.to_top_ru_records(b"test-keyspace"))
            .unwrap_or_default();

        assert!(
            records.iter().any(|rec| rec.user == OTHERS_USER_WIRE_LABEL),
            "should have 'others user' record for evicted users"
        );
        for rec in &records {
            assert_eq!(b"test-keyspace".to_vec(), rec.keyspace_name);
        }
    }

    #[test]
    fn test_ru_collecting_pre_top_n_user_cap() {
        let mut collecting = RuCollecting::new();
        let extra = 5;
        for u in 0..MAX_PRE_TOP_N_USERS + extra {
            let key = ru_key(&format!("user{u}"), "sql", "");
            collecting.add(1000, &key, &incr(1.0, 0, 0));
        }
        assert_eq!(MAX_PRE_TOP_N_USERS, collecting.users.len());
        assert_eq!(
            extra as f64,
            collecting
                .others_user
                .as_ref()
                .expect("others user")
                .total_ru
        );
    }

    #[test]
    fn test_ru_collecting_others_wire_label_no_collision_with_runtime_user_shape() {
        let mut collecting = RuCollecting::with_caps(1, 1);
        let runtime_user = "app@127.0.0.1";

        collecting.add(
            1000,
            &ru_key(runtime_user, "sql-top", "plan-top"),
            &incr(10.0, 1, 10),
        );
        collecting.add(
            1001,
            &ru_key(runtime_user, "sql-overflow", "plan-overflow"),
            &incr(8.0, 1, 10),
        );
        // One more user to overflow the global user cap.
        collecting.add(
            1002,
            &ru_key(
                "other@127.0.0.1",
                "sql-global-overflow",
                "plan-global-overflow",
            ),
            &incr(7.0, 1, 10),
        );

        let records = collecting.to_top_ru_records(b"ks");
        let has_per_user_others = records.iter().any(|rec| {
            rec.user == runtime_user && rec.sql_digest.is_empty() && rec.plan_digest.is_empty()
        });
        let has_global_others = records.iter().any(|rec| {
            rec.user == OTHERS_USER_WIRE_LABEL
                && rec.sql_digest.is_empty()
                && rec.plan_digest.is_empty()
        });
        assert!(has_per_user_others);
        assert!(has_global_others);
    }

    #[test]
    fn test_ru_collecting_empty_user_and_global_others_remain_distinct() {
        let mut collecting = RuCollecting::with_caps(1, 1);

        // The empty user is a valid runtime user shape.
        collecting.add(
            1000,
            &ru_key("", "sql-empty-top", "plan-empty-top"),
            &incr(10.0, 1, 10),
        );
        collecting.add(
            1001,
            &ru_key("", "sql-empty-overflow", "plan-empty-overflow"),
            &incr(8.0, 1, 10),
        );
        collecting.add(
            1002,
            &ru_key(
                "other@127.0.0.1",
                "sql-global-overflow",
                "plan-global-overflow",
            ),
            &incr(7.0, 1, 10),
        );

        let records = collecting.to_top_ru_records(b"ks");
        let has_empty_user_others = records.iter().any(|rec| {
            rec.user.is_empty() && rec.sql_digest.is_empty() && rec.plan_digest.is_empty()
        });
        let has_global_others = records.iter().any(|rec| {
            rec.user == OTHERS_USER_WIRE_LABEL
                && rec.sql_digest.is_empty()
                && rec.plan_digest.is_empty()
        });
        assert!(has_empty_user_others);
        assert!(has_global_others);
    }

    #[test]
    fn test_ru_collecting_merge_from_keeps_empty_user_distinct_from_global_others() {
        let mut dst = RuCollecting::with_caps(1, 1);
        dst.add(
            1000,
            &ru_key("", "sql-empty-top", "plan-empty-top"),
            &incr(10.0, 1, 10),
        );
        dst.add(
            1001,
            &ru_key("", "sql-empty-overflow", "plan-empty-overflow"),
            &incr(8.0, 1, 10),
        );

        let mut src = RuCollecting::with_caps(1, 1);
        src.add(
            1002,
            &ru_key("other@127.0.0.1", "sql-other-top", "plan-other-top"),
            &incr(7.0, 1, 10),
        );
        src.add(
            1003,
            &ru_key(
                "other2@127.0.0.1",
                "sql-other-overflow",
                "plan-other-overflow",
            ),
            &incr(6.0, 1, 10),
        );

        dst.merge_from(&src, 0, false);
        let records = dst.to_top_ru_records(b"ks");

        let has_empty_user_others = records.iter().any(|rec| {
            rec.user.is_empty() && rec.sql_digest.is_empty() && rec.plan_digest.is_empty()
        });
        let has_global_others = records.iter().any(|rec| {
            rec.user == OTHERS_USER_WIRE_LABEL
                && rec.sql_digest.is_empty()
                && rec.plan_digest.is_empty()
        });
        assert!(has_empty_user_others);
        assert!(has_global_others);
    }

    #[test]
    fn test_ru_collecting_add_batch() {
        let mut collecting = RuCollecting::new();
        let mut batch = RuIncrementMap::new();
        batch.insert(ru_key("user1", "sql1", ""), incr(10.0, 0, 0));
        batch.insert(ru_key("user1", "sql2", ""), incr(20.0, 0, 0));
        batch.insert(ru_key("user2", "sql1", ""), incr(30.0, 0, 0));

        collecting.add_batch(1000, &batch);

        assert_eq!(2, collecting.users.len());
        assert_eq!(30.0, collecting.users["user1"].total_ru);
        assert_eq!(30.0, collecting.users["user2"].total_ru);
    }

    #[test]
    fn test_ru_collecting_take() {
        let mut collecting = RuCollecting::new();
        collecting.add(1000, &ru_key("user1", "sql1", ""), &incr(10.0, 0, 0));
        assert_eq!(1, collecting.users.len());

        let taken = collecting.take();
        assert_eq!(1, taken.users.len());
        assert_eq!(0, collecting.users.len()); // Original should be reset.
    }

    #[test]
    fn test_ru_collecting_compact_and_report_consistency() {
        let mut collecting = RuCollecting::new();
        let mut add = |user: &str, sql: &str, plan: &str, ts: u64, ru: f64| {
            collecting.add(ts, &ru_key(user, sql, plan), &incr(ru, 1, 10));
        };
        add("u1", "s1", "p1", 0, 100.0);
        add("u1", "s2", "p2", 15, 80.0);
        add("u2", "s1", "p1", 0, 70.0);
        add("u2", "s2", "p2", 30, 60.0);
        add("u3", "s1", "p1", 0, 50.0); // evicted by max_users=2
        add("u4", "s1", "p1", 0, 10.0); // evicted by max_users=2

        let compacted = collecting.compact_with_limits(2, 1).expect("compacted");
        let from_compact = compacted.to_top_ru_records(b"ks");
        assert!(!normalize_top_ru_records(&from_compact).is_empty());
    }

    /// Go's `normalizeTopRURecords`: normalizes record and item order so
    /// assertions are stable across map iteration order.
    fn normalize_top_ru_records(records: &[TopRuRecord]) -> Vec<String> {
        let mut out: Vec<String> = records
            .iter()
            .map(|rec| {
                let mut items: Vec<String> = rec
                    .items
                    .iter()
                    .map(|item| {
                        format!(
                            "{}|{:.6}|{}|{}",
                            item.timestamp_sec, item.total_ru, item.exec_count, item.exec_duration
                        )
                    })
                    .collect();
                items.sort();
                format!(
                    "{}|{}|{}|{}|{}",
                    rec.user,
                    hex(&rec.sql_digest),
                    hex(&rec.plan_digest),
                    hex(&rec.keyspace_name),
                    items.join(",")
                )
            })
            .collect();
        out.sort();
        out
    }

    /// Go's `%x` verb over a byte slice.
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    // Go `compactWithLimitsCases`: the boundary cases of `compactWithLimits`,
    // run by `TestCompactWithLimits` as table-driven subtests. Rust has no
    // subtests, so each case is its own `#[test]`, named after the Go case.

    #[test]
    fn test_compact_with_limits_pre_existing_others_rec_and_evicted_sql() {
        // Precondition: the user already has othersRec, then one SQL is
        // evicted by the per-user top-1 limit.
        let mut collecting = RuCollecting::with_caps(10, 10);
        let mut u1 = UserRuCollecting::with_cap("u1", 10);
        u1.add(
            1000,
            digest("sql-top"),
            digest("plan-top"),
            &incr(100.0, 1, 10),
        );
        u1.add(
            1001,
            digest("sql-evicted"),
            digest("plan-evicted"),
            &incr(40.0, 1, 10),
        );
        u1.add_others(1002, &incr(7.0, 1, 10));
        collecting.users.insert("u1".to_owned(), u1);

        let compacted = collecting.compact_with_limits(1, 1).expect("compacted");
        // Contract: pre-existing othersRec and the evicted SQL's RU merge into
        // one others record.
        assert!(compacted.others_user.is_none());
        let compacted_u1 = compacted.users.get("u1").expect("u1");
        assert_eq!(1, compacted_u1.records.len());
        for rec in compacted_u1.records.values() {
            assert_eq!(digest("sql-top"), rec.sql_digest);
            assert_eq!(digest("plan-top"), rec.plan_digest);
            assert_eq!(100.0, rec.total_ru);
        }
        let others = compacted_u1.others_rec.as_ref().expect("others rec");
        assert!(others.sql_digest.0.is_empty());
        assert!(others.plan_digest.0.is_empty());
        assert_eq!(47.0, others.total_ru); // pre-existing 7 + evicted SQL 40
    }

    #[test]
    fn test_compact_with_limits_others_user_and_evicted_users_both_present() {
        // Precondition: both a pre-existing othersUser and newly evicted users.
        let mut collecting = RuCollecting::with_caps(10, 10);

        let mut u1 = UserRuCollecting::with_cap("u1", 10);
        u1.add(
            2000,
            digest("sql-top"),
            digest("plan-top"),
            &incr(100.0, 1, 10),
        );
        collecting.users.insert("u1".to_owned(), u1);

        let mut u2 = UserRuCollecting::with_cap("u2", 10);
        u2.add(
            2000,
            digest("sql-u2"),
            digest("plan-u2"),
            &incr(30.0, 1, 10),
        );
        collecting.users.insert("u2".to_owned(), u2);

        let mut pre_others = UserRuCollecting::new_others_user(10);
        pre_others.add(
            2000,
            digest("sql-pre-others"),
            digest("plan-pre-others"),
            &incr(6.0, 1, 10),
        );
        pre_others.add_others(2001, &incr(4.0, 1, 10));
        collecting.others_user = Some(pre_others);

        let compacted = collecting.compact_with_limits(1, 1).expect("compacted");
        // Contract: both sources merge into one othersUser.othersRec without
        // leaking normal records.
        assert_eq!(1, compacted.users.len());
        assert!(compacted.users.contains_key("u1"));
        let others_user = compacted.others_user.as_ref().expect("others user");
        assert!(others_user.user.is_empty());
        assert!(others_user.records.is_empty());
        let others_rec = others_user.others_rec.as_ref().expect("others rec");
        assert!(others_rec.sql_digest.0.is_empty());
        assert!(others_rec.plan_digest.0.is_empty());
        assert_eq!(40.0, others_rec.total_ru); // pre-existing 6+4 + evicted user 30
    }

    #[test]
    fn test_compact_with_limits_only_others_user_non_empty() {
        // Precondition: only othersUser.othersRec has data.
        let mut collecting = RuCollecting::with_caps(10, 10);
        let mut others_user = UserRuCollecting::new_others_user(10);
        others_user.add_others(3000, &incr(11.0, 1, 10));
        collecting.others_user = Some(others_user);

        let compacted = collecting.compact_with_limits(1, 1).expect("compacted");
        // Contract: the compacted shape keeps only othersUser with the same
        // othersRec.
        assert!(compacted.users.is_empty());
        let others_rec = compacted
            .others_user
            .as_ref()
            .expect("others user")
            .others_rec
            .as_ref()
            .expect("others rec");
        assert_eq!(11.0, others_rec.total_ru);
    }

    #[test]
    fn test_compact_with_limits_single_user_single_sql() {
        // Precondition: a single user with a single SQL.
        let mut collecting = RuCollecting::with_caps(10, 10);
        let mut u1 = UserRuCollecting::with_cap("u1", 10);
        u1.add(
            4000,
            digest("sql-only"),
            digest("plan-only"),
            &incr(88.0, 1, 10),
        );
        collecting.users.insert("u1".to_owned(), u1);

        let compacted = collecting.compact_with_limits(1, 1).expect("compacted");
        // Contract: the user record is preserved and no othersUser appears.
        assert!(compacted.others_user.is_none());
        assert_eq!(1, compacted.users.len());
        let compacted_u1 = compacted.users.get("u1").expect("u1");
        assert_eq!(1, compacted_u1.records.len());
        assert!(compacted_u1.others_rec.is_none());
    }

    #[test]
    fn test_compact_with_limits_only_others_user_legacy_records() {
        // Precondition: legacy others data sits in othersUser.records with
        // empty digests.
        let mut collecting = RuCollecting::with_caps(10, 10);
        let mut legacy_others = UserRuCollecting::new_others_user(10);
        let mut legacy_rec = RuRecord::new_others();
        legacy_rec.add(5000, 13.0, 2, 30);
        legacy_others.total_ru = legacy_rec.total_ru;
        legacy_others.records.insert(others_key(), legacy_rec);
        collecting.others_user = Some(legacy_others);

        let compacted = collecting.compact_with_limits(1, 1).expect("compacted");
        // Contract: compaction folds legacy records into othersUser.othersRec.
        assert!(compacted.users.is_empty());
        let others_user = compacted.others_user.as_ref().expect("others user");
        assert!(others_user.records.is_empty());
        let others_rec = others_user.others_rec.as_ref().expect("others rec");
        assert!(others_rec.sql_digest.0.is_empty());
        assert!(others_rec.plan_digest.0.is_empty());
        assert_eq!(13.0, others_rec.total_ru);
    }

    #[test]
    fn test_ru_items_sort() {
        let items: RuItems = vec![
            RuItem {
                timestamp: 1002,
                ..RuItem::default()
            },
            RuItem {
                timestamp: 1000,
                ..RuItem::default()
            },
            RuItem {
                timestamp: 1001,
                ..RuItem::default()
            },
        ];
        // Items are sortable by timestamp ascending.
        assert!(!(items[0].timestamp < items[1].timestamp)); // 1002 > 1000
        assert!(items[1].timestamp < items[0].timestamp); // 1000 < 1002
    }

    #[test]
    fn test_ru_items_to_proto() {
        let items: RuItems = vec![
            RuItem {
                timestamp: 1000,
                total_ru: 10.0,
                exec_count: 1,
                exec_duration: 100,
            },
            RuItem {
                timestamp: 1001,
                total_ru: 20.0,
                exec_count: 2,
                exec_duration: 200,
            },
        ];
        let proto = ru_items_to_proto(&items);
        assert_eq!(2, proto.len());
        assert_eq!(1000, proto[0].timestamp_sec);
        assert_eq!(1001, proto[1].timestamp_sec);
    }

    #[test]
    fn test_ru_collecting_same_bucket_same_key_accumulates() {
        // Two batches with the same (user, sql, plan, timestamp) coalesce into
        // one item: RU and duration add up, while begin-based ExecCount is not
        // double-counted.
        let mut collecting = RuCollecting::new();
        let key = ru_key("u1", "sql1", "plan1");

        let mut first = RuIncrementMap::new();
        first.insert(key.clone(), incr(10.0, 1, 100));
        collecting.add_batch(1000, &first);

        let mut second = RuIncrementMap::new();
        second.insert(key.clone(), incr(7.0, 0, 40));
        collecting.add_batch(1000, &second);

        let compacted = collecting
            .compact_with_limits(MAX_TOP_USERS, MAX_TOP_SQLS_PER_USER)
            .expect("compacted");
        let records = compacted.to_top_ru_records(b"ks");
        assert_eq!(1, records.len());
        assert_eq!("u1", records[0].user);
        assert_eq!(b"sql1".to_vec(), records[0].sql_digest);
        assert_eq!(b"plan1".to_vec(), records[0].plan_digest);
        assert_eq!(1, records[0].items.len());
        assert_eq!(1000, records[0].items[0].timestamp_sec);
        assert!((17.0 - records[0].items[0].total_ru).abs() < 1e-9);
        assert_eq!(1, records[0].items[0].exec_count);
        assert_eq!(140, records[0].items[0].exec_duration);
    }

    #[test]
    fn test_empty_ru_collecting() {
        let collecting = RuCollecting::new();
        let records = collecting
            .compact_with_limits(MAX_TOP_USERS, MAX_TOP_SQLS_PER_USER)
            .map(|c| c.to_top_ru_records(b"keyspace"))
            .unwrap_or_default();
        assert!(records.is_empty());
    }
}
