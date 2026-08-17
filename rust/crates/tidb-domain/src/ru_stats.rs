// Copyright 2025 PingCAP, Inc.
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

//! Go `pkg/domain/ru_stats.go`: the daily roll-up of per-resource-group
//! request-unit consumption into `mysql.request_unit_by_group`, plus the GC
//! of rows older than 92 days.
//!
//! Every symbol of that file that carries a decision is here:
//! `GetLastExpectedTime` ([`get_last_expected_time`]),
//! `GetLastExpectedTimeTZ` ([`get_last_expected_time_tz`]),
//! `RUStatsWriter` ([`RuStatsWriter`]), `DoWriteRUStatistics`
//! ([`RuStatsWriter::do_write_ru_statistics`]), `fetchResourceGroupStats`
//! ([`RuStatsWriter::fetch_resource_group_stats`]), `loadLatestRUStats`
//! ([`RuStatsWriter::load_latest_ru_stats`]), `persistLatestRUStats`
//! ([`RuStatsWriter::persist_latest_ru_stats`]), `isLatestDataInserted`
//! ([`RuStatsWriter::is_latest_data_inserted`]), `insertRUStats`
//! ([`RuStatsWriter::insert_ru_stats`]), `GCOutdatedRecords`
//! ([`RuStatsWriter::gc_outdated_records`]), and `generateSQL`
//! ([`generate_sql`]), along with the four file-level constants.
//!
//! ## Declined from this file, with the exact blocking Go symbols
//!
//! - `NewRUStatsWriter(do *Domain)` (`ru_stats.go:57`) — four field reads off
//!   `*Domain`: `do.GetPDClient()`, `do.infoCache`, `do.store`,
//!   `do.sysSessionPool`. There is no `Domain` in Rust yet (see the crate
//!   doc), and every one of those fields is a distinct unported package. The
//!   constructor is pure wiring; [`RuStatsWriter::new`] takes the same four
//!   capabilities as one [`RuStatsDeps`] implementor instead.
//! - `(*Domain).requestUnitsWriterLoop` (`ru_stats.go:67-110`) — the
//!   background loop. Blocking Go symbols: `intest.InTest`, `Domain.DDL()`,
//!   `owner.Manager.IsOwner()`, and `Domain.exit` (the `chan struct{}` that
//!   `domain.go` closes on shutdown). What the loop decides — retry up to
//!   `maxRetryCount` a second apart, GC after the write, sleep until
//!   `lastTime + ruStatsInterval` — is recorded as [`MAX_RETRY_COUNT`],
//!   [`RETRY_SLEEP`] and [`next_wakeup`] so the `domain.go` batch has the
//!   schedule without re-deriving it, but the loop body itself is not
//!   written: it would be a stub around four absent symbols.
//!
//! ## Narrowings, all named
//!
//! - `// boundary:` Go `pd.ResourceManagerClient.ListResourceGroups(ctx,
//!   pd.WithRUStats)` → [`RuStatsDeps::list_resource_groups_with_ru_stats`].
//!   Only `ResourceGroup.Name` and `ResourceGroup.RUStats` are read, so
//!   [`ResourceGroupWithRuStats`] carries exactly those. `RUStats` is a
//!   `*rmpb.Consumption` and the nil case is load-bearing (`generateSQL`
//!   skips such a group and logs), so it is an `Option`, per rule 3.
//! - `// boundary:` Go `rmpb.Consumption` → [`Consumption`]. That protobuf
//!   has a dozen fields; this file reads `RRU` and `WRU` and nothing else.
//! - `// boundary:` Go `infoschema.InfoCache.GetLatest()` plus
//!   `InfoSchema.ResourceGroupByName(ast.NewCIStr(g.Name))` →
//!   [`RuStatsDeps::resource_group_by_name`]. `ast.NewCIStr` builds a
//!   case-insensitive string whose lowercase form is the lookup key, so the
//!   implementor owns the case folding; it is named in that method's doc.
//!   There is no Rust `infoschema` yet. The returned
//!   [`ResourceGroupInfo`] carries `ID` and `Name.O` — the *original-case*
//!   name, which is what gets written into the table, not the probe name.
//! - `// boundary:` Go `meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetRUStats()`
//!   → [`RuStatsDeps::load_ru_stats`], and Go `kv.RunInNewTxn(...,
//!   meta.NewMutator(txn).SetRUStats(stats))` →
//!   [`RuStatsDeps::persist_ru_stats`]. `GetRUStats` returns `(nil, nil)`
//!   when the meta key is absent and the caller distinguishes that from an
//!   empty struct, so the load returns `Option<RuStats>`.
//!   `kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)` is a tracing tag
//!   with no effect on the value read or written; it is the implementor's.
//! - `// boundary:` Go `runaway.ExecRCRestrictedSQL(sessPool, sql, params)`
//!   (`pkg/resourcegroup/runaway/record.go:391`) → three methods on
//!   [`RuStatsDeps`], one per call site, because the three call sites consume
//!   three different shapes: a row-existence probe, a single `count(*)`, and
//!   a fire-and-forget statement. Splitting them keeps `chunk.Row` out of
//!   this crate and keeps each return type unable to express the wrong thing.
//!   The session-pool checkout/return that `ExecRCRestrictedSQL` performs is
//!   the implementor's business.
//! - `// boundary:` Go `logutil.BgLogger()` — every log line is dropped; none
//!   changes a result. The two that carry information a caller might want
//!   ([`generate_sql`]'s empty-consumption warning) are noted at the site.
//! - `// boundary:` Go `time.Local` — [`get_last_expected_time`] takes the
//!   local zone as a parameter rather than reading a process global, so the
//!   DST behavior this file documents is testable. Go's own test does the
//!   same thing through `GetLastExpectedTimeTZ`.
//!
//! ## Go behaviors reproduced rather than tidied
//!
//! 1. **The GC boundary is inclusive.** `GCOutdatedRecords` deletes
//!    `end_time <= lastEndTime - 92*24h`. Go's own test
//!    (`ru_stats_test.go:116-119`) calls it with
//!    `2023-12-26 + 92*24h` and asserts the `2023-12-26 00:00:00` rows are
//!    *gone*, so the endpoint itself is deleted. Reproduced exactly, and
//!    tested, because moving it either way silently changes how much data a
//!    cluster loses.
//! 2. **The GC loop count is computed once, before any delete.**
//!    `loopCount = ceil(total / gcBatchSize)` over a `count(*)` taken up
//!    front; rows inserted concurrently are not chased, and an empty table
//!    runs zero delete statements. Reproduced — see [`gc_loop_count`].
//! 3. **`isLatestDataInserted` and `generateSQL` derive `start_time` from the
//!    package constant `ruStatsInterval` (24h), not from the writer's
//!    `Interval` field**, while `lastEndTime` is derived from `Interval`. A
//!    writer configured with a non-24h interval therefore probes and writes a
//!    `start_time` that is 24h before its own `end_time` regardless. Since
//!    `Interval` is public "for unit test", this is visible; reproduced, and
//!    tested.
//! 4. **`GetLastExpectedTimeTZ` takes the calendar date from `now`'s own
//!    zone and then re-reads it in `tz`.** Go's `now.Date()` uses `now`'s
//!    location, and the result is fed to `time.Date(..., tz)`. When the two
//!    zones differ the day boundary comes from one and the offset from the
//!    other. Reproduced literally.
//! 5. **The bucket walk is done on the instant line, not the calendar
//!    line.** Go computes `count := now.Sub(start) / interval` and then
//!    `start.In(time.UTC).Add(targetDur)`, i.e. elapsed *absolute* time since
//!    local midnight. Across a DST transition the last bucket of the day is
//!    therefore offset by the jump — with a 24h interval the answer is still
//!    local midnight, which is why the file's doc comment insists the
//!    interval must not exceed 24h. Reproduced.
//!
//! ## Where this port is deliberately more conservative than Go
//!
//! - `GCOutdatedRecords` reads `rows[0].GetInt64(0)` with no length check and
//!   would panic on an empty result set. [`RuStatsDeps::query_single_count`]
//!   returns `Option<i64>` so an implementor cannot report "no rows" as
//!   zero, and [`RuStatsWriter::gc_outdated_records`] turns `None` into
//!   [`RuStatsError::MissingCountRow`]. Failing loudly is the safe direction:
//!   a fabricated zero would silently disable GC forever.
//! - `generateSQL` dereferences `stats.Latest` unguarded. Here `latest` is an
//!   `Option` and the absent case is [`RuStatsError::MissingLatestStats`]
//!   rather than a panic. That case is unreachable from
//!   [`RuStatsWriter::do_write_ru_statistics`], exactly as in Go.
//! - Go's `lastStats.Latest.EndTime != lastEndTime` compares `time.Time`
//!   *structs*, which means instant **and** `*time.Location` pointer
//!   identity. `lastStats` is always JSON-decoded from meta storage while
//!   `lastEndTime` comes from `time.Local`, so in production Go's comparison
//!   is effectively always "different" and the PD fetch always happens.
//!   [`needs_fetch_data`] compares the instant and the UTC offset, which is
//!   as close as Rust can get without modelling pointer identity: two values
//!   naming the same instant with the same offset but different `Location`
//!   objects compare equal here and unequal in Go. The divergence is
//!   confined to the case where the persisted end time is exactly the newly
//!   computed one — and that case is already caught one step earlier by
//!   `isLatestDataInserted`, which returns before reaching this comparison.

use std::fmt::Write as _;

use chrono::{DateTime, Datelike as _, Duration, LocalResult, NaiveDate, Offset as _, TimeZone};

/// Go `maxRetryCount` (`ru_stats.go:37`). The write is retried while
/// `count > maxRetryCount` is false, i.e. eleven attempts in total.
pub const MAX_RETRY_COUNT: i32 = 10;

/// Go `time.Sleep(time.Second)` between write retries (`ru_stats.go:90`).
pub const RETRY_SLEEP: Duration = Duration::seconds(1);

/// Go `ruStatsInterval` (`ru_stats.go:38`): 24 hours.
pub const RU_STATS_INTERVAL: Duration = Duration::hours(24);

/// Go `ruStatsGCDuration` (`ru_stats.go:40`): `92 * ruStatsInterval`, i.e.
/// "only keep stats rows for last 3 months (92 days at most)".
pub const RU_STATS_GC_DURATION: Duration = Duration::hours(92 * 24);

/// Go `gcBatchSize` (`ru_stats.go:41`).
pub const GC_BATCH_SIZE: i64 = 1000;

/// Go `time.DateTime`, the layout every timestamp in this file is rendered
/// with before it reaches SQL.
pub const GO_DATE_TIME_LAYOUT: &str = "%Y-%m-%d %H:%M:%S";

/// The table this file owns.
pub const RU_BY_GROUP_TABLE: &str = "mysql.request_unit_by_group";

/// Errors this module can produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuStatsError {
    /// Any error surfaced by a collaborator: PD, meta storage, or the
    /// restricted SQL executor. Go wraps these with `errors.Trace`, which
    /// adds a stack annotation and no semantics.
    Other(String),
    /// The `SELECT count(*)` for GC returned no row at all. Go would panic on
    /// `rows[0]`; see the module doc.
    MissingCountRow,
    /// [`generate_sql`] was handed an [`RuStats`] with no `latest`. Go would
    /// nil-dereference; see the module doc.
    MissingLatestStats,
}

impl std::fmt::Display for RuStatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(msg) => f.write_str(msg),
            Self::MissingCountRow => f.write_str("ru stats gc count query returned no row"),
            Self::MissingLatestStats => f.write_str("ru stats has no latest daily stats"),
        }
    }
}

impl std::error::Error for RuStatsError {}

/// The two fields of Go `rmpb.Consumption` this file reads.
///
/// boundary: Go `github.com/pingcap/kvproto/pkg/resource_manager.Consumption`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Consumption {
    /// Go `Consumption.RRU`: read request units.
    pub rru: f64,
    /// Go `Consumption.WRU`: write request units.
    pub wru: f64,
}

impl Consumption {
    /// Go's `g.RUConsumption.RRU + g.RUConsumption.WRU` (`ru_stats.go:278`).
    #[must_use]
    pub fn total(&self) -> f64 {
        self.rru + self.wru
    }
}

/// Go `meta.GroupRUStats` (`pkg/meta/meta.go:2154`).
#[derive(Clone, Debug, PartialEq)]
pub struct GroupRuStats {
    /// Go `GroupRUStats.ID`, taken from the infoschema group, not from PD.
    pub id: i64,
    /// Go `GroupRUStats.Name`, the infoschema group's original-case name.
    pub name: String,
    /// Go `GroupRUStats.RUConsumption *rmpb.Consumption`. Nil is meaningful:
    /// `generateSQL` warns and skips such a group, and a nil *previous*
    /// consumption means no delta is subtracted.
    pub ru_consumption: Option<Consumption>,
}

/// Go `meta.DailyRUStats` (`pkg/meta/meta.go:2161`).
#[derive(Clone, Debug, PartialEq)]
pub struct DailyRuStats {
    /// Go `DailyRUStats.EndTime`. Kept with its offset because every use of
    /// it — SQL rendering and the `!=` probe — is offset-sensitive.
    pub end_time: DateTime<chrono::FixedOffset>,
    /// Go `DailyRUStats.Stats`.
    pub stats: Vec<GroupRuStats>,
}

/// Go `meta.RUStats` (`pkg/meta/meta.go:2167`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuStats {
    /// Go `RUStats.Latest`.
    pub latest: Option<DailyRuStats>,
    /// Go `RUStats.Previous`.
    pub previous: Option<DailyRuStats>,
}

/// One entry of Go's `ListResourceGroups(ctx, pd.WithRUStats)` result, cut
/// down to the two fields `fetchResourceGroupStats` reads.
///
/// boundary: Go `rmpb.ResourceGroup`.
#[derive(Clone, Debug, PartialEq)]
pub struct ResourceGroupWithRuStats {
    /// Go `ResourceGroup.Name`, used only as the infoschema lookup key.
    pub name: String,
    /// Go `ResourceGroup.RUStats`.
    pub ru_stats: Option<Consumption>,
}

/// The two fields of Go `model.ResourceGroupInfo` this file reads.
///
/// boundary: Go `pkg/meta/model.ResourceGroupInfo`.
#[derive(Clone, Debug, PartialEq)]
pub struct ResourceGroupInfo {
    /// Go `ResourceGroupInfo.ID`.
    pub id: i64,
    /// Go `ResourceGroupInfo.Name.O` — the original-case name. This, not the
    /// name PD reported, is what lands in the table.
    pub name: String,
}

/// Everything `ru_stats.go` reaches for through its `*Domain`-derived fields.
///
/// boundary: Go `pkg/domain.RUStatsWriter`'s `RMClient`, `InfoCache`,
/// `store`, and `sessPool`. See the module doc for the Go symbol behind each
/// method.
pub trait RuStatsDeps {
    /// boundary: Go `RMClient.ListResourceGroups(ctx, pd.WithRUStats)`.
    ///
    /// # Errors
    /// Whatever PD reports; Go wraps it with `errors.Trace`.
    fn list_resource_groups_with_ru_stats(
        &self,
    ) -> Result<Vec<ResourceGroupWithRuStats>, RuStatsError>;

    /// boundary: Go `InfoCache.GetLatest().ResourceGroupByName(ast.NewCIStr(name))`.
    ///
    /// The Go key is a `CIStr`, matched on its lowercase form, so the
    /// implementor owns the case folding. `None` is Go's `exists == false`,
    /// which makes the caller drop the group entirely.
    fn resource_group_by_name(&self, name: &str) -> Option<ResourceGroupInfo>;

    /// boundary: Go `meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetRUStats()`.
    ///
    /// `Ok(None)` is Go's `(nil, nil)`, meaning the meta key has never been
    /// written. The caller distinguishes it from an empty [`RuStats`].
    ///
    /// # Errors
    /// Whatever the snapshot read or the JSON decode reports.
    fn load_ru_stats(&self) -> Result<Option<RuStats>, RuStatsError>;

    /// boundary: Go `kv.RunInNewTxn(kv.WithInternalSourceType(ctx,
    /// kv.InternalTxnOthers), store, true, meta.NewMutator(txn).SetRUStats)`.
    ///
    /// # Errors
    /// Whatever the transaction reports.
    fn persist_ru_stats(&self, stats: &RuStats) -> Result<(), RuStatsError>;

    /// boundary: Go `runaway.ExecRCRestrictedSQL(sessPool, sql, params)` at
    /// `ru_stats.go:220`, whose result is consumed as `len(rows) > 0`.
    ///
    /// `params` are the `%?` placeholders, in order.
    ///
    /// # Errors
    /// Whatever the executor reports.
    fn query_row_exists(&self, sql: &str, params: &[&str]) -> Result<bool, RuStatsError>;

    /// boundary: Go `runaway.ExecRCRestrictedSQL(sessPool, countSQL, nil)` at
    /// `ru_stats.go:241`, whose result is consumed as `rows[0].GetInt64(0)`.
    ///
    /// Returns `None` when the statement produced no row. Go indexes `rows[0]`
    /// unguarded; the `Option` exists so an implementor cannot answer "no
    /// rows" with a fabricated `0`, which would disable GC silently.
    ///
    /// # Errors
    /// Whatever the executor reports.
    fn query_single_count(&self, sql: &str) -> Result<Option<i64>, RuStatsError>;

    /// boundary: Go `runaway.ExecRCRestrictedSQL(sessPool, sql, nil)` at
    /// `ru_stats.go:233` and `:250`, whose result rows are discarded.
    ///
    /// # Errors
    /// Whatever the executor reports.
    fn exec_statement(&self, sql: &str) -> Result<(), RuStatsError>;
}

/// Go `GetLastExpectedTime` (`ru_stats.go:119`): the most recent interval
/// boundary at or before `now`, counted from local midnight.
///
/// Go reads `time.Local`; the zone is a parameter here so the DST behavior is
/// testable — see the module doc's `time.Local` boundary note.
pub fn get_last_expected_time<T1: TimeZone, T2: TimeZone>(
    now: &DateTime<T1>,
    interval: Duration,
    local: &T2,
) -> DateTime<T2> {
    get_last_expected_time_tz(now, interval, local)
}

/// Go `GetLastExpectedTimeTZ` (`ru_stats.go:125`).
///
/// The Go doc's three caveats hold here too: DST can make a specific day's
/// buckets shorter or longer than `interval`, every tidb-server must share a
/// zone, and `interval` must not exceed 24h.
///
/// # Panics
/// When `interval` is zero, as Go's `now.Sub(start) / interval` panics with
/// an integer divide by zero. Nothing here invents a bucket for that case.
pub fn get_last_expected_time_tz<T1: TimeZone, T2: TimeZone>(
    now: &DateTime<T1>,
    interval: Duration,
    tz: &T2,
) -> DateTime<T2> {
    assert!(
        interval != Duration::zero(),
        "ru stats interval must not be zero (Go: integer divide by zero in GetLastExpectedTimeTZ)"
    );
    // Go `year, month, day := now.Date()` — the calendar date as seen from
    // `now`'s own location, which need not be `tz`. Reproduced literally; see
    // the module doc, behavior 4.
    let naive = now.naive_local();
    let date = NaiveDate::from_ymd_opt(naive.year(), naive.month(), naive.day())
        .expect("date components taken from an existing timestamp are always valid");
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .expect("00:00:00 is always a valid time");
    // Go `time.Date(year, month, day, 0, 0, 0, 0, tz)`.
    let start = local_midnight(&midnight, tz);

    // Go `count := int64(now.Sub(start) / interval)`, truncating toward zero.
    let elapsed =
        now.clone().with_timezone(&chrono::Utc) - start.clone().with_timezone(&chrono::Utc);
    let count = elapsed.num_nanoseconds().map_or_else(
        // Go's time.Duration saturates at ~292 years; a difference that large
        // between "now" and local midnight cannot arise, and guessing a
        // bucket for it would be inventing a value.
        || panic!("ru stats: distance from local midnight overflows a duration"),
        |ns| {
            let unit = interval
                .num_nanoseconds()
                .expect("ru stats interval must fit in nanoseconds");
            ns / unit
        },
    );
    let target_dur = interval
        * i32::try_from(count).unwrap_or_else(|_| {
            panic!("ru stats: {count} intervals since local midnight is out of range")
        });
    // Go `start.In(time.UTC).Add(targetDur).In(tz)`: the `In` calls only
    // change how the value renders, so this is instant arithmetic, and that
    // is exactly the DST compatibility the comment claims.
    start + target_dur
}

/// Go `time.Date(y, m, d, 0, 0, 0, 0, tz)` for a local midnight that DST may
/// have made ambiguous or nonexistent.
///
/// Go's `time.Date` documents the ambiguous case as "the choice of time zone,
/// and therefore the time, is not guaranteed"; its implementation looks the
/// offset up at the naive instant and only corrects it when that lands
/// outside the zone interval, which resolves an ambiguity to the earlier
/// offset and a gap to the pre-transition offset. Both are reproduced.
fn local_midnight<T: TimeZone>(midnight: &chrono::NaiveDateTime, tz: &T) -> DateTime<T> {
    match tz.from_local_datetime(midnight) {
        LocalResult::Single(t) => t,
        // Ambiguous: the same local midnight happens twice. Take the earlier.
        LocalResult::Ambiguous(earlier, _later) => earlier,
        // Nonexistent: DST skipped over local midnight. Keep the offset that
        // applied before the jump, which places the instant just after it.
        LocalResult::None => {
            let before = *midnight - Duration::days(1);
            let offset = tz.offset_from_utc_datetime(&before).fix();
            tz.from_utc_datetime(&(*midnight - offset))
        }
    }
}

/// Go `nextTime := lastTime.Add(ruStatsInterval)` (`ru_stats.go:101`): when
/// `requestUnitsWriterLoop` wakes up next.
///
/// Recorded here because the loop itself is declined (see the module doc);
/// this is the one line of it that is a decision rather than wiring.
#[must_use]
pub fn next_wakeup<T: TimeZone>(last_time: &DateTime<T>) -> DateTime<T> {
    last_time.clone() + RU_STATS_INTERVAL
}

/// Go `RUStatsWriter` (`ru_stats.go:45`).
///
/// `Interval` and `StartTime` are public in Go "for unit test"; they stay
/// public here for the same reason, and Go's own test drives them directly.
pub struct RuStatsWriter<D, Tz: TimeZone> {
    /// Go `RUStatsWriter.Interval`.
    pub interval: Duration,
    /// Go `RUStatsWriter.StartTime`: "current time, cache it here to make
    /// unit test easier". The zero value in Go is the zero `time.Time`; here
    /// the caller must supply one, because there is no meaningful default.
    pub start_time: DateTime<Tz>,
    /// boundary: Go `time.Local`, read inside `GetLastExpectedTime`.
    pub local: Tz,
    /// `RMClient`, `InfoCache`, `store`, and `sessPool` as one implementor.
    pub deps: D,
}

impl<D: RuStatsDeps, Tz: TimeZone> RuStatsWriter<D, Tz> {
    /// Build a writer. Go's `NewRUStatsWriter(do *Domain)` is declined — see
    /// the module doc — so the four `*Domain` fields arrive as `deps`, and
    /// `interval` defaults to [`RU_STATS_INTERVAL`] as it does there.
    pub fn new(deps: D, start_time: DateTime<Tz>, local: Tz) -> Self {
        Self {
            interval: RU_STATS_INTERVAL,
            start_time,
            local,
            deps,
        }
    }

    /// Go `(*RUStatsWriter).DoWriteRUStatistics` (`ru_stats.go:139`).
    ///
    /// # Errors
    /// Propagates the first failure of the probe, the meta load, the PD
    /// fetch, the meta persist, or the insert — in that order, as Go does.
    pub fn do_write_ru_statistics(&self) -> Result<(), RuStatsError> {
        // check if is already inserted
        let last_end_time = get_last_expected_time(&self.start_time, self.interval, &self.local);
        let last_end_time = last_end_time.fixed_offset();
        if self.is_latest_data_inserted(&last_end_time)? {
            // boundary: Go logs "[ru_stats] ru data is already inserted, skip".
            return Ok(());
        }

        let last_stats = self.load_latest_ru_stats()?;
        let need_fetch_data = needs_fetch_data(last_stats.as_ref(), &last_end_time);

        let ru_stats = if need_fetch_data {
            let stats = self.fetch_resource_group_stats()?;
            let mut ru_stats = RuStats {
                latest: Some(DailyRuStats {
                    end_time: last_end_time,
                    stats,
                }),
                previous: None,
            };
            // Go: `if lastStats != nil { ruStats.Previous = lastStats.Latest }`
            // — note it takes `Latest`, which may itself be nil, and never
            // the old `Previous`.
            if let Some(last) = last_stats {
                ru_stats.previous = last.latest;
            }
            self.persist_latest_ru_stats(&ru_stats)?;
            ru_stats
        } else {
            // Unreachable unless `last_stats` had a `latest`; `needs_fetch_data`
            // only answers false in that case.
            last_stats.unwrap_or_default()
        };
        self.insert_ru_stats(&ru_stats)
    }

    /// Go `(*RUStatsWriter).fetchResourceGroupStats` (`ru_stats.go:183`).
    ///
    /// A PD group with no matching infoschema group is dropped silently, and
    /// the row that is kept carries the *infoschema* id and original-case
    /// name, not PD's.
    ///
    /// # Errors
    /// Whatever PD reports.
    pub fn fetch_resource_group_stats(&self) -> Result<Vec<GroupRuStats>, RuStatsError> {
        let groups = self.deps.list_resource_groups_with_ru_stats()?;
        let mut res = Vec::with_capacity(groups.len());
        for g in groups {
            let Some(group_info) = self.deps.resource_group_by_name(&g.name) else {
                continue;
            };
            res.push(GroupRuStats {
                id: group_info.id,
                name: group_info.name,
                ru_consumption: g.ru_stats,
            });
        }
        Ok(res)
    }

    /// Go `(*RUStatsWriter).loadLatestRUStats` (`ru_stats.go:204`).
    ///
    /// # Errors
    /// Whatever the meta read reports.
    pub fn load_latest_ru_stats(&self) -> Result<Option<RuStats>, RuStatsError> {
        self.deps.load_ru_stats()
    }

    /// Go `(*RUStatsWriter).persistLatestRUStats` (`ru_stats.go:210`).
    ///
    /// # Errors
    /// Whatever the transaction reports.
    pub fn persist_latest_ru_stats(&self, stats: &RuStats) -> Result<(), RuStatsError> {
        self.deps.persist_ru_stats(stats)
    }

    /// Go `(*RUStatsWriter).isLatestDataInserted` (`ru_stats.go:217`).
    ///
    /// Note the constant: `start` is `lastEndTime - ruStatsInterval`, the
    /// package constant, never `self.interval`. See the module doc,
    /// behavior 3.
    ///
    /// # Errors
    /// Whatever the executor reports.
    pub fn is_latest_data_inserted(
        &self,
        last_end_time: &DateTime<chrono::FixedOffset>,
    ) -> Result<bool, RuStatsError> {
        let end = last_end_time.format(GO_DATE_TIME_LAYOUT).to_string();
        let start = (*last_end_time - RU_STATS_INTERVAL)
            .format(GO_DATE_TIME_LAYOUT)
            .to_string();
        self.deps
            .query_row_exists(LATEST_INSERTED_PROBE_SQL, &[&start, &end])
    }

    /// Go `(*RUStatsWriter).insertRUStats` (`ru_stats.go:227`).
    ///
    /// An empty statement means "nothing worth writing" and is not executed.
    ///
    /// # Errors
    /// [`RuStatsError::MissingLatestStats`] when `stats.latest` is absent, or
    /// whatever the executor reports.
    pub fn insert_ru_stats(&self, stats: &RuStats) -> Result<(), RuStatsError> {
        let Some(sql) = generate_sql(stats)? else {
            return Ok(());
        };
        self.deps.exec_statement(&sql)
    }

    /// Go `(*RUStatsWriter).GCOutdatedRecords` (`ru_stats.go:238`): delete
    /// rows whose `end_time` is at or before `lastEndTime - 92 days`, in
    /// batches of [`GC_BATCH_SIZE`].
    ///
    /// The `<=` is Go's and is load-bearing; see the module doc, behavior 1.
    ///
    /// # Errors
    /// [`RuStatsError::MissingCountRow`] when the count query yields no row,
    /// or whatever the executor reports. A failed delete aborts the loop
    /// with the remaining batches undone, as in Go.
    pub fn gc_outdated_records(
        &self,
        last_end_time: &DateTime<chrono::FixedOffset>,
    ) -> Result<(), RuStatsError> {
        let gc_end_date = (*last_end_time - RU_STATS_GC_DURATION)
            .format(GO_DATE_TIME_LAYOUT)
            .to_string();
        let count_sql = gc_count_sql(&gc_end_date);
        let total_count = self
            .deps
            .query_single_count(&count_sql)?
            .ok_or(RuStatsError::MissingCountRow)?;

        let delete_sql = gc_delete_sql(&gc_end_date);
        for _ in 0..gc_loop_count(total_count) {
            self.deps.exec_statement(&delete_sql)?;
        }
        Ok(())
    }
}

/// The probe `isLatestDataInserted` runs, with its two `%?` placeholders.
pub const LATEST_INSERTED_PROBE_SQL: &str =
    "SELECT 1 from mysql.request_unit_by_group where start_time = %? and end_time = %? limit 1";

/// Go's `countSQL` in `GCOutdatedRecords`.
///
/// The date is interpolated with `fmt.Sprintf`, not bound as a parameter;
/// that is Go's choice and the value is a formatted timestamp, never user
/// input.
#[must_use]
pub fn gc_count_sql(gc_end_date: &str) -> String {
    format!("SELECT count(*) FROM {RU_BY_GROUP_TABLE} where end_time <= '{gc_end_date}'")
}

/// Go's per-batch delete in `GCOutdatedRecords`. The statement is identical
/// on every iteration, including the `order by end_time`.
#[must_use]
pub fn gc_delete_sql(gc_end_date: &str) -> String {
    format!(
        "DELETE FROM {RU_BY_GROUP_TABLE} where end_time <= '{gc_end_date}' order by end_time limit {GC_BATCH_SIZE}"
    )
}

/// Go `loopCount := (totalCount + gcBatchSize - 1) / gcBatchSize`.
///
/// Computed once from the up-front count, so a zero count runs no deletes and
/// concurrent inserts are left for the next round. A negative count — which
/// no `count(*)` produces — yields zero iterations here, where Go's `for
/// range` over a negative value also runs zero times.
#[must_use]
pub fn gc_loop_count(total_count: i64) -> i64 {
    if total_count <= 0 {
        return 0;
    }
    (total_count + GC_BATCH_SIZE - 1) / GC_BATCH_SIZE
}

/// Go's `needFetchData` decision (`ru_stats.go:155-158`).
///
/// True unless the persisted stats already carry a `Latest` whose `EndTime`
/// is the one about to be written. See the module doc for how Go's
/// `time.Time` inequality differs from this instant-and-offset comparison.
#[must_use]
pub fn needs_fetch_data(
    last_stats: Option<&RuStats>,
    last_end_time: &DateTime<chrono::FixedOffset>,
) -> bool {
    match last_stats.and_then(|s| s.latest.as_ref()) {
        Some(latest) => {
            !(latest.end_time == *last_end_time
                && latest.end_time.offset() == last_end_time.offset())
        }
        None => true,
    }
}

/// Go `generateSQL` (`ru_stats.go:259`): one `REPLACE INTO` carrying the
/// delta of each group's consumption since the previous roll-up.
///
/// Returns `None` for Go's `""`, meaning there is nothing to write.
///
/// The details that are easy to get wrong and are reproduced exactly:
/// `start_time` is `end_time - 24h` from the package constant; a group whose
/// consumption is nil is skipped (Go also logs it); the previous value is
/// subtracted only when the *name* matches **and** the id matches too, so a
/// dropped-and-recreated group starts from zero rather than going negative;
/// a delta below `1.0` is dropped; and the value written is `int64(ru)`,
/// truncating toward zero.
///
/// # Errors
/// [`RuStatsError::MissingLatestStats`] when `stats.latest` is absent, where
/// Go nil-dereferences.
pub fn generate_sql(stats: &RuStats) -> Result<Option<String>, RuStatsError> {
    let latest = stats
        .latest
        .as_ref()
        .ok_or(RuStatsError::MissingLatestStats)?;

    let mut buf = String::from(
        "REPLACE INTO mysql.request_unit_by_group(start_time, end_time, resource_group, total_ru) VALUES ",
    );
    // Go builds `map[string]meta.GroupRUStats` keyed by name, keeping only
    // entries with a non-nil consumption. A duplicated name keeps the last.
    let mut prev_stats: std::collections::HashMap<&str, &GroupRuStats> =
        std::collections::HashMap::new();
    if let Some(previous) = stats.previous.as_ref() {
        for g in &previous.stats {
            if g.ru_consumption.is_some() {
                prev_stats.insert(g.name.as_str(), g);
            }
        }
    }

    let end = latest.end_time.format(GO_DATE_TIME_LAYOUT).to_string();
    let start = (latest.end_time - RU_STATS_INTERVAL)
        .format(GO_DATE_TIME_LAYOUT)
        .to_string();
    let mut count = 0usize;
    for g in &latest.stats {
        let Some(consumption) = g.ru_consumption else {
            // boundary: Go logs "group ru consumption statistics data is
            // empty" with the group's name and id, then skips it.
            continue;
        };
        let mut ru = consumption.total();
        if let Some(prev) = prev_stats.get(g.name.as_str()) {
            if let Some(prev_consumption) = prev.ru_consumption {
                if g.id == prev.id {
                    ru -= prev_consumption.total();
                }
            }
        }
        // ignore too small delta value
        if ru < 1.0 {
            continue;
        }
        if count > 0 {
            buf.push(',');
        }
        // Go `fmt.Sprintf("(\"%s\", \"%s\", \"%s\", %d)", start, end, g.Name,
        // int64(ru))`. The group name is interpolated unquoted-escaped, as in
        // Go; group names are validated identifiers upstream.
        let truncated = ru as i64;
        let _ = write!(buf, "(\"{start}\", \"{end}\", \"{}\", {truncated})", g.name);
        count += 1;
    }
    if count == 0 {
        return Ok(None);
    }
    buf.push(';');
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use chrono::{FixedOffset, TimeZone as _, Utc};
    use chrono_tz::Tz;

    use super::*;

    /// Upstream `ru_stats_test.go` is testkit-bound: `TestWriteRUStatistics`
    /// needs `testkit.CreateMockStoreAndDomain`, a bootstrapped
    /// `mysql.request_unit_by_group`, and a real `*Domain`. Its *assertions*
    /// are transcreated here against a scripted [`RuStatsDeps`] instead —
    /// same timestamps, same group ids, same expected totals — so the
    /// arithmetic and the GC boundary are covered even though the SQL is not
    /// executed. `TestGetLastExpectedTime` needs nothing but the function and
    /// is transcreated case for case.
    #[derive(Default)]
    struct MockDeps {
        groups: RefCell<Vec<ResourceGroupWithRuStats>>,
        infoschema: Vec<ResourceGroupInfo>,
        stored: RefCell<Option<RuStats>>,
        inserted_probe: RefCell<bool>,
        gc_count: Option<i64>,
        gc_count_missing: bool,
        statements: RefCell<Vec<String>>,
        probes: RefCell<Vec<(String, Vec<String>)>>,
        exec_err_after: Option<usize>,
    }

    impl RuStatsDeps for MockDeps {
        fn list_resource_groups_with_ru_stats(
            &self,
        ) -> Result<Vec<ResourceGroupWithRuStats>, RuStatsError> {
            Ok(self.groups.borrow().clone())
        }
        fn resource_group_by_name(&self, name: &str) -> Option<ResourceGroupInfo> {
            self.infoschema
                .iter()
                .find(|g| g.name.eq_ignore_ascii_case(name))
                .cloned()
        }
        fn load_ru_stats(&self) -> Result<Option<RuStats>, RuStatsError> {
            Ok(self.stored.borrow().clone())
        }
        fn persist_ru_stats(&self, stats: &RuStats) -> Result<(), RuStatsError> {
            *self.stored.borrow_mut() = Some(stats.clone());
            Ok(())
        }
        fn query_row_exists(&self, sql: &str, params: &[&str]) -> Result<bool, RuStatsError> {
            self.probes.borrow_mut().push((
                sql.to_owned(),
                params.iter().map(|p| (*p).to_owned()).collect(),
            ));
            Ok(*self.inserted_probe.borrow())
        }
        fn query_single_count(&self, sql: &str) -> Result<Option<i64>, RuStatsError> {
            self.statements.borrow_mut().push(sql.to_owned());
            if self.gc_count_missing {
                return Ok(None);
            }
            Ok(Some(self.gc_count.unwrap_or(0)))
        }
        fn exec_statement(&self, sql: &str) -> Result<(), RuStatsError> {
            self.statements.borrow_mut().push(sql.to_owned());
            if let Some(limit) = self.exec_err_after {
                if self.statements.borrow().len() > limit {
                    return Err(RuStatsError::Other("boom".to_owned()));
                }
            }
            Ok(())
        }
    }

    fn shanghai() -> Tz {
        "Asia/Shanghai".parse().unwrap()
    }

    fn lord_howe() -> Tz {
        "Australia/Lord_Howe".parse().unwrap()
    }

    fn at(tz: Tz, y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Tz> {
        tz.with_ymd_and_hms(y, m, d, h, mi, s).unwrap()
    }

    /// TRANSCREATED from `testGetLastExpectedTimeTz` (`ru_stats_test.go:156`),
    /// case for case, for the two zones the upstream test names explicitly.
    #[test]
    fn get_last_expected_time_matches_upstream_cases() {
        for tz in [shanghai(), lord_howe(), Tz::UTC] {
            let now = at(tz, 2023, 12, 28, 10, 46, 23);
            let expect = |h, mi| at(tz, 2023, 12, 28, h, mi, 0);
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::minutes(5), &tz),
                expect(10, 45)
            );
            assert_eq!(
                get_last_expected_time_tz(
                    &at(tz, 2023, 12, 28, 10, 45, 0),
                    Duration::minutes(5),
                    &tz
                ),
                expect(10, 45)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::minutes(10), &tz),
                expect(10, 40)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::minutes(30), &tz),
                expect(10, 30)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::hours(1), &tz),
                expect(10, 0)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::hours(3), &tz),
                expect(9, 0)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::hours(4), &tz),
                expect(8, 0)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, Duration::hours(12), &tz),
                expect(0, 0)
            );
            assert_eq!(
                get_last_expected_time_tz(&now, RU_STATS_INTERVAL, &tz),
                expect(0, 0)
            );
            assert_eq!(
                get_last_expected_time_tz(&at(tz, 2023, 12, 28, 0, 0, 0), RU_STATS_INTERVAL, &tz),
                expect(0, 0)
            );
        }
    }

    /// WRITTEN. The interval walk is absolute-time arithmetic from local
    /// midnight, so on a DST day the buckets shift with the jump. Lord Howe
    /// moves its clock by 30 minutes on 2023-10-01 at 02:00 local.
    #[test]
    fn dst_shifts_the_intra_day_buckets_but_not_local_midnight() {
        let tz = lord_howe();
        let now = at(tz, 2023, 10, 1, 12, 0, 0);
        // A 24h interval still lands on local midnight.
        assert_eq!(
            get_last_expected_time_tz(&now, RU_STATS_INTERVAL, &tz),
            at(tz, 2023, 10, 1, 0, 0, 0)
        );
        // A 3h interval counts absolute time, so after the 30-minute jump the
        // boundary sits at 12:30 local, not 12:00.
        assert_eq!(
            get_last_expected_time_tz(&at(tz, 2023, 10, 1, 13, 0, 0), Duration::hours(3), &tz),
            at(tz, 2023, 10, 1, 12, 30, 0)
        );
    }

    /// WRITTEN. Go's `now.Date()` reads `now`'s own zone, so a `now` in one
    /// zone with a `tz` in another takes the day boundary from the former.
    /// Module doc behavior 4.
    #[test]
    fn the_calendar_date_comes_from_nows_zone_not_the_target_zone() {
        let tz = shanghai();
        // 2023-12-27 20:00 UTC is already 2023-12-28 04:00 in Shanghai.
        let now = Utc.with_ymd_and_hms(2023, 12, 27, 20, 0, 0).unwrap();
        // Go takes the date from `now`'s zone — 12-27 — and reads midnight of
        // *that* day in Shanghai, 28 hours back, then walks 5-hour buckets
        // forward: 12-28 01:00 local. Had the date come from `tz` (12-28) the
        // walk would have started 24h later and landed on 12-28 00:00.
        // A 24h interval hides the difference, because the two candidate
        // midnights are exactly one interval apart; a 5h interval does not.
        assert_eq!(
            get_last_expected_time_tz(&now, Duration::hours(5), &tz),
            at(tz, 2023, 12, 28, 1, 0, 0)
        );
        assert_eq!(
            get_last_expected_time_tz(&now, RU_STATS_INTERVAL, &tz),
            at(tz, 2023, 12, 28, 0, 0, 0)
        );
    }

    #[test]
    #[should_panic(expected = "must not be zero")]
    fn a_zero_interval_panics_like_gos_divide_by_zero() {
        let tz = shanghai();
        let _ = get_last_expected_time_tz(&at(tz, 2023, 12, 28, 1, 0, 0), Duration::zero(), &tz);
    }

    fn writer(deps: MockDeps, start: DateTime<Tz>, tz: Tz) -> RuStatsWriter<MockDeps, Tz> {
        RuStatsWriter::new(deps, start, tz)
    }

    fn default_and_test_groups() -> MockDeps {
        MockDeps {
            groups: RefCell::new(vec![
                ResourceGroupWithRuStats {
                    name: "default".to_owned(),
                    ru_stats: Some(Consumption {
                        rru: 200.0,
                        wru: 150.0,
                    }),
                },
                ResourceGroupWithRuStats {
                    name: "test".to_owned(),
                    ru_stats: Some(Consumption {
                        rru: 100.0,
                        wru: 50.0,
                    }),
                },
            ]),
            infoschema: vec![
                ResourceGroupInfo {
                    id: 1,
                    name: "default".to_owned(),
                },
                ResourceGroupInfo {
                    id: 2,
                    name: "test".to_owned(),
                },
            ],
            ..MockDeps::default()
        }
    }

    fn last_statement(deps: &MockDeps) -> String {
        deps.statements.borrow().last().cloned().unwrap_or_default()
    }

    /// TRANSCREATED from `testWriteRUStatisticsTz` (`ru_stats_test.go:45`).
    /// The upstream test checks the rows in the table; this checks the
    /// statement that would have produced them, day by day, with the same
    /// dates and the same expected totals — 350/150, then 400 for `test`
    /// alone, then nothing, then 50 for `default` alone.
    #[test]
    fn write_ru_statistics_day_by_day() {
        for tz in [shanghai(), lord_howe(), Tz::UTC] {
            let deps = default_and_test_groups();
            let mut w = writer(deps, at(tz, 2023, 12, 26, 0, 0, 1), tz);

            w.do_write_ru_statistics().unwrap();
            assert_eq!(
                last_statement(&w.deps),
                "REPLACE INTO mysql.request_unit_by_group(start_time, end_time, resource_group, total_ru) VALUES (\"2023-12-25 00:00:00\", \"2023-12-26 00:00:00\", \"default\", 350),(\"2023-12-25 00:00:00\", \"2023-12-26 00:00:00\", \"test\", 150);"
            );

            // after 1 day, only 1 group has delta ru.
            w.deps.groups.borrow_mut()[1].ru_stats = Some(Consumption {
                rru: 500.0,
                wru: 50.0,
            });
            w.start_time = at(tz, 2023, 12, 27, 0, 0, 1);
            w.do_write_ru_statistics().unwrap();
            assert_eq!(
                last_statement(&w.deps),
                "REPLACE INTO mysql.request_unit_by_group(start_time, end_time, resource_group, total_ru) VALUES (\"2023-12-26 00:00:00\", \"2023-12-27 00:00:00\", \"test\", 400);"
            );

            // after 1 day with 0 delta ru, no statement at all.
            w.start_time = at(tz, 2023, 12, 28, 0, 0, 1);
            let before = w.deps.statements.borrow().len();
            w.do_write_ru_statistics().unwrap();
            assert_eq!(w.deps.statements.borrow().len(), before);

            w.start_time = at(tz, 2023, 12, 29, 0, 0, 0);
            w.deps.groups.borrow_mut()[0].ru_stats = Some(Consumption {
                rru: 200.0,
                wru: 200.0,
            });
            w.do_write_ru_statistics().unwrap();
            assert_eq!(
                last_statement(&w.deps),
                "REPLACE INTO mysql.request_unit_by_group(start_time, end_time, resource_group, total_ru) VALUES (\"2023-12-28 00:00:00\", \"2023-12-29 00:00:00\", \"default\", 50);"
            );

            // After less than a day, even with changed ru, nothing new: the
            // probe reports the row is there. (Upstream relies on the real
            // table; here the probe is scripted, which is the same signal.)
            *w.deps.inserted_probe.borrow_mut() = true;
            w.deps.groups.borrow_mut()[0].ru_stats = Some(Consumption {
                rru: 1000.0,
                wru: 200.0,
            });
            w.start_time = at(tz, 2023, 12, 29, 1, 0, 0);
            let before = w.deps.statements.borrow().len();
            w.do_write_ru_statistics().unwrap();
            assert_eq!(w.deps.statements.borrow().len(), before);
        }
    }

    /// TRANSCREATED from `ru_stats_test.go:116-119`: GC called with
    /// `2023-12-26 + 92 days` must remove the `2023-12-26` rows. That fixes
    /// the boundary as inclusive.
    #[test]
    fn gc_boundary_is_inclusive_of_the_endpoint() {
        let tz = shanghai();
        let deps = MockDeps {
            gc_count: Some(2),
            ..MockDeps::default()
        };
        let start = at(tz, 2023, 12, 26, 0, 0, 0) + Duration::hours(92 * 24);
        let w = writer(deps, start, tz);
        w.gc_outdated_records(&start.fixed_offset()).unwrap();
        let stmts = w.deps.statements.borrow();
        assert_eq!(
            stmts[0],
            "SELECT count(*) FROM mysql.request_unit_by_group where end_time <= '2023-12-26 00:00:00'"
        );
        assert_eq!(
            stmts[1],
            "DELETE FROM mysql.request_unit_by_group where end_time <= '2023-12-26 00:00:00' order by end_time limit 1000"
        );
        // Two rows, one batch.
        assert_eq!(stmts.len(), 2);
    }

    /// WRITTEN. The batch count comes from the up-front `count(*)`.
    #[test]
    fn gc_loop_count_rounds_up_and_bottoms_out_at_zero() {
        assert_eq!(gc_loop_count(0), 0);
        assert_eq!(gc_loop_count(1), 1);
        assert_eq!(gc_loop_count(1000), 1);
        assert_eq!(gc_loop_count(1001), 2);
        assert_eq!(gc_loop_count(2000), 2);
        assert_eq!(gc_loop_count(-5), 0);
    }

    /// WRITTEN. An empty table runs no delete at all.
    #[test]
    fn gc_with_no_rows_issues_only_the_count() {
        let tz = Tz::UTC;
        let deps = MockDeps {
            gc_count: Some(0),
            ..MockDeps::default()
        };
        let now = at(tz, 2024, 5, 1, 0, 0, 0);
        let w = writer(deps, now, tz);
        w.gc_outdated_records(&now.fixed_offset()).unwrap();
        assert_eq!(w.deps.statements.borrow().len(), 1);
    }

    /// WRITTEN. Go would panic on `rows[0]`; this reports instead, and does
    /// not delete anything.
    #[test]
    fn gc_reports_a_missing_count_row_instead_of_inventing_zero() {
        let tz = Tz::UTC;
        let deps = MockDeps {
            gc_count_missing: true,
            ..MockDeps::default()
        };
        let now = at(tz, 2024, 5, 1, 0, 0, 0);
        let w = writer(deps, now, tz);
        assert_eq!(
            w.gc_outdated_records(&now.fixed_offset()),
            Err(RuStatsError::MissingCountRow)
        );
        assert_eq!(w.deps.statements.borrow().len(), 1);
    }

    /// WRITTEN. A failing delete stops the loop; the remaining batches stay
    /// undone, as in Go.
    #[test]
    fn gc_stops_at_the_first_failing_batch() {
        let tz = Tz::UTC;
        let deps = MockDeps {
            gc_count: Some(3500),
            // count(*) is statement 1; allow one delete, fail the next.
            exec_err_after: Some(2),
            ..MockDeps::default()
        };
        let now = at(tz, 2024, 5, 1, 0, 0, 0);
        let w = writer(deps, now, tz);
        assert_eq!(
            w.gc_outdated_records(&now.fixed_offset()),
            Err(RuStatsError::Other("boom".to_owned()))
        );
        assert_eq!(w.deps.statements.borrow().len(), 3);
    }

    /// WRITTEN. Module doc behavior 3: the probe's `start_time` is always
    /// 24h before `end_time`, even when the writer's interval is not 24h.
    #[test]
    fn the_probe_start_time_uses_the_constant_not_the_writers_interval() {
        let tz = Tz::UTC;
        let mut w = writer(default_and_test_groups(), at(tz, 2024, 5, 1, 7, 30, 0), tz);
        w.interval = Duration::hours(6);
        w.do_write_ru_statistics().unwrap();
        let probes = w.deps.probes.borrow();
        assert_eq!(probes[0].0, LATEST_INSERTED_PROBE_SQL);
        // end_time is the 06:00 bucket; start_time is 24h earlier, not 6h.
        assert_eq!(
            probes[0].1,
            vec![
                "2024-04-30 06:00:00".to_owned(),
                "2024-05-01 06:00:00".to_owned()
            ]
        );
    }

    fn fixed(y: i32, mo: u32, d: u32) -> DateTime<FixedOffset> {
        FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(y, mo, d, 0, 0, 0)
            .unwrap()
    }

    fn group(id: i64, name: &str, rru: f64, wru: f64) -> GroupRuStats {
        GroupRuStats {
            id,
            name: name.to_owned(),
            ru_consumption: Some(Consumption { rru, wru }),
        }
    }

    /// WRITTEN. The previous value is subtracted only when name *and* id
    /// match, so a recreated group restarts from zero instead of going
    /// negative.
    #[test]
    fn a_recreated_group_does_not_subtract_the_old_ids_consumption() {
        let stats = RuStats {
            latest: Some(DailyRuStats {
                end_time: fixed(2024, 5, 2),
                stats: vec![group(7, "g", 100.0, 0.0)],
            }),
            previous: Some(DailyRuStats {
                end_time: fixed(2024, 5, 1),
                stats: vec![group(3, "g", 90.0, 0.0)],
            }),
        };
        let sql = generate_sql(&stats).unwrap().unwrap();
        assert!(sql.contains("\"g\", 100"), "{sql}");
    }

    /// WRITTEN. A nil consumption on the latest side skips the group; a nil
    /// consumption on the previous side means no subtraction.
    #[test]
    fn nil_consumption_skips_the_group_and_suppresses_the_delta() {
        let stats = RuStats {
            latest: Some(DailyRuStats {
                end_time: fixed(2024, 5, 2),
                stats: vec![
                    GroupRuStats {
                        id: 1,
                        name: "empty".to_owned(),
                        ru_consumption: None,
                    },
                    group(2, "kept", 10.0, 5.0),
                ],
            }),
            previous: Some(DailyRuStats {
                end_time: fixed(2024, 5, 1),
                stats: vec![GroupRuStats {
                    id: 2,
                    name: "kept".to_owned(),
                    ru_consumption: None,
                }],
            }),
        };
        let sql = generate_sql(&stats).unwrap().unwrap();
        assert!(!sql.contains("empty"), "{sql}");
        assert!(sql.contains("\"kept\", 15"), "{sql}");
    }

    /// WRITTEN. Deltas under 1.0 — including negative ones — are dropped, and
    /// the surviving value truncates toward zero.
    #[test]
    fn sub_one_deltas_are_dropped_and_the_rest_truncate() {
        let stats = RuStats {
            latest: Some(DailyRuStats {
                end_time: fixed(2024, 5, 2),
                stats: vec![
                    group(1, "tiny", 0.75, 0.0),
                    group(2, "shrunk", 5.0, 0.0),
                    group(3, "frac", 9.99, 0.0),
                ],
            }),
            previous: Some(DailyRuStats {
                end_time: fixed(2024, 5, 1),
                stats: vec![group(2, "shrunk", 50.0, 0.0)],
            }),
        };
        let sql = generate_sql(&stats).unwrap().unwrap();
        assert!(!sql.contains("tiny"), "{sql}");
        assert!(!sql.contains("shrunk"), "{sql}");
        assert!(sql.contains("\"frac\", 9"), "{sql}");
    }

    /// WRITTEN. Nothing above the threshold means no statement at all, which
    /// is Go's `""`.
    #[test]
    fn nothing_to_write_yields_no_statement() {
        let stats = RuStats {
            latest: Some(DailyRuStats {
                end_time: fixed(2024, 5, 2),
                stats: vec![group(1, "tiny", 0.5, 0.0)],
            }),
            previous: None,
        };
        assert_eq!(generate_sql(&stats).unwrap(), None);
    }

    /// WRITTEN. Go nil-dereferences here; this reports.
    #[test]
    fn generate_sql_without_latest_reports_instead_of_panicking() {
        assert_eq!(
            generate_sql(&RuStats::default()),
            Err(RuStatsError::MissingLatestStats)
        );
    }

    /// WRITTEN. The row carries the infoschema id and original-case name, and
    /// a PD group with no infoschema entry is dropped.
    #[test]
    fn fetch_uses_infoschema_identity_and_drops_unknown_groups() {
        let tz = Tz::UTC;
        let deps = MockDeps {
            groups: RefCell::new(vec![
                ResourceGroupWithRuStats {
                    name: "DEFAULT".to_owned(),
                    ru_stats: Some(Consumption { rru: 1.0, wru: 2.0 }),
                },
                ResourceGroupWithRuStats {
                    name: "gone".to_owned(),
                    ru_stats: Some(Consumption { rru: 9.0, wru: 9.0 }),
                },
            ]),
            infoschema: vec![ResourceGroupInfo {
                id: 1,
                name: "default".to_owned(),
            }],
            ..MockDeps::default()
        };
        let w = writer(deps, at(tz, 2024, 5, 1, 0, 0, 0), tz);
        let got = w.fetch_resource_group_stats().unwrap();
        assert_eq!(
            got,
            vec![GroupRuStats {
                id: 1,
                name: "default".to_owned(),
                ru_consumption: Some(Consumption { rru: 1.0, wru: 2.0 }),
            }]
        );
    }

    /// WRITTEN. `Previous` is taken from the old `Latest`, never from the old
    /// `Previous`, so the chain advances by exactly one day per write.
    #[test]
    fn persisted_previous_is_the_old_latest() {
        let tz = Tz::UTC;
        let deps = default_and_test_groups();
        *deps.stored.borrow_mut() = Some(RuStats {
            latest: Some(DailyRuStats {
                end_time: fixed(2024, 4, 30),
                stats: vec![group(1, "default", 1.0, 1.0)],
            }),
            previous: Some(DailyRuStats {
                end_time: fixed(2024, 4, 29),
                stats: vec![group(1, "default", 0.0, 0.0)],
            }),
        });
        let w = writer(deps, at(tz, 2024, 5, 1, 3, 0, 0), tz);
        w.do_write_ru_statistics().unwrap();
        let stored = w.deps.stored.borrow().clone().unwrap();
        assert_eq!(stored.latest.unwrap().end_time, fixed(2024, 5, 1));
        assert_eq!(stored.previous.unwrap().end_time, fixed(2024, 4, 30));
    }

    /// WRITTEN. `needs_fetch_data` is the guard that keeps a second write for
    /// the same end time from diffing the stats against themselves.
    #[test]
    fn needs_fetch_data_is_false_only_for_a_matching_latest_end_time() {
        assert!(needs_fetch_data(None, &fixed(2024, 5, 1)));
        assert!(needs_fetch_data(
            Some(&RuStats::default()),
            &fixed(2024, 5, 1)
        ));
        let same = RuStats {
            latest: Some(DailyRuStats {
                end_time: fixed(2024, 5, 1),
                stats: vec![],
            }),
            previous: None,
        };
        assert!(!needs_fetch_data(Some(&same), &fixed(2024, 5, 1)));
        assert!(needs_fetch_data(Some(&same), &fixed(2024, 5, 2)));
        // Same instant, different offset: Go's struct comparison also calls
        // these unequal, so a fetch happens.
        let east8 = FixedOffset::east_opt(8 * 3600)
            .unwrap()
            .with_ymd_and_hms(2024, 5, 1, 8, 0, 0)
            .unwrap();
        assert_eq!(east8, fixed(2024, 5, 1));
        assert!(needs_fetch_data(Some(&same), &east8));
    }

    /// WRITTEN. Nothing is fetched, persisted, or inserted once the probe
    /// says the row is already there.
    #[test]
    fn an_already_inserted_bucket_short_circuits_everything() {
        let tz = Tz::UTC;
        let deps = default_and_test_groups();
        *deps.inserted_probe.borrow_mut() = true;
        let w = writer(deps, at(tz, 2024, 5, 1, 3, 0, 0), tz);
        w.do_write_ru_statistics().unwrap();
        assert!(w.deps.statements.borrow().is_empty());
        assert!(w.deps.stored.borrow().is_none());
    }

    /// WRITTEN. The wake-up is one full interval past the bucket that was
    /// just written, not one interval past "now".
    #[test]
    fn next_wakeup_is_one_interval_past_the_bucket() {
        let tz = shanghai();
        let last = at(tz, 2024, 5, 1, 0, 0, 0);
        assert_eq!(next_wakeup(&last), at(tz, 2024, 5, 2, 0, 0, 0));
    }
}
