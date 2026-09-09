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

//! Port of `pkg/domain/ru_stats_test.go` (origin/master):
//! `TestWriteRUStatistics` (+ `testWriteRUStatisticsTz`) and
//! `TestGetLastExpectedTime` (+ `testGetLastExpectedTimeTz`), driving
//! `tidb_domain::ru_stats` — the transcreation of `pkg/domain/ru_stats.go`.
//!
//! Go's test replaces two collaborators with fakes and keeps two real: the
//! RM client is a scripted `testRMClient`, the infoschema a `testInfoschema`
//! wrapper — and this port scripts the same two through the
//! [`RuStatsDeps`] boundary. The remaining real surface, the SQL table
//! `mysql.request_unit_by_group`, has no storage engine in this crate, so
//! the boundary methods that Go runs through
//! `runaway.ExecRCRestrictedSQL` execute against a tiny in-memory model of
//! that table: the `REPLACE INTO` rows `generateSQL` produces are parsed and
//! upserted, the GC `DELETE ... end_time <= ... limit N` deletes in
//! `end_time` order, and the two probe shapes answer from the stored rows.
//! The assertions are then Go's own: queries over the TABLE CONTENT (group
//! totals per `end_time`), not over emitted statement text.

#![cfg(test)]

use std::cell::RefCell;

use chrono::{DateTime, TimeZone};
use chrono_tz::Tz;

use tidb_domain::ru_stats::{
    get_last_expected_time_tz, Consumption, ResourceGroupInfo, ResourceGroupWithRuStats, RuStats,
    RuStatsDeps, RuStatsError, RuStatsWriter, RU_STATS_INTERVAL,
};

/// One row of `mysql.request_unit_by_group`:
/// `(start_time, end_time, resource_group, total_ru)`, all as Go renders
/// them (timestamps in `time.DateTime` layout).
type Row = (String, String, String, i64);

/// Go `testRMClient` + `testInfoschema` + the table `mysql.request_unit_by_group`.
#[derive(Default)]
struct MockDeps {
    /// Go `testRMClient.groups`.
    groups: RefCell<Vec<ResourceGroupWithRuStats>>,
    /// Go `testInfoschema.groups`.
    infoschema: Vec<ResourceGroupInfo>,
    /// Go `meta.GetRUStats` / `SetRUStats` storage.
    stored: RefCell<Option<RuStats>>,
    /// The table rows, in REPLACE (insertion) order.
    table: RefCell<Vec<Row>>,
}

impl RuStatsDeps for MockDeps {
    fn list_resource_groups_with_ru_stats(
        &self,
    ) -> Result<Vec<ResourceGroupWithRuStats>, RuStatsError> {
        Ok(self.groups.borrow().clone())
    }

    fn resource_group_by_name(&self, name: &str) -> Option<ResourceGroupInfo> {
        // Go looks the name up through `ast.NewCIStr`, i.e. case-insensitively.
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

    fn query_row_exists(&self, _sql: &str, params: &[&str]) -> Result<bool, RuStatsError> {
        let (start, end) = (params[0], params[1]);
        Ok(self
            .table
            .borrow()
            .iter()
            .any(|(s, e, _, _)| s == start && e == end))
    }

    fn query_single_count(&self, sql: &str) -> Result<Option<i64>, RuStatsError> {
        let date = sql
            .split("end_time <= '")
            .nth(1)
            .and_then(|rest| rest.split('\'').next())
            .expect("count SQL carries its cutoff");
        Ok(Some(
            self.table
                .borrow()
                .iter()
                .filter(|(_, end, _, _)| end.as_str() <= date)
                .count() as i64,
        ))
    }

    fn exec_statement(&self, sql: &str) -> Result<(), RuStatsError> {
        if let Some(values) = sql.split("VALUES ").nth(1) {
            // REPLACE INTO ... VALUES ("s", "e", "g", n),(...);
            for tuple in values.trim_end_matches(';').split("),(") {
                let tuple = tuple.trim().trim_start_matches('(').trim_end_matches(')');
                // Fields never contain commas (timestamps, group names, an
                // integer literal), so a plain comma split with quote/space
                // trimming is exact.
                let fields: Vec<&str> = tuple
                    .split(',')
                    .map(|field| field.trim().trim_matches('"'))
                    .collect();
                let [start, end, group, total] = fields.as_slice() else {
                    panic!("REPLACE tuple does not have 4 fields: {tuple}");
                };
                let total: i64 = total.parse().expect("total_ru is an integer literal");
                let mut table = self.table.borrow_mut();
                table.retain(|(s, e, g, _)| {
                    (s.as_str(), e.as_str(), g.as_str()) != (*start, *end, *group)
                });
                table.push((start.to_string(), end.to_string(), group.to_string(), total));
            }
            return Ok(());
        }
        if let Some(rest) = sql.split("DELETE FROM ").nth(1) {
            let date = rest
                .split("end_time <= '")
                .nth(1)
                .and_then(|r| r.split('\'').next())
                .expect("delete SQL carries its cutoff");
            let limit: usize = rest
                .split("limit ")
                .nth(1)
                .and_then(|l| l.trim_end_matches(';').parse().ok())
                .expect("delete SQL carries its limit");
            let mut table = self.table.borrow_mut();
            // `order by end_time`: survivors keep insertion order among equal
            // keys, which is all this model needs for Go's assertions.
            let mut victims: Vec<usize> = table
                .iter()
                .enumerate()
                .filter(|(_, (_, end, _, _))| end.as_str() <= date)
                .map(|(i, _)| i)
                .collect();
            victims.sort_by(|&a, &b| table[a].1.cmp(&table[b].1));
            victims.truncate(limit);
            for i in victims.into_iter().rev() {
                table.remove(i);
            }
            return Ok(());
        }
        panic!("model does not speak this statement: {sql}");
    }
}

impl MockDeps {
    /// `WHERE end_time = '<filter>'` against a DATETIME column: MySQL widens
    /// a date-only literal to midnight, so `2023-12-27` matches
    /// `2023-12-27 00:00:00`.
    fn end_matches(stored: &str, filter: &str) -> bool {
        stored == filter || (filter.len() == 10 && stored == format!("{filter} 00:00:00"))
    }

    /// `SELECT count(*) FROM mysql.request_unit_by_group [WHERE ...]`.
    fn count_where_end(&self, end: Option<&str>) -> usize {
        self.table
            .borrow()
            .iter()
            .filter(|(_, e, _, _)| end.is_none_or(|want| Self::end_matches(e, want)))
            .count()
    }

    /// `SELECT resource_group, total_ru FROM mysql.request_unit_by_group
    /// [WHERE end_time = ...]`, in insertion order.
    fn totals_where_end(&self, end: Option<&str>) -> Vec<(String, i64)> {
        self.table
            .borrow()
            .iter()
            .filter(|(_, e, _, _)| end.is_none_or(|want| Self::end_matches(e, want)))
            .map(|(_, _, g, t)| (g.clone(), *t))
            .collect()
    }
    /// Go's two group fixtures: `default` (RRU 200, WRU 150) and `test`
    /// (RRU 100, WRU 50), with infoschema ids 1 and 2.
    fn with_default_groups() -> Self {
        Self {
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
            ..Self::default()
        }
    }
}

fn at<Tz: TimeZone>(tz: &Tz, date: (i32, u32, u32), time: (u32, u32, u32)) -> DateTime<Tz> {
    tz.with_ymd_and_hms(date.0, date.1, date.2, time.0, time.1, time.2)
        .single()
        .expect("unambiguous local timestamp")
}

/// Go `ru_stats_test.go:146::TestGetLastExpectedTime` +
/// `ru_stats_test.go:156::testGetLastExpectedTimeTz`, all three zones and
/// all ten cases each.
#[test]
fn get_last_expected_time_matches_upstream_cases() {
    let shanghai: Tz = "Asia/Shanghai".parse().unwrap();
    let lord_howe: Tz = "Australia/Lord_Howe".parse().unwrap();
    for tz in [shanghai, lord_howe] {
        test_get_last_expected_time_tz(&tz);
    }
    test_get_last_expected_time_tz(&chrono::Local);
}

fn test_get_last_expected_time_tz<Tz: TimeZone>(tz: &Tz) {
    // 2023-12-28 10:46:23.000
    let now = at(tz, (2023, 12, 28), (10, 46, 23));
    let new_time = |hour, minute| at(tz, (2023, 12, 28), (hour, minute, 0));

    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::minutes(5), tz),
        new_time(10, 45)
    );
    assert_eq!(
        get_last_expected_time_tz(
            &at(tz, (2023, 12, 28), (10, 45, 0)),
            chrono::Duration::minutes(5),
            tz
        ),
        new_time(10, 45)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::minutes(10), tz),
        new_time(10, 40)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::minutes(30), tz),
        new_time(10, 30)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::hours(1), tz),
        new_time(10, 0)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::hours(3), tz),
        new_time(9, 0)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::hours(4), tz),
        new_time(8, 0)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::hours(12), tz),
        new_time(0, 0)
    );
    assert_eq!(
        get_last_expected_time_tz(&now, chrono::Duration::hours(24), tz),
        new_time(0, 0)
    );
    assert_eq!(
        get_last_expected_time_tz(
            &at(tz, (2023, 12, 28), (0, 0, 0)),
            chrono::Duration::hours(24),
            tz
        ),
        new_time(0, 0)
    );
}

/// Go `ru_stats_test.go:33::TestWriteRUStatistics` +
/// `ru_stats_test.go:45::testWriteRUStatisticsTz`, over the same four zones
/// (Shanghai, Lord Howe for DST, machine-local, UTC).
#[test]
fn write_ru_statistics_day_by_day() {
    let shanghai: Tz = "Asia/Shanghai".parse().unwrap();
    let lord_howe: Tz = "Australia/Lord_Howe".parse().unwrap();
    for tz in [shanghai, lord_howe] {
        test_write_ru_statistics_tz(&tz);
    }
    test_write_ru_statistics_tz(&chrono::Local);
    test_write_ru_statistics_tz(&chrono::Utc);
}

fn test_write_ru_statistics_tz<Tz: TimeZone + Clone>(tz: &Tz) {
    let mut writer = RuStatsWriter::new(
        MockDeps::with_default_groups(),
        at(tz, (2023, 12, 26), (0, 0, 1)),
        tz.clone(),
    );

    // The table starts empty.
    assert_eq!(writer.deps.count_where_end(None), 0);

    writer
        .do_write_ru_statistics()
        .expect("first daily write succeeds");
    assert_eq!(
        writer.deps.totals_where_end(None),
        [("default".to_owned(), 350), ("test".to_owned(), 150)]
    );

    // after 1 day, only 1 group has delta ru.
    writer.deps.groups.borrow_mut()[1].ru_stats = Some(Consumption {
        rru: 500.0,
        wru: 50.0,
    });
    writer.start_time = at(tz, (2023, 12, 27), (0, 0, 1));
    writer
        .do_write_ru_statistics()
        .expect("second daily write succeeds");
    assert_eq!(
        writer.deps.totals_where_end(Some("2023-12-27")),
        [("test".to_owned(), 400)]
    );

    // test after 1 day with 0 delta ru, no data inserted.
    writer.start_time = at(tz, (2023, 12, 28), (0, 0, 1));
    writer
        .do_write_ru_statistics()
        .expect("third daily write succeeds");
    assert_eq!(writer.deps.count_where_end(Some("2023-12-28")), 0);

    writer.start_time = at(tz, (2023, 12, 29), (0, 0, 0));
    writer.deps.groups.borrow_mut()[0].ru_stats = Some(Consumption {
        rru: 200.0,
        wru: 200.0,
    });
    writer
        .do_write_ru_statistics()
        .expect("fourth daily write succeeds");
    assert_eq!(
        writer.deps.totals_where_end(Some("2023-12-29")),
        [("default".to_owned(), 50)]
    );

    // after less than 1 day, even if ru changes, no new rows inserted.
    // This is to test after restart, no unexpected data are inserted.
    writer.deps.groups.borrow_mut()[0].ru_stats = Some(Consumption {
        rru: 1000.0,
        wru: 200.0,
    });
    writer.deps.groups.borrow_mut()[1].ru_stats = Some(Consumption {
        rru: 500.0,
        wru: 2000.0,
    });
    writer.start_time = at(tz, (2023, 12, 29), (1, 0, 0));
    writer
        .do_write_ru_statistics()
        .expect("same-bucket rewrite succeeds");
    assert_eq!(
        writer.deps.totals_where_end(Some("2023-12-29")),
        [("default".to_owned(), 50)]
    );

    // after 61 days, old record should be GCed.
    writer.start_time = at(tz, (2023, 12, 26), (0, 0, 0)) + RU_STATS_INTERVAL * 92;
    assert_eq!(writer.deps.count_where_end(Some("2023-12-26")), 2);
    writer
        .gc_outdated_records(&writer.start_time.clone().fixed_offset())
        .expect("gc succeeds");
    assert_eq!(writer.deps.count_where_end(Some("2023-12-26")), 0);
    assert_eq!(writer.deps.count_where_end(Some("2023-12-27")), 1);
}
