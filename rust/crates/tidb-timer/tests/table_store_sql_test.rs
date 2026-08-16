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

//! Transcreation of Go `pkg/timer/tablestore/sql_test.go`.
//!
//! Go's tests live in-package and reach unexported builders; the Rust port
//! exercises the same symbols through the crate's public surface, where the
//! `table_store` module deliberately exports them.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tidb_timer::go_time::{GoTime, MINUTE, SECOND};
use tidb_timer::store::{and, not, or, Cond, OptionalVal, TimerCond, TimerUpdate};
use tidb_timer::table_store::sql::{
    build_cond_criteria, build_delete_timer_sql, build_insert_timer_sql, build_select_timer_sql,
    build_update_criteria, build_update_timer_sql, SqlArg,
};
use tidb_timer::table_store::store::{
    execute_sql, run_in_txn, Datum, Row, SessionContext, SessionPool, SqlContext, SqlExecutor,
    SysSession, TableTimerStoreCore,
};
use tidb_timer::timer::{
    EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec,
};
use tidb_timer::{Result, TimerError};
use tidb_util::timeutil::TimeZone;

/// Go `strings.Count(sql, "%?")`.
fn count_placeholders(sql: &str) -> usize {
    sql.matches("%?").count()
}

/// The case tuples below carry a boxed condition; naming the shape keeps
/// clippy's `type_complexity` lint quiet without splitting the tables up.
type CondCase<S> = (Option<Arc<dyn Cond>>, S, Vec<SqlArg>);

fn timer_cond(cond: TimerCond) -> Arc<dyn Cond> {
    Arc::new(cond)
}

#[test]
fn test_build_insert_timer_sql() {
    let now = GoTime::now();
    let sql1 = "INSERT INTO `db1`.`t1` (NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, \
HOOK_CLASS, WATERMARK, ENABLE, TIMER_EXT, EVENT_ID, EVENT_STATUS, EVENT_START, EVENT_DATA, SUMMARY_DATA, VERSION) \
VALUES (%?, %?, %?, %?, %?, %?, %?, FROM_UNIXTIME(%?), %?, JSON_MERGE_PATCH('{}', %?), %?, %?, FROM_UNIXTIME(%?), %?, %?, 1)";
    let sql2 = "INSERT INTO `db1`.`t1` (NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, \
HOOK_CLASS, WATERMARK, ENABLE, TIMER_EXT, EVENT_ID, EVENT_STATUS, EVENT_START, EVENT_DATA, SUMMARY_DATA, VERSION) \
VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, JSON_MERGE_PATCH('{}', %?), %?, %?, %?, %?, %?, 1)";

    let cases: Vec<(&str, TimerRecord, Vec<SqlArg>)> = vec![
        (
            sql1,
            TimerRecord {
                spec: TimerSpec {
                    namespace: "n1".to_string(),
                    key: "k1".to_string(),
                    data: b"data1".to_vec(),
                    time_zone: "Asia/Shanghai".to_string(),
                    sched_policy_type: SchedPolicyType::interval(),
                    sched_policy_expr: "1h".to_string(),
                    hook_class: "h1".to_string(),
                    watermark: now.clone(),
                    enable: true,
                    tags: vec!["l1".to_string(), "l2".to_string()],
                },
                manual_request: ManualRequest {
                    manual_request_id: "req1".to_string(),
                    manual_request_time: GoTime::from_unix(123, 0),
                    manual_timeout: MINUTE,
                    manual_processed: true,
                    manual_event_id: "event1".to_string(),
                },
                event_extra: EventExtra {
                    event_manual_request_id: "req1".to_string(),
                    event_watermark: GoTime::from_unix(456, 0),
                },
                event_id: "e1".to_string(),
                event_status: SchedEventStatus::trigger(),
                event_start: now.add(SECOND),
                event_data: b"event1".to_vec(),
                summary_data: b"summary1".to_vec(),
                ..Default::default()
            },
            vec![
                SqlArg::str("n1"),
                SqlArg::str("k1"),
                SqlArg::Bytes(b"data1".to_vec()),
                SqlArg::str("Asia/Shanghai"),
                SqlArg::str("INTERVAL"),
                SqlArg::str("1h"),
                SqlArg::str("h1"),
                SqlArg::Int64(now.unix()),
                SqlArg::Bool(true),
                SqlArg::Json(
                    r#"{"tags":["l1","l2"],"manual":{"request_id":"req1","request_time_unix":123,"timeout_sec":60,"processed":true,"event_id":"event1"},"event":{"manual_request_id":"req1","watermark_unix":456}}"#
                        .to_string(),
                ),
                SqlArg::str("e1"),
                SqlArg::str("TRIGGER"),
                SqlArg::Int64(now.unix() + 1),
                SqlArg::Bytes(b"event1".to_vec()),
                SqlArg::Bytes(b"summary1".to_vec()),
            ],
        ),
        (
            sql2,
            TimerRecord {
                spec: TimerSpec {
                    namespace: "n1".to_string(),
                    key: "k1".to_string(),
                    sched_policy_type: SchedPolicyType::interval(),
                    sched_policy_expr: "1h".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            vec![
                SqlArg::str("n1"),
                SqlArg::str("k1"),
                SqlArg::Bytes(Vec::new()),
                SqlArg::str(""),
                SqlArg::str("INTERVAL"),
                SqlArg::str("1h"),
                SqlArg::str(""),
                SqlArg::Null,
                SqlArg::Bool(false),
                SqlArg::Json("{}".to_string()),
                SqlArg::str(""),
                SqlArg::str("IDLE"),
                SqlArg::Null,
                SqlArg::Bytes(Vec::new()),
                SqlArg::Bytes(Vec::new()),
            ],
        ),
    ];

    for (expected_sql, record, expected_args) in cases {
        assert_eq!(count_placeholders(expected_sql), expected_args.len());
        let (sql, args) = build_insert_timer_sql("db1", "t1", &record).unwrap();
        assert_eq!(expected_sql, sql);
        assert_eq!(expected_args, args);
    }
}

#[test]
fn test_build_cond_criteria() {
    let cases: Vec<CondCase<&str>> = vec![
        (None, "1", vec![]),
        (Some(timer_cond(TimerCond::default())), "1", vec![]),
        (
            Some(timer_cond(TimerCond {
                id: OptionalVal::new("1".to_string()),
                ..Default::default()
            })),
            "ID = %?",
            vec![SqlArg::str("1")],
        ),
        (
            Some(timer_cond(TimerCond {
                namespace: OptionalVal::new("ns1".to_string()),
                ..Default::default()
            })),
            "NAMESPACE = %?",
            vec![SqlArg::str("ns1")],
        ),
        (
            Some(timer_cond(TimerCond {
                key: OptionalVal::new("key1".to_string()),
                ..Default::default()
            })),
            "TIMER_KEY = %?",
            vec![SqlArg::str("key1")],
        ),
        (
            Some(timer_cond(TimerCond {
                key: OptionalVal::new("key1".to_string()),
                key_prefix: true,
                ..Default::default()
            })),
            "TIMER_KEY LIKE %?",
            vec![SqlArg::str("key1%")],
        ),
        (
            Some(timer_cond(TimerCond {
                namespace: OptionalVal::new("ns1".to_string()),
                key: OptionalVal::new("key1".to_string()),
                ..Default::default()
            })),
            "NAMESPACE = %? AND TIMER_KEY = %?",
            vec![SqlArg::str("ns1"), SqlArg::str("key1")],
        ),
        (
            Some(timer_cond(TimerCond {
                namespace: OptionalVal::new("ns1".to_string()),
                key: OptionalVal::new("key1".to_string()),
                key_prefix: true,
                ..Default::default()
            })),
            "NAMESPACE = %? AND TIMER_KEY LIKE %?",
            vec![SqlArg::str("ns1"), SqlArg::str("key1%")],
        ),
        (
            Some(timer_cond(TimerCond {
                tags: OptionalVal::new(Vec::new()),
                ..Default::default()
            })),
            "1",
            vec![],
        ),
        (
            Some(timer_cond(TimerCond {
                tags: OptionalVal::new(vec!["l1".to_string()]),
                ..Default::default()
            })),
            "JSON_EXTRACT(TIMER_EXT, '$.tags') IS NOT NULL AND JSON_CONTAINS((TIMER_EXT->'$.tags'), %?)",
            vec![SqlArg::Json(r#"["l1"]"#.to_string())],
        ),
        (
            Some(timer_cond(TimerCond {
                tags: OptionalVal::new(vec!["l1".to_string(), "l2".to_string()]),
                ..Default::default()
            })),
            "JSON_EXTRACT(TIMER_EXT, '$.tags') IS NOT NULL AND JSON_CONTAINS((TIMER_EXT->'$.tags'), %?)",
            vec![SqlArg::Json(r#"["l1","l2"]"#.to_string())],
        ),
        (
            Some(Arc::new(and(vec![
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    key: OptionalVal::new("key1".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "(NAMESPACE = %? AND TIMER_KEY = %?) AND (ID = %?)",
            vec![SqlArg::str("ns1"), SqlArg::str("key1"), SqlArg::str("2")],
        ),
        (
            Some(Arc::new(and(vec![
                timer_cond(TimerCond::default()),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "1 AND (ID = %?)",
            vec![SqlArg::str("2")],
        ),
        (
            Some(Arc::new(and(vec![
                Arc::new(not(timer_cond(TimerCond::default()))),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "0 AND (ID = %?)",
            vec![SqlArg::str("2")],
        ),
        (
            Some(Arc::new(and(vec![
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond::default()),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "(NAMESPACE = %?) AND 1 AND (ID = %?)",
            vec![SqlArg::str("ns1"), SqlArg::str("2")],
        ),
        (
            Some(Arc::new(not(Arc::new(and(vec![
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    key: OptionalVal::new("key1".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))))),
            "!((NAMESPACE = %? AND TIMER_KEY = %?) AND (ID = %?))",
            vec![SqlArg::str("ns1"), SqlArg::str("key1"), SqlArg::str("2")],
        ),
        (
            Some(Arc::new(or(vec![
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    key: OptionalVal::new("key1".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "(NAMESPACE = %? AND TIMER_KEY = %?) OR (ID = %?)",
            vec![SqlArg::str("ns1"), SqlArg::str("key1"), SqlArg::str("2")],
        ),
        (
            Some(Arc::new(not(Arc::new(or(vec![
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    key: OptionalVal::new("key1".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))))),
            "!((NAMESPACE = %? AND TIMER_KEY = %?) OR (ID = %?))",
            vec![SqlArg::str("ns1"), SqlArg::str("key1"), SqlArg::str("2")],
        ),
        (
            Some(Arc::new(or(vec![
                timer_cond(TimerCond::default()),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "1 OR (ID = %?)",
            vec![SqlArg::str("2")],
        ),
        (
            Some(Arc::new(or(vec![
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond::default()),
                timer_cond(TimerCond {
                    id: OptionalVal::new("2".to_string()),
                    ..Default::default()
                }),
            ]))),
            "(NAMESPACE = %?) OR 1 OR (ID = %?)",
            vec![SqlArg::str("ns1"), SqlArg::str("2")],
        ),
        (
            Some(Arc::new(not(timer_cond(TimerCond {
                id: OptionalVal::new("3".to_string()),
                ..Default::default()
            })))),
            "!(ID = %?)",
            vec![SqlArg::str("3")],
        ),
        (
            Some(Arc::new(not(timer_cond(TimerCond::default())))),
            "0",
            vec![],
        ),
        (
            Some(Arc::new(not(Arc::new(not(timer_cond(
                TimerCond::default(),
            )))))),
            "1",
            vec![],
        ),
        (
            Some(Arc::new(not(timer_cond(TimerCond {
                namespace: OptionalVal::new("ns1".to_string()),
                key: OptionalVal::new("key1".to_string()),
                ..Default::default()
            })))),
            "!(NAMESPACE = %? AND TIMER_KEY = %?)",
            vec![SqlArg::str("ns1"), SqlArg::str("key1")],
        ),
    ];

    for (cond, expected_criteria, expected_args) in cases {
        assert_eq!(count_placeholders(expected_criteria), expected_args.len());
        let borrowed = cond.as_ref().map(|cond| cond.as_ref());

        let (criteria, args) = build_cond_criteria(borrowed, Vec::new()).unwrap();
        assert_eq!(expected_criteria, criteria);
        assert_eq!(expected_args, args);

        let prefix = vec![SqlArg::str("a"), SqlArg::str("b")];
        let (criteria, args) = build_cond_criteria(borrowed, prefix.clone()).unwrap();
        assert_eq!(expected_criteria, criteria);
        let mut want = prefix;
        want.extend(expected_args);
        assert_eq!(want, args);
    }
}

#[test]
fn test_build_select_timer_sql() {
    let prefix = "SELECT ID, NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, \
HOOK_CLASS, WATERMARK, ENABLE, TIMER_EXT, EVENT_STATUS, EVENT_ID, EVENT_DATA, EVENT_START, SUMMARY_DATA, \
CREATE_TIME, UPDATE_TIME, VERSION FROM `db1`.`t1`";

    let cases: Vec<CondCase<String>> = vec![
        (None, format!("{prefix} WHERE 1"), vec![]),
        (
            Some(timer_cond(TimerCond {
                id: OptionalVal::new("2".to_string()),
                ..Default::default()
            })),
            format!("{prefix} WHERE ID = %?"),
            vec![SqlArg::str("2")],
        ),
        (
            Some(timer_cond(TimerCond {
                namespace: OptionalVal::new("ns1".to_string()),
                key: OptionalVal::new("key1".to_string()),
                ..Default::default()
            })),
            format!("{prefix} WHERE NAMESPACE = %? AND TIMER_KEY = %?"),
            vec![SqlArg::str("ns1"), SqlArg::str("key1")],
        ),
        (
            Some(Arc::new(or(vec![
                timer_cond(TimerCond {
                    id: OptionalVal::new("3".to_string()),
                    ..Default::default()
                }),
                timer_cond(TimerCond {
                    namespace: OptionalVal::new("ns1".to_string()),
                    ..Default::default()
                }),
            ]))),
            format!("{prefix} WHERE (ID = %?) OR (NAMESPACE = %?)"),
            vec![SqlArg::str("3"), SqlArg::str("ns1")],
        ),
    ];

    for (cond, expected_sql, expected_args) in cases {
        assert_eq!(count_placeholders(&expected_sql), expected_args.len());
        let (sql, args) =
            build_select_timer_sql("db1", "t1", cond.as_ref().map(|cond| cond.as_ref())).unwrap();
        assert_eq!(expected_sql, sql);
        assert_eq!(expected_args, args);
    }
}

#[test]
fn test_build_update_criteria() {
    let now = GoTime::now();
    let zero_time = GoTime::zero();

    let cases: Vec<(TimerUpdate, String, Vec<SqlArg>)> = vec![
        (
            TimerUpdate::default(),
            "VERSION = VERSION + 1".to_string(),
            vec![],
        ),
        (
            TimerUpdate {
                enable: OptionalVal::new(true),
                ..Default::default()
            },
            "ENABLE = %?, VERSION = VERSION + 1".to_string(),
            vec![SqlArg::Bool(true)],
        ),
        (
            TimerUpdate {
                enable: OptionalVal::new(false),
                tags: OptionalVal::new(vec!["l1".to_string(), "l2".to_string()]),
                time_zone: OptionalVal::new("Asia/Shanghai".to_string()),
                sched_policy_type: OptionalVal::new(SchedPolicyType::interval()),
                sched_policy_expr: OptionalVal::new("1h".to_string()),
                manual_request: OptionalVal::new(ManualRequest {
                    manual_request_id: "req1".to_string(),
                    manual_request_time: GoTime::from_unix(123, 0),
                    manual_timeout: MINUTE,
                    manual_processed: true,
                    manual_event_id: "event1".to_string(),
                }),
                event_status: OptionalVal::new(SchedEventStatus::trigger()),
                event_id: OptionalVal::new("event1".to_string()),
                event_data: OptionalVal::new(b"data1".to_vec()),
                event_start: OptionalVal::new(now.clone()),
                event_extra: OptionalVal::new(EventExtra {
                    event_manual_request_id: "req2".to_string(),
                    event_watermark: GoTime::from_unix(456, 0),
                }),
                watermark: OptionalVal::new(now.add(SECOND)),
                summary_data: OptionalVal::new(b"summary".to_vec()),
                check_event_id: OptionalVal::new("ee".to_string()),
                check_version: OptionalVal::new(1),
            },
            "ENABLE = %?, TIMEZONE = %?, SCHED_POLICY_TYPE = %?, SCHED_POLICY_EXPR = %?, EVENT_STATUS = %?, \
EVENT_ID = %?, EVENT_DATA = %?, EVENT_START = FROM_UNIXTIME(%?), \
WATERMARK = FROM_UNIXTIME(%?), SUMMARY_DATA = %?, \
TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), \
VERSION = VERSION + 1"
                .to_string(),
            vec![
                SqlArg::Bool(false),
                SqlArg::str("Asia/Shanghai"),
                SqlArg::str("INTERVAL"),
                SqlArg::str("1h"),
                SqlArg::str("TRIGGER"),
                SqlArg::str("event1"),
                SqlArg::Bytes(b"data1".to_vec()),
                SqlArg::Int64(now.unix()),
                SqlArg::Int64(now.unix() + 1),
                SqlArg::Bytes(b"summary".to_vec()),
                SqlArg::Json(
                    r#"{"event":{"manual_request_id":"req2","watermark_unix":456},"manual":{"request_id":"req1","request_time_unix":123,"timeout_sec":60,"processed":true,"event_id":"event1"},"tags":["l1","l2"]}"#
                        .to_string(),
                ),
            ],
        ),
        (
            TimerUpdate {
                event_extra: OptionalVal::new(EventExtra {
                    event_manual_request_id: "req1".to_string(),
                    ..Default::default()
                }),
                manual_request: OptionalVal::new(ManualRequest {
                    manual_request_id: "req2".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            "TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), VERSION = VERSION + 1".to_string(),
            vec![SqlArg::Json(
                r#"{"event":{"manual_request_id":"req1","watermark_unix":null},"manual":{"request_id":"req2","request_time_unix":null,"timeout_sec":null,"processed":null,"event_id":null}}"#
                    .to_string(),
            )],
        ),
        (
            TimerUpdate {
                event_extra: OptionalVal::new(EventExtra {
                    event_watermark: GoTime::from_unix(123, 0),
                    ..Default::default()
                }),
                manual_request: OptionalVal::new(ManualRequest {
                    manual_request_time: GoTime::from_unix(456, 0),
                    ..Default::default()
                }),
                ..Default::default()
            },
            "TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), VERSION = VERSION + 1".to_string(),
            vec![SqlArg::Json(
                r#"{"event":{"manual_request_id":null,"watermark_unix":123},"manual":{"request_id":null,"request_time_unix":456,"timeout_sec":null,"processed":null,"event_id":null}}"#
                    .to_string(),
            )],
        ),
        (
            TimerUpdate {
                time_zone: OptionalVal::new(String::new()),
                sched_policy_expr: OptionalVal::new(String::new()),
                event_id: OptionalVal::new(String::new()),
                event_data: OptionalVal::new(Vec::new()),
                event_start: OptionalVal::new(zero_time.clone()),
                event_extra: OptionalVal::new(EventExtra::default()),
                manual_request: OptionalVal::new(ManualRequest::default()),
                watermark: OptionalVal::new(zero_time),
                summary_data: OptionalVal::new(Vec::new()),
                tags: OptionalVal::new(Vec::new()),
                ..Default::default()
            },
            "TIMEZONE = %?, SCHED_POLICY_EXPR = %?, EVENT_ID = %?, EVENT_DATA = %?, \
EVENT_START = NULL, WATERMARK = NULL, SUMMARY_DATA = %?, \
TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), \
VERSION = VERSION + 1"
                .to_string(),
            vec![
                SqlArg::str(""),
                SqlArg::str(""),
                SqlArg::str(""),
                SqlArg::Bytes(Vec::new()),
                SqlArg::Bytes(Vec::new()),
                SqlArg::Json(r#"{"event":null,"manual":null,"tags":null}"#.to_string()),
            ],
        ),
        (
            TimerUpdate {
                check_event_id: OptionalVal::new("ee".to_string()),
                check_version: OptionalVal::new(1),
                ..Default::default()
            },
            "VERSION = VERSION + 1".to_string(),
            vec![],
        ),
    ];

    for (update, expected_criteria, expected_args) in cases {
        assert_eq!(count_placeholders(&expected_criteria), expected_args.len());

        let (criteria, args) = build_update_criteria(&update, Vec::new()).unwrap();
        assert_eq!(expected_criteria, criteria);
        assert_eq!(expected_args, args);

        let prefix = vec![SqlArg::Int64(1), SqlArg::str("2"), SqlArg::str("3")];
        let (criteria, args) = build_update_criteria(&update, prefix.clone()).unwrap();
        assert_eq!(expected_criteria, criteria);
        let mut want = prefix;
        want.extend(expected_args);
        assert_eq!(want, args);
    }
}

#[test]
fn test_build_update_timer_sql() {
    let timer_id = "123";
    let cases: Vec<(TimerUpdate, &str, Vec<SqlArg>)> = vec![
        (
            TimerUpdate::default(),
            "UPDATE `db1`.`tbl1` SET VERSION = VERSION + 1 WHERE ID = %?",
            vec![SqlArg::str(timer_id)],
        ),
        (
            TimerUpdate {
                sched_policy_type: OptionalVal::new(SchedPolicyType::interval()),
                sched_policy_expr: OptionalVal::new("1h".to_string()),
                ..Default::default()
            },
            "UPDATE `db1`.`tbl1` SET SCHED_POLICY_TYPE = %?, SCHED_POLICY_EXPR = %?, VERSION = VERSION + 1 WHERE ID = %?",
            vec![
                SqlArg::str("INTERVAL"),
                SqlArg::str("1h"),
                SqlArg::str(timer_id),
            ],
        ),
    ];

    for (update, expected_sql, expected_args) in cases {
        assert_eq!(count_placeholders(expected_sql), expected_args.len());
        let (sql, args) = build_update_timer_sql("db1", "tbl1", timer_id, &update).unwrap();
        assert_eq!(expected_sql, sql);
        assert_eq!(expected_args, args);
    }
}

#[test]
fn test_build_delete_timer_sql() {
    let (sql, args) = build_delete_timer_sql("db1", "tbl1", "123");
    assert_eq!("DELETE FROM `db1`.`tbl1` WHERE ID = %?", sql);
    assert_eq!(vec![SqlArg::str("123")], args);
}

// ---------------------------------------------------------------------------
// The session-level tests: Go `TestWithSession` and `TestRunInTxn`.
//
// Go drives these with `stretchr/testify/mock`, whose expectations match by
// method plus arguments and are consumed once each. The mock below reproduces
// exactly that contract for the single mocked method (`ExecuteInternal`):
// `expect` queues an expectation, a call consumes the first *matching* one, and
// `assert_expectations` requires the queue to be empty. `Outcome::Panic` is
// testify's `.Panic(...)`.
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum SqlMatcher {
    Exact(String),
    Prefix(String),
}

impl SqlMatcher {
    fn matches(&self, sql: &str) -> bool {
        match self {
            Self::Exact(text) => text == sql,
            Self::Prefix(text) => sql.starts_with(text.as_str()),
        }
    }
}

enum Outcome {
    Rows(Option<Vec<Row>>),
    Err(&'static str),
    Panic(&'static str),
}

struct Expectation {
    sql: SqlMatcher,
    /// `None` is testify's `mock.Anything` for the argument list.
    args: Option<Vec<SqlArg>>,
    outcome: Outcome,
}

#[derive(Default)]
struct MockSessionState {
    expectations: VecDeque<Expectation>,
}

/// Go's `mockSession`.
#[derive(Default)]
struct MockSession {
    state: Mutex<MockSessionState>,
    index_merge: Mutex<bool>,
}

impl MockSession {
    fn lock(&self) -> std::sync::MutexGuard<'_, MockSessionState> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn expect(&self, sql: SqlMatcher, args: Option<Vec<SqlArg>>, outcome: Outcome) {
        self.lock()
            .expectations
            .push_back(Expectation { sql, args, outcome });
    }

    fn expect_exact(&self, sql: &str, args: Vec<SqlArg>, outcome: Outcome) {
        self.expect(SqlMatcher::Exact(sql.to_string()), Some(args), outcome);
    }

    /// Go `sctx.AssertExpectations(t)`.
    fn assert_expectations(&self) {
        assert!(
            self.lock().expectations.is_empty(),
            "mock has unfulfilled expectations"
        );
    }
}

impl SqlExecutor for MockSession {
    fn execute_internal(
        &self,
        ctx: &SqlContext,
        sql: &str,
        args: &[SqlArg],
    ) -> Result<Option<Vec<Row>>> {
        // Go's `matchCtx`.
        assert_eq!(ctx.internal_source.as_deref(), Some("Timer"));

        let outcome = {
            let mut state = self.lock();
            let position = state.expectations.iter().position(|expectation| {
                expectation.sql.matches(sql)
                    && expectation
                        .args
                        .as_ref()
                        .is_none_or(|expected| expected.as_slice() == args)
            });
            match position {
                Some(position) => state.expectations.remove(position).map(|e| e.outcome),
                None => panic!("unexpected ExecuteInternal call: {sql:?} with {args:?}"),
            }
        };

        match outcome.expect("expectation was present") {
            Outcome::Rows(rows) => Ok(rows),
            Outcome::Err(message) => Err(TimerError::message(message)),
            Outcome::Panic(message) => panic!("{message}"),
        }
    }
}

impl SessionContext for MockSession {
    fn get_enable_index_merge(&self) -> bool {
        *self.index_merge.lock().unwrap()
    }

    fn set_enable_index_merge(&self, enable: bool) {
        *self.index_merge.lock().unwrap() = enable;
    }

    fn location(&self) -> TimeZone {
        TimeZone::Named(chrono_tz::Tz::UTC)
    }

    fn get_global_system_var(&self, _name: &str) -> Result<String> {
        Ok("UTC".to_string())
    }

    fn sql_executor(&self) -> Arc<dyn SqlExecutor> {
        unreachable!("the session-level tests never route through GetSQLExecutor")
    }
}

/// A `SessionContext` that hands out a separate executor, as Go's
/// `mockSession.GetSQLExecutor` returns the session itself.
struct RoutedSessionContext {
    exec: Arc<MockSession>,
}

impl SessionContext for RoutedSessionContext {
    fn get_enable_index_merge(&self) -> bool {
        self.exec.get_enable_index_merge()
    }

    fn set_enable_index_merge(&self, enable: bool) {
        self.exec.set_enable_index_merge(enable);
    }

    fn location(&self) -> TimeZone {
        self.exec.location()
    }

    fn get_global_system_var(&self, name: &str) -> Result<String> {
        self.exec.get_global_system_var(name)
    }

    fn sql_executor(&self) -> Arc<dyn SqlExecutor> {
        self.exec.clone()
    }
}

/// Go's `mockSessionPool`.
#[derive(Default)]
struct MockSessionPool {
    session: Mutex<Option<Arc<SysSession>>>,
    err: Mutex<Option<&'static str>>,
}

impl SessionPool for MockSessionPool {
    fn with_session(&self, callback: &mut dyn FnMut(&SysSession) -> Result<()>) -> Result<()> {
        if let Some(message) = *self.err.lock().unwrap() {
            return Err(TimerError::message(message));
        }
        let session = self.session.lock().unwrap().clone().expect("session set");
        callback(&session)
    }
}

fn tz_row() -> Option<Vec<Row>> {
    Some(vec![Row::new(vec![Datum::Str("tz1".to_string())])])
}

fn expect_success_init(sctx: &MockSession) {
    sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Rows(None));
    sctx.expect_exact("SELECT @@time_zone", Vec::new(), Outcome::Rows(tz_row()));
    sctx.expect_exact("SET @@time_zone='UTC'", Vec::new(), Outcome::Rows(None));
}

fn expect_restore(sctx: &MockSession) {
    sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Rows(None));
    sctx.expect_exact(
        "SET @@time_zone=%?",
        vec![SqlArg::str("tz1")],
        Outcome::Rows(None),
    );
}

/// Runs `body`, returning the panic payload as a string when it panics.
fn catch_panic(body: impl FnOnce()) -> Option<String> {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
    std::panic::set_hook(previous);
    result.err().map(|payload| {
        payload
            .downcast_ref::<&str>()
            .map(|text| (*text).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_default()
    })
}

#[test]
fn test_with_session() {
    let sctx = Arc::new(MockSession::default());
    let pool = Arc::new(MockSessionPool::default());
    let core = TableTimerStoreCore::new(pool.clone(), "db1", "t1");

    let reset_se = || {
        *pool.session.lock().unwrap() =
            Some(Arc::new(SysSession::new(Arc::new(RoutedSessionContext {
                exec: sctx.clone(),
            }))));
    };
    reset_se();

    // Go's `cb1`: assert the init expectations were consumed, register the
    // restore expectations (Go's `defer mockRestore()`), then return.
    let mut cb1_calls = 0usize;
    let cb1 = |result: Result<()>| {
        let sctx = sctx.clone();
        move |_se: &SysSession| -> Result<()> {
            sctx.assert_expectations();
            expect_restore(&sctx);
            result.clone()
        }
    };

    // Pool has an error
    *pool.err.lock().unwrap() = Some("mockErr");
    assert_eq!(
        core.with_session(&mut cb1(Ok(()))).unwrap_err().to_string(),
        "mockErr"
    );

    // init session returns error
    *pool.err.lock().unwrap() = None;
    sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Err("mockErr1"));
    assert_eq!(
        core.with_session(&mut cb1(Ok(()))).unwrap_err().to_string(),
        "mockErr1"
    );
    sctx.assert_expectations();

    // init session returns error2
    sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Rows(None));
    sctx.expect_exact("SELECT @@time_zone", Vec::new(), Outcome::Err("mockErr2"));
    assert_eq!(
        core.with_session(&mut cb1(Ok(()))).unwrap_err().to_string(),
        "mockErr2"
    );
    sctx.assert_expectations();

    // init session panic
    sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Panic("mockPanic"));
    let payload = catch_panic(|| {
        let _ = core.with_session(&mut |_se| Ok(()));
    });
    assert_eq!(payload.as_deref(), Some("mockPanic"));
    sctx.assert_expectations();

    // returns a session
    expect_success_init(&sctx);
    assert!(core.with_session(&mut cb1(Ok(()))).is_ok());
    cb1_calls += 1;
    sctx.assert_expectations();

    // callback failed
    expect_success_init(&sctx);
    assert_eq!(
        core.with_session(&mut cb1(Err(TimerError::message("mockErr3"))))
            .unwrap_err()
            .to_string(),
        "mockErr3"
    );
    cb1_calls += 1;
    sctx.assert_expectations();

    // callback panic
    expect_success_init(&sctx);
    let payload = catch_panic(|| {
        let _ = core.with_session(&mut |_se| {
            sctx.assert_expectations();
            expect_restore(&sctx);
            panic!("panic2");
        });
    });
    assert_eq!(payload.as_deref(), Some("panic2"));
    sctx.assert_expectations();

    // rollback in restore failed, should avoid re-use session
    expect_success_init(&sctx);
    assert!(core
        .with_session(&mut |_se| {
            sctx.assert_expectations();
            sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Err("ROLLBACK error"));
            Ok(())
        })
        .is_ok());
    assert!(pool
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .is_avoid_reuse());
    sctx.assert_expectations();
    reset_se();

    // set timezone in restore failed should avoid re-use
    expect_success_init(&sctx);
    assert!(core
        .with_session(&mut |_se| {
            sctx.assert_expectations();
            sctx.expect_exact("ROLLBACK", Vec::new(), Outcome::Rows(None));
            sctx.expect(
                SqlMatcher::Exact("SET @@time_zone=%?".to_string()),
                None,
                Outcome::Err("SET tz error"),
            );
            Ok(())
        })
        .is_ok());
    assert!(pool
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .is_avoid_reuse());
    sctx.assert_expectations();
    reset_se();

    // withSctx
    let cb2 = |result: Result<()>| {
        let sctx = sctx.clone();
        move |_ctx: &dyn SessionContext| -> Result<()> {
            sctx.assert_expectations();
            expect_restore(&sctx);
            result.clone()
        }
    };

    expect_success_init(&sctx);
    assert!(core.with_sctx(&mut cb2(Ok(()))).is_ok());
    sctx.assert_expectations();

    // withSctx error
    expect_success_init(&sctx);
    assert_eq!(
        core.with_sctx(&mut cb2(Err(TimerError::message("mockErr4"))))
            .unwrap_err()
            .to_string(),
        "mockErr4"
    );
    sctx.assert_expectations();

    // withSctx panic
    expect_success_init(&sctx);
    let payload = catch_panic(|| {
        let _ = core.with_sctx(&mut |_ctx| {
            sctx.assert_expectations();
            expect_restore(&sctx);
            panic!("panic3");
        });
    });
    assert_eq!(payload.as_deref(), Some("panic3"));
    sctx.assert_expectations();

    assert_eq!(cb1_calls, 2);
}

#[test]
fn test_run_in_txn() {
    let se = Arc::new(MockSession::default());

    // success
    se.expect_exact("BEGIN PESSIMISTIC", Vec::new(), Outcome::Rows(None));
    se.expect(
        SqlMatcher::Prefix("insert".to_string()),
        None,
        Outcome::Rows(None),
    );
    se.expect_exact("COMMIT", Vec::new(), Outcome::Rows(None));
    assert!(run_in_txn(se.as_ref(), &mut || {
        execute_sql(se.as_ref(), "insert into t value(?)", &[SqlArg::Int64(1)])?;
        Ok(())
    })
    .is_ok());
    se.assert_expectations();

    // start txn failed
    se.expect_exact(
        "BEGIN PESSIMISTIC",
        Vec::new(),
        Outcome::Err("mockBeginErr"),
    );
    let err = run_in_txn(se.as_ref(), &mut || Ok(())).unwrap_err();
    assert_eq!(err.to_string(), "mockBeginErr");
    se.assert_expectations();

    // exec failed, rollback success
    se.expect_exact("BEGIN PESSIMISTIC", Vec::new(), Outcome::Rows(None));
    se.expect_exact("ROLLBACK", Vec::new(), Outcome::Rows(None));
    let err = run_in_txn(se.as_ref(), &mut || Err(TimerError::message("mockFuncErr"))).unwrap_err();
    assert_eq!(err.to_string(), "mockFuncErr");
    se.assert_expectations();

    // commit failed
    se.expect_exact("BEGIN PESSIMISTIC", Vec::new(), Outcome::Rows(None));
    se.expect_exact("COMMIT", Vec::new(), Outcome::Err("commitErr"));
    se.expect_exact("ROLLBACK", Vec::new(), Outcome::Rows(None));
    let err = run_in_txn(se.as_ref(), &mut || Ok(())).unwrap_err();
    assert_eq!(err.to_string(), "commitErr");
    se.assert_expectations();

    // rollback failed
    se.expect_exact("BEGIN PESSIMISTIC", Vec::new(), Outcome::Rows(None));
    se.expect_exact("ROLLBACK", Vec::new(), Outcome::Err("rollbackErr"));
    let err = run_in_txn(se.as_ref(), &mut || Err(TimerError::message("mockFuncErr"))).unwrap_err();
    assert_eq!(err.to_string(), "mockFuncErr");
    se.assert_expectations();
}
