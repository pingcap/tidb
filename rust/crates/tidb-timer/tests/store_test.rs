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

//! Go `pkg/timer/api/store_test.go`.
//!
//! Go's `FieldsSet(unsafe.Pointer(&cond.ID))` exclusions are spelled here with
//! the Go field name (`"ID"`), the same name the result reports; see
//! `TimerCond::fields_set`.

use std::sync::Arc;

use tidb_timer::go_time::{GoTime, MINUTE, SECOND};
use tidb_timer::store::{and, not, or, Cond, OptionalVal, TimerCond, TimerUpdate};
use tidb_timer::timer::{
    EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec,
};
use tidb_util::timeutil::{set_system_tz, TimeZone};

/// Go `TestFieldOptional`.
#[test]
fn test_field_optional() {
    let mut opt1: OptionalVal<String> = OptionalVal::default();
    assert!(!opt1.present());
    let (s, ok) = opt1.get_or_zero();
    assert!(!ok);
    assert_eq!(s, "");

    opt1.set("a1".to_string());
    assert!(opt1.present());
    let (s, ok) = opt1.get_or_zero();
    assert!(ok);
    assert_eq!(s, "a1");

    opt1.set("a2".to_string());
    assert!(opt1.present());
    let (s, ok) = opt1.get_or_zero();
    assert!(ok);
    assert_eq!(s, "a2");

    opt1.clear();
    assert!(!opt1.present());
    let (s, ok) = opt1.get_or_zero();
    assert!(!ok);
    assert_eq!(s, "");

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Foo {
        v: i32,
    }

    // Go's `OptionalVal[*Foo]`: the payload itself can be nil while present.
    let mut opt2: OptionalVal<Option<Arc<Foo>>> = OptionalVal::default();
    let foo = Arc::new(Foo { v: 1 });

    let (f, ok) = opt2.get_or_zero();
    assert!(!ok);
    assert!(f.is_none());

    opt2.set(Some(Arc::clone(&foo)));
    assert!(opt2.present());
    let (f, ok) = opt2.get_or_zero();
    assert!(ok);
    assert!(Arc::ptr_eq(&f.unwrap(), &foo));

    opt2.set(None);
    assert!(opt2.present());
    let (f, ok) = opt2.get_or_zero();
    assert!(ok);
    assert!(f.is_none());

    opt2.clear();
    let (f, ok) = opt2.get_or_zero();
    assert!(!ok);
    assert!(f.is_none());
}

/// Go `TestFieldsReflect`.
#[test]
fn test_fields_reflect() {
    let mut cond = TimerCond::default();
    assert!(cond.fields_set(&[]).is_empty());

    cond.key.set("k1".to_string());
    assert_eq!(cond.fields_set(&[]), vec!["Key"]);

    cond.id.set("22".to_string());
    assert_eq!(cond.fields_set(&[]), vec!["ID", "Key"]);
    assert_eq!(cond.fields_set(&["ID"]), vec!["Key"]);

    cond.key.clear();
    assert_eq!(cond.fields_set(&[]), vec!["ID"]);

    cond.key_prefix = true;
    cond.clear();
    assert!(cond.fields_set(&[]).is_empty());
    assert!(!cond.key_prefix);

    let mut update = TimerUpdate::default();
    assert!(update.fields_set(&[]).is_empty());

    update.watermark.set(GoTime::now());
    assert_eq!(update.fields_set(&[]), vec!["Watermark"]);

    update.enable.set(true);
    assert_eq!(update.fields_set(&[]), vec!["Enable", "Watermark"]);
    assert_eq!(update.fields_set(&["Enable"]), vec!["Watermark"]);

    update.watermark.clear();
    assert_eq!(update.fields_set(&[]), vec!["Enable"]);

    update.clear();
    assert!(update.fields_set(&[]).is_empty());
}

fn record_with_tags(tags: &[&str]) -> TimerRecord {
    TimerRecord {
        id: "123".to_string(),
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: "/path/to/key".to_string(),
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn id_cond(id: &str) -> TimerCond {
    TimerCond {
        id: OptionalVal::new(id.to_string()),
        ..Default::default()
    }
}

fn namespace_cond(namespace: &str) -> TimerCond {
    TimerCond {
        namespace: OptionalVal::new(namespace.to_string()),
        ..Default::default()
    }
}

fn key_cond(key: &str, prefix: bool) -> TimerCond {
    TimerCond {
        key: OptionalVal::new(key.to_string()),
        key_prefix: prefix,
        ..Default::default()
    }
}

fn tags_cond(tags: &[&str]) -> TimerCond {
    TimerCond {
        tags: OptionalVal::new(tags.iter().map(|tag| (*tag).to_string()).collect()),
        ..Default::default()
    }
}

/// Go `TestTimerRecordCond`.
#[test]
fn test_timer_record_cond() {
    let tm = record_with_tags(&["tagA1", "tagA2"]);

    // ID
    assert!(id_cond("123").match_record(&tm));
    assert!(!id_cond("1").match_record(&tm));

    // Namespace
    assert!(namespace_cond("n1").match_record(&tm));
    assert!(!namespace_cond("n2").match_record(&tm));

    // Key
    assert!(key_cond("/path/to/key", false).match_record(&tm));
    assert!(!key_cond("/path/to/", false).match_record(&tm));

    // keyPrefix
    assert!(key_cond("/path/to/", true).match_record(&tm));
    assert!(!key_cond("/path/to2", true).match_record(&tm));

    // Tags
    let mut tm2 = tm.clone_record();
    tm2.spec.tags = Vec::new();

    assert!(tags_cond(&[]).match_record(&tm));
    assert!(tags_cond(&[]).match_record(&tm2));

    assert!(!tags_cond(&["tagA"]).match_record(&tm));
    assert!(!tags_cond(&["tagA"]).match_record(&tm2));

    assert!(tags_cond(&["tagA1"]).match_record(&tm));
    assert!(!tags_cond(&["tagA1"]).match_record(&tm2));

    assert!(tags_cond(&["tagA1", "tagA2"]).match_record(&tm));
    assert!(!tags_cond(&["tagA1", "tagB1"]).match_record(&tm));

    // Combined condition
    let combined = TimerCond {
        id: OptionalVal::new("123".to_string()),
        key: OptionalVal::new("/path/to/key".to_string()),
        ..Default::default()
    };
    assert!(combined.match_record(&tm));

    let combined = TimerCond {
        id: OptionalVal::new("123".to_string()),
        key: OptionalVal::new("/path/to/".to_string()),
        ..Default::default()
    };
    assert!(!combined.match_record(&tm));
}

/// Go `TestOperatorCond`.
#[test]
fn test_operator_cond() {
    let tm = TimerRecord {
        id: "123".to_string(),
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: "/path/to/key".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    let cond1: Arc<dyn Cond> = Arc::new(id_cond("123"));
    let cond2: Arc<dyn Cond> = Arc::new(id_cond("456"));
    let cond3: Arc<dyn Cond> = Arc::new(namespace_cond("n1"));
    let cond4: Arc<dyn Cond> = Arc::new(namespace_cond("n2"));

    assert!(and(vec![cond1.clone(), cond3.clone()]).match_record(&tm));
    assert!(!and(vec![cond1.clone(), cond2.clone(), cond3.clone()]).match_record(&tm));
    assert!(!or(vec![cond2.clone(), cond4.clone()]).match_record(&tm));
    assert!(or(vec![cond2.clone(), cond1.clone(), cond4.clone()]).match_record(&tm));

    assert!(!not(Arc::new(and(vec![cond1.clone(), cond3.clone()]))).match_record(&tm));
    assert!(not(Arc::new(and(vec![
        cond1.clone(),
        cond2.clone(),
        cond3.clone()
    ])))
    .match_record(&tm));
    assert!(not(Arc::new(or(vec![cond2.clone(), cond4.clone()]))).match_record(&tm));
    assert!(!not(Arc::new(or(vec![
        cond2.clone(),
        cond1.clone(),
        cond4.clone()
    ])))
    .match_record(&tm));

    assert!(!not(cond1).match_record(&tm));
    assert!(not(cond2).match_record(&tm));
}

/// Go `TestTimerUpdate`.
#[test]
fn test_timer_update() {
    set_system_tz("Asia/Shanghai");
    let tpl = TimerRecord {
        id: "123".to_string(),
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: "/path/to/key".to_string(),
            ..Default::default()
        },
        version: 567,
        ..Default::default()
    };
    let tm = tpl.clone_record();

    // test check version
    let update = TimerUpdate {
        enable: OptionalVal::new(true),
        check_version: OptionalVal::new(0),
        ..Default::default()
    };
    let err = update.apply(&tm).unwrap_err();
    assert!(err.error_equal(&tidb_timer::TimerError::VersionNotMatch));
    assert_eq!(tpl, tm);

    // test check event id
    let update = TimerUpdate {
        enable: OptionalVal::new(true),
        check_event_id: OptionalVal::new("aa".to_string()),
        ..Default::default()
    };
    let err = update.apply(&tm).unwrap_err();
    assert!(err.error_equal(&tidb_timer::TimerError::EventIDNotMatch));
    assert_eq!(tpl, tm);

    // test apply without check for some common fields
    let now = GoTime::now();
    let update = TimerUpdate {
        enable: OptionalVal::new(true),
        time_zone: OptionalVal::new("UTC".to_string()),
        sched_policy_type: OptionalVal::new(SchedPolicyType::interval()),
        sched_policy_expr: OptionalVal::new("5h".to_string()),
        watermark: OptionalVal::new(now.clone()),
        summary_data: OptionalVal::new(b"summarydata1".to_vec()),
        event_status: OptionalVal::new(SchedEventStatus::trigger()),
        event_id: OptionalVal::new("event1".to_string()),
        event_data: OptionalVal::new(b"eventdata1".to_vec()),
        event_start: OptionalVal::new(now.add(SECOND)),
        tags: OptionalVal::new(vec!["l1".to_string(), "l2".to_string()]),
        manual_request: OptionalVal::new(ManualRequest {
            manual_request_id: "req1".to_string(),
            manual_request_time: GoTime::from_unix(123, 0),
            manual_timeout: MINUTE,
            manual_processed: true,
            manual_event_id: "event1".to_string(),
        }),
        event_extra: OptionalVal::new(EventExtra {
            event_manual_request_id: "req".to_string(),
            event_watermark: GoTime::from_unix(456, 0),
        }),
        ..Default::default()
    };

    // Go asserts `NumField()-2`: every field but the two Check* guards.
    assert_eq!(update.fields_set(&[]).len(), 13);
    let record = update.apply(&tm).unwrap();
    assert!(record.spec.enable);
    assert_eq!(record.spec.time_zone, "UTC");
    assert_eq!(record.location, Some(TimeZone::Named(chrono_tz::Tz::UTC)));
    assert_eq!(record.spec.sched_policy_type, SchedPolicyType::interval());
    assert_eq!(record.spec.sched_policy_expr, "5h");
    assert_eq!(record.spec.watermark, now);
    assert_eq!(record.summary_data, b"summarydata1".to_vec());
    assert_eq!(record.event_status, SchedEventStatus::trigger());
    assert_eq!(record.event_id, "event1");
    assert_eq!(record.event_data, b"eventdata1".to_vec());
    assert_eq!(record.event_start, now.add(SECOND));
    assert_eq!(record.spec.tags, vec!["l1".to_string(), "l2".to_string()]);
    assert_eq!(
        record.manual_request,
        ManualRequest {
            manual_request_id: "req1".to_string(),
            manual_request_time: GoTime::from_unix(123, 0),
            manual_timeout: MINUTE,
            manual_processed: true,
            manual_event_id: "event1".to_string(),
        }
    );
    assert!(!record.is_manual_requesting());
    assert_eq!(
        record.event_extra,
        EventExtra {
            event_manual_request_id: "req".to_string(),
            event_watermark: GoTime::from_unix(456, 0),
        }
    );
    assert_eq!(tpl, tm);

    // test apply without check for ManualRequest and EventExtra
    let tpl = record.clone_record();
    let tm = tpl.clone_record();
    let update = TimerUpdate {
        manual_request: OptionalVal::new(ManualRequest {
            manual_request_id: "req2".to_string(),
            manual_request_time: GoTime::from_unix(789, 0),
            manual_timeout: MINUTE,
            ..Default::default()
        }),
        event_extra: OptionalVal::new(EventExtra {
            event_manual_request_id: "req2".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let record = update.apply(&tm).unwrap();
    assert_eq!(
        record.manual_request,
        ManualRequest {
            manual_request_id: "req2".to_string(),
            manual_request_time: GoTime::from_unix(789, 0),
            manual_timeout: MINUTE,
            ..Default::default()
        }
    );
    assert!(record.is_manual_requesting());
    assert_eq!(
        record.event_extra,
        EventExtra {
            event_manual_request_id: "req2".to_string(),
            ..Default::default()
        }
    );
    assert_eq!(tpl, tm);

    // test apply without check for empty ManualRequest and EventExtra
    let tpl = record.clone_record();
    let tm = tpl.clone_record();
    let update = TimerUpdate {
        manual_request: OptionalVal::new(ManualRequest::default()),
        event_extra: OptionalVal::new(EventExtra::default()),
        ..Default::default()
    };
    let record = update.apply(&tm).unwrap();
    assert_eq!(record.manual_request, ManualRequest::default());
    assert!(!record.is_manual_requesting());
    assert_eq!(record.event_extra, EventExtra::default());
    assert_eq!(tpl, tm);

    let empty_update = TimerUpdate::default();
    let record = empty_update.apply(&tm).unwrap();
    assert_eq!(tpl, record);
}
