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

//! Go `pkg/timer/api/timer_test.go`.

use tidb_timer::go_time::{GoTime, HOUR};
use tidb_timer::timer::{SchedPolicyType, TimerRecord, TimerSpec};
use tidb_util::timeutil::TimeZone;

/// Go `TestTimerValidate`.
#[test]
fn test_timer_validate() {
    // invalid insert
    let mut record = TimerRecord::default();
    assert_eq!(
        record.validate().unwrap_err().to_string(),
        "field 'Namespace' should not be empty"
    );

    record.spec.namespace = "n1".to_string();
    assert_eq!(
        record.validate().unwrap_err().to_string(),
        "field 'Key' should not be empty"
    );

    record.spec.key = "k1".to_string();
    assert_eq!(
        record.validate().unwrap_err().to_string(),
        "field 'SchedPolicyType' should not be empty"
    );

    record.spec.sched_policy_type = SchedPolicyType::from("aa");
    assert_eq!(
        record.validate().unwrap_err().to_string(),
        "schedule event configuration is not valid: invalid schedule event type: 'aa'"
    );

    record.spec.sched_policy_type = SchedPolicyType::interval();
    record.spec.sched_policy_expr = "1x".to_string();
    assert_eq!(
        record.validate().unwrap_err().to_string(),
        "schedule event configuration is not valid: invalid schedule event expr '1x': unknown unit x"
    );

    record.spec.sched_policy_expr = "1h".to_string();
    assert!(record.validate().is_ok());

    record.spec.time_zone = "a123".to_string();
    assert!(record
        .validate()
        .unwrap_err()
        .to_string()
        .contains("Unknown or incorrect time zone: 'a123'"));

    record.spec.time_zone = "tidb".to_string();
    assert!(record
        .validate()
        .unwrap_err()
        .to_string()
        .contains("Unknown or incorrect time zone: 'tidb'"));

    record.spec.time_zone = "+0800".to_string();
    assert!(record.validate().is_ok());

    record.spec.time_zone = "Asia/Shanghai".to_string();
    assert!(record.validate().is_ok());

    record.spec.time_zone = String::new();
    assert!(record.validate().is_ok());
}

/// Go `TestTimerNextEventTime`.
#[test]
fn test_timer_next_event_time() {
    let now = GoTime::now().in_location(&TimeZone::Named(chrono_tz::Tz::UTC));
    let mut record = TimerRecord {
        spec: TimerSpec {
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "1h".to_string(),
            watermark: now.clone(),
            enable: true,
            ..Default::default()
        },
        ..Default::default()
    };

    let (next, ok) = record.next_event_time().unwrap();
    assert!(ok);
    assert_eq!(next, now.add(HOUR));

    let loc = TimeZone::Fixed {
        name: "UTC+1".to_string(),
        offset_secs: 60 * 60,
    };
    record.location = Some(loc.clone());
    let (next, ok) = record.next_event_time().unwrap();
    assert!(ok);
    assert_eq!(next, now.add(HOUR).in_location(&loc));

    record.spec.enable = false;
    let (next, ok) = record.next_event_time().unwrap();
    assert!(!ok);
    assert!(next.is_zero());

    record.spec.sched_policy_expr = "abcde".to_string();
    let (next, ok) = record.next_event_time().unwrap();
    assert!(!ok);
    assert!(next.is_zero());

    record.spec.enable = true;
    let err = record.next_event_time().unwrap_err();
    assert!(err.to_string().contains("invalid schedule event expr"));

    record.spec.sched_policy_type = SchedPolicyType::cron();
    record.spec.sched_policy_expr = "0 0 30 2 *".to_string();
    let (next, ok) = record.next_event_time().unwrap();
    assert!(!ok);
    assert!(next.is_zero());
}
