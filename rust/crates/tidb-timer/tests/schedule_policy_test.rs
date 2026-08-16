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

//! Go `pkg/timer/api/schedule_policy_test.go`.

use tidb_timer::go_time::{GoTime, HOUR, MINUTE};
use tidb_timer::timer::{
    create_sched_event_policy, SchedEventPolicy, SchedPolicy, SchedPolicyType,
};
use tidb_util::timeutil::TimeZone;

fn utc() -> TimeZone {
    TimeZone::Named(chrono_tz::Tz::UTC)
}

/// Go `TestIntervalPolicy`.
#[test]
fn test_interval_policy() {
    let watermark1 = GoTime::now();
    // Go parses "2021-11-21T11:21:31Z" with `time.RFC3339`, i.e. a UTC instant.
    let watermark2 = GoTime::date(2021, 11, 21, 11, 21, 31, 0, &utc());

    struct Case {
        expr: &'static str,
        err: bool,
        interval: i64,
    }

    let cases = [
        Case {
            expr: "6m",
            err: false,
            interval: 6 * MINUTE,
        },
        Case {
            expr: "7h",
            err: false,
            interval: 7 * HOUR,
        },
        Case {
            expr: "8d",
            err: false,
            interval: 8 * 24 * HOUR,
        },
        Case {
            expr: "11",
            err: true,
            interval: 0,
        },
    ];

    for case in cases {
        let policy = create_sched_event_policy(&SchedPolicyType::interval(), case.expr);
        if case.err {
            let err = policy.unwrap_err();
            assert!(err
                .to_string()
                .contains(&format!("invalid schedule event expr '{}'", case.expr)));
            continue;
        }

        let policy = policy.unwrap();
        assert!(matches!(policy, SchedPolicy::Interval(_)));
        let (tm, ok) = policy.next_event_time(&watermark1);
        assert!(ok);
        assert_eq!(tm, watermark1.add(case.interval));
        let (tm, ok) = policy.next_event_time(&watermark2);
        assert!(ok);
        assert_eq!(tm, watermark2.add(case.interval));
    }
}

/// Go `TestCronPolicy`.
#[test]
fn test_cron_policy() {
    let loc_e2 = TimeZone::Fixed {
        name: "UTC+1".to_string(),
        offset_secs: 2 * 60 * 60,
    };
    let loc_w2 = TimeZone::Fixed {
        name: "UTC-1".to_string(),
        offset_secs: -2 * 60 * 60,
    };
    let local = TimeZone::Local;

    struct Case {
        expr: &'static str,
        err: bool,
        tm: GoTime,
        next: GoTime,
    }

    let error_case = |expr: &'static str| Case {
        expr,
        err: true,
        tm: GoTime::zero(),
        next: GoTime::zero(),
    };

    let cases = [
        error_case(""),
        error_case("aaa"),
        error_case("61 1 * * *"),
        Case {
            expr: "@hourly",
            err: false,
            tm: GoTime::date(2021, 11, 21, 11, 21, 31, 0, &utc()),
            next: GoTime::date(2021, 11, 21, 12, 0, 0, 0, &utc()),
        },
        Case {
            expr: "@hourly",
            err: false,
            tm: GoTime::date(2021, 11, 21, 12, 0, 0, 0, &local),
            next: GoTime::date(2021, 11, 21, 13, 0, 0, 0, &local),
        },
        Case {
            expr: "@daily",
            err: false,
            tm: GoTime::date(2021, 11, 21, 11, 21, 31, 0, &local),
            next: GoTime::date(2021, 11, 22, 0, 0, 0, 0, &local),
        },
        Case {
            expr: "@weekly",
            err: false,
            // Friday
            tm: GoTime::date(2021, 11, 19, 11, 21, 31, 0, &loc_e2),
            // Sunday
            next: GoTime::date(2021, 11, 21, 0, 0, 0, 0, &loc_e2),
        },
        Case {
            expr: "@monthly",
            err: false,
            tm: GoTime::date(2021, 12, 19, 11, 21, 31, 0, &loc_w2),
            next: GoTime::date(2022, 1, 1, 0, 0, 0, 0, &loc_w2),
        },
        Case {
            expr: "@yearly",
            err: false,
            tm: GoTime::date(2021, 12, 19, 11, 21, 31, 0, &utc()),
            next: GoTime::date(2022, 1, 1, 0, 0, 0, 0, &utc()),
        },
        Case {
            expr: "12 12 * * *",
            err: false,
            tm: GoTime::date(2021, 12, 19, 11, 21, 31, 0, &local),
            next: GoTime::date(2021, 12, 19, 12, 12, 0, 0, &local),
        },
        Case {
            expr: "5 4 21 2 *",
            err: false,
            tm: GoTime::date(2021, 12, 19, 11, 21, 31, 0, &loc_e2),
            next: GoTime::date(2022, 2, 21, 4, 5, 0, 0, &loc_e2),
        },
        Case {
            expr: "55 16 * 12 0",
            err: false,
            tm: GoTime::date(2021, 12, 21, 11, 21, 31, 0, &loc_w2),
            next: GoTime::date(2021, 12, 26, 16, 55, 0, 0, &loc_w2),
        },
        Case {
            expr: "12 8,16,19 * * *",
            err: false,
            tm: GoTime::date(2021, 12, 21, 2, 21, 31, 0, &loc_w2),
            next: GoTime::date(2021, 12, 21, 8, 12, 0, 0, &loc_w2),
        },
        Case {
            expr: "12 8,16,19 * * *",
            err: false,
            tm: GoTime::date(2021, 12, 21, 9, 21, 31, 0, &loc_w2),
            next: GoTime::date(2021, 12, 21, 16, 12, 0, 0, &loc_w2),
        },
        Case {
            expr: "12 8,16,19 * * *",
            err: false,
            tm: GoTime::date(2021, 12, 21, 19, 21, 31, 0, &loc_w2),
            next: GoTime::date(2021, 12, 22, 8, 12, 0, 0, &loc_w2),
        },
        Case {
            expr: "12 8,16,19 * * *",
            err: false,
            tm: GoTime::date(2021, 12, 21, 16, 12, 0, 0, &local),
            next: GoTime::date(2021, 12, 21, 19, 12, 0, 0, &local),
        },
        Case {
            expr: "* * 29 2 *",
            err: false,
            tm: GoTime::date(2021, 12, 21, 16, 12, 0, 0, &local),
            next: GoTime::date(2024, 2, 29, 0, 0, 0, 0, &local),
        },
        Case {
            expr: "* * 30 2 *",
            err: false,
            tm: GoTime::date(2021, 12, 21, 16, 12, 0, 0, &local),
            next: GoTime::zero(),
        },
    ];

    for case in cases {
        let policy = create_sched_event_policy(&SchedPolicyType::cron(), case.expr);
        if case.err {
            let err = policy.unwrap_err();
            assert!(
                err.to_string()
                    .contains(&format!("invalid cron expr '{}'", case.expr)),
                "{err}"
            );
            continue;
        }

        let policy = policy.unwrap();
        assert!(matches!(policy, SchedPolicy::Cron(_)));
        let (next, ok) = policy.next_event_time(&case.tm);
        assert_eq!(next, case.next, "expr {}", case.expr);
        assert_eq!(!next.is_zero(), ok);
    }
}
