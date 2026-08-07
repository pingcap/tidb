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

//! Go-oracle boundaries for `pkg/meta/model/resource_group.go`.

use tidb_ast::CiString;
use tidb_model::serde_helpers::to_go_json;
use tidb_model::{
    ResourceGroupBackgroundSettings, ResourceGroupInfo, ResourceGroupRunawayAction,
    ResourceGroupRunawaySettings, ResourceGroupRunawayWatch, ResourceGroupSettings,
    ResourceGroupShared, SchemaState,
};

fn go_json<T: serde::Serialize>(value: &T) -> String {
    String::from_utf8(to_go_json(value).expect("Go-compatible JSON encoding")).unwrap()
}

#[test]
fn zero_settings_json_matches_go() {
    assert_eq!(
        go_json(&ResourceGroupSettings::default()),
        r#"{"ru_per_sec":0,"priority":0,"cpu_limit":"","io_read_bandwidth":"","io_write_bandwidth":"","burst_limit":0,"runaway":null,"background":null}"#
    );
    assert_eq!(
        go_json(&ResourceGroupSettings::new()),
        r#"{"ru_per_sec":0,"priority":8,"cpu_limit":"","io_read_bandwidth":"","io_write_bandwidth":"","burst_limit":0,"runaway":null,"background":null}"#
    );
}

#[test]
fn info_nil_embedding_and_full_json_match_go() {
    let zero = ResourceGroupInfo::default();
    assert_eq!(
        go_json(&zero),
        r#"{"id":0,"name":{"O":"","L":""},"state":0}"#
    );
    assert_eq!(
        serde_json::to_string(&Option::<ResourceGroupInfo>::None).unwrap(),
        "null"
    );

    let settings_zero = ResourceGroupInfo {
        settings: Some(Box::new(ResourceGroupSettings::default())),
        ..ResourceGroupInfo::default()
    };
    assert_eq!(
        go_json(&settings_zero),
        r#"{"ru_per_sec":0,"priority":0,"cpu_limit":"","io_read_bandwidth":"","io_write_bandwidth":"","burst_limit":0,"runaway":null,"background":null,"id":0,"name":{"O":"","L":""},"state":0}"#
    );

    let full = ResourceGroupInfo {
        settings: Some(Box::new(ResourceGroupSettings {
            ru_rate: u64::MAX,
            priority: u64::MAX,
            cpu_limiter: "x<y&\u{2028}".to_owned(),
            io_read_bandwidth: "read".to_owned(),
            io_write_bandwidth: "write".to_owned(),
            burst_limit: i64::MIN,
            runaway: Some(ResourceGroupShared::new(ResourceGroupRunawaySettings {
                exec_elapsed_time_ms: u64::MAX,
                processed_keys: i64::MIN,
                request_unit: i64::MAX,
                action: ResourceGroupRunawayAction(i32::MAX),
                switch_group_name: "NeXt".to_owned(),
                watch_type: ResourceGroupRunawayWatch(i32::MIN),
                watch_duration_ms: -7,
            })),
            background: Some(ResourceGroupShared::new(ResourceGroupBackgroundSettings {
                job_types: Some(vec!["stats".to_owned(), "x<y".to_owned(), String::new()]),
                resource_util_limit: u64::MAX,
            })),
        })),
        id: i64::MIN,
        name: CiString::new("MiXeD"),
        state: SchemaState(255),
    };
    const GO_FULL: &str = r#"{"ru_per_sec":18446744073709551615,"priority":18446744073709551615,"cpu_limit":"x\u003cy\u0026\u2028","io_read_bandwidth":"read","io_write_bandwidth":"write","burst_limit":-9223372036854775808,"runaway":{"exec_elapsed_time_ms":18446744073709551615,"processed_keys":-9223372036854775808,"request_unit":9223372036854775807,"action":2147483647,"switch_group_name":"NeXt","watch_type":-2147483648,"watch_duration_ms":-7},"background":{"job_types":["stats","x\u003cy",""],"utilization_limit":18446744073709551615},"id":-9223372036854775808,"name":{"O":"MiXeD","L":"mixed"},"state":255}"#;
    assert_eq!(go_json(&full), GO_FULL);
    let decoded: ResourceGroupInfo = serde_json::from_str(GO_FULL).unwrap();
    assert_eq!(decoded, full);
}

#[test]
fn decode_matches_go_embedding_null_case_duplicate_and_ci_string_rules() {
    let empty: ResourceGroupInfo = serde_json::from_str("{}").unwrap();
    assert!(empty.settings.is_none());

    let null_field: ResourceGroupInfo = serde_json::from_str(r#"{"ru_per_sec":null}"#).unwrap();
    assert_eq!(null_field.settings.unwrap().ru_rate, 0);

    let ignored_embedded_name: ResourceGroupInfo =
        serde_json::from_str(r#"{"ResourceGroupSettings":{"ru_per_sec":9},"id":1}"#).unwrap();
    assert!(ignored_embedded_name.settings.is_none());
    assert_eq!(ignored_embedded_name.id, 1);

    let duplicate: ResourceGroupInfo = serde_json::from_str(
        r#"{"RU_PER_SEC":1,"ru_per_sec":2,"PRIORITY":9,"priority":null,"ID":3,"id":4,"NAME":{"o":"A","O":"B","l":"a","L":null},"name":null,"STATE":5,"state":null}"#,
    )
    .unwrap();
    let settings = duplicate.settings.unwrap();
    assert_eq!(settings.ru_rate, 2);
    assert_eq!(settings.priority, 9);
    assert_eq!(duplicate.id, 4);
    assert_eq!(duplicate.name.original(), "B");
    assert_eq!(duplicate.name.lowercase(), "a");
    assert_eq!(duplicate.state, SchemaState::PUBLIC);

    let lowercase_only_name: ResourceGroupInfo =
        serde_json::from_str(r#"{"name":{"o":"Only","l":"folded"}}"#).unwrap();
    assert_eq!(lowercase_only_name.name.original(), "Only");
    assert_eq!(lowercase_only_name.name.lowercase(), "folded");

    let nested: ResourceGroupInfo = serde_json::from_str(
        r#"{"runaway":{"ACTION":1,"action":2,"WATCH_TYPE":1,"watch_type":null},"background":{"JOB_TYPES":["a"],"job_types":null,"UTILIZATION_LIMIT":1,"utilization_limit":2}}"#,
    )
    .unwrap();
    let settings = nested.settings.unwrap();
    let runaway = settings.runaway.unwrap();
    assert_eq!(runaway.read().action, ResourceGroupRunawayAction::COOLDOWN);
    assert_eq!(runaway.read().watch_type, ResourceGroupRunawayWatch::EXACT);
    let background = settings.background.unwrap();
    assert_eq!(background.read().job_types, None);
    assert_eq!(background.read().resource_util_limit, 2);

    let pointer_duplicate: ResourceGroupSettings = serde_json::from_str(
        r#"{"runaway":{"action":1},"runaway":null,"background":null,"background":{"job_types":[]}}"#,
    )
    .unwrap();
    assert!(pointer_duplicate.runaway.is_none());
    assert_eq!(
        pointer_duplicate.background.unwrap().read().job_types,
        Some(Vec::new())
    );
}

#[test]
fn nil_and_allocated_empty_job_slices_remain_distinct() {
    assert_eq!(
        go_json(&ResourceGroupBackgroundSettings::default()),
        r#"{"job_types":null,"utilization_limit":0}"#
    );
    assert_eq!(
        go_json(&ResourceGroupBackgroundSettings {
            job_types: Some(Vec::new()),
            resource_util_limit: 0,
        }),
        r#"{"job_types":[],"utilization_limit":0}"#
    );
}

#[test]
fn settings_string_pins_every_source_branch_boundary() {
    let cases = [
        (ResourceGroupSettings::default(), "PRIORITY=MEDIUM"),
        (
            ResourceGroupSettings {
                ru_rate: 1,
                priority: 1,
                cpu_limiter: "a\"b".to_owned(),
                io_read_bandwidth: "r".to_owned(),
                io_write_bandwidth: "w".to_owned(),
                burst_limit: -2,
                ..ResourceGroupSettings::default()
            },
            r#"RU_PER_SEC=1, PRIORITY=LOW, CPU="a\"b", IO_READ_BANDWIDTH="r", IO_WRITE_BANDWIDTH="w", BURSTABLE(MODERATED)"#,
        ),
        (
            ResourceGroupSettings {
                priority: 16,
                burst_limit: -1,
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=HIGH, BURSTABLE(UNLIMITED)",
        ),
        (
            ResourceGroupSettings {
                priority: 999,
                burst_limit: -3,
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                runaway: Some(ResourceGroupRunawaySettings::default().into()),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, QUERY_LIMIT=( ACTION=DRYRUN)",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                runaway: Some(
                    ResourceGroupRunawaySettings {
                        exec_elapsed_time_ms: 1,
                        processed_keys: 2,
                        request_unit: 3,
                        action: ResourceGroupRunawayAction::SWITCH_GROUP,
                        switch_group_name: "g\"x".to_owned(),
                        watch_type: ResourceGroupRunawayWatch::EXACT,
                        watch_duration_ms: 4,
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            r#"PRIORITY=MEDIUM, QUERY_LIMIT=(EXEC_ELAPSED="1ms" PROCESSED_KEYS=2 RU=3 ACTION=SWITCH_GROUP(g"x) WATCH=EXACT DURATION="4ms")"#,
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                runaway: Some(
                    ResourceGroupRunawaySettings {
                        exec_elapsed_time_ms: u64::MAX,
                        action: ResourceGroupRunawayAction(i32::MAX),
                        watch_type: ResourceGroupRunawayWatch(i32::MAX),
                        watch_duration_ms: i64::MAX,
                        ..ResourceGroupRunawaySettings::default()
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            r#"PRIORITY=MEDIUM, QUERY_LIMIT=(EXEC_ELAPSED="-1ms" ACTION=DRYRUN WATCH=NONE DURATION="-1ms")"#,
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                runaway: Some(
                    ResourceGroupRunawaySettings {
                        action: ResourceGroupRunawayAction::KILL,
                        watch_type: ResourceGroupRunawayWatch::EXACT,
                        watch_duration_ms: 0,
                        ..ResourceGroupRunawaySettings::default()
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, QUERY_LIMIT=( ACTION=KILL WATCH=EXACT DURATION=UNLIMITED)",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                runaway: Some(
                    ResourceGroupRunawaySettings {
                        processed_keys: 2,
                        ..ResourceGroupRunawaySettings::default()
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, QUERY_LIMIT=(PROCESSED_KEYS=2 ACTION=DRYRUN)",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                runaway: Some(
                    ResourceGroupRunawaySettings {
                        request_unit: 3,
                        ..ResourceGroupRunawaySettings::default()
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, QUERY_LIMIT=(RU=3 ACTION=DRYRUN)",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                background: Some(ResourceGroupBackgroundSettings::default().into()),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, BACKGROUND=()",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                background: Some(
                    ResourceGroupBackgroundSettings {
                        job_types: Some(vec!["a".to_owned(), "b'c".to_owned()]),
                        resource_util_limit: 0,
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, BACKGROUND=(TASK_TYPES='a,b'c')",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                background: Some(
                    ResourceGroupBackgroundSettings {
                        job_types: None,
                        resource_util_limit: 7,
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, BACKGROUND=(UTILIZATION_LIMIT=7)",
        ),
        (
            ResourceGroupSettings {
                priority: 8,
                background: Some(
                    ResourceGroupBackgroundSettings {
                        job_types: Some(vec!["a".to_owned()]),
                        resource_util_limit: 7,
                    }
                    .into(),
                ),
                ..ResourceGroupSettings::default()
            },
            "PRIORITY=MEDIUM, BACKGROUND=(TASK_TYPES='a', UTILIZATION_LIMIT=7)",
        ),
    ];
    for (settings, expected) in cases {
        assert_eq!(settings.to_string(), expected);
    }
}

#[test]
fn numeric_json_rejects_values_outside_go_underlying_widths() {
    assert!(serde_json::from_str::<ResourceGroupInfo>(r#"{"state":256}"#).is_err());
    assert!(
        serde_json::from_str::<ResourceGroupSettings>(r#"{"runaway":{"action":2147483648}}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<ResourceGroupSettings>(
        r#"{"runaway":{"watch_type":-2147483649}}"#
    )
    .is_err());
}
