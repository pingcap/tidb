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

use tidb_codec::decode_bytes;
use tidb_model::PlacementSettings;
use tidb_tablecodec::table_key::gen_table_prefix;

use super::{
    encode_bytes_owned, get_range_start_and_end_key_hex, hex_encode, new_bundle,
    new_bundle_from_constraints_options, new_bundle_from_options, new_bundle_from_options_untidied,
    new_bundle_from_sugar_options, set_mock_marshal_failure, Bundle,
};
use crate::common::{
    group_id, ENGINE_LABEL_KEY, ENGINE_LABEL_TIFLASH, META_PREFIX, RULE_INDEX_TABLE,
    TIDB_BUNDLE_RANGE_PREFIX_FOR_META,
};
use crate::constraint::new_constraint_direct;
use crate::constraints::new_constraints_direct;
use crate::errors::PlacementErrorKind;
use crate::pd::{LabelConstraint, LabelConstraintOp, PeerRoleType, Rule};
use crate::rule::tests::match_rules;
use crate::rule::{new_rule, new_rules};

/// Go `hex.DecodeString`, for the range-key round trip.
fn hex_decode(encoded: &str) -> Vec<u8> {
    assert!(encoded.len().is_multiple_of(2), "odd-length hex string");
    (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).expect("valid hex digits"))
        .collect()
}

fn rule_with_id(id: &str) -> Rule {
    Rule {
        id: id.to_owned(),
        ..Rule::default()
    }
}

/// Go `TestEmpty` (`bundle_test.go`).
#[test]
fn test_empty() {
    let bundle = Bundle {
        id: group_id(1),
        ..Bundle::default()
    };
    assert!(bundle.is_empty());

    let bundle = Bundle {
        id: group_id(1),
        index: 1,
        ..Bundle::default()
    };
    assert!(!bundle.is_empty());

    let bundle = Bundle {
        id: group_id(1),
        r#override: true,
        ..Bundle::default()
    };
    assert!(!bundle.is_empty());

    let bundle = Bundle {
        id: group_id(1),
        rules: vec![rule_with_id("434")],
        ..Bundle::default()
    };
    assert!(!bundle.is_empty());

    let bundle = Bundle {
        id: group_id(1),
        index: 1,
        r#override: true,
        ..Bundle::default()
    };
    assert!(!bundle.is_empty());
}

/// Go `TestCloneBundle` (`bundle_test.go`).
#[test]
fn test_clone_bundle() {
    let bundle = Bundle {
        id: group_id(1),
        rules: vec![rule_with_id("434")],
        ..Bundle::default()
    };

    let mut new_bundle_value = bundle.clone_bundle();
    new_bundle_value.id = group_id(2);
    new_bundle_value.rules[0] = rule_with_id("121");

    assert_eq!(
        Bundle {
            id: group_id(1),
            rules: vec![rule_with_id("434")],
            ..Bundle::default()
        },
        bundle
    );
    assert_eq!(
        Bundle {
            id: group_id(2),
            rules: vec![rule_with_id("121")],
            ..Bundle::default()
        },
        new_bundle_value
    );
}

/// Go `TestObjectID` (`bundle_test.go`).
#[test]
fn test_object_id() {
    struct TestCase {
        name: &'static str,
        bundle_id: &'static str,
        expected_id: i64,
        err: Option<PlacementErrorKind>,
    }
    let tests = [
        TestCase {
            name: "non tidb bundle",
            bundle_id: "pd",
            expected_id: 0,
            err: Some(PlacementErrorKind::InvalidBundleIdFormat),
        },
        TestCase {
            name: "id of words",
            bundle_id: "TiDB_DDL_foo",
            expected_id: 0,
            err: Some(PlacementErrorKind::InvalidBundleId),
        },
        TestCase {
            name: "id of words and nums",
            bundle_id: "TiDB_DDL_3x",
            expected_id: 0,
            err: Some(PlacementErrorKind::InvalidBundleId),
        },
        TestCase {
            name: "id of floats",
            bundle_id: "TiDB_DDL_3.0",
            expected_id: 0,
            err: Some(PlacementErrorKind::InvalidBundleId),
        },
        TestCase {
            name: "id of negatives",
            bundle_id: "TiDB_DDL_-10",
            expected_id: 0,
            err: Some(PlacementErrorKind::InvalidBundleId),
        },
        TestCase {
            name: "id of positive integer",
            bundle_id: "TiDB_DDL_10",
            expected_id: 10,
            err: None,
        },
    ];
    for test in tests {
        let bundle = Bundle {
            id: test.bundle_id.to_owned(),
            ..Bundle::default()
        };
        let result = bundle.object_id();
        match test.err {
            None => assert_eq!(test.expected_id, result.expect(test.name), "{}", test.name),
            Some(kind) => assert!(
                result.as_ref().is_err_and(|err| err.is(kind)),
                "{}: {result:?}",
                test.name
            ),
        }
    }
}

/// Go `TestGetLeaderDCByBundle` (`bundle_test.go`).
#[test]
fn test_get_leader_dc_by_bundle() {
    let zone_rule = |id: &str,
                     role: PeerRoleType,
                     op: LabelConstraintOp,
                     key: &str,
                     values: &[&str],
                     count: i64| Rule {
        id: id.to_owned(),
        role,
        label_constraints: vec![new_constraint_direct(key, op, values)],
        count,
        ..Rule::default()
    };

    let testcases: Vec<(&str, Bundle, &str)> = vec![
        (
            "only leader",
            Bundle {
                id: group_id(1),
                rules: vec![zone_rule(
                    "12",
                    PeerRoleType::LEADER,
                    LabelConstraintOp::IN,
                    "zone",
                    &["bj"],
                    1,
                )],
                ..Bundle::default()
            },
            "bj",
        ),
        (
            "no leader",
            Bundle {
                id: group_id(1),
                rules: vec![zone_rule(
                    "12",
                    PeerRoleType::VOTER,
                    LabelConstraintOp::IN,
                    "zone",
                    &["bj"],
                    3,
                )],
                ..Bundle::default()
            },
            "",
        ),
        (
            "voter and leader",
            Bundle {
                id: group_id(1),
                rules: vec![
                    zone_rule(
                        "11",
                        PeerRoleType::LEADER,
                        LabelConstraintOp::IN,
                        "zone",
                        &["sh"],
                        1,
                    ),
                    zone_rule(
                        "12",
                        PeerRoleType::VOTER,
                        LabelConstraintOp::IN,
                        "zone",
                        &["bj"],
                        3,
                    ),
                ],
                ..Bundle::default()
            },
            "sh",
        ),
        (
            "wrong label key",
            Bundle {
                id: group_id(1),
                rules: vec![zone_rule(
                    "11",
                    PeerRoleType::LEADER,
                    LabelConstraintOp::IN,
                    "fake",
                    &["sh"],
                    1,
                )],
                ..Bundle::default()
            },
            "",
        ),
        (
            "wrong operator",
            Bundle {
                id: group_id(1),
                rules: vec![zone_rule(
                    "11",
                    PeerRoleType::LEADER,
                    LabelConstraintOp::NOT_IN,
                    "zone",
                    &["sh"],
                    1,
                )],
                ..Bundle::default()
            },
            "",
        ),
        (
            "leader have multi values",
            Bundle {
                id: group_id(1),
                rules: vec![zone_rule(
                    "11",
                    PeerRoleType::LEADER,
                    LabelConstraintOp::IN,
                    "zone",
                    &["sh", "bj"],
                    1,
                )],
                ..Bundle::default()
            },
            "",
        ),
        (
            "irrelvant rules",
            Bundle {
                id: group_id(1),
                rules: vec![
                    zone_rule(
                        "15",
                        PeerRoleType::LEADER,
                        LabelConstraintOp::NOT_IN,
                        ENGINE_LABEL_KEY,
                        &[ENGINE_LABEL_TIFLASH],
                        1,
                    ),
                    zone_rule(
                        "14",
                        PeerRoleType::LEADER,
                        LabelConstraintOp::NOT_IN,
                        "disk",
                        &["ssd", "hdd"],
                        1,
                    ),
                    zone_rule(
                        "13",
                        PeerRoleType::LEADER,
                        LabelConstraintOp::IN,
                        "zone",
                        &["bj"],
                        1,
                    ),
                ],
                ..Bundle::default()
            },
            "bj",
        ),
        (
            "multi leaders 1",
            Bundle {
                id: group_id(1),
                rules: vec![zone_rule(
                    "16",
                    PeerRoleType::LEADER,
                    LabelConstraintOp::IN,
                    "zone",
                    &["sh"],
                    2,
                )],
                ..Bundle::default()
            },
            "",
        ),
        (
            "multi leaders 2",
            Bundle {
                id: group_id(1),
                rules: vec![
                    zone_rule(
                        "17",
                        PeerRoleType::LEADER,
                        LabelConstraintOp::IN,
                        "zone",
                        &["sh"],
                        1,
                    ),
                    zone_rule(
                        "18",
                        PeerRoleType::LEADER,
                        LabelConstraintOp::IN,
                        "zone",
                        &["bj"],
                        1,
                    ),
                ],
                ..Bundle::default()
            },
            "sh",
        ),
    ];

    for (name, bundle, expected_dc) in testcases {
        let result = bundle.get_leader_dc("zone");
        if expected_dc.is_empty() {
            assert!(result.is_none(), "{name}");
        } else {
            assert!(result.is_some(), "{name}");
        }
        assert_eq!(expected_dc, result.unwrap_or_default(), "{name}");
    }
}

/// Go `TestString` (`bundle_test.go`).
#[test]
fn test_string() {
    let mut bundle = Bundle {
        id: group_id(1),
        ..Bundle::default()
    };

    let rules1 = new_rules(&PeerRoleType::VOTER, 3, r#"["+zone=sh", "+zone=sh"]"#)
        .expect("valid constraints");
    let rules2 = new_rules(&PeerRoleType::VOTER, 4, r#"["-zone=sh", "+zone=bj"]"#)
        .expect("valid constraints");
    let rules3 = new_rules(
        &PeerRoleType::VOTER,
        3,
        r#"["-engine=tiflash", "-engine=tiflash_compute"]"#,
    )
    .expect("valid constraints");
    bundle.rules = rules1.into_iter().chain(rules2).chain(rules3).collect();

    assert_eq!(
        "{\"group_id\":\"TiDB_DDL_1\",\"group_index\":0,\"group_override\":false,\"rules\":[{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"is_witness\":false,\"count\":3,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"sh\"]}]},{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"is_witness\":false,\"count\":4,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"notIn\",\"values\":[\"sh\"]},{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"bj\"]}]},{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"is_witness\":false,\"count\":3,\"label_constraints\":[{\"key\":\"engine\",\"op\":\"notIn\",\"values\":[\"tiflash\"]},{\"key\":\"engine\",\"op\":\"notIn\",\"values\":[\"tiflash_compute\"]}]}]}",
        bundle.to_string()
    );

    set_mock_marshal_failure(true);
    assert_eq!("", bundle.to_string());
    set_mock_marshal_failure(false);
}

/// Go `TestNewBundle` (`bundle_test.go`).
#[test]
fn test_new_bundle() {
    assert_eq!(
        Bundle {
            id: group_id(3),
            ..Bundle::default()
        },
        new_bundle(3)
    );
    assert_eq!(
        Bundle {
            id: group_id(-1),
            ..Bundle::default()
        },
        new_bundle(-1)
    );
    new_bundle_from_constraints_options(None).expect_err("nil options");
    new_bundle_from_sugar_options(None).expect_err("nil options");
    new_bundle_from_options(None).expect_err("nil options");
}

/// Go `TestNewBundleFromOptions` (`bundle_test.go`).
#[test]
fn test_new_bundle_from_options() {
    struct TestCase {
        name: &'static str,
        input: Option<PlacementSettings>,
        output: Vec<Rule>,
        err: Option<PlacementErrorKind>,
    }
    let region = |value: &str| {
        new_constraints_direct(vec![new_constraint_direct(
            "region",
            LabelConstraintOp::IN,
            &[value],
        )])
    };
    let mut tests = vec![TestCase {
        name: "empty 1",
        input: Some(PlacementSettings::default()),
        output: vec![new_rule(
            PeerRoleType::VOTER,
            3,
            new_constraints_direct(vec![]),
        )],
        err: None,
    }];

    tests.push(TestCase {
        name: "empty 2",
        input: None,
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidPlacementOptions),
    });

    tests.push(TestCase {
        name: "empty 3",
        input: Some(PlacementSettings {
            learner_constraints: "[+region=us]".to_owned(),
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsReplicas),
    });

    tests.push(TestCase {
        name: "sugar syntax: normal case 1",
        input: Some(PlacementSettings {
            primary_region: "us".to_owned(),
            regions: "us".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 2, region("us")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "sugar syntax: normal case 2",
        input: Some(PlacementSettings {
            primary_region: "us".to_owned(),
            regions: "us".to_owned(),
            schedule: "majority_in_primary".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 1, new_constraints_direct(vec![])),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "sugar syntax: few followers",
        input: Some(PlacementSettings {
            primary_region: "us".to_owned(),
            regions: "bj,sh,us".to_owned(),
            followers: 1,
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(
                PeerRoleType::VOTER,
                1,
                new_constraints_direct(vec![new_constraint_direct(
                    "region",
                    LabelConstraintOp::IN,
                    &["bj", "sh"],
                )]),
            ),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "sugar syntax: omit regions 1",
        input: Some(PlacementSettings {
            followers: 2,
            schedule: "even".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![new_rule(
            PeerRoleType::VOTER,
            3,
            new_constraints_direct(vec![]),
        )],
        err: None,
    });

    tests.push(TestCase {
        name: "sugar syntax: omit regions 2",
        input: Some(PlacementSettings {
            followers: 2,
            schedule: "majority_in_primary".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![new_rule(
            PeerRoleType::VOTER,
            3,
            new_constraints_direct(vec![]),
        )],
        err: None,
    });

    tests.push(TestCase {
        name: "sugar syntax: wrong schedule prop",
        input: Some(PlacementSettings {
            primary_region: "us".to_owned(),
            regions: "us".to_owned(),
            schedule: "wrong".to_owned(),
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidPlacementOptions),
    });

    tests.push(TestCase {
        name: "sugar syntax: invalid region name 1",
        input: Some(PlacementSettings {
            primary_region: ",=,".to_owned(),
            regions: ",=,".to_owned(),
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidPlacementOptions),
    });

    tests.push(TestCase {
        name: "sugar syntax: invalid region name 2",
        input: Some(PlacementSettings {
            primary_region: "f".to_owned(),
            regions: ",=".to_owned(),
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidPlacementOptions),
    });

    tests.push(TestCase {
        name: "sugar syntax: invalid region name 4",
        input: Some(PlacementSettings {
            primary_region: String::new(),
            regions: "g".to_owned(),
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidPlacementOptions),
    });

    tests.push(TestCase {
        name: "sugar syntax: normal case 2",
        input: Some(PlacementSettings {
            primary_region: "us".to_owned(),
            regions: "sh,us".to_owned(),
            followers: 5,
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 2, region("us")),
            new_rule(PeerRoleType::VOTER, 3, region("sh")),
        ],
        err: None,
    });
    // Go appends a copy of the previous case and mutates its shared
    // options pointer, which also sets SCHEDULE on the case just above.
    tests.push(TestCase {
        name: "sugar syntax: explicit schedule",
        input: Some(PlacementSettings {
            primary_region: "us".to_owned(),
            regions: "sh,us".to_owned(),
            followers: 5,
            schedule: "even".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 2, region("us")),
            new_rule(PeerRoleType::VOTER, 3, region("sh")),
        ],
        err: None,
    });
    let previous = tests.len() - 2;
    if let Some(settings) = tests[previous].input.as_mut() {
        settings.schedule = "even".to_owned();
    }

    tests.push(TestCase {
        name: "sugar syntax: majority schedule",
        input: Some(PlacementSettings {
            primary_region: "sh".to_owned(),
            regions: "bj,sh".to_owned(),
            followers: 4,
            schedule: "majority_in_primary".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("sh")),
            new_rule(PeerRoleType::VOTER, 2, region("sh")),
            new_rule(PeerRoleType::VOTER, 2, region("bj")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: normal case 1",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 2, region("us")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: normal case 3",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            followers: 2,
            learners: 2,
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("us")),
            new_rule(PeerRoleType::VOTER, 2, region("us")),
            new_rule(PeerRoleType::LEARNER, 2, region("us")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: only leader constraints",
        input: Some(PlacementSettings {
            leader_constraints: "[+region=as]".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("as")),
            new_rule(PeerRoleType::VOTER, 2, new_constraints_direct(vec![])),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: only leader constraints",
        input: Some(PlacementSettings {
            leader_constraints: "[+region=as]".to_owned(),
            followers: 4,
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("as")),
            new_rule(PeerRoleType::VOTER, 4, new_constraints_direct(vec![])),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: leader and follower constraints",
        input: Some(PlacementSettings {
            leader_constraints: "[+region=as]".to_owned(),
            follower_constraints: r#"{"+region=us": 2}"#.to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("as")),
            new_rule(PeerRoleType::VOTER, 2, region("us")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: lack count 1",
        input: Some(PlacementSettings {
            leader_constraints: "[+region=as]".to_owned(),
            follower_constraints: "[-region=us]".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, region("as")),
            new_rule(
                PeerRoleType::VOTER,
                2,
                new_constraints_direct(vec![new_constraint_direct(
                    "region",
                    LabelConstraintOp::NOT_IN,
                    &["us"],
                )]),
            ),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: lack count 2",
        input: Some(PlacementSettings {
            leader_constraints: "[+region=as]".to_owned(),
            learner_constraints: "[-region=us]".to_owned(),
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsReplicas),
    });

    tests.push(TestCase {
        name: "direct syntax: omit leader",
        input: Some(PlacementSettings {
            followers: 2,
            follower_constraints: "[+region=bj]".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, new_constraints_direct(vec![])),
            new_rule(PeerRoleType::VOTER, 2, region("bj")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: conflicts 1",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            leader_constraints: "[-region=us]".to_owned(),
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::ConflictingConstraints),
    });

    tests.push(TestCase {
        name: "direct syntax: conflicts 3",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            follower_constraints: "[-region=us]".to_owned(),
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::ConflictingConstraints),
    });

    tests.push(TestCase {
        name: "direct syntax: conflicts 4",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            learner_constraints: "[-region=us]".to_owned(),
            followers: 2,
            learners: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::ConflictingConstraints),
    });

    tests.push(TestCase {
        name: "direct syntax: invalid format 1",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            leader_constraints: "-region=us]".to_owned(),
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsFormat),
    });

    tests.push(TestCase {
        name: "direct syntax: invalid format 2",
        input: Some(PlacementSettings {
            constraints: "+region=us]".to_owned(),
            leader_constraints: "[-region=us]".to_owned(),
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsFormat),
    });

    tests.push(TestCase {
        name: "direct syntax: invalid format 4",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            follower_constraints: "-region=us]".to_owned(),
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsFormat),
    });

    tests.push(TestCase {
        name: "direct syntax: invalid format 5",
        input: Some(PlacementSettings {
            constraints: "[+region=us]".to_owned(),
            leader_constraints: "-region=us]".to_owned(),
            learners: 2,
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsFormat),
    });

    tests.push(TestCase {
        name: "direct syntax: follower dict constraints",
        input: Some(PlacementSettings {
            follower_constraints: "{+disk=ssd: 1}".to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, new_constraints_direct(vec![])),
            new_rule(
                PeerRoleType::VOTER,
                1,
                new_constraints_direct(vec![new_constraint_direct(
                    "disk",
                    LabelConstraintOp::IN,
                    &["ssd"],
                )]),
            ),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: invalid follower dict constraints",
        input: Some(PlacementSettings {
            follower_constraints: "{+disk=ssd: 1}".to_owned(),
            followers: 2,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsReplicas),
    });

    tests.push(TestCase {
        name: "direct syntax: learner dict constraints",
        input: Some(PlacementSettings {
            learner_constraints: r#"{"+region=us": 2}"#.to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::LEADER, 1, new_constraints_direct(vec![])),
            new_rule(PeerRoleType::VOTER, 2, new_constraints_direct(vec![])),
            new_rule(PeerRoleType::LEARNER, 2, region("us")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: learner dict constraints, with count",
        input: Some(PlacementSettings {
            learner_constraints: r#"{"+region=us": 2}"#.to_owned(),
            learners: 4,
            ..PlacementSettings::default()
        }),
        output: Vec::new(),
        err: Some(PlacementErrorKind::InvalidConstraintsReplicas),
    });

    tests.push(TestCase {
        name: "direct syntax: dict constraints",
        input: Some(PlacementSettings {
            constraints: r#"{"+region=us": 3}"#.to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![new_rule(PeerRoleType::VOTER, 3, region("us"))],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: dict constraints, 2:2:1",
        input: Some(PlacementSettings {
            constraints:
                r#"{ "+region=us-east-1":2, "+region=us-east-2": 2, "+region=us-west-1": 1}"#
                    .to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::VOTER, 2, region("us-east-1")),
            new_rule(PeerRoleType::VOTER, 2, region("us-east-2")),
            new_rule(PeerRoleType::VOTER, 1, region("us-west-1")),
        ],
        err: None,
    });

    tests.push(TestCase {
        name: "direct syntax: dict constraints",
        input: Some(PlacementSettings {
            constraints: r#"{"+region=us-east": 3}"#.to_owned(),
            learner_constraints: r#"{"+region=us-west": 1}"#.to_owned(),
            ..PlacementSettings::default()
        }),
        output: vec![
            new_rule(PeerRoleType::VOTER, 3, region("us-east")),
            new_rule(PeerRoleType::LEARNER, 1, region("us-west")),
        ],
        err: None,
    });

    for test in tests {
        let result = new_bundle_from_options_untidied(test.input.as_ref());
        let comment = format!("[{}]", test.name);
        match test.err {
            Some(kind) => assert!(
                result.as_ref().is_err_and(|err| err.is(kind)),
                "{comment}\nerr1 {result:?}\nerr2 {}",
                kind.text()
            ),
            None => {
                let bundle = result.unwrap_or_else(|err| panic!("{comment}: {err}"));
                match_rules(&test.output, &bundle.rules, &comment);
            }
        }
    }
}

/// Go `TestResetBundleWithSingleRule` (`bundle_test.go`).
#[test]
fn test_reset_bundle_with_single_rule() {
    let mut bundle = Bundle {
        id: group_id(1),
        ..Bundle::default()
    };

    bundle.rules = new_rules(&PeerRoleType::VOTER, 3, r#"["+zone=sh", "+zone=sh"]"#)
        .expect("valid constraints");

    bundle.reset(RULE_INDEX_TABLE, &[3]);
    assert_eq!(group_id(3), bundle.id);
    assert!(bundle.r#override);
    assert_eq!(RULE_INDEX_TABLE, bundle.index);
    assert_eq!(1, bundle.rules.len());
    assert_eq!(bundle.id, bundle.rules[0].group_id);

    let start_key = hex_encode(&encode_bytes_owned(&gen_table_prefix(3)));
    assert_eq!(start_key, bundle.rules[0].start_key_hex);

    let end_key = hex_encode(&encode_bytes_owned(&gen_table_prefix(4)));
    assert_eq!(end_key, bundle.rules[0].end_key_hex);
}

/// Go `TestResetBundleWithMultiRules` (`bundle_test.go`).
#[test]
fn test_reset_bundle_with_multi_rules() {
    // Build a bundle with three rules.
    let mut bundle = new_bundle_from_options(Some(&PlacementSettings {
        leader_constraints: r#"["+zone=bj"]"#.to_owned(),
        followers: 2,
        follower_constraints: r#"["+zone=hz"]"#.to_owned(),
        learners: 1,
        learner_constraints: r#"["+zone=cd"]"#.to_owned(),
        constraints: r#"["+disk=ssd"]"#.to_owned(),
        ..PlacementSettings::default()
    }))
    .expect("valid options");
    assert_eq!(3, bundle.rules.len());

    let key = |id: i64| hex_encode(&encode_bytes_owned(&gen_table_prefix(id)));

    // Test if all the three rules are basic rules even if the start key is
    // not set.
    bundle.reset(RULE_INDEX_TABLE, &[1, 2, 3]);
    assert_eq!(group_id(1), bundle.id);
    assert_eq!(RULE_INDEX_TABLE, bundle.index);
    assert!(bundle.r#override);
    assert_eq!(3 * 3, bundle.rules.len());
    for (offset, id) in [1_i64, 2, 3].into_iter().enumerate() {
        for position in 0..3 {
            let rule = &bundle.rules[offset * 3 + position];
            assert_eq!(key(id), rule.start_key_hex);
            assert_eq!(key(id + 1), rule.end_key_hex);
        }
    }

    // Test if the bundle has redundant rules.
    // For now, the bundle has 9 rules, each table id or partition id has
    // three of them. Once we reset this bundle for other ids, for example
    // adding partitions, we should extend the basic rules (3 of them) to
    // the new partition id.
    bundle.reset(RULE_INDEX_TABLE, &[1, 3, 4, 5]);
    assert_eq!(group_id(1), bundle.id);
    assert_eq!(RULE_INDEX_TABLE, bundle.index);
    assert!(bundle.r#override);
    assert_eq!(3 * 4, bundle.rules.len());
    for (offset, id) in [1_i64, 3, 4, 5].into_iter().enumerate() {
        for position in 0..3 {
            let rule = &bundle.rules[offset * 3 + position];
            assert_eq!(key(id), rule.start_key_hex);
            assert_eq!(key(id + 1), rule.end_key_hex);
        }
    }
}

/// Go `TestTidy` (`bundle_test.go`).
#[test]
fn test_tidy() {
    let mut bundle = Bundle {
        id: group_id(1),
        ..Bundle::default()
    };

    let mut rules0 = new_rules(&PeerRoleType::VOTER, 1, r#"["+zone=sh", "+zone=sh"]"#)
        .expect("valid constraints");
    assert_eq!(1, rules0.len());
    rules0[0].count = 0; // Test pruning useless rules.

    let rules1 = new_rules(&PeerRoleType::VOTER, 4, r#"["-zone=sh", "+zone=bj"]"#)
        .expect("valid constraints");
    assert_eq!(1, rules1.len());
    let rules2 = new_rules(&PeerRoleType::VOTER, 0, r#"{"-zone=sh,+zone=bj": 4}}"#)
        .expect("valid constraints");
    bundle.rules.extend(rules0.clone());
    bundle.rules.extend(rules1);
    bundle.rules.extend(rules2);

    assert_eq!(3, bundle.rules.len());
    bundle.tidy().expect("tidy");
    assert_eq!(1, bundle.rules.len());
    assert_eq!("0", bundle.rules[0].id);
    assert_eq!(2, bundle.rules[0].label_constraints.len());

    // Merge.
    let rules3 = new_rules(&PeerRoleType::FOLLOWER, 4, "").expect("valid constraints");
    assert_eq!(1, rules3.len());

    let rules4 = new_rules(&PeerRoleType::FOLLOWER, 5, "").expect("valid constraints");
    assert_eq!(1, rules4.len());

    rules0[0].role = PeerRoleType::VOTER;
    bundle.rules.extend(rules0);
    bundle.rules.extend(rules3);
    bundle.rules.extend(rules4);

    for rule in &mut bundle.rules {
        rule.location_labels = vec!["zone".to_owned(), "host".to_owned()];
    }
    let check = |bundle: &Bundle| {
        assert_eq!(2, bundle.rules.len());
        assert_eq!("0", bundle.rules[0].id);
        assert_eq!("1", bundle.rules[1].id);
        assert_eq!(9, bundle.rules[1].count);
        assert_eq!(0, bundle.rules[1].label_constraints.len());
        assert_eq!(
            vec!["zone".to_owned(), "host".to_owned()],
            bundle.rules[1].location_labels
        );
    };
    bundle.tidy().expect("tidy");
    check(&bundle);

    // Tidy again; it should be stable.
    bundle.tidy().expect("tidy");
    check(&bundle);

    // Tidy again; it should be stable.
    let mut bundle2 = bundle.clone_bundle();
    bundle2.tidy().expect("tidy");
    assert_eq!(bundle, bundle2);
}

/// Go `TestTidy2` (`bundle_test.go`).
#[test]
fn test_tidy2() {
    let rack = |value: &str| {
        vec![new_constraint_direct(
            "rack",
            LabelConstraintOp::IN,
            &[value],
        )]
    };
    let rule = |id: &str, role: PeerRoleType, constraints: Vec<LabelConstraint>, count: i64| Rule {
        id: id.to_owned(),
        role,
        label_constraints: constraints,
        count,
        location_labels: vec!["region".to_owned()],
        ..Rule::default()
    };

    let tests: Vec<(&str, Bundle, Bundle)> = vec![
        (
            "Empty bundle",
            Bundle {
                rules: Vec::new(),
                ..Bundle::default()
            },
            Bundle {
                rules: Vec::new(),
                ..Bundle::default()
            },
        ),
        (
            "Rules with empty constraints are merged",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, Vec::new(), 1),
                    rule("2", PeerRoleType::VOTER, Vec::new(), 2),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![rule("0", PeerRoleType::VOTER, Vec::new(), 3)],
                ..Bundle::default()
            },
        ),
        (
            "Rules with same constraints are merged, Leader + Follower",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::FOLLOWER, rack("1"), 2),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![rule("0", PeerRoleType::VOTER, rack("1"), 3)],
                ..Bundle::default()
            },
        ),
        (
            "Rules with same constraints are merged, Leader + Voter",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::VOTER, rack("1"), 2),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![rule("0", PeerRoleType::VOTER, rack("1"), 3)],
                ..Bundle::default()
            },
        ),
        (
            "Rules with same constraints and role are merged,  Leader + Follower + Voter",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::FOLLOWER, rack("1"), 1),
                    rule("3", PeerRoleType::VOTER, rack("1"), 1),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![rule("0", PeerRoleType::VOTER, rack("1"), 3)],
                ..Bundle::default()
            },
        ),
        (
            "Rules with same constraints and role are merged,  Leader + Follower + Voter + Learner",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::FOLLOWER, rack("1"), 1),
                    rule("3", PeerRoleType::VOTER, rack("1"), 1),
                    rule("4", PeerRoleType::LEARNER, rack("1"), 2),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![
                    rule("0", PeerRoleType::VOTER, rack("1"), 3),
                    rule("3", PeerRoleType::LEARNER, rack("1"), 2),
                ],
                ..Bundle::default()
            },
        ),
        (
            "Rules with same constraints and role are merged,  Leader + Follower + Learner | Follower",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::FOLLOWER, rack("1"), 1),
                    rule("3", PeerRoleType::LEARNER, rack("1"), 1),
                    rule("4", PeerRoleType::FOLLOWER, rack("2"), 1),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![
                    rule("0", PeerRoleType::VOTER, rack("1"), 2),
                    rule("2", PeerRoleType::LEARNER, rack("1"), 1),
                    rule("3", PeerRoleType::FOLLOWER, rack("2"), 1),
                ],
                ..Bundle::default()
            },
        ),
        (
            "Rules with same constraints and role are merged,  Leader + Follower + Learner | Voter",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::FOLLOWER, rack("1"), 1),
                    rule("3", PeerRoleType::LEARNER, rack("1"), 1),
                    rule("4", PeerRoleType::VOTER, rack("2"), 1),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![
                    rule("0", PeerRoleType::LEADER, rack("1"), 1),
                    rule("1", PeerRoleType::FOLLOWER, rack("1"), 1),
                    rule("2", PeerRoleType::LEARNER, rack("1"), 1),
                    rule("3", PeerRoleType::VOTER, rack("2"), 1),
                ],
                ..Bundle::default()
            },
        ),
        (
            "Rules with different constraints are kept separate",
            Bundle {
                rules: vec![
                    rule("1", PeerRoleType::LEADER, rack("1"), 1),
                    rule("2", PeerRoleType::FOLLOWER, rack("2"), 1),
                ],
                ..Bundle::default()
            },
            Bundle {
                rules: vec![
                    rule("0", PeerRoleType::LEADER, rack("1"), 1),
                    rule("1", PeerRoleType::FOLLOWER, rack("2"), 1),
                ],
                ..Bundle::default()
            },
        ),
    ];

    for (name, mut bundle, expected) in tests {
        bundle.tidy().expect("tidy");

        assert_eq!(expected.rules.len(), bundle.rules.len(), "{name}");

        for (index, rule) in bundle.rules.iter().enumerate() {
            let expected_rule = &expected.rules[index];
            assert!(
                rule == expected_rule,
                "{name}: unexpected rule at index {index}:\nactual={rule:#?},\nexpected={expected_rule:#?}\n"
            );
        }
    }
}

/// Go `TestGetRangeStartAndEndKeyHex` (`bundle_test.go`).
#[test]
fn test_get_range_start_and_end_key_hex() {
    let (start_key, end_key) = get_range_start_and_end_key_hex(TIDB_BUNDLE_RANGE_PREFIX_FOR_META);

    // Check that startKey is properly encoded in table mode.
    let start_key_bytes = hex_decode(&start_key);

    // Both keys should be valid codec encoded bytes.
    let (_, start_key_decoded) = decode_bytes(&start_key_bytes).expect("codec encoded");
    assert_eq!(
        META_PREFIX, start_key_decoded,
        "metaPrefix and startKeyDecoded should have the same content"
    );

    let end_key_bytes = hex_decode(&end_key);
    let (_, end_key_decoded) = decode_bytes(&end_key_bytes).expect("codec encoded");
    assert_eq!(
        gen_table_prefix(0),
        end_key_decoded,
        "tablePrefix and endKeyDecoded should have the same content"
    );
}
