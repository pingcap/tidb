// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

fn region(id: i64, start_key: &str, end_key: &str) -> RegionInfo {
    RegionInfo {
        id,
        start_key: start_key.to_owned(),
        end_key: end_key.to_owned(),
        ..Default::default()
    }
}

fn regions(regions: Vec<RegionInfo>) -> Option<RegionsInfo> {
    Some(RegionsInfo {
        count: regions.len() as i64,
        regions,
    })
}

// Go `TestMergeRegionsInfo`.
#[test]
fn test_merge_regions_info() {
    let test_cases: Vec<(Option<RegionsInfo>, Option<RegionsInfo>)> = vec![
        // Different regions.
        (
            regions(vec![region(1, "", "a")]),
            regions(vec![region(2, "a", "")]),
        ),
        // Same region.
        (
            regions(vec![region(1, "", "a")]),
            regions(vec![region(1, "", "a")]),
        ),
        (regions(vec![region(1, "", "a")]), None),
        (None, regions(vec![region(2, "a", "")])),
        (None, None),
        (regions(vec![region(1, "", "a")]), Some(new_regions_info(0))),
        (Some(new_regions_info(0)), regions(vec![region(2, "a", "")])),
        (Some(new_regions_info(0)), Some(new_regions_info(0))),
    ];
    for (idx, (source, target)) in test_cases.into_iter().enumerate() {
        let regions_info = merge_regions_info(source.as_ref(), target.as_ref());
        // The method form agrees with the nil-receiver form.
        if let Some(source) = &source {
            let by_method = source.merge(target.as_ref());
            assert_eq!(by_method.count, regions_info.count, "case {idx}");
        }
        let source = source.unwrap_or_else(|| new_regions_info(0));
        let target = target.unwrap_or_else(|| new_regions_info(0));
        let mut m: HashMap<i64, RegionInfo> = HashMap::new();
        for region in &source.regions {
            m.insert(region.id, region.clone());
        }
        for region in &target.regions {
            m.insert(region.id, region.clone());
        }
        let merged_count = m.len();
        assert_eq!(merged_count as i64, regions_info.count, "case {idx}");
        assert_eq!(regions_info.regions.len(), merged_count, "case {idx}");
        // All regions in source and target should be in the merged result.
        for region in source.regions.iter().chain(target.regions.iter()) {
            assert!(regions_info.regions.contains(region), "case {idx}");
        }
    }
}

// Go `TestRegionInfoApproximateKvSizeDecode`.
#[test]
fn test_region_info_approximate_kv_size_decode() {
    let region: RegionInfo = serde_json::from_str(r#"{"approximate_kv_size":123}"#).unwrap();
    assert_eq!(123_i64, region.approximate_kv_size);
}

// Go `TestRegionInfoApproximateKvSizeDefaultZero`.
#[test]
fn test_region_info_approximate_kv_size_default_zero() {
    let region: RegionInfo = serde_json::from_str("{}").unwrap();
    assert_eq!(0, region.approximate_kv_size);
}

fn must_marshal_and_unmarshal(rule: &Rule) -> Rule {
    let rule_json = serde_json::to_string(rule).unwrap();
    serde_json::from_str(&rule_json).unwrap()
}

// Go `TestRuleStartEndKey`.
#[test]
fn test_rule_start_end_key() {
    // Empty start/end key and key hex.
    let rule_to_marshal = Rule::default();
    let rule = must_marshal_and_unmarshal(&rule_to_marshal);
    assert!(rule.start_key_hex.is_empty());
    assert!(rule.end_key_hex.is_empty());
    assert_eq!(b"".to_vec(), rule.start_key);
    assert_eq!(b"".to_vec(), rule.end_key);
    // Empty start/end key and non-empty key hex.
    let rule_to_marshal = Rule {
        start_key_hex: raw_key_to_key_hex_str(b"a"),
        end_key_hex: raw_key_to_key_hex_str(b"b"),
        ..Default::default()
    };
    let rule = must_marshal_and_unmarshal(&rule_to_marshal);
    assert_eq!(b"a".to_vec(), rule.start_key);
    assert_eq!(b"b".to_vec(), rule.end_key);
    assert_eq!(rule_to_marshal.start_key_hex, rule.start_key_hex);
    assert_eq!(rule_to_marshal.end_key_hex, rule.end_key_hex);
    // Non-empty start/end key and empty key hex.
    let rule_to_marshal = Rule {
        start_key: b"a".to_vec(),
        end_key: b"b".to_vec(),
        ..Default::default()
    };
    let rule = must_marshal_and_unmarshal(&rule_to_marshal);
    assert_eq!(rule_to_marshal.start_key, rule.start_key);
    assert_eq!(rule_to_marshal.end_key, rule.end_key);
    assert_eq!(
        raw_key_to_key_hex_str(&rule_to_marshal.start_key),
        rule.start_key_hex
    );
    assert_eq!(
        raw_key_to_key_hex_str(&rule_to_marshal.end_key),
        rule.end_key_hex
    );
    // Non-empty start/end key and non-empty key hex.
    let rule_to_marshal = Rule {
        start_key: b"a".to_vec(),
        end_key: b"b".to_vec(),
        start_key_hex: raw_key_to_key_hex_str(b"c"),
        end_key_hex: raw_key_to_key_hex_str(b"d"),
        ..Default::default()
    };
    let rule = must_marshal_and_unmarshal(&rule_to_marshal);
    assert_eq!(b"c".to_vec(), rule.start_key);
    assert_eq!(b"d".to_vec(), rule.end_key);
    assert_eq!(rule_to_marshal.start_key_hex, rule.start_key_hex);
    assert_eq!(rule_to_marshal.end_key_hex, rule.end_key_hex);
    // Half of each pair of keys is empty.
    let rule_to_marshal = Rule {
        start_key: b"a".to_vec(),
        end_key_hex: raw_key_to_key_hex_str(b"d"),
        ..Default::default()
    };
    let rule = must_marshal_and_unmarshal(&rule_to_marshal);
    assert_eq!(rule_to_marshal.start_key, rule.start_key);
    assert_eq!(b"d".to_vec(), rule.end_key);
    assert_eq!(
        raw_key_to_key_hex_str(&rule_to_marshal.start_key),
        rule.start_key_hex
    );
    assert_eq!(rule_to_marshal.end_key_hex, rule.end_key_hex);
}

fn must_marshal_and_unmarshal_rule_op(rule_op: &RuleOp) -> RuleOp {
    let rule_op_json = serde_json::to_string(rule_op).unwrap();
    serde_json::from_str(&rule_op_json).unwrap()
}

// Go `TestRuleOpStartEndKey`.
#[test]
fn test_rule_op_start_end_key() {
    // Empty start/end key and key hex.
    let rule_op_to_marshal = RuleOp {
        rule: Rule::default(),
        ..Default::default()
    };
    let rule_op = must_marshal_and_unmarshal_rule_op(&rule_op_to_marshal);
    assert!(rule_op.start_key_hex.is_empty());
    assert!(rule_op.end_key_hex.is_empty());
    assert_eq!(b"".to_vec(), rule_op.start_key);
    assert_eq!(b"".to_vec(), rule_op.end_key);
    // Empty start/end key and non-empty key hex.
    let rule_op_to_marshal = RuleOp {
        rule: Rule {
            start_key_hex: raw_key_to_key_hex_str(b"a"),
            end_key_hex: raw_key_to_key_hex_str(b"b"),
            ..Default::default()
        },
        action: RULE_OP_ADD,
        delete_by_id_prefix: true,
    };
    let rule_op = must_marshal_and_unmarshal_rule_op(&rule_op_to_marshal);
    assert_eq!(b"a".to_vec(), rule_op.start_key);
    assert_eq!(b"b".to_vec(), rule_op.end_key);
    assert_eq!(rule_op_to_marshal.start_key_hex, rule_op.start_key_hex);
    assert_eq!(rule_op_to_marshal.end_key_hex, rule_op.end_key_hex);
    assert_eq!(rule_op_to_marshal.action, rule_op.action);
    assert_eq!(
        rule_op_to_marshal.delete_by_id_prefix,
        rule_op.delete_by_id_prefix
    );
    // Non-empty start/end key and empty key hex.
    let rule_op_to_marshal = RuleOp {
        rule: Rule {
            start_key: b"a".to_vec(),
            end_key: b"b".to_vec(),
            ..Default::default()
        },
        action: RULE_OP_ADD,
        delete_by_id_prefix: true,
    };
    let rule_op = must_marshal_and_unmarshal_rule_op(&rule_op_to_marshal);
    assert_eq!(rule_op_to_marshal.start_key, rule_op.start_key);
    assert_eq!(rule_op_to_marshal.end_key, rule_op.end_key);
    assert_eq!(
        raw_key_to_key_hex_str(&rule_op_to_marshal.start_key),
        rule_op.start_key_hex
    );
    assert_eq!(
        raw_key_to_key_hex_str(&rule_op_to_marshal.end_key),
        rule_op.end_key_hex
    );
    assert_eq!(rule_op_to_marshal.action, rule_op.action);
    assert_eq!(
        rule_op_to_marshal.delete_by_id_prefix,
        rule_op.delete_by_id_prefix
    );
    // Non-empty start/end key and non-empty key hex.
    let rule_op_to_marshal = RuleOp {
        rule: Rule {
            start_key: b"a".to_vec(),
            end_key: b"b".to_vec(),
            start_key_hex: raw_key_to_key_hex_str(b"c"),
            end_key_hex: raw_key_to_key_hex_str(b"d"),
            ..Default::default()
        },
        action: RULE_OP_ADD,
        delete_by_id_prefix: true,
    };
    let rule_op = must_marshal_and_unmarshal_rule_op(&rule_op_to_marshal);
    assert_eq!(b"c".to_vec(), rule_op.start_key);
    assert_eq!(b"d".to_vec(), rule_op.end_key);
    assert_eq!(rule_op_to_marshal.start_key_hex, rule_op.start_key_hex);
    assert_eq!(rule_op_to_marshal.end_key_hex, rule_op.end_key_hex);
    assert_eq!(rule_op_to_marshal.action, rule_op.action);
    assert_eq!(
        rule_op_to_marshal.delete_by_id_prefix,
        rule_op.delete_by_id_prefix
    );
    // Half of each pair of keys is empty.
    let rule_op_to_marshal = RuleOp {
        rule: Rule {
            start_key: b"a".to_vec(),
            end_key_hex: raw_key_to_key_hex_str(b"d"),
            ..Default::default()
        },
        action: RULE_OP_DEL,
        delete_by_id_prefix: false,
    };
    let rule_op = must_marshal_and_unmarshal_rule_op(&rule_op_to_marshal);
    assert_eq!(rule_op_to_marshal.start_key, rule_op.start_key);
    assert_eq!(b"d".to_vec(), rule_op.end_key);
    assert_eq!(
        raw_key_to_key_hex_str(&rule_op_to_marshal.start_key),
        rule_op.start_key_hex
    );
    assert_eq!(rule_op_to_marshal.end_key_hex, rule_op.end_key_hex);
    assert_eq!(rule_op_to_marshal.action, rule_op.action);
    assert_eq!(
        rule_op_to_marshal.delete_by_id_prefix,
        rule_op.delete_by_id_prefix
    );
}

// Go `TestHotRegionStatsCPUFields`.
#[test]
fn test_hot_region_stats_cpu_fields() {
    let raw = r#"{
		"as_peer": {
			"1": {
				"store_bytes": 10,
				"store_keys": 11,
				"store_query": 12,
				"store_cpu": 13,
				"total_flow_bytes": 20,
				"total_flow_keys": 21,
				"total_flow_query": 22,
				"total_flow_cpu": 23,
				"regions_count": 1,
				"statistics": [{
					"store_id": 1,
					"stores": [1, 2],
					"is_leader": true,
					"is_learner": false,
					"region_id": 100,
					"hot_degree": 6,
					"flow_bytes": 30,
					"flow_keys": 31,
					"flow_query": 32,
					"flow_cpu": 33,
					"anti_count": 4
				}]
			}
		},
		"as_leader": {
			"2": {
				"store_bytes": 40,
				"store_keys": 41,
				"store_query": 42,
				"store_cpu": 43,
				"total_flow_bytes": 50,
				"total_flow_keys": 51,
				"total_flow_query": 52,
				"total_flow_cpu": 53,
				"regions_count": 1,
				"statistics": [{
					"store_id": 2,
					"stores": [2, 3],
					"is_leader": true,
					"is_learner": false,
					"region_id": 200,
					"hot_degree": 7,
					"flow_bytes": 60,
					"flow_keys": 61,
					"flow_query": 62,
					"flow_cpu": 63,
					"anti_count": 5
				}]
			}
		}
	}"#;

    let stats: StoreHotPeersInfos = serde_json::from_str(raw).unwrap();

    assert_eq!(13.0, stats.as_peer[&1].store_cpu_rate);
    assert_eq!(23.0, stats.as_peer[&1].total_cpu_rate);
    assert_eq!(1, stats.as_peer[&1].stats.len());
    assert_eq!(33.0, stats.as_peer[&1].stats[0].cpu_rate);

    assert_eq!(43.0, stats.as_leader[&2].store_cpu_rate);
    assert_eq!(53.0, stats.as_leader[&2].total_cpu_rate);
    assert_eq!(1, stats.as_leader[&2].stats.len());
    assert_eq!(63.0, stats.as_leader[&2].stats[0].cpu_rate);
}

// Go `TestRuleKeyClone`: start_key and end_key are `json:"-"`, which means they
// cannot be unmarshaled from JSON, so `Clone` must copy them.
#[test]
fn test_rule_key_clone() {
    let mut r = Rule {
        start_key: vec![1, 2, 3],
        end_key: vec![4, 5, 6],
        ..Default::default()
    };

    let mut clone = r.clone();
    // Modify the original rule
    r.start_key[0] = 9;
    r.end_key[0] = 9;

    // The clone should not be affected
    assert_eq!(vec![1, 2, 3], clone.start_key);
    assert_eq!(vec![4, 5, 6], clone.end_key);

    // Modify the clone
    clone.start_key[0] = 8;
    clone.end_key[0] = 8;

    // The original rule should not be affected
    assert_eq!(vec![9, 2, 3], r.start_key);
    assert_eq!(vec![9, 5, 6], r.end_key);
}
