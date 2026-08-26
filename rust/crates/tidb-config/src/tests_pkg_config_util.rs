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

//! Direct ports of Go `pkg/config/config_util_test.go` (origin/master).

use crate::config_tree::helpers::{flatten_config_items, merge_config_items};
use crate::config_tree::new_config;

// Go TestCloneConf (config_util_test.go): a clone is deep — mutating the
// copy leaves the original untouched.
#[test]
fn clone_conf() {
    let c1 = new_config();
    let c2 = c1.clone();
    assert_eq!(c1, c2);

    let mut c1 = c1;
    c1.store.0 = "abc".to_owned();
    c1.port = 2333;
    c1.instance.enable_slow_log = crate::config_tree::AtomicBool::new(
        !c1.instance.enable_slow_log.load(),
    );
    c1.repair_table_list.push("abc".to_owned());

    assert_ne!(c2.store.0, c1.store.0);
    assert_ne!(c2.port, c1.port);
    assert_ne!(
        c2.instance.enable_slow_log.load(),
        c1.instance.enable_slow_log.load()
    );
    assert_ne!(c2.repair_table_list, c1.repair_table_list);
}

// Go TestMergeConfigItems (config_util_test.go): dynamic items are applied
// to the destination and reported accepted; everything else is rejected
// unchanged.
#[test]
fn merge_config_items_port() {
    use crate::config_tree::helpers::DYNAMIC_CONFIG_ITEMS;

    let ori_conf = new_config();
    let mut old_conf = ori_conf.clone();
    let mut new_conf = old_conf.clone();

    // allowed
    new_conf.performance.max_procs = 123;
    new_conf.performance.max_memory = 123;
    new_conf.performance.cross_join = false;
    new_conf.performance.pseudo_estimate_ratio = 123.0;
    new_conf.tikv_client.store_limit = 123;
    // Instance.SlowThreshold is dynamic in the source too.
    new_conf.instance.slow_threshold = 2345;

    // rejected
    new_conf.store.0 = "tiflash".to_owned();
    new_conf.port = 2333;
    new_conf.advertise_address = "1.2.3.4".to_owned();

    let (as_items, rs_items) = merge_config_items(&mut old_conf, &new_conf);
    assert_eq!(as_items.len(), 6, "accepted: {as_items:?}");
    assert_eq!(rs_items.len(), 3, "rejected: {rs_items:?}");
    for a in &as_items {
        assert!(
            DYNAMIC_CONFIG_ITEMS.contains(&a.as_str()),
            "{a} not dynamic"
        );
    }
    for r in &rs_items {
        assert!(!DYNAMIC_CONFIG_ITEMS.contains(&r.as_str()), "{r} dynamic");
    }

    // Dynamic items were merged into oldConf.
    assert_eq!(old_conf.performance.max_procs, new_conf.performance.max_procs);
    assert_eq!(old_conf.performance.max_memory, new_conf.performance.max_memory);
    assert_eq!(old_conf.performance.cross_join, new_conf.performance.cross_join);
    assert_eq!(
        old_conf.performance.pseudo_estimate_ratio,
        new_conf.performance.pseudo_estimate_ratio
    );
    assert_eq!(old_conf.tikv_client.store_limit, new_conf.tikv_client.store_limit);
    assert_eq!(
        old_conf.instance.slow_threshold,
        new_conf.instance.slow_threshold
    );

    // Rejected items left oldConf untouched.
    assert_eq!(ori_conf.store, old_conf.store);
    assert_eq!(ori_conf.port, old_conf.port);
    assert_eq!(ori_conf.advertise_address, old_conf.advertise_address);
}

// Go TestFlattenConfig (config_util_test.go): nested JSON and TOML values
// flatten to dotted keys; arrays stay whole.
#[test]
fn flatten_config() {
    fn to_json_str(v: &serde_json::Value) -> String {
        serde_json::to_string(v).unwrap()
    }

    let json_conf = r#"{
	"k0": 233333,
	"k1": "v1",
	"k2": ["v2-1", "v2-2", "v2-3"],
	"k3": [{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}],
	"k4": {
		"k4-1": [1, 2, 3, 4],
		"k4-2": [5, 6, 7, 8],
		"k4-3": [666]
	}}"#;
    let nested: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(json_conf).unwrap();
    let flat_map = flatten_config_items(&nested);
    assert_eq!(flat_map.len(), 7);
    assert_eq!(flat_map["k0"].to_string(), "233333");
    assert_eq!(flat_map["k1"], "v1");
    assert_eq!(
        to_json_str(&flat_map["k2"]),
        r#"["v2-1","v2-2","v2-3"]"#
    );
    assert_eq!(
        to_json_str(&flat_map["k3"]),
        r#"[{"k3-1":"v3-1"},{"k3-2":"v3-2"},{"k3-3":"v3-3"}]"#
    );
    assert_eq!(to_json_str(&flat_map["k4.k4-1"]), "[1,2,3,4]");
    assert_eq!(to_json_str(&flat_map["k4.k4-2"]), "[5,6,7,8]");
    assert_eq!(to_json_str(&flat_map["k4.k4-3"]), "[666]");

    let toml_conf = r#"
port=4000
[log]
level='info'
format='text'
[isolation-read]
engines = ["tikv", "tiflash", "tidb"]
"#;
    let table: toml::Table = toml::from_str(toml_conf).unwrap();
    let nested = serde_json::to_value(table).unwrap();
    let flat_map = flatten_config_items(nested.as_object().unwrap());
    assert_eq!(flat_map.len(), 4);
    assert_eq!(flat_map["port"].to_string(), "4000");
    assert_eq!(to_json_str(&flat_map["log.level"]), "\"info\"");
    assert_eq!(to_json_str(&flat_map["log.format"]), "\"text\"");
    assert_eq!(
        to_json_str(&flat_map["isolation-read.engines"]),
        r#"["tikv","tiflash","tidb"]"#
    );
}
