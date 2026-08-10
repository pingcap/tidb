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

//! Source tests for Go `pkg/config/config.go` and `pkg/config/config_test.go`
//! basic config primitives.

use serde::{Deserialize, Serialize};
use tidb_config::config_tree::{
    new_config, AtomicBool, Log, NullableBool, NB_FALSE, NB_TRUE, NB_UNSET,
};

#[derive(Serialize, Deserialize)]
struct AtomicData {
    ab: AtomicBool,
}

#[test]
fn atomic_bool_matches_source() {
    let data: AtomicData = toml::from_str("ab=true").unwrap();
    assert!(data.ab.load());
    assert_eq!(toml::to_string(&data).unwrap(), "ab = \"true\"\n");

    let data: AtomicData = toml::from_str("ab=false").unwrap();
    assert!(!data.ab.load());
    assert_eq!(toml::to_string(&data).unwrap(), "ab = \"false\"\n");

    assert!(toml::from_str::<AtomicData>("ab = 1").is_err());
}

#[test]
fn nullable_bool_matches_source() {
    for (nb, expect) in [(NB_UNSET, "null"), (NB_FALSE, "false"), (NB_TRUE, "true")] {
        let data = serde_json::to_string(&nb).unwrap();
        assert_eq!(data, expect);
        let back: NullableBool = serde_json::from_str(&data).unwrap();
        assert_eq!(back, nb);
    }

    let log: Log = toml::from_str("enable-error-stack = true").unwrap();
    assert_eq!(log.enable_error_stack, NB_TRUE);

    let log: Log = toml::from_str("enable-error-stack = \"\"").unwrap();
    assert_eq!(log.enable_error_stack, NB_UNSET);

    assert!(toml::from_str::<Log>("enable-error-stack = 1").is_err());

    let log: Log = serde_json::from_str(r#"{"enable-timestamp":false}"#).unwrap();
    assert_eq!(log.enable_timestamp, NB_FALSE);

    let log: Log = serde_json::from_str(r#"{"disable-timestamp":null}"#).unwrap();
    assert_eq!(log.disable_timestamp, NB_UNSET);

    assert!(serde_json::from_str::<NullableBool>("1").is_err());
}

#[test]
fn tcp_no_delay_default_matches_source() {
    assert!(new_config().performance.tcp_no_delay);
}
