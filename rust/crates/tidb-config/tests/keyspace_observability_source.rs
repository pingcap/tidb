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

//! Source tests for Go `pkg/config/config_test.go::TestKeyspaceObservability`
//! and `TestKeyspaceObservabilityInvalid`.

use std::collections::HashMap;

use tidb_config::config_tree::Config;
use tidb_config::keyspace_observability::KeyspaceObservabilityLogField;

#[test]
fn keyspace_observability_matches_source() {
    let content = r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "keyspace_meta_label_a"
slow-log-field = "Keyspace_meta_slow_a"
stmt-log-field = "stmt_meta_a"
required = true

[[keyspace-observability.fields]]
source = "meta_b"
metric-label = "keyspace_meta_label_b"
slow-log-field = "Keyspace_meta_slow_b"
"#;
    let mut config: Config = toml::from_str(content).unwrap();
    config.keyspace_observability.valid().unwrap();
    config
        .resolve_keyspace_observability(&HashMap::from([
            ("meta_a".to_owned(), "value_a".to_owned()),
            ("meta_b".to_owned(), "value_b".to_owned()),
        ]))
        .unwrap();
    assert_eq!(
        config.get_keyspace_observability_metric_labels(),
        &HashMap::from([
            ("keyspace_meta_label_a".to_owned(), "value_a".to_owned()),
            ("keyspace_meta_label_b".to_owned(), "value_b".to_owned()),
        ])
    );
    assert_eq!(
        config.get_keyspace_observability_slow_log_fields(),
        &[
            KeyspaceObservabilityLogField {
                name: "Keyspace_meta_slow_a".to_owned(),
                value: "value_a".to_owned(),
            },
            KeyspaceObservabilityLogField {
                name: "Keyspace_meta_slow_b".to_owned(),
                value: "value_b".to_owned(),
            },
        ]
    );
    assert_eq!(
        config.get_keyspace_observability_stmt_log_fields(),
        &HashMap::from([("stmt_meta_a".to_owned(), "value_a".to_owned())])
    );

    assert!(config
        .resolve_keyspace_observability(&HashMap::from([(
            "meta_b".to_owned(),
            "value_b".to_owned()
        )]))
        .unwrap_err()
        .contains("missing required keyspace metadata entry \"meta_a\""));
}

#[test]
fn keyspace_observability_invalid_matches_source() {
    let cases = [
        (
            "empty source",
            r#"
[[keyspace-observability.fields]]
source = ""
metric-label = "keyspace_meta_label_a"
"#,
            "source cannot be empty",
        ),
        (
            "empty output",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
"#,
            "at least one output must be set",
        ),
        (
            "invalid label",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "1_label"
"#,
            r#"invalid metric-label "1_label""#,
        ),
        (
            "duplicate label",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "keyspace_meta_label_a"

[[keyspace-observability.fields]]
source = "meta_b"
metric-label = "KEYSPACE_META_LABEL_A"
"#,
            r#"duplicated metric-label "KEYSPACE_META_LABEL_A""#,
        ),
        (
            "reserved label without prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "KEYSPACE_ID"
"#,
            r#"metric-label "KEYSPACE_ID" must start with "keyspace_meta_""#,
        ),
        (
            "metric variable label without prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "TYPE"
"#,
            r#"metric-label "TYPE" must start with "keyspace_meta_""#,
        ),
        (
            "api label without prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "api"
"#,
            r#"metric-label "api" must start with "keyspace_meta_""#,
        ),
        (
            "service scope label without prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "service_scope"
"#,
            r#"metric-label "service_scope" must start with "keyspace_meta_""#,
        ),
        (
            "task id label without prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "task_id"
"#,
            r#"metric-label "task_id" must start with "keyspace_meta_""#,
        ),
        (
            "slow log field without prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
slow-log-field = "Digest"
"#,
            r#"slow-log-field "Digest" must start with "Keyspace_meta_""#,
        ),
        (
            "slow log field with lowercase prefix",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
slow-log-field = "keyspace_meta_slow"
"#,
            r#"slow-log-field "keyspace_meta_slow" must start with "Keyspace_meta_""#,
        ),
        (
            "invalid slow log field",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
slow-log-field = "Bad Field"
"#,
            r#"invalid slow-log-field "Bad Field""#,
        ),
        (
            "duplicate slow log field",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
slow-log-field = "Keyspace_meta_slow"

[[keyspace-observability.fields]]
source = "meta_b"
slow-log-field = "Keyspace_meta_SLOW"
"#,
            r#"duplicated slow-log-field "Keyspace_meta_SLOW""#,
        ),
        (
            "duplicate stmt log field",
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
stmt-log-field = "stmt_meta"

[[keyspace-observability.fields]]
source = "meta_b"
stmt-log-field = "stmt_meta"
"#,
            r#"duplicated stmt-log-field "stmt_meta""#,
        ),
    ];

    for (name, content, want) in cases {
        let config: Config = toml::from_str(content).unwrap();
        assert!(
            config
                .keyspace_observability
                .valid()
                .unwrap_err()
                .contains(want),
            "{name}"
        );
    }

    let mut config: Config = toml::from_str(
        r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "keyspace_meta_label_a"
"#,
    )
    .unwrap();
    assert!(config.valid().unwrap_err().contains(
        "keyspace-observability.fields can only be configured when deploy-mode is starter"
    ));
}
