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

use super::*;

fn rule(schema: &str, table: &str, target_schema: &str, target_table: &str) -> TableRule {
    TableRule::new(schema, table, target_schema, target_table)
}

// Go TestRoute.
#[test]
fn route_rule_lifecycle_and_error_boundaries() {
    let mut rules = vec![
        rule("Test_1_*", "abc*", "t1", "abc"),
        rule("test_1_*", "test*", "t2", "test"),
        rule("test_1_*", "", "test", ""),
        rule("test_2_*", "abc*", "t1", "abc"),
        rule("test_2_*", "test*", "t2", "test"),
    ];
    let mut cases = vec![
        ("test_1_a", "abc1", "t1", "abc"),
        ("test_2_a", "abc2", "t1", "abc"),
        ("test_1_a", "test1", "t2", "test"),
        ("test_2_a", "test2", "t2", "test"),
        ("test_1_a", "xyz", "test", "xyz"),
    ];

    let router = Table::new(false, &mut rules).unwrap();
    for rule in &mut rules {
        assert!(router.add_rule(rule).is_err());
    }
    for &(schema, table, target_schema, target_table) in &cases {
        assert_eq!(
            router.route(schema, table).unwrap(),
            (target_schema.to_owned(), target_table.to_owned())
        );
    }

    rules[0].target_table = "xxx".to_owned();
    cases[0].3 = "xxx";
    router.update_rule(&mut rules[0]).unwrap();
    for &(schema, table, target_schema, target_table) in &cases {
        assert_eq!(
            router.route(schema, table).unwrap(),
            (target_schema.to_owned(), target_table.to_owned())
        );
    }

    router.remove_rule(&mut rules[0]).unwrap();
    assert!(router.remove_rule(&mut rules[0]).is_err());
    assert_eq!(
        router.route("test_1_a", "abc1").unwrap(),
        ("test".to_owned(), "abc1".to_owned())
    );
    assert_eq!(
        router.route("test_3_a", "").unwrap(),
        ("test_3_a".to_owned(), String::new())
    );

    let mut overlapping_schema = rule("test_*", "", "error", "");
    router.add_rule(&mut overlapping_schema).unwrap();
    assert!(router.route("test_1_a", "").is_err());

    let mut overlapping_table = rule("test_1_*", "tes*", "error", "error");
    router.add_rule(&mut overlapping_table).unwrap();
    assert!(router.route("test_1_a", "test").is_err());

    router
        .insert_invalid_for_test("test_1_*", "abc*", "error")
        .unwrap();
    assert!(router.route("test_1_a", "abc").is_err());

    let mut invalid = rule("test*", "abc*", "", "");
    assert!(router.add_rule(&mut invalid).is_err());
    assert!(router.update_rule(&mut invalid).is_err());
}

// Go TestCaseSensitive.
#[test]
fn case_sensitive_patterns_remain_distinct() {
    let mut rules = vec![
        rule("Test_1_*", "abc*", "t1", "abc"),
        rule("test_1_*", "test*", "t2", "test"),
        rule("test_1_*", "", "test", ""),
        rule("test_2_*", "abc*", "t1", "abc"),
        rule("test_2_*", "test*", "t2", "test"),
    ];
    let router = Table::new(true, &mut rules).unwrap();
    for rule in &mut rules {
        assert!(router.add_rule(rule).is_err());
    }
    let cases = [
        ("test_1_a", "abc1", "test", "abc1"),
        ("test_2_a", "abc2", "t1", "abc"),
        ("test_1_a", "test1", "t2", "test"),
        ("test_2_a", "test2", "t2", "test"),
        ("test_1_a", "xyz", "test", "xyz"),
    ];
    for (schema, table, target_schema, target_table) in cases {
        assert_eq!(
            router.route(schema, table).unwrap(),
            (target_schema.to_owned(), target_table.to_owned())
        );
    }
}

// Go TestFetchExtendColumn.
#[test]
fn table_extractors_have_priority_and_concatenate_capture_groups() {
    let mut table_rule = rule("schema*", "t*", "test", "t");
    table_rule.table_extractor = Some(TableExtractor::new("table_name", "table_(.*)"));
    table_rule.schema_extractor = Some(SchemaExtractor::new("schema_name", "schema_(.*)"));
    table_rule.source_extractor = Some(SourceExtractor::new("source_name", "source_(.*)_(.*)"));
    let mut schema_rule = rule("schema*", "", "test", "t2");
    schema_rule.schema_extractor = Some(SchemaExtractor::new("schema_name", "(.*)"));
    schema_rule.source_extractor = Some(SourceExtractor::new("source_name", "(.*)"));
    let mut rules = vec![table_rule, schema_rule];
    let router = Table::new(false, &mut rules).unwrap();

    assert_eq!(
        router.fetch_extend_column("schema_s1", "table_t1", "source_s1_s1"),
        (
            vec![
                "table_name".into(),
                "schema_name".into(),
                "source_name".into()
            ],
            vec!["t1".into(), "s1".into(), "s1s1".into()]
        )
    );
    assert_eq!(
        router.fetch_extend_column("schema_s2", "a_table_t2", "source_s2"),
        (
            vec!["schema_name".into(), "source_name".into()],
            vec!["schema_s2".into(), "source_s2".into()]
        )
    );
    assert_eq!(
        router.fetch_extend_column("SCHEMA_S2", "a_table_t2", "source_s2"),
        (Vec::new(), Vec::new())
    );
}

#[test]
fn validation_order_and_config_tags_match_the_source() {
    let mut empty_schema = rule("", "t*", "target", "t");
    assert_eq!(
        empty_schema.valid().unwrap_err().to_string(),
        "schema pattern of table route rule should not be empty"
    );
    let mut empty_target = rule("s*", "t*", "", "t");
    assert_eq!(
        empty_target.valid().unwrap_err().to_string(),
        "target schema of table route rule should not be empty"
    );

    let extractor_cases = [
        (
            Some(TableExtractor::new("column", "[")),
            None,
            None,
            "table extractor table regexp illegal [",
        ),
        (
            None,
            Some(SchemaExtractor::new("column", "[")),
            None,
            "schema extractor schema regexp illegal [",
        ),
        (
            None,
            None,
            Some(SourceExtractor::new("column", "[")),
            "source extractor source regexp illegal [",
        ),
    ];
    for (table, schema, source, expected) in extractor_cases {
        let mut rule = rule("s*", "t*", "target", "t");
        rule.table_extractor = table;
        rule.schema_extractor = schema;
        rule.source_extractor = source;
        assert_eq!(rule.valid().unwrap_err().to_string(), expected);
    }

    let empty_column_cases = [
        (
            Some(TableExtractor::new("", ".*")),
            None,
            None,
            "table extractor target column cannot be empty",
        ),
        (
            None,
            Some(SchemaExtractor::new("", ".*")),
            None,
            "schema extractor target column cannot be empty",
        ),
        (
            None,
            None,
            Some(SourceExtractor::new("", ".*")),
            "source extractor target column cannot be empty",
        ),
    ];
    for (table, schema, source, expected) in empty_column_cases {
        let mut rule = rule("s*", "t*", "target", "t");
        rule.table_extractor = table;
        rule.schema_extractor = schema;
        rule.source_extractor = source;
        assert_eq!(rule.valid().unwrap_err().to_string(), expected);
    }

    let raw = r#"{
        "extract-table":{"target-column":"c","table-regexp":"t_(.*)"},
        "schema-pattern":"S*","table-pattern":"T*",
        "target-schema":"dst","target-table":"tbl"
    }"#;
    let mut decoded: TableRule = serde_json::from_str(raw).unwrap();
    decoded.valid().unwrap();
    decoded.to_lower();
    assert_eq!(decoded.schema_pattern, "s*");
    assert_eq!(decoded.table_pattern, "t*");
    let encoded = serde_json::to_value(&decoded).unwrap();
    assert_eq!(encoded["extract-table"]["target-column"], "c");
    assert!(encoded.get("table_extractor").is_none());

    let zero_value: TableRule = serde_json::from_str("{}").unwrap();
    assert_eq!(zero_value.schema_pattern, "");
    assert_eq!(zero_value.target_table, "");
}

#[test]
fn unmatched_and_optional_groups_follow_go_find_string_submatch() {
    let regexp = Regex::new(r"source_(a)?_(.*)").unwrap();
    assert_eq!(extract_value("source__tail", Some(&regexp)), "tail");
    assert_eq!(extract_value("missing", Some(&regexp)), "");
    assert_eq!(extract_value("source_a_tail", Some(&regexp)), "atail");
}
