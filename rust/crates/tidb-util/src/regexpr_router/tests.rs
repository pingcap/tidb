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
use crate::table_router::{SchemaExtractor, SourceExtractor, Table as OldRouter, TableExtractor};

fn rule(schema: &str, table: &str, target_schema: &str, target_table: &str) -> TableRule {
    TableRule::new(schema, table, target_schema, target_table)
}

// Go TestCreateRouter.
#[test]
fn create_router() {
    assert!(RouteTable::new(true, &mut []).is_ok());
    assert!(RouteTable::new(false, &mut []).is_ok());
}

// Go TestAddRule.
#[test]
fn add_rule() {
    let mut rules = vec![
        rule("test1", "", "dtest1", ""),
        rule("test2", "table2", "dtest2", "dtable2"),
    ];
    let mut router = RouteTable::new(true, &mut []).unwrap();
    for route_rule in &mut rules {
        router.add_rule(route_rule).unwrap();
    }

    let mut router = RouteTable::new(false, &mut []).unwrap();
    for route_rule in &mut rules {
        router.add_rule(route_rule).unwrap();
    }
}

// Go TestSchemaRoute.
#[test]
fn schema_route() {
    let rules = vec![
        rule("test1", "", "dtest1", ""),
        rule("gtest*", "", "dtest", ""),
    ];
    let mut old_rules = rules.clone();
    let old_router = OldRouter::new(true, &mut old_rules).unwrap();
    let mut new_rules = rules;
    let new_router = RouteTable::new(true, &mut new_rules).unwrap();
    let cases = [
        ("test1", "table1", "dtest1", "table1"),
        ("gtesttest", "atable", "dtest", "atable"),
        ("ptest", "atableg", "ptest", "atableg"),
    ];
    for (schema, table, target_schema, target_table) in cases {
        let expected = (target_schema.to_owned(), target_table.to_owned());
        assert_eq!(old_router.route(schema, table).unwrap(), expected);
        assert_eq!(new_router.route(schema, table).unwrap(), expected);
    }
}

// Go TestTableRoute.
#[test]
fn table_route() {
    let rules = vec![
        rule("test1", "table1", "dtest1", "dtable1"),
        rule("test*", "table2", "dtest2", "dtable2"),
        rule("test3", "table*", "dtest3", "dtable3"),
    ];
    let mut old_rules = rules.clone();
    let old_router = OldRouter::new(true, &mut old_rules).unwrap();
    let mut new_rules = rules;
    let new_router = RouteTable::new(true, &mut new_rules).unwrap();
    for index in 1..=3 {
        let schema = format!("test{index}");
        let table = format!("table{index}");
        let expected = (format!("dtest{index}"), format!("dtable{index}"));
        assert_eq!(old_router.route(&schema, &table).unwrap(), expected);
        assert_eq!(new_router.route(&schema, &table).unwrap(), expected);
    }
}

// Go TestRegExprRoute.
#[test]
fn regexp_route() {
    let mut rules = vec![
        rule("~test.[0-9]+", "", "dtest1", ""),
        rule(
            "~test2?[animal|human]",
            "~tbl.*[cat|dog]+",
            "dtest2",
            "dtable2",
        ),
        rule("~test3_(schema)?.*", "test3_*", "dtest3", "dtable3"),
        rule(
            "test4s_*",
            "~testtable_[donot_delete]?",
            "dtest4",
            "dtable4",
        ),
    ];
    let router = RouteTable::new(true, &mut rules).unwrap();
    let cases = [
        ("tests100", "table1", "dtest1", "table1"),
        ("test2animal", "tbl_animal_dogcat", "dtest2", "dtable2"),
        ("test3_schema_meta", "test3_tail", "dtest3", "dtable3"),
        ("test4s_2022", "testtable_donot_delete", "dtest4", "dtable4"),
        ("mytst5566", "gtable", "mytst5566", "gtable"),
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
fn fetch_extend_column() {
    let mut table_rule = rule("schema*", "t*", "test", "t");
    table_rule.table_extractor = Some(TableExtractor::new("table_name", "table_(.*)"));
    table_rule.schema_extractor = Some(SchemaExtractor::new("schema_name", "schema_(.*)"));
    table_rule.source_extractor = Some(SourceExtractor::new("source_name", "source_(.*)_(.*)"));
    let mut schema_rule = rule("~s?chema.*", "", "test", "t2");
    schema_rule.schema_extractor = Some(SchemaExtractor::new("schema_name", "(.*)"));
    schema_rule.source_extractor = Some(SourceExtractor::new("source_name", "(.*)"));
    let mut rules = vec![table_rule, schema_rule];
    let router = RouteTable::new(false, &mut rules).unwrap();

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
}

// Go TestAllRule.
#[test]
fn all_rules() {
    let mut rules = vec![
        rule("~test.[0-9]+", "", "dtest1", ""),
        rule(
            "~test2?[animal|human]",
            "~tbl.*[cat|dog]+",
            "dtest2",
            "dtable2",
        ),
        rule("~test3_(schema)?.*", "test3_*", "dtest3", "dtable3"),
        rule(
            "test4s_*",
            "~testtable_[donot_delete]?",
            "dtest4",
            "dtable4",
        ),
    ];
    let expected = rules.clone();
    let router = RouteTable::new(true, &mut rules).unwrap();
    let (schema_rules, table_rules) = router.all_rules();
    assert_eq!(schema_rules.len(), 1);
    assert_eq!(table_rules.len(), 3);
    assert_eq!(schema_rules[0].schema_pattern, expected[0].schema_pattern);
    for index in 0..3 {
        assert_eq!(
            table_rules[index].schema_pattern,
            expected[index + 1].schema_pattern
        );
        assert_eq!(
            table_rules[index].table_pattern,
            expected[index + 1].table_pattern
        );
    }
}

// Go TestDupMatch.
#[test]
fn duplicate_match() {
    let mut rules = vec![
        rule("~test[0-9]+.*", "~.*", "dtest1", ""),
        rule("~test2?[a|b]", "~tbl2", "dtest2", "dtable2"),
        rule("mytest*", "", "mytest", ""),
        rule("~mytest(_meta)?_schema", "", "test", ""),
    ];
    let router = RouteTable::new(true, &mut rules).unwrap();
    for (schema, table) in [("test2a", "tbl2"), ("mytest_meta_schema", "")] {
        let error = router.route(schema, table).unwrap_err();
        assert!(error.to_string().contains("matches more than one rule"));
    }
}
