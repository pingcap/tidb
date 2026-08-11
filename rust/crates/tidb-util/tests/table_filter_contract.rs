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

//! Public concurrency contract for Go `pkg/util/table-filter`.

use tidb_util::filter::Filter as ReplicationFilter;
use tidb_util::table_filter::{
    case_insensitive, parse, parse_column_filter, ColumnFilter, Filter, MySQLReplicationRules,
    Table,
};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn parsed_filter_objects_can_cross_and_be_shared_between_workers() {
    assert_send_sync::<Box<dyn Filter>>();
    assert_send_sync::<Box<dyn ColumnFilter>>();
}

#[test]
fn replication_rules_preserve_public_config_keys() {
    let rules: MySQLReplicationRules = serde_json::from_str(
        r#"{
            "do-tables": [{"db-name": "sales", "tbl-name": "orders"}],
            "ignore-dbs": ["archive"]
        }"#,
    )
    .unwrap();

    assert_eq!(rules.do_tables, vec![Table::new("sales", "orders")]);
    assert!(rules.do_dbs.is_empty());
    assert!(rules.ignore_tables.is_empty());
    assert_eq!(rules.ignore_dbs, vec!["archive"]);

    assert_eq!(
        serde_json::to_value(&rules).unwrap(),
        serde_json::json!({
            "do-tables": [{"db-name": "sales", "tbl-name": "orders"}],
            "do-dbs": [],
            "ignore-tables": [],
            "ignore-dbs": ["archive"]
        })
    );
}

#[test]
fn case_insensitive_filters_use_go_simple_unicode_folding() {
    let tables = case_insensitive(parse(&["İ.*"]).unwrap());
    assert!(tables.match_table("i", "orders"));

    let columns = parse_column_filter(&["İ"]).unwrap();
    assert!(columns.match_column("i"));

    let mut rules = MySQLReplicationRules {
        do_dbs: vec!["İ".to_owned()],
        ..Default::default()
    };
    rules.to_lower();
    assert_eq!(rules.do_dbs, vec!["i"]);

    let filter = ReplicationFilter::new(
        false,
        Some(MySQLReplicationRules {
            do_dbs: vec!["İ".to_owned()],
            ..Default::default()
        }),
    )
    .unwrap();
    assert!(filter.matches(&Table::new("i", "orders")));
}
