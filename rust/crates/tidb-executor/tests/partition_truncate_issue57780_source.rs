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

//! `pkg/ddl/tests/partition/db_partition_test.go::TestIssue57780`.
//! The separate truncate schema-version obligation remains recorded in
//! `docs/parity/current-audit/test-owner-cleanup-validation.json`.

use tidb_datatype::Datum;
use tidb_executor::{run_alter_table_in, run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Go `db_partition_test.go:3896::TestIssue57780`: the `cis_assay_report_detail`
/// table — a RANGE COLUMNS(datetime) partitioned table with a composite
/// NONCLUSTERED primary key led by the partition column and several
/// timestamp(6) DEFAULT CURRENT_TIMESTAMP(6) columns — accepts
/// `add column test_decimal decimal(9,2)` and then a same-name
/// `change column test_decimal test_decimal decimal(11,2)` widening.
/// Go asserts both succeed; here the surviving data additionally proves the
/// widened column reads back.
#[test]
fn issue57780_range_columns_datetime_add_and_widen_column() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE `cis_assay_report_detail` (\
         `org_code` varchar(9) NOT NULL, \
         `branch_code` varchar(2) NOT NULL DEFAULT '00', \
         `report_no` varchar(20) NOT NULL, \
         `report_seqno` varchar(22) NOT NULL, \
         `report_seq` varchar(22) NOT NULL, \
         `reg_id` varchar(22) DEFAULT NULL, \
         report_time datetime, \
         `modify_empid` varchar(10) DEFAULT NULL, \
         `modify_empid_code_org` varchar(64) DEFAULT NULL, \
         `modify_empid_name_org` varchar(256) DEFAULT NULL, \
         `create_time_sys` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6), \
         `create_empid` varchar(10) DEFAULT NULL, \
         `create_empid_code_org` varchar(64) DEFAULT NULL, \
         `create_empid_name_org` varchar(256) DEFAULT NULL, \
         `modify_time_mfs` datetime DEFAULT NULL, \
         `create_time_mfs` datetime DEFAULT NULL, \
         `batch_version` varchar(40) DEFAULT NULL, \
         `batch_type` varchar(10) DEFAULT NULL, \
         `time_correlation_mark` varchar(2) NOT NULL DEFAULT '0', \
         `modify_time_center` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
         `create_time_center` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6), \
         PRIMARY KEY (`report_time`,`org_code`,`branch_code`,`report_no`,`report_seqno`,`report_seq`) NONCLUSTERED\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin \
         PARTITION BY RANGE COLUMNS(`report_time`)\
         (PARTITION `p201001` VALUES LESS THAN ('2010-02-01 00:00:00'), \
          PARTITION `p201002` VALUES LESS THAN ('2010-03-01 00:00:00'), \
          PARTITION `p201003` VALUES LESS THAN ('2010-04-01 00:00:00'), \
          PARTITION `p201004` VALUES LESS THAN ('2010-05-01 00:00:00'), \
          PARTITION `p201005` VALUES LESS THAN ('2010-06-01 00:00:00'), \
          PARTITION `pmax` VALUES LESS THAN (MAXVALUE))",
        &mut catalog,
    )
    .expect("Go: the issue-57780 table builds");
    run_alter_table_in(
        "alter table cis_assay_report_detail add column test_decimal decimal(9,2)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect("Go: add column succeeds");
    run_alter_table_in(
        "alter table cis_assay_report_detail change column test_decimal test_decimal decimal(11,2)",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect("Go: same-name widening change column succeeds");

    // Not in the Go test, but the cheapest proof the widened column is a
    // real column of the partitioned table: a routed insert reads back.
    run_insert_on(
        "insert into cis_assay_report_detail values \
         ('o','00','r1','s1','seq',null,'2010-01-15 12:00:00',null,null,null,null,null,null,null,null,null,null,null,'0',null,null,1.5)",
        &mut catalog,
        &ctx(),
    )
    .expect("the routed insert fills the widened column");
    let rows = run_select_on(
        "select test_decimal from cis_assay_report_detail",
        &catalog,
        &ctx(),
    )
    .expect("select succeeds");
    assert_eq!(rows.len(), 1);
    let Datum::Decimal(value) = &rows[0][0] else {
        panic!("expected the decimal column, got {:?}", rows[0][0]);
    };
    assert_eq!(value.to_string(), "1.50", "decimal(9,2) -> decimal(11,2) keeps the value");
}
