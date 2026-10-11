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

//! BDR roles over the local DDL owner, as `ddl/bdr_mode.test` records them:
//! `ADMIN SET BDR ROLE` stores the role, and a DDL its role denies fails
//! with 8263 (Go `jobsubmit.SubmitBatch` and the ADD/MODIFY COLUMN checks).

use crate::tests_support::row_text;
use crate::Session;

const PRIMARY_DENIED: &str =
    "The operation is not allowed while the bdr role of this cluster is set to primary.";
const SECONDARY_DENIED: &str =
    "The operation is not allowed while the bdr role of this cluster is set to secondary.";

fn denied(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_string()
}

#[test]
fn the_primary_role_allows_safe_ddl_and_denies_the_rest() {
    let mut session = Session::new();
    session.run("admin set bdr role primary").unwrap();
    assert_eq!(
        row_text(session.run("admin show bdr role")),
        vec![vec!["primary"]]
    );
    session.run("create table t(a int)").unwrap();
    session.run("alter table t add column b int").unwrap();
    assert_eq!(
        denied(&mut session, "alter table t drop column b"),
        PRIMARY_DENIED
    );
    session.run("alter table t add index idx_a(a)").unwrap();
    session
        .run("alter table t rename index idx_a to idx_b")
        .unwrap();
    session.run("alter table t drop index idx_b").unwrap();
    // A unique index is refused on the primary role, whatever its spelling.
    assert_eq!(
        denied(&mut session, "alter table t add unique index (a)"),
        PRIMARY_DENIED
    );
    assert_eq!(
        denied(&mut session, "create unique index u on t(a)"),
        PRIMARY_DENIED
    );
    session.run("create index i on t(a)").unwrap();
    session.run("create table t2(a int primary key)").unwrap();
    assert_eq!(
        denied(&mut session, "rename table t to t3, t2 to t4"),
        PRIMARY_DENIED
    );
    assert_eq!(denied(&mut session, "truncate table t2"), PRIMARY_DENIED);
    assert_eq!(denied(&mut session, "drop table t2"), PRIMARY_DENIED);
    assert_eq!(
        denied(&mut session, "alter table t auto_increment = 6000"),
        PRIMARY_DENIED
    );
    session
        .run("alter table t alter column a set default 1")
        .unwrap();
    session.run("alter table t comment = 'test'").unwrap();
    // ADD COLUMN: NOT NULL needs a default.
    assert_eq!(
        denied(&mut session, "alter table t add column d int not null"),
        PRIMARY_DENIED
    );
    session
        .run("alter table t add column d int not null default 10")
        .unwrap();
    // MODIFY COLUMN: the type may not change, and only the default (with an
    // optional comment) may.
    assert_eq!(
        denied(&mut session, "alter table t modify column a bigint"),
        PRIMARY_DENIED
    );
    session
        .run("alter table t modify column a int default 10")
        .unwrap();
    assert_eq!(
        denied(
            &mut session,
            "alter table t modify column a int comment 'c'"
        ),
        PRIMARY_DENIED
    );
    assert_eq!(denied(&mut session, "create sequence seq"), PRIMARY_DENIED);
    session.run("create view v as select 1 as b").unwrap();
    session.run("drop view v").unwrap();
    // A missing table is the builder's error, not the role's.
    assert_eq!(
        denied(&mut session, "truncate table nosuch"),
        "Table 'test.nosuch' doesn't exist"
    );

    session.run("admin unset bdr role").unwrap();
    assert_eq!(row_text(session.run("admin show bdr role")), vec![vec![""]]);
    session.run("alter table t drop column b").unwrap();
    session.run("drop table t2").unwrap();
}

#[test]
fn the_secondary_role_allows_only_unmanaged_ddl() {
    let mut session = Session::new();
    session.run("create table t(a int, b int)").unwrap();
    session.run("admin set bdr role secondary").unwrap();
    assert_eq!(
        denied(&mut session, "alter table t add column c int"),
        SECONDARY_DENIED
    );
    assert_eq!(
        denied(&mut session, "alter table t add index i(a)"),
        SECONDARY_DENIED
    );
    assert_eq!(
        denied(&mut session, "create table t2(a int)"),
        SECONDARY_DENIED
    );
    assert_eq!(denied(&mut session, "create database d2"), SECONDARY_DENIED);
    assert_eq!(denied(&mut session, "drop database test"), SECONDARY_DENIED);
    // Placement policies are unmanaged DDL.
    session
        .run("create placement policy pp followers=4")
        .unwrap();
    session.run("drop placement policy pp").unwrap();
    session.run("admin unset bdr role").unwrap();
}

/// Go copies `tidb_cdc_write_source` into each job, and a job replicated by
/// TiCDC skips the submitter's admission -- but not the ADD COLUMN check.
#[test]
fn a_ddl_from_ticdc_skips_the_submit_admission() {
    let mut session = Session::new();
    session.run("create table t(a int, b int)").unwrap();
    session.run("admin set bdr role primary").unwrap();
    session.run("set @@tidb_cdc_write_source = 1").unwrap();
    session.run("alter table t drop column b").unwrap();
    assert_eq!(
        denied(&mut session, "alter table t add column d int not null"),
        PRIMARY_DENIED
    );
    session.run("admin unset bdr role").unwrap();
}

/// Go `executor.AlterTablePartitionPlacement` (`ddl/bdr_mode.test`'s
/// placement block, and oracle-captured edges): a partition takes the policy
/// reference itself -- SHOW CREATE TABLE and `information_schema.partitions`
/// read it back, and the policy is in use while it holds it -- after Go's
/// unknown-partition (1735), unknown-policy (8239) and unpartitioned-table
/// (1505) refusals, and the job is one the primary BDR role denies.
#[test]
fn a_partition_takes_its_own_placement_policy() {
    let mut session = Session::new();
    session
        .run(
            "create table t(a int) partition by range(a) (partition p0 values less than (5), \
             partition p1 values less than (10), partition p3 values less than (100))",
        )
        .unwrap();
    session
        .run("create placement policy pp4 followers=4")
        .unwrap();
    session
        .run("alter table t partition p3 placement policy=pp4")
        .unwrap();
    let show = row_text(session.run("show create table t"));
    assert!(
        show[0][1].ends_with(
            " PARTITION `p3` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`pp4` */)"
        ),
        "{show:?}"
    );
    let placements = |session: &mut Session| {
        row_text(session.run(
            "select partition_name, tidb_placement_policy_name from information_schema.partitions \
             where table_name = 't' order by partition_name",
        ))
    };
    assert_eq!(
        placements(&mut session),
        [["p0", "NULL"], ["p1", "NULL"], ["p3", "pp4"]]
    );
    session
        .run("alter table t partition p3 placement policy=default")
        .unwrap();
    session
        .run("alter table t partition P1 placement policy=pp4")
        .unwrap();
    assert_eq!(
        placements(&mut session),
        [["p0", "NULL"], ["p1", "pp4"], ["p3", "NULL"]]
    );
    assert_eq!(
        denied(&mut session, "drop placement policy pp4"),
        "Placement policy 'pp4' is still in use"
    );
    assert_eq!(
        denied(
            &mut session,
            "alter table t partition p9 placement policy=pp4"
        ),
        "Unknown partition 'p9' in table 't'"
    );
    assert_eq!(
        denied(
            &mut session,
            "alter table t partition p3 placement policy=nosuch"
        ),
        "Unknown placement policy 'nosuch'"
    );
    session.run("create table t2 (a int)").unwrap();
    assert_eq!(
        denied(
            &mut session,
            "alter table t2 partition p3 placement policy=pp4"
        ),
        "Partition management on a not partitioned table is not possible"
    );
    session.run("admin set bdr role primary").unwrap();
    assert_eq!(
        denied(
            &mut session,
            "alter table t partition p3 placement policy=pp4"
        ),
        PRIMARY_DENIED
    );
    session.run("admin unset bdr role").unwrap();
}
