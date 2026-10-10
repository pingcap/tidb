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

//! `ALTER TABLE ... EXCHANGE PARTITION` and the primary-key and AUTO_RANDOM
//! rules around it, as `ddl/db.test`, `ddl/db_partition.test`,
//! `ddl/exchange_partition_global_index.test` and
//! `ddl/primary_key_handle.test` record.

use crate::tests_support::row_text;
use crate::Session;

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

fn sorted(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let mut rows = row_text(session.run(sql));
    rows.sort();
    rows
}

/// Go `onExchangeTablePartition`: the partition and the table trade their
/// rows, and the analyze warning follows.
#[test]
fn exchange_partition_trades_rows_with_the_table() {
    let mut session = Session::new();
    session
        .run("create table t1 (id int) partition by list (id) (partition p0 values in (1,2,3), partition p1 values in (4,5,6))")
        .unwrap();
    session.run("insert into t1 values (1), (2), (5)").unwrap();
    session.run("create table t2 (id int)").unwrap();
    session.run("insert into t2 values (3)").unwrap();
    session
        .run("alter table t1 exchange partition p0 with table t2")
        .unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        vec![vec![
            "Warning",
            "1105",
            "after the exchange, please analyze related table of the exchange to update statistics"
        ]]
    );
    assert_eq!(
        sorted(&mut session, "select * from t2"),
        vec![vec!["1"], vec!["2"]]
    );
    assert_eq!(
        sorted(&mut session, "select * from t1"),
        vec![vec!["3"], vec!["5"]]
    );
    assert_eq!(
        sorted(&mut session, "select * from t1 partition (p0)"),
        vec![vec!["3"]]
    );
    session.run("insert into t2 values (9)").unwrap();
    session.run("insert into t1 values (4)").unwrap();
    assert_eq!(
        sorted(&mut session, "select * from t1 partition (p1)"),
        vec![vec!["4"], vec!["5"]]
    );
}

/// Go `checkExchangePartitionRecordValidation`: a row of the table that does
/// not belong to the partition is 1737 unless WITHOUT VALIDATION; HASH is
/// Go's literal `mod(expr, num) != index`.
#[test]
fn exchange_partition_validates_the_rows_exchanged_in() {
    let mut session = Session::new();
    let mismatch = (
        1737,
        "Found a row that does not match the partition".to_owned(),
    );
    session
        .run("create table pt (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
        .unwrap();
    session.run("create table nt (a int)").unwrap();
    session.run("insert into nt values (15)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p0 with table nt"
        ),
        mismatch
    );
    session
        .run("alter table pt exchange partition p1 with table nt")
        .unwrap();
    session.run("insert into nt values (25)").unwrap();
    session
        .run("alter table pt exchange partition p0 with table nt without validation")
        .unwrap();

    session
        .run("create table ph (a int) partition by hash (a) partitions 4")
        .unwrap();
    session.run("create table nh (a int)").unwrap();
    session.run("insert into nh values (-1)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table ph exchange partition p1 with table nh"
        ),
        mismatch
    );
    session
        .run("create table pk (a int) partition by key (a) partitions 2")
        .unwrap();
    session.run("create table nk (a int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pk exchange partition p0 with table nk"
        ),
        (
            8200,
            "Unsupported partition type of table pk when exchanging partition".to_owned()
        )
    );
    session
        .run("alter table pk exchange partition p0 with table nk without validation")
        .unwrap();
}

/// Go `checkExchangePartition` and `checkTableDefCompatible`.
#[test]
fn exchange_partition_refuses_incompatible_tables() {
    let mut session = Session::new();
    session
        .run("create table pt (a int, b int) partition by hash (a) partitions 2")
        .unwrap();
    session
        .run("create table wide (a int, b int, c int)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p0 with table wide"
        ),
        (1736, "Tables have different definitions".to_owned())
    );
    session.run("create table typed (a int, b bigint)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p0 with table typed"
        )
        .0,
        1736
    );
    session
        .run("create table gc (a int, b int as (a + 1) virtual)")
        .unwrap();
    assert_eq!(
        code(&mut session, "alter table pt exchange partition p0 with table gc"),
        (
            3106,
            "'Exchanging partitions for non-generated columns' is not supported for generated columns.".to_owned()
        )
    );
    session
        .run("create table other (a int, b int) partition by hash (a) partitions 2")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p0 with table other"
        ),
        (
            1732,
            "Table to exchange with partition is partitioned: 'other'".to_owned()
        )
    );
    session.run("create view v as select 1 a, 2 b").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p0 with table v"
        ),
        (1177, "Can't open table".to_owned())
    );
    session
        .run("create temporary table tmp (a int, b int)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p0 with table tmp"
        ),
        (
            1733,
            "Table to exchange with partition is temporary: 'tmp'".to_owned()
        )
    );
    session.run("create table nt (a int, b int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table pt exchange partition p9 with table nt"
        ),
        (1735, "Unknown partition 'p9' in table 'pt'".to_owned())
    );
    session.run("create table plain (a int, b int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table plain exchange partition p0 with table nt"
        )
        .0,
        1505
    );
}

/// Go `setGlobalIndexVersion`: a non-unique global index keys its entries by
/// partition, so rows of two partitions sharing a `_tidb_rowid` after an
/// exchange each keep their entry.
#[test]
fn a_global_index_after_an_exchange_keeps_every_row() {
    let mut session = Session::new();
    session
        .run("create table t (a int, b int, dt date, primary key (a) nonclustered)")
        .unwrap();
    session
        .run("create table tp (a int, b int, dt date, primary key (a) nonclustered) partition by range (a) (partition p0 values less than (5), partition p1 values less than (11), partition p2 values less than (20))")
        .unwrap();
    session
        .run("insert into tp (a, b) values (2, 2), (4, 4), (6, 6)")
        .unwrap();
    session
        .run("insert into t (a, b) values (12, 2), (14, 4), (16, 6)")
        .unwrap();
    session
        .run("alter table tp exchange partition p2 with table t")
        .unwrap();
    session.run("create index idx_b on tp(b) global").unwrap();
    assert_eq!(
        row_text(session.run("select count(*) from tp use index(idx_b)")),
        vec![vec!["6"]]
    );
}

/// Go `onExchangeTablePartition` puts the larger AUTO_RANDOM counter under
/// both tables, so neither reuses the other's ids.
#[test]
fn exchanged_tables_keep_allocating_past_each_other() {
    let mut session = Session::new();
    session
        .run("create table e1 (a bigint primary key clustered auto_random(3)) partition by hash(a) partitions 1")
        .unwrap();
    session.run("insert into e1 values (), (), ()").unwrap();
    session
        .run("create table e4 (a bigint primary key auto_random(3))")
        .unwrap();
    session.run("insert into e4 values ()").unwrap();
    session
        .run("alter table e1 exchange partition p0 with table e4")
        .unwrap();
    session.run("insert into e1 values ()").unwrap();
    session.run("insert into e4 values ()").unwrap();
    assert_eq!(
        row_text(session.run("select count(*) from e1")),
        vec![vec!["2"]]
    );
    assert_eq!(
        row_text(session.run("select count(*) from e4")),
        vec![vec!["4"]]
    );
}

/// Go `checkInvisibleIndexOnPK`, run on every table a DDL leaves: the
/// explicit or implicit primary key cannot be invisible.
#[test]
fn a_primary_key_index_cannot_be_invisible() {
    let mut session = Session::new();
    let refused = (3522, "A primary key index cannot be invisible".to_owned());
    for sql in [
        "create table t (c1 int not null, primary key(c1) invisible)",
        "create table t (a int, primary key (a) nonclustered invisible)",
        "create table t (a int not null, unique (a) invisible)",
        "create table t (a int not null, b int not null, unique (b) invisible, unique (a))",
    ] {
        assert_eq!(code(&mut session, sql), refused, "{sql}");
    }
    session.run("create table t2 (a int not null)").unwrap();
    assert_eq!(
        code(&mut session, "alter table t2 add unique (a) invisible"),
        refused
    );
    session
        .run("create table t3 (a int, unique (a) invisible)")
        .unwrap();
    assert_eq!(
        code(&mut session, "alter table t3 modify column a int not null"),
        refused
    );
    session
        .run("create table t5 (a int not null, b int not null, unique (a), unique (b) invisible)")
        .unwrap();
    assert_eq!(code(&mut session, "alter table t5 drop index a"), refused);
}

/// Go `allocAutoRandomID` passes the session's increment and offset as they
/// are, and `checkAutoRandom` refuses converting a column that is not the
/// AUTO_INCREMENT clustered handle or changing its type.
#[test]
fn auto_random_follows_increment_offset_and_conversion_rules() {
    let mut session = Session::new();
    session
        .run("create table t (a bigint auto_random(6) primary key clustered)")
        .unwrap();
    session
        .run("set @@auto_increment_increment = 5, @@auto_increment_offset = 10")
        .unwrap();
    session.run("insert into t values (), (), ()").unwrap();
    assert_eq!(
        row_text(session.run(
            "select a & b'111111111111111111111111111111111111111111111111111111111' x from t order by x"
        )),
        vec![vec!["10"], vec!["15"], vec!["20"]]
    );
    session
        .run("set @@auto_increment_increment = 1, @@auto_increment_offset = 1")
        .unwrap();

    session
        .run(
            "create table c (a bigint auto_increment unique key, b bigint auto_random primary key)",
        )
        .unwrap();
    assert_eq!(
        code(&mut session, "alter table c modify column a bigint auto_random"),
        (
            8216,
            "Invalid auto random: auto_random can only be converted from auto_increment clustered primary key".to_owned()
        )
    );
    session
        .run("create table i (a int auto_increment primary key)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table i modify column a bigint auto_random"
        ),
        (
            8216,
            "Invalid auto random: modifying the auto_random column type is not supported"
                .to_owned()
        )
    );

    session.run("set @@tidb_allow_remove_auto_inc = 1").unwrap();
    session
        .run("create table r (a bigint auto_increment primary key)")
        .unwrap();
    session.run("insert into r values (), (), ()").unwrap();
    session
        .run("alter table r modify column a bigint auto_random(3)")
        .unwrap();
    session.run("insert into r values (), (), ()").unwrap();
    assert_eq!(
        row_text(session.run("show table r next_row_id"))[0][3],
        "60002"
    );
}
