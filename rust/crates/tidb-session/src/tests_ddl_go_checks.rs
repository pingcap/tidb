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

//! DDL checks and table metadata Go keeps, as `ddl/db.test`,
//! `ddl/db_integration.test` and `executor/ddl.test` record: TTL and
//! foreign-key validation, temporary-table allocators, column auto-convert,
//! and key-part checks.

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

fn warnings(session: &mut Session) -> Vec<Vec<String>> {
    row_text(session.run("show warnings"))
}

fn create_text(session: &mut Session, table: &str) -> String {
    row_text(session.run(&format!("show create table {table}")))[0][1].clone()
}

/// Go `handleTableOptions` and `checkTTLInfoValid`, in Go's order, and
/// `updateTTLInfoWhenModifyColumn`.
#[test]
fn ttl_options_are_checked_and_follow_a_renamed_column() {
    let mut session = Session::new();
    assert_eq!(
        code(&mut session, "create table t (id int) ttl_enable = 'ON'"),
        (
            8150,
            "Cannot set TTL_ENABLE on a table without TTL config".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "create table t (id int) ttl_job_interval = '1h'"
        )
        .0,
        8150
    );
    session
        .run("create table t (created_at datetime, updated_at datetime) ttl = `updated_at` + interval 2 year")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t change updated_at updated_at_new int"
        ),
        (
            8148,
            "Field 'updated_at_new' is of a not supported type for TTL config, expect DATETIME, DATE or TIMESTAMP".to_owned()
        )
    );
    session
        .run("alter table t rename column updated_at to updated_at_2")
        .unwrap();
    assert!(create_text(&mut session, "t").contains("TTL=`updated_at_2` + INTERVAL 2 YEAR"));
    session.run("create table c like t").unwrap();
    assert!(create_text(&mut session, "c").contains("TTL=`updated_at_2` + INTERVAL 2 YEAR"));
}

/// Go `checkTableForeignKey` and `checkTTLInfoValid`: a TTL table cannot be
/// referenced by a foreign key, nor gain a TTL while referenced.
#[test]
fn a_ttl_table_and_a_foreign_key_parent_exclude_each_other() {
    let mut session = Session::new();
    let refused = (
        8152,
        "Set TTL for a table referenced by foreign key is not allowed".to_owned(),
    );
    session
        .run("create table t (id int primary key, created_at datetime)")
        .unwrap();
    session
        .run("create table c (t_id int, foreign key fk_t_id(t_id) references t(id))")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t ttl = created_at + interval 5 year"
        ),
        refused
    );
    session.run("drop table c, t").unwrap();
    session
        .run("create table t (id int primary key, created_at datetime) ttl = created_at + interval 5 year")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "create table c (t_id int, foreign key fk_t_id(t_id) references t(id))"
        ),
        refused
    );
    session.run("create table c (t_id int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table c add foreign key fk_t_id(t_id) references t(id)"
        ),
        refused
    );
}

/// Go `buildFKInfo`: a writing referential action may not change the base
/// column of a stored generated column.
#[test]
fn a_foreign_key_writing_a_stored_generated_base_is_refused() {
    let mut session = Session::new();
    session.run("create table t2 (a int primary key)").unwrap();
    for action in ["on update cascade", "on delete set null"] {
        assert_eq!(
            code(
                &mut session,
                &format!(
                    "create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) {action})"
                )
            ),
            (1215, "Cannot add foreign key constraint".to_owned())
        );
    }
    session
        .run("create table t1 (a int, b int generated always as (a+1) stored)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t1 add foreign key (a) references t2(a) on delete cascade"
        )
        .0,
        1215
    );
    session
        .run("alter table t1 add foreign key (a) references t2(a) on delete restrict")
        .unwrap();
}

/// Go `checkGeneratedColumn` on ADD COLUMN, naming the column in lowercase.
#[test]
fn an_added_generated_column_reading_auto_increment_is_refused() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key auto_increment, b int)")
        .unwrap();
    assert_eq!(
        code(&mut session, "alter table t add column C int as (a + 1)"),
        (
            3109,
            "Generated column 'c' cannot refer to auto-increment column.".to_owned()
        )
    );
}

/// Go names a view's columns as its body's plan does, and records the
/// session's connection charset and collation on the view.
#[test]
fn a_view_takes_its_column_names_and_collation_from_the_session() {
    let mut session = Session::new();
    session
        .run("set names utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    session
        .run("create view v as select 'cccccccccc', 'a' as b")
        .unwrap();
    let crate::StmtOutput::Rows { columns, .. } =
        session.run_with_columns("select * from v").unwrap()
    else {
        panic!("expected rows");
    };
    let names: Vec<_> = columns.into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["cccccccccc", "b"]);
    assert_eq!(
        row_text(session.run(
            "select character_set_client, collation_connection from information_schema.views where table_name = 'v'"
        )),
        vec![vec!["utf8mb4", "utf8mb4_general_ci"]]
    );
}

/// Go `NewAllocatorFromTempTblInfo`: a temporary table allocates one id at a
/// time, a global one afresh in each transaction, and SHOW TABLE NEXT_ROW_ID
/// reads the domain's infoschema, which holds no local temporary table.
#[test]
fn temporary_tables_allocate_in_memory_one_id_at_a_time() {
    let mut session = Session::new();
    let next_row_id = |session: &mut Session, table: &str| {
        row_text(session.run(&format!("show table {table} next_row_id")))[0][3].clone()
    };
    session
        .run("create global temporary table g (id int primary key auto_increment) auto_increment = 100 on commit delete rows")
        .unwrap();
    assert_eq!(next_row_id(&mut session, "g"), "100");
    session.run("begin").unwrap();
    session.run("insert into g values (null)").unwrap();
    assert_eq!(next_row_id(&mut session, "g"), "101");
    session.run("commit").unwrap();
    assert_eq!(next_row_id(&mut session, "g"), "100");

    session
        .run("create temporary table l (id int primary key auto_increment) auto_increment = 100")
        .unwrap();
    assert_eq!(code(&mut session, "show table l next_row_id").0, 1146);
    session.run("insert into l values (null)").unwrap();
    assert_eq!(
        row_text(session.run("select @@last_insert_id")),
        vec![vec!["100"]]
    );
    assert!(create_text(&mut session, "l").contains("AUTO_INCREMENT=101"));
}

/// Go `checkTooBigFieldLengthAndTryAutoConvert` (issue #30328): outside strict
/// mode an over-long VARCHAR becomes TEXT/BLOB with a warning.
#[test]
fn an_over_long_varchar_converts_outside_strict_mode() {
    let mut session = Session::new();
    session
        .run("set @@sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    session
        .run("create table t (a varbinary(70000), b varchar(70000000))")
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![
            vec![
                "Warning",
                "1246",
                "Converting column 'a' from VARBINARY to BLOB"
            ],
            vec![
                "Warning",
                "1246",
                "Converting column 'b' from VARCHAR to TEXT"
            ],
        ]
    );
    session.run("create table u (a varchar(200))").unwrap();
    session
        .run("alter table u modify a varchar(70000000)")
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![vec![
            "Warning",
            "1246",
            "Converting column 'a' from VARCHAR to TEXT"
        ]]
    );
    assert!(create_text(&mut session, "u").contains("`a` longtext"));
}

/// Go raises a truncated index comment's warning once per ALTER.
#[test]
fn a_long_index_comment_warns_once() {
    let mut session = Session::new();
    session.run("set sql_mode = ''").unwrap();
    session.run("create table t (c int, e int)").unwrap();
    session
        .run(&format!(
            "alter table t add key (e) comment '{}'",
            "a".repeat(1025)
        ))
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![vec![
            "Warning",
            "1688",
            "Comment for index 'e' is too long (max = 1024)"
        ]]
    );
}

/// Go `checkModifyTypes` refuses a primary key change only when it needs
/// reorganization: widening an INT handle is metadata-only.
#[test]
fn a_primary_key_column_may_widen_without_reorganization() {
    let mut session = Session::new();
    session
        .run("create table t (k int primary key, v int)")
        .unwrap();
    session
        .run("alter table t change column k k bigint")
        .unwrap();
    assert_eq!(row_text(session.run("show columns from t"))[0][1], "bigint");
    assert_eq!(
        code(&mut session, "alter table t modify k int").1,
        "Unsupported modify column: this column has primary key flag"
    );
}

/// Go keeps `FlagIgnoreZeroDateErr` on a write, so a zero date written as
/// `0` is decided by `NO_ZERO_DATE` alone, and UNIX_TIMESTAMP of a zero
/// TIMESTAMP is 0.
#[test]
fn a_zero_timestamp_is_written_and_read_as_go_does() {
    let mut session = Session::new();
    session
        .run("set session sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE'")
        .unwrap();
    session
        .run("create table t (a timestamp default 0)")
        .unwrap();
    session.run("insert into t values (0)").unwrap();
    assert_eq!(
        row_text(session.run("select a, unix_timestamp(a) from t")),
        vec![vec!["0000-00-00 00:00:00", "0"]]
    );
    session
        .run("set session sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'")
        .unwrap();
    assert_eq!(
        code(&mut session, "update t set a = 0 where a = 0"),
        (1292, "Incorrect timestamp value: '0'".to_owned())
    );
}

/// Go `checkIndexColumn` reaches inline keys, and `checkIsDroppableColumn`
/// looks only at visible columns.
#[test]
fn inline_keys_and_hidden_columns_are_checked() {
    let mut session = Session::new();
    for sql in [
        "create table t (a text primary key)",
        "create table t (a text unique)",
    ] {
        assert_eq!(
            code(&mut session, sql),
            (
                1170,
                "BLOB/TEXT column 'a' used in key specification without a key length".to_owned()
            )
        );
    }
    session.run("create table t (a int, b int)").unwrap();
    session.run("alter table t add index ((a + 1))").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t drop column _V$_expression_index_0"
        ),
        (
            1091,
            "Can't DROP '_V$_expression_index_0'; check that column/key exists".to_owned()
        )
    );
}

/// `ddl/table_create_compat.test`: Go's parser warns, at the next token, for
/// a table option no storage engine honours (`HandParser.warnNear`), and
/// `util.SyntaxWarn` reports it as 1064 behind the syntax prefix.
#[test]
fn an_ignored_table_option_warns_as_go_parser_does() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a (id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M")
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        [[
            "Warning",
            "1064",
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             TiDB version for the right syntax to use line 1 column 54 near \"\"The \
             AUTOEXTEND_SIZE option is parsed but ignored by all storage engines. "
        ]]
    );
}

/// Go `CreatePrimaryKey` / `onCreateIndex(isPK)` and `CheckIsDropPrimaryKey`:
/// ALTER TABLE adds a NONCLUSTERED primary key (its nullable columns become
/// NOT NULL and PRI, a NULL row is 1138, a second key 1068, CLUSTERED 8200)
/// and drops it, refusing a clustered one (8200).
#[test]
fn alter_table_adds_and_drops_a_nonclustered_primary_key() {
    let mut s = Session::new();
    s.run("create table t (a int)").unwrap();
    s.run("insert into t values (123)").unwrap();
    s.run("alter table t add index i(a), add primary key (a)")
        .unwrap();
    let ddl = row_text(s.run("show create table t"))[0][1].clone();
    assert!(ddl.contains("`a` int NOT NULL"), "{ddl}");
    assert!(
        ddl.contains("PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */"),
        "{ddl}"
    );
    assert_eq!(row_text(s.run("desc t"))[0][3], "PRI");
    assert_eq!(code(&mut s, "alter table t add primary key (a)").0, 1068);
    s.run("alter table t drop primary key").unwrap();
    assert_eq!(code(&mut s, "select * from t use index(primary)").0, 1176);
    assert_eq!(row_text(s.run("desc t"))[0][3], "MUL");

    s.run("create table n (a int, b int)").unwrap();
    s.run("insert into n values (null, 1)").unwrap();
    assert_eq!(code(&mut s, "alter table n add primary key (a)").0, 1138);
    assert_eq!(
        code(&mut s, "alter table n add primary key (b) clustered"),
        (
            8200,
            "Adding clustered primary key is not supported. Please consider adding NONCLUSTERED primary key instead".to_owned()
        )
    );
    s.run("create table c (a int primary key)").unwrap();
    assert_eq!(
        code(&mut s, "alter table c drop primary key"),
        (
            8200,
            "Unsupported drop primary key when the table is using clustered index".to_owned()
        )
    );
}

/// Go `checkOperateSameColAndIdx` registers position columns before modify
/// columns, so `MODIFY a ... AFTER a` passes the multi-schema check and fails
/// in the job (`validatePosition`, 1054); a sibling index on `a` conflicts
/// first (8200). Sub-job rebase adjustments are Notes.
#[test]
fn multi_schema_position_and_rebase_follow_go() {
    let mut s = Session::new();
    s.run("create table t(a int, b int)").unwrap();
    assert_eq!(
        code(
            &mut s,
            "alter table t add index i(b), modify column a int null default 1 after a"
        ),
        (1054, "Unknown column 'a' in 't'".to_owned())
    );
    assert_eq!(
        code(
            &mut s,
            "alter table t add index i(a), modify column a int null default 1 after a"
        )
        .0,
        8200
    );
    s.run("create table ai (a int auto_increment primary key, b int)")
        .unwrap();
    s.run("alter table ai auto_increment = 110, auto_increment = 90")
        .unwrap();
    assert_eq!(
        warnings(&mut s),
        vec![vec![
            "Note",
            "1105",
            "Can't reset AUTO_INCREMENT to 90 without FORCE option, using 110 instead"
        ]]
    );
}

/// Go `GetTimeValue` -> `ParseTime`: a temporal DEFAULT with a seventh part is
/// truncated to six with the warning `parseDatetime` appends directly, and
/// kept (DATE stores `2020-03-27`), not refused as an invalid default.
#[test]
fn truncated_temporal_default_warns_and_is_kept() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session
        .run("alter table t add column da1 date default '2020-03-27 20:20:20 123456'")
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![vec![
            "Warning".to_owned(),
            "1292".to_owned(),
            "Truncated incorrect datetime value: '2020-03-27 20:20:20 123456'".to_owned(),
        ]]
    );
    assert!(create_text(&mut session, "t").contains("`da1` date DEFAULT '2020-03-27'"));
    assert_eq!(
        code(
            &mut session,
            "alter table t add column da2 date default '2020-02-30'"
        )
        .0,
        1067
    );
}

/// Go `decodeEnumSetBinaryLiteralToUTF8`: an ENUM/SET member written as a
/// hex literal is text in the column charset, decoded with `?` for each
/// invalid sequence, before the duplicate check runs. Captured from Go TiDB.
#[test]
fn enum_set_binary_literal_members_decode_in_the_column_charset() {
    let mut session = Session::new();
    session
        .run("create table e1 (a enum('a', 0x91) charset gbk)")
        .unwrap();
    assert!(create_text(&mut session, "e1")
        .contains("`a` enum('a','?') CHARACTER SET gbk COLLATE gbk_chinese_ci DEFAULT NULL"));
    session.run("create table e4 (a enum('a', 0xff))").unwrap();
    assert!(create_text(&mut session, "e4").contains("`a` enum('a','?') DEFAULT NULL"));
    session
        .run("create table e5 (a set('x', 0x61) charset gbk)")
        .unwrap();
    assert!(create_text(&mut session, "e5").contains("`a` set('x','a') CHARACTER SET gbk"));
    session
        .run("create table e3 (a enum('a', 0xE4BDA0E5A5BD) charset gbk)")
        .unwrap();
    session.run("insert into e3 values (1), (2)").unwrap();
    assert_eq!(
        row_text(session.run("select a from e3")),
        vec![vec!["a".to_owned()], vec!["浣犲ソ".to_owned()]]
    );
    assert_eq!(
        code(
            &mut session,
            "create table e2 (a set('a', 0x91, '?') charset gbk)"
        ),
        (
            1291,
            "Column 'a' has duplicated value '?' in SET".to_owned()
        )
    );
}

/// Go `ALTER TABLE ... [PARTITION p] ATTRIBUTES` over the mock store's label
/// manager, read back through `information_schema.ATTRIBUTES`, and the rule
/// upkeep of the jobs that rename, re-ID, add, drop or exchange what a rule
/// names. Captured from Go TiDB (rule ranges are each ID's mem-comparable
/// table prefix, up to the next ID's).
#[test]
fn attributes_label_rules_follow_go() {
    fn key_range(id: i64) -> String {
        let prefix = |id: i64| {
            let mut encoded = Vec::new();
            tidb_codec::encode_bytes(&mut encoded, &tidb_tablecodec::table_key::gen_table_prefix(id));
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        format!("[{}, {}]", prefix(id), prefix(id + 1))
    }
    fn ranges(ids: &[i64]) -> String {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.iter()
            .map(|id| key_range(*id))
            .collect::<Vec<_>>()
            .join(", ")
    }
    fn attributes(session: &mut Session) -> Vec<Vec<String>> {
        row_text(session.run("select * from information_schema.attributes order by id"))
    }
    fn table_id(session: &mut Session, table: &str) -> i64 {
        row_text(session.run(&format!(
            "select tidb_table_id from information_schema.tables where table_name = '{table}'"
        )))[0][0]
            .parse()
            .unwrap()
    }
    fn partition_ids(session: &mut Session, table: &str) -> Vec<(String, i64)> {
        row_text(session.run(&format!(
            "select partition_name, tidb_partition_id from information_schema.partitions \
             where table_name = '{table}' order by partition_ordinal_position"
        )))
        .into_iter()
        .map(|row| (row[0].clone(), row[1].parse().unwrap()))
        .collect()
    }
    fn row(id: &str, labels: &str, ids: &[i64]) -> Vec<String> {
        vec![
            id.to_owned(),
            "key-range".to_owned(),
            labels.to_owned(),
            ranges(ids),
        ]
    }

    let mut session = Session::new();
    session.run("use test").unwrap();
    session
        .run(
            "create table t (c int) partition by range (c) \
             (partition p0 values less than (6), partition p1 values less than (11))",
        )
        .unwrap();
    session
        .run("alter table t attributes=\"key=value\"")
        .unwrap();
    session
        .run("alter table t partition p0 attributes=\"key1=value1\"")
        .unwrap();
    let t = table_id(&mut session, "t");
    let parts = partition_ids(&mut session, "t");
    let (p0, p1) = (parts[0].1, parts[1].1);
    assert_eq!(
        attributes(&mut session),
        [
            row("schema/test/t", "\"key=value\"", &[t, p0, p1]),
            row("schema/test/t/p0", "\"key1=value1\"", &[p0]),
        ]
    );

    // RENAME moves both rules to the new name.
    session.run("rename table t to t2").unwrap();
    assert_eq!(
        attributes(&mut session),
        [
            row("schema/test/t2", "\"key=value\"", &[t, p0, p1]),
            row("schema/test/t2/p0", "\"key1=value1\"", &[p0]),
        ]
    );

    // TRUNCATE PARTITION follows the partition's new ID.
    session.run("alter table t2 truncate partition p0").unwrap();
    let p0 = partition_ids(&mut session, "t2")[0].1;
    assert_eq!(
        attributes(&mut session),
        [
            row("schema/test/t2", "\"key=value\"", &[t, p0, p1]),
            row("schema/test/t2/p0", "\"key1=value1\"", &[p0]),
        ]
    );

    // ADD widens the table's rule; DROP removes the dropped partition's.
    session
        .run("alter table t2 add partition (partition p2 values less than (20))")
        .unwrap();
    let p2 = partition_ids(&mut session, "t2")[2].1;
    session.run("alter table t2 drop partition p0").unwrap();
    assert_eq!(
        attributes(&mut session),
        [row("schema/test/t2", "\"key=value\"", &[t, p1, p2])]
    );

    // REORGANIZE spans the reorganized partition as well as its successors.
    session
        .run(
            "alter table t2 reorganize partition p1 into \
             (partition p1a values less than (8), partition p1b values less than (11))",
        )
        .unwrap();
    let parts = partition_ids(&mut session, "t2");
    let (p1a, p1b) = (parts[0].1, parts[1].1);
    assert_eq!(
        attributes(&mut session),
        [row(
            "schema/test/t2",
            "\"key=value\"",
            &[t, p1, p2, p1a, p1b]
        )]
    );

    assert_eq!(
        code(&mut session, "alter table t2 attributes=\"a=b,c\""),
        (
            8237,
            "Invalid attributes: attributes should be in format 'key=value': c".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t2 attributes=\"merge_option=allow,merge_option=deny\""
        ),
        (
            8237,
            "Invalid attributes: 'merge_option=deny' and 'merge_option=allow' are conflicted"
                .to_owned()
        )
    );
    // An empty list clears the rule, as DEFAULT does.
    session.run("alter table t2 attributes=\"\"").unwrap();
    assert_eq!(attributes(&mut session), Vec::<Vec<String>>::new());
    session
        .run("alter table t2 attributes=\"key=value,key2=value2\"")
        .unwrap();
    assert_eq!(
        attributes(&mut session),
        [row(
            "schema/test/t2",
            "\"key=value\",\"key2=value2\"",
            &[t, p1a, p1b, p2]
        )]
    );

    session.run("create table n (c int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table n partition p0 attributes=\"k=v\""
        )
        .0,
        1505
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t2 partition px attributes=\"k=v\""
        ),
        (1735, "Unknown partition 'px' in table 't2'".to_owned())
    );
    session.run("create temporary table tt (c int)").unwrap();
    assert_eq!(
        code(&mut session, "alter table tt attributes=\"k=v\""),
        (
            8200,
            "TiDB doesn't support ALTER TABLE for local temporary table".to_owned()
        )
    );
    session.run("create view v as select 1").unwrap();
    assert_eq!(
        code(&mut session, "alter table v attributes=\"k=v\"").0,
        1347
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t2 attributes=\"x=y\", add column d int"
        ),
        (
            8200,
            "Unsupported multi schema change for alter table attributes".to_owned()
        )
    );

    // EXCHANGE swaps the standalone table's rule and the partition's.
    session.run("create table nt (c int)").unwrap();
    session.run("alter table nt attributes=\"n=t\"").unwrap();
    session
        .run("alter table t2 partition p2 attributes=\"p=two\"")
        .unwrap();
    session
        .run("alter table t2 exchange partition p2 with table nt")
        .unwrap();
    let nt = table_id(&mut session, "nt");
    let p2 = partition_ids(&mut session, "t2")[2].1;
    session.run("alter table t2 attributes=default").unwrap();
    assert_eq!(
        attributes(&mut session),
        [
            row("schema/test/nt", "\"p=two\"", &[nt]),
            row("schema/test/t2/p2", "\"n=t\"", &[p2]),
        ]
    );

    // A dropped table's rule stays in the manager but is not shown, and a
    // dropped schema's rules go with it.
    session.run("drop table nt").unwrap();
    session.run("create database adb").unwrap();
    session.run("create table adb.a (c int)").unwrap();
    session.run("alter table adb.a attributes=\"k=v\"").unwrap();
    session.run("rename table adb.a to adb.a2").unwrap();
    let a2 = table_id(&mut session, "a2");
    assert_eq!(
        attributes(&mut session),
        [
            row("schema/adb/a2", "\"k=v\"", &[a2]),
            row("schema/test/t2/p2", "\"n=t\"", &[p2]),
        ]
    );
    session.run("drop database adb").unwrap();
    session.run("create database adb").unwrap();
    session.run("create table adb.a2 (c int)").unwrap();
    assert_eq!(
        attributes(&mut session),
        [row("schema/test/t2/p2", "\"n=t\"", &[p2])]
    );
}

/// Go `CompactTableStmt` (`executor/compact_table`): the replica kind and
/// partition list are checked by the executor builder, a table with no
/// TiFlash replica is skipped with a warning, and the statement keeps the
/// open transaction, as it is not a DDL node.
#[test]
fn compact_table_follows_gos_checks() {
    let mut session = Session::new();
    session.run("create table t(a int)").unwrap();
    session
        .run("create table pt(a int) partition by range(a) (partition p0 values less than (5), partition p1 values less than (10))")
        .unwrap();
    for sql in [
        "alter table t compact tiflash replica",
        "alter table t compact",
        "alter table pt compact partition p1 tiflash replica",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
        let warnings = session.warnings();
        assert_eq!(warnings.len(), 1, "{sql}");
        assert_eq!(warnings[0].code(), 1105, "{sql}");
        assert_eq!(
            warnings[0].message(), "compact skipped: no tiflash replica in the table",
            "{sql}"
        );
    }
    let error = session
        .run("alter table t compact partition p1,p2 tiflash replica")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1105);
    assert_eq!(
        error.message,
        "table:t is not a partition table, but user specify partition name list:[p1 p2]"
    );
    let error = session
        .run("alter table pt compact partition p1,p4 tiflash replica")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1735, "{}", error.message);
    assert_eq!(error.message, "Unknown partition 'p4' in table 'pt'");
    let error = session
        .run("alter table t compact tikv replica")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.message, "compact tikv replica is not supported");
    // Not a DDL node: the transaction's row is not committed by it.
    session.run("begin").unwrap();
    session.run("insert into t values (1)").unwrap();
    session.run("alter table t compact").unwrap();
    session.run("rollback").unwrap();
    assert!(row_text(session.run("select * from t")).is_empty());
}

/// Go `ast.ProcedureInfo` is a plain `stmtNode`, not a `DDLNode`, and
/// `PlanBuilder.Build` has no builder for it (pkg/planner/core/planbuilder.go,
/// the closing `ErrUnsupportedType`): CREATE PROCEDURE fails at compile with
/// 8108 before anything commits, so the open transaction survives it.
#[test]
fn create_procedure_is_unsupported_and_keeps_the_transaction() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    for sql in [
        "create procedure p() begin select 1; end",
        "create procedure if not exists p(in x int) begin select x; end",
    ] {
        assert_eq!(
            code(&mut session, sql),
            (8108, "Unsupported type *ast.ProcedureInfo".to_owned()),
            "{sql}"
        );
    }
    // Not a DDL node: no implicit commit, so the ROLLBACK takes back the
    // row inserted before the refused statement.
    session.run("begin").unwrap();
    session.run("insert into t values (1)").unwrap();
    assert_eq!(
        code(&mut session, "create procedure p() begin select 1; end").0,
        8108
    );
    session.run("insert into t values (2)").unwrap();
    session.run("rollback").unwrap();
    assert!(row_text(session.run("select * from t")).is_empty());
}

/// Go `buildSimple` answers `DropProcedureStmt` with `ErrSpDoesNotExist`
/// (1305) unless `IF EXISTS`, which only appends the same error as a note.
/// `SimpleExec` has no arm for it and `autoNewTxn` does not list it, so it
/// never commits the open transaction.
#[test]
fn drop_procedure_follows_build_simple() {
    let mut session = Session::new();
    session.run("create database pdb").unwrap();
    session.run("use pdb").unwrap();
    session.run("create table t (a int)").unwrap();
    assert_eq!(
        code(&mut session, "drop procedure p"),
        (1305, "PROCEDURE pdb.p does not exist".to_owned())
    );
    session.run("drop procedure if exists p").unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![vec![
            "Note".to_owned(),
            "1305".to_owned(),
            "PROCEDURE pdb.p does not exist".to_owned()
        ]]
    );
    session.run("begin").unwrap();
    session.run("insert into t values (1)").unwrap();
    session.run("drop procedure if exists p").unwrap();
    session.run("rollback").unwrap();
    assert!(row_text(session.run("select * from t")).is_empty());
}

/// Go `TestDefaultCollationForUTF8MB4` (`pkg/ddl/db_integration_test.go`):
/// `BuildTableInfoWithStmt` hands `@@default_collation_for_utf8mb4` to
/// `GetCharsetAndCollateInTableOption` and `ResolveCharsetCollation`, so a
/// table naming only `CHARSET utf8mb4` takes it, while one naming no charset
/// takes the database's pair and one naming a collation keeps it.
#[test]
fn create_table_takes_the_default_collation_for_utf8mb4() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    session
        .run("set @@session.default_collation_for_utf8mb4='utf8mb4_general_ci'")
        .unwrap();
    session.run("create table t1 (b char(1) default null)").unwrap();
    assert_eq!(
        create_text(&mut session, "t1"),
        "CREATE TABLE `t1` (\n  `b` char(1) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    session
        .run("create table t4 (b char(1) default null) engine=InnoDB default charset=utf8mb4")
        .unwrap();
    assert_eq!(
        create_text(&mut session, "t4"),
        "CREATE TABLE `t4` (\n  `b` char(1) COLLATE utf8mb4_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
    );
    session
        .run("create table t2 (b char(1) default null) engine=InnoDB default charset=utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        .unwrap();
    assert_eq!(
        create_text(&mut session, "t2"),
        "CREATE TABLE `t2` (\n  `b` char(1) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    );
}

/// Go `checkTableInfoValidWithStmt`: under `sql_require_primary_key` a table
/// without a primary key is `ErrTableWithoutPrimaryKey` (3750) -- written
/// out, copied by `LIKE`, and as a LOCAL temporary table built either way
/// (`BuildSessionTemporaryTableInfo` runs the check for both branches).
#[test]
fn sql_require_primary_key_refuses_a_table_without_one() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    session.run("create table nopk (a int)").unwrap();
    session.run("set @@session.sql_require_primary_key = 1").unwrap();
    for sql in [
        "create table t (a int)",
        "create table t (a int, unique key (a))",
        "create table t like nopk",
        "create temporary table t (a int)",
        "create temporary table t like nopk",
    ] {
        assert_eq!(code(&mut session, sql).0, 3750, "{sql}");
    }
    session.run("create table t (a int primary key)").unwrap();
    session.run("create table t2 like t").unwrap();
    session.run("create temporary table t3 (a int primary key)").unwrap();
    session.run("set @@session.sql_require_primary_key = 0").unwrap();
    session.run("create table t4 like nopk").unwrap();
}

/// Go `checkIndexColumn` refuses an ordinary index over a VECTOR column
/// (`TestVectorIndexSyntax`, `pkg/expression/integration_test`): only a
/// VECTOR INDEX may cover one.
#[test]
fn an_ordinary_index_over_a_vector_column_is_refused() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    for sql in [
        "CREATE TABLE t1 (embedding VECTOR UNIQUE)",
        "CREATE TABLE t1 (embedding VECTOR, INDEX idx (embedding))",
    ] {
        let (code, message) = code(&mut session, sql);
        assert_eq!(code, 8200, "{sql}");
        assert!(
            message.contains("only VECTOR INDEX can be added to vector column"),
            "{sql}: {message}"
        );
    }
    session.run("create table t2 (embedding VECTOR)").unwrap();
    let (code, message) = code(&mut session, "alter table t2 add index idx (embedding)");
    assert_eq!(code, 8200);
    assert!(message.contains("only VECTOR INDEX can be added to vector column"), "{message}");
}
