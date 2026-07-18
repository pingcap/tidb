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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `INSERT`/`UPDATE`/`DELETE` execution tests, including
//! foreign-key enforcement.

use super::*;

#[test]
fn dml_envelope_dispatches_insert_update_and_delete() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table dml_envelope (a int)"), "OK");
    assert_eq!(step(&mut db, "insert into dml_envelope values (1)"), "OK");
    assert_eq!(step(&mut db, "update dml_envelope set a = 2"), "OK");
    assert_eq!(step(&mut db, "delete from dml_envelope"), "OK");
    assert_eq!(step(&mut db, "select * from dml_envelope"), "RS:");
}

#[test]
fn update_derived_target_is_rejected_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table update_derived_boundary (a int)");
    step(&mut db, "begin");
    step(&mut db, "insert into update_derived_boundary values (1)");
    assert!(matches!(
        db.run(
            &tidb_parser::parse("update (select * from update_derived_boundary) d set a = default")
                .expect("parse UPDATE derived target")
        ),
        Err(ExecError::Unsupported("UPDATE derived table target"))
    ));
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select a from update_derived_boundary"),
        "RS:"
    );
}

#[test]
fn with_dml_is_rejected_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table with_dml_boundary (a int)");
    step(&mut db, "begin");
    step(&mut db, "insert into with_dml_boundary values (1)");
    assert!(matches!(
        db.run(
            &tidb_parser::parse("with cte(a) as (select 1) update with_dml_boundary set a=2")
                .expect("parse WITH UPDATE")
        ),
        Err(ExecError::Unsupported("WITH DML"))
    ));
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select a from with_dml_boundary"), "RS:");
}

#[test]
fn insert_select_executes_typed_cte_source() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table cte_target (a int)"), "OK");
    assert_eq!(
        step(
            &mut db,
            "insert into cte_target with cte(a) as (select 7) select a from cte",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "select * from cte_target"), "RS:7");
}

#[test]
fn insert_with_table_lock_is_rejected_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table insert_table_lock_boundary (a int)");
    step(&mut db, "insert into insert_table_lock_boundary values (1)");
    step(&mut db, "begin");
    assert!(matches!(
        db.run(&tidb_parser::parse(
            "insert into insert_table_lock_boundary with ta2 as (table insert_table_lock_boundary) table ta2 for update of ta2"
        ).expect("parse INSERT WITH TABLE lock")),
        Err(ExecError::Unsupported("SELECT locking"))
    ));
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select a from insert_table_lock_boundary"),
        "RS:1"
    );
}

#[test]
fn insert_values_empty_materializes_one_all_default_row() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table insert_defaults (a int default 7, b int, id int auto_increment primary key)",
    );
    assert_eq!(step(&mut db, "insert into insert_defaults values ()"), "OK");
    assert_eq!(
        step(&mut db, "select a, b, id from insert_defaults"),
        "RS:7|<nil>|1"
    );
}

#[test]
fn auto_increment_consumes_before_conflicts_rebases_and_survives_rollback() {
    let mut db = Database::new();
    assert_eq!(
        step(
            &mut db,
            "create table ai (id bigint unsigned auto_increment primary key, v int unique) auto_increment=100",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "insert into ai (v) values (1), (2)"), "OK");
    assert_eq!(
        step(&mut db, "select id, v from ai order by id"),
        "RS:100|1;101|2"
    );
    assert_eq!(
        step(&mut db, "select last_insert_id(), @@identity"),
        "RS:100|100"
    );

    // Both a hard duplicate and IGNORE consume an ID before their conflict
    // result. Only the hard error publishes it as the statement's insert ID.
    assert_eq!(
        step(&mut db, "insert into ai (v) values (1)"),
        "DuplicateKey"
    );
    assert_eq!(step(&mut db, "select last_insert_id()"), "RS:102");
    assert_eq!(step(&mut db, "insert ignore into ai (v) values (1)"), "OK");
    assert_eq!(step(&mut db, "select last_insert_id()"), "RS:102");
    assert_eq!(step(&mut db, "insert into ai (v) values (3)"), "OK");
    assert_eq!(
        step(&mut db, "select id, v from ai order by id"),
        "RS:100|1;101|2;104|3"
    );

    assert_eq!(step(&mut db, "insert into ai values (200, 4)"), "OK");
    assert_eq!(step(&mut db, "insert into ai (v) values (5)"), "OK");
    assert_eq!(
        step(&mut db, "select id, v from ai order by id"),
        "RS:100|1;101|2;104|3;200|4;201|5"
    );

    step(&mut db, "begin");
    step(&mut db, "insert into ai (v) values (6)");
    step(&mut db, "rollback");
    step(&mut db, "insert into ai (v) values (7)");
    assert_eq!(
        step(&mut db, "select id, v from ai order by id"),
        "RS:100|1;101|2;104|3;200|4;201|5;203|7"
    );

    step(&mut db, "update ai set id = 300 where v = 7");
    step(&mut db, "insert into ai (v) values (8)");
    step(&mut db, "update ai set id = 0 where v = 8");
    step(&mut db, "insert into ai (v) values (9)");
    assert_eq!(
        step(&mut db, "select id, v from ai order by id"),
        "RS:0|8;100|1;101|2;104|3;200|4;201|5;300|7;302|9"
    );
}

#[test]
fn insert_on_duplicate_key_update_and_ignore() {
    let mut db = Database::new();
    step(&mut db, "create table cnt (id int primary key, v int)");
    step(&mut db, "insert into cnt values (1, 10)");
    // A conflict applies the ON DUPLICATE KEY UPDATE clause.
    step(
        &mut db,
        "insert into cnt (id, v) values (1, 20) on duplicate key update v = 20",
    );
    // A non-conflicting row inserts normally despite the clause's presence.
    step(
        &mut db,
        "insert into cnt (id, v) values (2, 30) on duplicate key update v = 999",
    );
    assert_eq!(
        step(&mut db, "select id, v from cnt order by id"),
        "RS:1|20;2|30"
    );
    // VALUES(col) refers to the proposed row's own value.
    step(
        &mut db,
        "insert into cnt (id, v) values (1, 5) on duplicate key update v = v + values(v)",
    );
    assert_eq!(step(&mut db, "select v from cnt where id = 1"), "RS:25");
    // IGNORE silently keeps the existing row on conflict.
    step(&mut db, "insert ignore into cnt (id, v) values (1, 777)");
    step(&mut db, "insert ignore into cnt (id, v) values (3, 40)");
    assert_eq!(
        step(&mut db, "select id, v from cnt order by id"),
        "RS:1|25;2|30;3|40"
    );

    // A bare DEFAULT in the conflict assignment resolves against its target
    // column's declared default, not the proposed row's value.
    let mut defaults = Database::new();
    step(
        &mut defaults,
        "create table default_cnt (id int primary key, v int default 7)",
    );
    step(&mut defaults, "insert into default_cnt values (1, 20)");
    step(
        &mut defaults,
        "insert into default_cnt values (1, 99) on duplicate key update v=default",
    );
    assert_eq!(step(&mut defaults, "select v from default_cnt"), "RS:7");
}

#[test]
fn update_and_delete() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int)");
    step(&mut db, "insert into t values (1, 100), (2, 200), (3, 300)");
    step(&mut db, "update t set b = 999 where a = 2");
    assert_eq!(
        step(&mut db, "select a, b from t order by a"),
        "RS:1|100;2|999;3|300"
    );
    // No WHERE touches every row.
    step(&mut db, "update t set b = b + 1");
    assert_eq!(
        step(&mut db, "select a, b from t order by a"),
        "RS:1|101;2|1000;3|301"
    );
    // A SET clause's expressions see the ORIGINAL row simultaneously,
    // not chained: `b = a` reads the OLD `a`, not the `a + 1` just
    // computed in the same clause (confirmed via gorun, not assumed).
    step(&mut db, "update t set a = a + 1, b = a where a = 1");
    assert_eq!(
        step(&mut db, "select a, b from t order by a, b"),
        "RS:2|1;2|1000;3|301"
    );
    // A WHERE matching nothing is a no-op, not an error.
    step(&mut db, "update t set b = -1 where a = 999");
    step(&mut db, "delete from t where a = 3");
    assert_eq!(
        step(&mut db, "select a, b from t order by a, b"),
        "RS:2|1;2|1000"
    );
    // No WHERE deletes every row.
    step(&mut db, "delete from t");
    assert_eq!(step(&mut db, "select * from t"), "RS:");

    // A table alias is usable in both UPDATE's and DELETE's own
    // WHERE/SET.
    step(&mut db, "create table u (id int, v int)");
    step(&mut db, "insert into u values (1, 10), (2, 20)");
    step(&mut db, "update u as x set x.v = x.v * 10 where x.id = 1");
    step(&mut db, "delete from u as y where y.id = 2");
    assert_eq!(step(&mut db, "select * from u"), "RS:1|100");

    // The `IGNORE` modifier executes as a normal UPDATE/DELETE here (the
    // type-erased executor raises none of the errors `IGNORE` suppresses
    // in real MySQL on this happy path).
    step(&mut db, "create table w (a int, b int)");
    step(&mut db, "insert into w values (1, 10), (2, 20)");
    step(&mut db, "update ignore w set b = 99 where a = 1");
    assert_eq!(
        step(&mut db, "select a, b from w order by a"),
        "RS:1|99;2|20"
    );
    step(&mut db, "delete ignore from w where a = 2");
    assert_eq!(step(&mut db, "select a, b from w"), "RS:1|99");
}

#[test]
fn single_table_update_default_uses_the_target_column_default() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table defaults (id int, a int default 7, b int)",
    );
    step(&mut db, "insert into defaults values (1, 99, 4)");
    assert_eq!(
        step(
            &mut db,
            "update defaults set a=default, b=default where id=1"
        ),
        "OK"
    );
    assert_eq!(
        step(&mut db, "select id, a, b from defaults"),
        "RS:1|7|<nil>"
    );
}

#[test]
fn multi_table_update_exec() {
    // Cross-table RHS + both-table SET, evaluated against the ORIGINAL
    // joined row: `t1.b = t2.c` writes t2's OLD c, not the just-set 99
    // (gorun-verified).
    let mut db = Database::new();
    step(&mut db, "create table t1 (a int, b int)");
    step(&mut db, "create table t2 (a int, c int)");
    step(&mut db, "insert into t1 values (1, 10), (2, 20)");
    step(&mut db, "insert into t2 values (1, 100), (2, 200)");
    assert_eq!(
        step(
            &mut db,
            "update t1, t2 set t1.b = t2.c, t2.c = 99 where t1.a = t2.a"
        ),
        "OK"
    );
    assert_eq!(
        step(&mut db, "select a, b from t1 order by a"),
        "RS:1|100;2|200"
    );
    assert_eq!(
        step(&mut db, "select a, c from t2 order by a"),
        "RS:1|99;2|99"
    );

    // Update-once: a base row matched by several joined rows is updated a
    // single time (gorun-verified: +1, not +2).
    let mut db2 = Database::new();
    step(&mut db2, "create table t1 (a int, x int)");
    step(&mut db2, "create table t2 (a int)");
    step(&mut db2, "insert into t1 values (1, 10)");
    step(&mut db2, "insert into t2 values (1), (1)");
    assert_eq!(
        step(
            &mut db2,
            "update t1 join t2 on t1.a = t2.a set t1.x = t1.x + 1"
        ),
        "OK"
    );
    assert_eq!(step(&mut db2, "select a, x from t1"), "RS:1|11");
}

#[test]
fn multi_table_update_default_uses_each_target_column_default() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table default_left (id int, v int default 7)",
    );
    step(
        &mut db,
        "create table default_right (id int, v int default 9)",
    );
    step(&mut db, "insert into default_left values (1, 100)");
    step(&mut db, "insert into default_right values (1, 200)");
    assert_eq!(
        step(
            &mut db,
            "update default_left as l join default_right as r on l.id = r.id set l.v = default, r.v = default",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "select v from default_left"), "RS:7");
    assert_eq!(step(&mut db, "select v from default_right"), "RS:9");
}

#[test]
fn multi_table_delete_exec() {
    // Both targets: rows where the join matches are removed from BOTH
    // tables (gorun-verified).
    let mut db = Database::new();
    step(&mut db, "create table t1 (a int, b int)");
    step(&mut db, "create table t2 (a int, c int)");
    step(&mut db, "insert into t1 values (1, 10), (2, 20), (3, 30)");
    step(&mut db, "insert into t2 values (1, 100), (2, 200)");
    assert_eq!(
        step(&mut db, "delete t1, t2 from t1 join t2 on t1.a = t2.a"),
        "OK"
    );
    assert_eq!(step(&mut db, "select * from t1 order by a"), "RS:3|30");
    assert_eq!(step(&mut db, "select * from t2 order by a"), "RS:");

    // A single target from a two-table join keeps the other table intact.
    let mut db2 = Database::new();
    step(&mut db2, "create table t1 (a int)");
    step(&mut db2, "create table t2 (a int)");
    step(&mut db2, "insert into t1 values (1), (2), (3)");
    step(&mut db2, "insert into t2 values (2)");
    assert_eq!(
        step(&mut db2, "delete t1 from t1 join t2 on t1.a = t2.a"),
        "OK"
    );
    assert_eq!(step(&mut db2, "select a from t1 order by a"), "RS:1;3");

    // The `USING` spelling with aliased targets — the target names the
    // ALIAS, and a base row joining several times is deleted once.
    let mut db3 = Database::new();
    step(&mut db3, "create table t1 (a int)");
    step(&mut db3, "create table t2 (a int)");
    step(&mut db3, "insert into t1 values (1), (2), (3)");
    step(&mut db3, "insert into t2 values (2), (3)");
    assert_eq!(
        step(
            &mut db3,
            "delete from x using t1 as x join t2 as y on x.a = y.a"
        ),
        "OK"
    );
    assert_eq!(step(&mut db3, "select a from t1 order by a"), "RS:1");

    // Naming the base table when it was aliased is an "unknown table"
    // error, matching MySQL's MULTI DELETE rule.
    let mut db4 = Database::new();
    step(&mut db4, "create table t1 (a int)");
    step(&mut db4, "insert into t1 values (1)");
    assert_eq!(
        step(&mut db4, "delete t1 from t1 as x where x.a = 1"),
        "UnknownTable(\"t1\")"
    );
}

#[test]
fn foreign_key_child_side_enforcement() {
    let mut db = Database::new();
    step(&mut db, "create table parent (id int primary key)");
    step(
        &mut db,
        "create table child (id int, pid int, foreign key (pid) references parent(id))",
    );
    step(&mut db, "insert into parent values (1)");
    step(&mut db, "insert into child values (10, 1)");
    // A referencing value with no matching parent row is rejected.
    assert_eq!(
        step(&mut db, "insert into child values (20, 99)"),
        "ForeignKeyViolation"
    );
    // A NULL referencing value always bypasses the check.
    step(&mut db, "insert into child values (30, null)");
    assert_eq!(
        step(&mut db, "select id, pid from child order by id"),
        "RS:10|1;30|<nil>"
    );
    // UPDATE re-validates a changed referencing value too.
    step(&mut db, "update child set pid = 1 where id = 30");
    assert_eq!(
        step(&mut db, "update child set pid = 99 where id = 30"),
        "ForeignKeyViolation"
    );
    // The rejected UPDATE left the row unchanged.
    assert_eq!(step(&mut db, "select pid from child where id = 30"), "RS:1");

    // Composite FK: MATCH SIMPLE — ANY null local column skips the
    // check entirely, even in a composite key (confirmed via gorun).
    step(
        &mut db,
        "create table cparent (x int, y int, primary key(x, y))",
    );
    step(
        &mut db,
        "create table cchild (a int, b int, foreign key (a, b) references cparent(x, y))",
    );
    step(&mut db, "insert into cparent values (1, 1)");
    step(&mut db, "insert into cchild values (1, null)");
    step(&mut db, "insert into cchild values (null, 1)");
    step(&mut db, "insert into cchild values (1, 1)");
    assert_eq!(
        step(&mut db, "insert into cchild values (1, 2)"),
        "ForeignKeyViolation"
    );

    // TiDB's `INSERT IGNORE` downgrades a child-side FK violation to a
    // warning and skips only that VALUES row. Valid rows (including
    // duplicates when the child has no key) and MATCH SIMPLE NULL rows still
    // insert, and processing continues after each skipped row.
    let mut ignore_db = Database::new();
    step(&mut ignore_db, "create table parent (id int primary key)");
    step(
        &mut ignore_db,
        "create table child (id int, foreign key (id) references parent(id))",
    );
    step(&mut ignore_db, "insert into parent values (1), (3)");
    assert_eq!(
        step(
            &mut ignore_db,
            "insert ignore into child values (1), (null), (1), (2), (3), (4)",
        ),
        "OK"
    );
    assert_eq!(
        step(&mut ignore_db, "select id from child order by id"),
        "RS:<nil>;1;1;3"
    );
}

#[test]
fn foreign_key_parent_side_delete() {
    let mut db = Database::new();
    // Default (no ON DELETE clause) rejects deleting a referenced
    // parent row, but a parent row with no dependents deletes fine.
    step(&mut db, "create table parent (id int primary key)");
    step(
        &mut db,
        "create table child (id int, pid int, foreign key (pid) references parent(id))",
    );
    step(&mut db, "insert into parent values (1), (2)");
    step(&mut db, "insert into child values (10, 1)");
    assert_eq!(
        step(&mut db, "delete from parent where id = 1"),
        "ForeignKeyViolation"
    );
    step(&mut db, "delete from parent where id = 2");
    assert_eq!(step(&mut db, "select id from parent"), "RS:1");

    // CASCADE recursively removes dependents through multiple FK hops
    // (confirmed via gorun, not assumed one-level-only).
    step(&mut db, "create table p (id int primary key)");
    step(
        &mut db,
        "create table c (id int primary key, pid int, foreign key (pid) references p(id) on delete cascade)",
    );
    step(
        &mut db,
        "create table g (id int, cid int, foreign key (cid) references c(id) on delete cascade)",
    );
    step(&mut db, "insert into p values (1), (2)");
    step(&mut db, "insert into c values (10, 1), (11, 1), (12, 2)");
    step(&mut db, "insert into g values (100, 10)");
    step(&mut db, "delete from p where id = 1");
    assert_eq!(step(&mut db, "select id from p order by id"), "RS:2");
    assert_eq!(step(&mut db, "select id from c order by id"), "RS:12");
    assert_eq!(step(&mut db, "select id from g"), "RS:");

    // SET NULL nulls out the dependents' referencing columns instead
    // of removing them.
    step(&mut db, "create table np (id int primary key)");
    step(
        &mut db,
        "create table nc (id int, pid int, foreign key (pid) references np(id) on delete set null)",
    );
    step(&mut db, "insert into np values (1), (2)");
    step(&mut db, "insert into nc values (10, 1), (11, 2)");
    step(&mut db, "delete from np where id = 1");
    assert_eq!(
        step(&mut db, "select id, pid from nc order by id"),
        "RS:10|<nil>;11|2"
    );
}

#[test]
fn delete_ignore_skips_each_foreign_key_restricted_parent_target() {
    let mut db = Database::new();
    step(&mut db, "create table parent (a int primary key)");
    step(
        &mut db,
        "create table child (a int, foreign key (a) references parent(a))",
    );
    step(&mut db, "insert into parent values (1), (2)");
    step(&mut db, "insert into child values (1)");
    assert_eq!(
        step(&mut db, "delete from parent where a = 1"),
        "ForeignKeyViolation"
    );
    assert_eq!(step(&mut db, "delete ignore from parent where a = 1"), "OK");
    assert_eq!(step(&mut db, "delete ignore from parent"), "OK");
    assert_eq!(step(&mut db, "select a from parent"), "RS:1");

    step(&mut db, "insert into parent values (2)");
    step(&mut db, "create table parent2 (a int primary key)");
    step(
        &mut db,
        "create table child2 (a int, foreign key (a) references parent2(a))",
    );
    step(&mut db, "insert into parent2 values (1), (2)");
    step(&mut db, "insert into child2 values (1)");
    assert_eq!(
        step(
            &mut db,
            "delete from parent, parent2 using parent inner join parent2 where parent.a = parent2.a",
        ),
        "ForeignKeyViolation"
    );
    assert_eq!(
        step(
            &mut db,
            "delete ignore from parent, parent2 using parent inner join parent2 where parent.a = parent2.a",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "select a from parent"), "RS:1");
    assert_eq!(step(&mut db, "select a from parent2"), "RS:1");
}

#[test]
fn foreign_key_parent_side_update() {
    let mut db = Database::new();
    // Updating a non-referenced column, or "updating" a referenced
    // column to the SAME value, never checks dependents at all
    // (confirmed via gorun, not assumed).
    step(
        &mut db,
        "create table parent (id int primary key, name varchar(10))",
    );
    step(
        &mut db,
        "create table child (id int, pid int, foreign key (pid) references parent(id))",
    );
    step(&mut db, "insert into parent values (1, 'a'), (2, 'b')");
    step(&mut db, "insert into child values (10, 1)");
    step(&mut db, "update parent set name = 'z' where id = 1");
    step(&mut db, "update parent set id = 1 where id = 1");
    assert_eq!(
        step(&mut db, "select id, name from parent order by id"),
        "RS:1|z;2|b"
    );
    // Default (no ON UPDATE clause) rejects changing a referenced
    // value with dependents, but a parent row with none is free.
    assert_eq!(
        step(&mut db, "update parent set id = 99 where id = 1"),
        "ForeignKeyViolation"
    );
    step(&mut db, "update parent set id = 88 where id = 2");
    assert_eq!(
        step(&mut db, "select id from parent order by id"),
        "RS:1;88"
    );
    assert_eq!(step(&mut db, "select pid from child"), "RS:1");

    // CASCADE propagates the new value through multiple FK hops when
    // the cascaded column is itself referenced further; here it
    // isn't (c.id never changes), so g stays untouched — matching
    // gorun exactly, not assumed one-level-only.
    step(&mut db, "create table p (id int primary key)");
    step(
        &mut db,
        "create table c (id int primary key, pid int, foreign key (pid) references p(id) on update cascade)",
    );
    step(
        &mut db,
        "create table g (id int, cid int, foreign key (cid) references c(id) on update cascade)",
    );
    step(&mut db, "insert into p values (1)");
    step(&mut db, "insert into c values (10, 1)");
    step(&mut db, "insert into g values (100, 10)");
    step(&mut db, "update p set id = 99 where id = 1");
    assert_eq!(step(&mut db, "select id from p"), "RS:99");
    assert_eq!(step(&mut db, "select id, pid from c"), "RS:10|99");
    assert_eq!(step(&mut db, "select id, cid from g"), "RS:100|10");

    // SET NULL nulls the dependents' referencing columns instead of
    // propagating the new value.
    step(&mut db, "create table np (id int primary key)");
    step(
        &mut db,
        "create table nc (id int, pid int, foreign key (pid) references np(id) on update set null)",
    );
    step(&mut db, "insert into np values (1)");
    step(&mut db, "insert into nc values (10, 1)");
    step(&mut db, "update np set id = 99 where id = 1");
    assert_eq!(step(&mut db, "select id, pid from nc"), "RS:10|<nil>");
}

/// `REPLACE INTO` = insert, or on a `PRIMARY KEY`/`UNIQUE` conflict,
/// delete the conflicting row(s) then insert the new one (task #139).
/// Every outcome copied from a `gorun` probe.
#[test]
fn replace_statement_exec() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int primary key, b int)");
    step(&mut db, "insert into t values (1, 10)");
    // Conflict on the PK: the old (1,10) is deleted, (1,99) inserted.
    assert_eq!(step(&mut db, "replace into t values (1, 99)"), "OK");
    assert_eq!(step(&mut db, "select b from t where a = 1"), "RS:99");
    // No conflict: a plain insert.
    assert_eq!(step(&mut db, "replace into t values (2, 20)"), "OK");
    assert_eq!(step(&mut db, "select b from t order by a"), "RS:99;20");
    // A column-list REPLACE that again conflicts on the PK.
    assert_eq!(step(&mut db, "replace into t (a,b) values (1, 50)"), "OK");
    assert_eq!(step(&mut db, "select b from t where a = 1"), "RS:50");
    // Row count stays 2 — replaces never accumulate duplicate keys.
    assert_eq!(step(&mut db, "select count(*) from t"), "RS:2");
    // Deleting the conflicting row across a UNIQUE key too.
    let mut db2 = Database::new();
    step(&mut db2, "create table u (a int primary key, b int unique)");
    step(&mut db2, "insert into u values (1, 10)");
    // (2, 10) conflicts on b's UNIQUE key with (1,10): (1,10) is
    // deleted, (2,10) inserted.
    assert_eq!(step(&mut db2, "replace into u values (2, 10)"), "OK");
    assert_eq!(step(&mut db2, "select a from u"), "RS:2");
}

#[test]
fn insert_select_exec() {
    let mut db = Database::new();
    step(&mut db, "create table s (a int, b int)");
    step(&mut db, "insert into s values (1, 10), (2, 20)");
    step(&mut db, "create table t (a int, b int)");
    // INSERT ... SELECT copies the query's rows into the target.
    assert_eq!(step(&mut db, "insert into t select a, b from s"), "OK");
    assert_eq!(step(&mut db, "select * from t order by a"), "RS:1|10;2|20");
    // An explicit column list still routes through the same per-row path.
    assert_eq!(
        step(&mut db, "insert into t (a, b) select a + 100, b from s"),
        "OK"
    );
    assert_eq!(
        step(&mut db, "select a from t order by a"),
        "RS:1;2;101;102"
    );
    // REPLACE ... SELECT: a produced row conflicting on the PK deletes
    // the old row (delete-then-insert), just like REPLACE ... VALUES.
    let mut db2 = Database::new();
    step(&mut db2, "create table src (a int, b int)");
    step(&mut db2, "insert into src values (1, 99)");
    step(&mut db2, "create table dst (a int primary key, b int)");
    step(&mut db2, "insert into dst values (1, 10)");
    assert_eq!(
        step(&mut db2, "replace into dst select a, b from src"),
        "OK"
    );
    assert_eq!(step(&mut db2, "select b from dst where a = 1"), "RS:99");
    assert_eq!(step(&mut db2, "select count(*) from dst"), "RS:1");

    // TiDB also accepts the same typed result-set source inside INSERT-owned
    // parentheses. It must take the same execution path, not be mistaken for
    // a column list or a no-op parser-only spelling.
    let mut db3 = Database::new();
    step(&mut db3, "create table src (a int, b int)");
    step(&mut db3, "insert into src values (3, 30)");
    step(&mut db3, "create table dst (a int primary key, b int)");
    assert_eq!(
        step(&mut db3, "insert into dst (select a, b from src)"),
        "OK"
    );
    assert_eq!(step(&mut db3, "select * from dst"), "RS:3|30");
    assert_eq!(
        step(
            &mut db3,
            "replace into dst (a, b) (select a, b + 1 from src)"
        ),
        "OK"
    );
    assert_eq!(step(&mut db3, "select * from dst"), "RS:3|31");
}

#[test]
fn insert_set_form_and_defaults_exec() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int default 7, c int)");
    // SET form fills unlisted columns with their defaults (b=7, c=NULL).
    assert_eq!(step(&mut db, "insert into t set a = 1"), "OK");
    // Bare DEFAULT in a column-list VALUES resolves to the column default.
    assert_eq!(
        step(&mut db, "insert into t (a, b) values (2, default)"),
        "OK"
    );
    // Positional row with a bare DEFAULT in the middle.
    assert_eq!(step(&mut db, "insert into t values (3, default, 30)"), "OK");
    // SET form naming a subset out of declaration order.
    assert_eq!(step(&mut db, "insert into t set a = 4, c = 40"), "OK");
    assert_eq!(
        step(&mut db, "select a, b, c from t order by a"),
        "RS:1|7|<nil>;2|7|<nil>;3|7|30;4|7|40"
    );
    // A column with no declared default fills as NULL when omitted.
    let mut db2 = Database::new();
    step(&mut db2, "create table u (a int, b int)");
    assert_eq!(step(&mut db2, "insert into u set a = 5"), "OK");
    assert_eq!(step(&mut db2, "select a, b from u"), "RS:5|<nil>");
    // REPLACE ... SET honors the delete-then-insert conflict rule.
    let mut db3 = Database::new();
    step(
        &mut db3,
        "create table r (a int primary key, b int default 1)",
    );
    step(&mut db3, "insert into r set a = 1, b = 10");
    assert_eq!(step(&mut db3, "replace into r set a = 1, b = 20"), "OK");
    assert_eq!(step(&mut db3, "select b from r where a = 1"), "RS:20");
    assert_eq!(step(&mut db3, "select count(*) from r"), "RS:1");

    // The qualifier remains in the AST for restore; the single-table
    // executor resolves its final component as the target column.
    let mut qualified = Database::new();
    step(&mut qualified, "create table q (c int default 7)");
    step(&mut qualified, "insert into q set q.c=1");
    assert_eq!(step(&mut qualified, "select c from q"), "RS:1");
}

/// `INT UNSIGNED` and `BIGINT UNSIGNED` must store a real `Datum::UInt` at
/// the DML boundary. This covers the shared VALUES/default/UPDATE coercion
/// path and guards strict errors from changing an already-stored row.
#[test]
fn unsigned_integer_columns_keep_uint_storage_and_strict_bounds() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table uint_dml_unit (ui int unsigned, ub bigint unsigned primary key, si int)",
    );
    step(
        &mut db,
        "insert into uint_dml_unit values (4294967295, 18446744073709551615, -2147483648)",
    );
    step(
        &mut db,
        "insert into uint_dml_unit values (1, 9223372036854775808, 1)",
    );
    assert_eq!(
        step(&mut db, "select ui, ub, si from uint_dml_unit order by ub"),
        "RS:1|9223372036854775808|1;4294967295|18446744073709551615|-2147483648"
    );
    assert!(step(
        &mut db,
        "insert into uint_dml_unit values (4294967296, 2, 1)",
    )
    .starts_with("OutOfRange(\"ui\")"));
    assert!(step(&mut db, "update uint_dml_unit set ub = -1").starts_with("OutOfRange(\"ub\")"));
    assert_eq!(
        step(&mut db, "select ui, ub, si from uint_dml_unit order by ub"),
        "RS:1|9223372036854775808|1;4294967295|18446744073709551615|-2147483648"
    );

    // Decimal literals and numeric strings use source rounding on assignment,
    // then become UInt values before ordering/key handling can observe them.
    step(
        &mut db,
        "update uint_dml_unit set ui=1.5, ub='2.5' where ub=9223372036854775808",
    );
    assert_eq!(
        step(&mut db, "select ui, ub from uint_dml_unit order by ub"),
        "RS:2|3;4294967295|18446744073709551615"
    );

    let mut defaults = Database::new();
    step(
        &mut defaults,
        "create table uint_defaults (ui int unsigned default 4294967295, ub bigint unsigned default 18446744073709551615, s int)",
    );
    step(&mut defaults, "insert into uint_defaults (s) values (7)");
    assert_eq!(
        step(&mut defaults, "select ui, ub, s from uint_defaults"),
        "RS:4294967295|18446744073709551615|7"
    );

    // Source `RoundFloat` is ties-to-even, unlike DECIMAL/string assignment's
    // ties-away rule: DOUBLE 2.5 -> 2 while 3.5 -> 4.
    let mut floats = Database::new();
    step(
        &mut floats,
        "create table uint_float_round (u bigint unsigned, s bigint)",
    );
    step(
        &mut floats,
        "insert into uint_float_round values (cast(2.5 as double), cast(-2.5 as double))",
    );
    step(
        &mut floats,
        "insert into uint_float_round values (cast(3.5 as double), cast(-3.5 as double))",
    );
    assert_eq!(
        step(&mut floats, "select u, s from uint_float_round order by u"),
        "RS:2|-2;4|-4"
    );

    // Scientific strings follow the source decimal-expansion path, not f64:
    // the MaxUint64 spelling must survive exactly and the next value errors.
    let mut scientific = Database::new();
    step(
        &mut scientific,
        "create table uint_scientific (u bigint unsigned, s bigint)",
    );
    step(
        &mut scientific,
        "insert into uint_scientific values ('2.5e0', '2.5e0'), ('5e-1', '-5e-1'), ('4e-1', '-4e-1')",
    );
    step(
        &mut scientific,
        "insert into uint_scientific values ('18446744073709551615e0', '9223372036854775807e0')",
    );
    assert!(step(
        &mut scientific,
        "insert into uint_scientific values ('18446744073709551616e0', '9223372036854775808e0')",
    )
    .starts_with("OutOfRange("));
    assert_eq!(
        step(
            &mut scientific,
            "select u, s from uint_scientific order by u, s"
        ),
        "RS:0|0;1|-1;3|3;18446744073709551615|9223372036854775807"
    );
}
