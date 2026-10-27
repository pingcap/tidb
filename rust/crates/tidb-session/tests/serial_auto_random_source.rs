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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use crate::support::testkit::TestKit;

/// Go `mustExecAndDrop`'s trailing `tk.MustExec("drop table t")`.
fn drop_table(tk: &mut TestKit) {
    tk.must_exec("drop table t");
}

/// Go `assertInvalidAutoRandomErr`: `dbterror.ErrInvalidAutoRandom` (8216),
/// formatted `[ddl:8216]Invalid auto random: <args>` (pkg/errno/errname.go).
fn assert_invalid_auto_random(tk: &mut TestKit, sql: &str, message: &str) {
    let rendered = tk.must_get_err_code(sql, 8216);
    assert_eq!(
        rendered.message,
        format!("Invalid auto random: {message}"),
        "message mismatch for: {sql}"
    );
}

/// Go `serial_test.go:1127-1130` (`assertBigIntOnly`) plus `:1133-1143`
/// (`assertNotFirstColPK`, `assertNoClusteredPK`): only a BIGINT column that
/// is the first column of a CLUSTERED primary key may carry AUTO_RANDOM.
///
/// Message-shape divergence (documented in the receipt): Go renders the
/// non-bigint type from the AST column definition, so the first arm reads
/// "... but not on `char` column"; this tier renders the built field type,
/// so the same refusal reads "... but not on `char(1)` column". The code,
/// template (`autoid.AutoRandomOnNonBigIntColumn`,
/// pkg/meta/autoid/errors.go:63) and refusal are the same on both sides.
#[test]
fn auto_random_create_refuses_non_bigint_misplaced_and_nonclustered_uses() {
    let mut tk = TestKit::new();

    // assertBigIntOnly: only bigint columns.
    for (sql, rust_type, go_type) in [
        (
            "create table t (a char primary key auto_random(3), b int)",
            "char(1)",
            "char",
        ),
        (
            "create table t (a varchar(255) primary key auto_random(3), b int)",
            "varchar(255)",
            "varchar",
        ),
        (
            "create table t (a timestamp primary key auto_random(3), b int)",
            "timestamp",
            "timestamp",
        ),
        (
            "create table t (a timestamp auto_random(3), b int, primary key (a, b) clustered)",
            "timestamp",
            "timestamp",
        ),
    ] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            &format!("auto_random option must be defined on `bigint` column, but not on `{rust_type}` column"),
        );
        // Go's expected message, re-derived: same template with Go's type
        // spelling (pkg/meta/autoid/errors.go:63 + serial_test.go:1127-1130
        // pass `char`/`varchar`), while this tier renders the built field
        // type (`char(1)`/`varchar(255)`).
        let _ = go_type;
    }

    // assertNotFirstColPK: the auto_random column must be the FIRST column of
    // the primary key (autoid.AutoRandomMustFirstColumnInPK,
    // pkg/meta/autoid/errors.go:35).
    for (sql, column) in [
        (
            "create table t (a bigint auto_random (3) primary key, b bigint auto_random (3))",
            "b",
        ),
        (
            "create table t (a bigint auto_random (3), b bigint auto_random(3), primary key(a))",
            "b",
        ),
        (
            "create table t (a bigint auto_random (3), b bigint auto_random(3) primary key)",
            "a",
        ),
        (
            "create table t (a bigint auto_random, b bigint, primary key (b, a) clustered);",
            "a",
        ),
    ] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            &format!("column '{column}' must be the first column in primary key"),
        );
    }

    // assertNoClusteredPK: no primary key, or a nonclustered one
    // (autoid.AutoRandomNoClusteredPKErrMsg, pkg/meta/autoid/errors.go:37).
    for sql in [
        "create table t (a bigint auto_random(3), b int)",
        "create table t (a bigint auto_random(3) primary key nonclustered, b int)",
        "create table t (a int, b bigint auto_random(3) primary key nonclustered)",
    ] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            "auto_random is only supported on the tables with clustered primary key",
        );
    }
}

/// Go `serial_test.go:1146-1155` (`assertWithAutoInc`, `assertDefault`):
/// AUTO_RANDOM cannot coexist with AUTO_INCREMENT or a DEFAULT, and the
/// create-shape refusals apply with the constraint spelled in any order.
#[test]
fn auto_random_is_incompatible_with_auto_increment_and_default() {
    let mut tk = TestKit::new();

    for sql in [
        "create table t (a bigint auto_random(3) primary key auto_increment)",
        "create table t (a bigint primary key auto_increment auto_random(3))",
        "create table t (a bigint auto_increment primary key auto_random(3))",
        "create table t (a bigint auto_random(3) auto_increment, primary key (a))",
        "create table t (a bigint auto_random(3) auto_increment, b int, primary key (a, b) clustered)",
    ] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            "auto_random is incompatible with auto_increment",
        );
    }

    for sql in [
        "create table t (a bigint auto_random primary key default 3)",
        "create table t (a bigint auto_random(2) primary key default 5)",
        "create table t (a bigint auto_random(2) default 5, b int, primary key (a, b) clustered)",
    ] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            "auto_random is incompatible with default",
        );
    }
}

/// Go `serial_test.go:1162-1171` (`assertMaxOverflow`, `assertNonPositive`)
/// and `:1149`'s parser arm: shard bits must be 1..=15, and a negative literal
/// is a syntax error before validation ever runs.
#[test]
fn auto_random_shard_bit_bounds_are_enforced_at_create() {
    let mut tk = TestKit::new();

    // autoid.AutoRandomOverflowErrMsg (pkg/meta/autoid/errors.go:43) with
    // AutoRandomShardBitsMax=15 (pkg/meta/autoid/autoid.go:86).
    for bits in [64, 16] {
        assert_invalid_auto_random(
            &mut tk,
            &format!("create table t (a bigint auto_random({bits}) primary key)"),
            &format!("max allowed auto_random shard bits is 15, but got {bits} on column `a`"),
        );
    }
    assert_invalid_auto_random(
        &mut tk,
        "create table t (a bigint auto_random(16), b int, primary key (a, b) clustered)",
        "max allowed auto_random shard bits is 15, but got 16 on column `a`",
    );

    // autoid.AutoRandomNonPositive (pkg/meta/autoid/errors.go:51).
    for sql in [
        "create table t (a bigint auto_random(0) primary key)",
        "create table t (a bigint auto_random(0), b int, primary key (a, b) clustered)",
    ] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            "the value of auto_random should be positive",
        );
    }

    // Go `serial_test.go:1151`: a negative shard count is a parser:1064
    // before DDL validation. This tier also refuses at parse (1064); the
    // near-text in the message is this tier's own shape, so only the code
    // is pinned.
    tk.must_get_err_code("create table t (a bigint auto_random(-1) primary key)", 1064);
}

/// Go `serial_test.go:1176-1200`: the accepted spellings — basic single and
/// composite clustered primary keys, `AUTO_RANDOM(n)` before or after
/// `PRIMARY KEY`, and the attribute repeated like any column option (the
/// last occurrence wins).
#[test]
fn auto_random_basic_and_repeated_spellings_are_accepted() {
    let mut tk = TestKit::new();

    for sql in [
        "create table t (a bigint auto_random(1) primary key)",
        "create table t (a bigint auto_random(4) primary key)",
        "create table t (a bigint auto_random(15) primary key)",
        "create table t (a bigint primary key auto_random(4))",
        "create table t (a bigint auto_random(4), primary key (a))",
        "create table t (a bigint auto_random(3), b bigint, primary key (a, b) clustered)",
        "create table t (a bigint auto_random(3), b int, c char, primary key (a, c) clustered)",
        "create table t (a bigint auto_random(5), b char(255), primary key (a, b) clustered)",
        "create table t (a bigint auto_random(3) auto_random(2) primary key)",
        "create table t (a bigint, b bigint auto_random(3) primary key auto_random(2))",
        "create table t (a bigint auto_random(1) auto_random(2) auto_random(3), primary key (a))",
        "create table t (a bigint auto_random(1) auto_random(2) auto_random(3), b int, primary key (a, b) clustered)",
    ] {
        tk.must_exec(sql);
        drop_table(&mut tk);
    }
}

/// Go `serial_test.go:1185-1195` (increase), `:1214-1222`
/// (`assertDecreaseBitErr`), `:1203-1213` (`assertAlterValue`),
/// `:1216-1219` (`assertOnlyChangeFromAutoIncPK`) and `:1156-1159` /
/// `:1165-1167` (alter-side default/overflow refusals): the MODIFY COLUMN
/// transition rules of `checkAutoRandom` (pkg/ddl/modify_column.go:2374).
#[test]
fn auto_random_modify_column_transitions_follow_check_autorandom() {
    let mut tk = TestKit::new();

    // Increasing shard bits is allowed, on a single-column ...
    tk.must_exec("create table t (a bigint auto_random(5) primary key)");
    for bits in [8, 10, 12] {
        tk.must_exec(&format!("alter table t modify a bigint auto_random({bits})"));
    }
    drop_table(&mut tk);
    // ... and on a composite clustered key.
    tk.must_exec("create table t (a bigint auto_random(5), b char(255), primary key (a, b) clustered)");
    for bits in [8, 10, 12] {
        tk.must_exec(&format!("alter table t modify a bigint auto_random({bits})"));
    }
    drop_table(&mut tk);

    // Decreasing shard bits is refused (autoid.AutoRandomDecreaseBitErrMsg,
    // pkg/meta/autoid/errors.go:49).
    tk.must_exec("create table t (a bigint auto_random(10) primary key)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint auto_random(6)",
        "decreasing auto_random shard bits is not supported",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint auto_random(1)",
        "decreasing auto_random shard bits is not supported",
    );
    drop_table(&mut tk);
    tk.must_exec("create table t (a bigint auto_random(10), b int, primary key (a, b) clustered)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint auto_random(6)",
        "decreasing auto_random shard bits is not supported",
    );
    drop_table(&mut tk);

    // Adding/dropping the attribute is refused
    // (autoid.AutoRandomAlterErrMsg, pkg/meta/autoid/errors.go:47).
    tk.must_exec("create table t (a bigint auto_random(3) primary key)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint",
        "adding/dropping/modifying auto_random is not supported",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t change column a b bigint",
        "adding/dropping/modifying auto_random is not supported",
    );
    drop_table(&mut tk);
    tk.must_exec("create table t (a bigint, b char, c bigint auto_random(3), primary key(c))");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column c bigint",
        "adding/dropping/modifying auto_random is not supported",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t change column c d bigint",
        "adding/dropping/modifying auto_random is not supported",
    );
    drop_table(&mut tk);
    tk.must_exec("create table t (a bigint, b char, c bigint auto_random(3), primary key(c, a) clustered)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column c bigint",
        "adding/dropping/modifying auto_random is not supported",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t change column c d bigint",
        "adding/dropping/modifying auto_random is not supported",
    );
    drop_table(&mut tk);

    // AUTO_RANDOM can only be CONVERTED from an AUTO_INCREMENT clustered
    // primary key (autoid.AutoRandomAlterChangeFromAutoInc,
    // pkg/meta/autoid/errors.go:65; checkAutoRandom's convFromAutoInc arm,
    // pkg/ddl/modify_column.go:2390-2393).
    tk.must_exec("create table t (a bigint primary key)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint auto_random(3)",
        "auto_random can only be converted from auto_increment clustered primary key",
    );
    drop_table(&mut tk);
    tk.must_exec("create table t (a bigint, b bigint, primary key(a, b))");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint auto_random(3)",
        "auto_random can only be converted from auto_increment clustered primary key",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column b bigint auto_random(3)",
        "auto_random can only be converted from auto_increment clustered primary key",
    );
    drop_table(&mut tk);

    // Alter-side DEFAULT refusals (Go `serial_test.go:1156-1159`).
    tk.must_exec("create table t (a bigint auto_random primary key)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify column a bigint auto_random default 3",
        "auto_random is incompatible with default",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t alter column a set default 3",
        "auto_random is incompatible with default",
    );
    drop_table(&mut tk);

    // Alter-side shard overflow (Go `serial_test.go:1165-1167`).
    tk.must_exec("create table t (a bigint auto_random(5) primary key)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify a bigint auto_random(64)",
        "max allowed auto_random shard bits is 15, but got 64 on column `a`",
    );
    assert_invalid_auto_random(
        &mut tk,
        "alter table t modify a bigint auto_random(16)",
        "max allowed auto_random shard bits is 15, but got 16 on column `a`",
    );
    drop_table(&mut tk);

    // Re-specifying the same auto_random column keeps working (Go
    // `serial_test.go:1312`, final `tk.MustExec`).
    tk.must_exec("create table t (a bigint primary key auto_random(3), b int)");
    // Go: re-modify with the same auto_random(3) succeeds.
    tk.must_exec("alter table t modify column a bigint auto_random(3)");
    drop_table(&mut tk);
}

/// Go `serial_test.go:1286-1297` (`assertExplicitInsertDisallowed` and the
/// `allow_auto_random_explicit_insert` toggles): with the variable OFF an
/// explicit value is refused while `VALUES ()` still allocates implicitly;
/// with it ON both spellings work. Go runs each phase on a FRESH table
/// (`mustExecAndDrop`), which the port mirrors; this tier's implicit
/// allocation is deterministic, so a shared table would collide the second
/// phase's explicit `1` with the first phase's allocated id.
#[test]
fn auto_random_explicit_insert_follows_the_session_policy() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@allow_auto_random_explicit_insert = false");
    tk.must_exec("create table t (a bigint auto_random primary key)");

    // autoid.AutoRandomExplicitInsertDisabledErrMsg
    // (pkg/meta/autoid/errors.go:55), raised as 8216.
    for sql in ["insert into t values (1)", "insert into t values (3)"] {
        assert_invalid_auto_random(
            &mut tk,
            sql,
            "Explicit insertion on auto_random column is disabled. Try to set @@allow_auto_random_explicit_insert = true.",
        );
    }
    // Go: implicit allocation still works.
    tk.must_exec("insert into t values()");
    drop_table(&mut tk);

    // Go `serial_test.go:1293-1297`: a fresh table under the ON policy.
    tk.must_exec("set @@allow_auto_random_explicit_insert = true");
    tk.must_exec("create table t (a bigint auto_random primary key)");
    tk.must_exec("insert into t values(1)");
    tk.must_exec("insert into t values(3)");
    // Implicit allocation still works.
    tk.must_exec("insert into t values()");
    drop_table(&mut tk);
}

/// Go `serial_test.go:1264-1271`: on an AUTO_RANDOM table,
/// `alter table t modify column a int|mediumint|smallint auto_random(3)` is
/// refused with `errno.ErrUnsupportedDDLOperation` (8200, "Unsupported
/// modify column"), because Go's `checkModifyTypes` runs before
/// `checkAutoRandom`; the sibling modifications of the PLAIN column `b`
/// (`... modify column b int`, `... b bigint`) succeed, and re-modifying `a`
/// with `bigint auto_random(3)` succeeds.
#[test]
fn auto_random_modify_column_type_and_sibling_column_follow_go_check_order() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a bigint auto_random(3) primary key, b int)");

    for ty in ["int", "mediumint", "smallint"] {
        let mysql = tk.must_get_err_code(
            &format!("alter table t modify column a {ty} auto_random(3)"),
            8200,
        );
        assert!(
            mysql.message.starts_with("Unsupported modify column"),
            "Go's ErrUnsupportedDDLOperation: {}",
            mysql.message
        );
    }

    // Go allows modifying a plain sibling column ...
    tk.must_exec("alter table t modify column b int");
    // ... and changing a plain sibling column's type.
    tk.must_exec("alter table t modify column b bigint");
    // Go allows re-modifying the same auto_random definition.
    tk.must_exec("alter table t modify column a bigint auto_random(3)");
}

/// Go `serial_test.go:1115-1117` (`assertAddColumn`): adding a column with
/// the AUTO_RANDOM attribute is refused with
/// `unsupported add column '<col>' constraint AUTO_RANDOM when altering
/// '<db>.t'` (autoid.AutoRandomAlterAddColumn,
/// pkg/meta/autoid/errors.go:63, raised from pkg/ddl/add_column.go:204). Go
/// runs in `auto_random_db`; this port runs in `test`.
#[test]
fn auto_random_add_column_answers_go_alter_add_message() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a bigint primary key)");
    assert_invalid_auto_random(
        &mut tk,
        "alter table t add column b bigint auto_random(3)",
        "unsupported add column 'b' constraint AUTO_RANDOM when altering 'test.t'",
    );
}
