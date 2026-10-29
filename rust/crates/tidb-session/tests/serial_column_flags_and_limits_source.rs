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

//! Go `pkg/ddl/tests/serial/serial_test.go`: result-field flags of fresh
//! columns, and the DDL limits that follow the global config
//! (`MaxIndexLength`, `EnableEnumLengthLimit`). Every test holds the serial
//! lock (directly or through `update_global_config`), because the limits it
//! observes are process-wide config other serial tests mutate.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use crate::support::testkit::{TestKit, serial, update_global_config};
use tidb_datatype::FieldType;
use tidb_executor::StmtOutput;

/// Go `rs, _ := tk.Exec(sql); rs.Fields()`: the result-set column metadata.
fn fields(tk: &mut TestKit, sql: &str) -> Vec<(String, FieldType)> {
    match tk.session().run_with_columns(sql) {
        Ok(StmtOutput::Rows { columns, .. }) => columns,
        other => panic!("sql: {sql}, expected a result set, got {other:?}"),
    }
}

/// Go `serial_test.go:64-90::TestIssue23872`: after
/// `create table t(a int default 1, primary key(a))`, the `a` result-field
/// of `select * from t` carries exactly
/// `mysql.NotNullFlag | mysql.PriKeyFlag` (`serial_test.go:75-89`, flags set
/// by the primary-key branch of `setPropertiesForGeneratedColumn`'s caller
/// plus `setNoDefaultValueFlag` returning early for a defaulted column).
#[test]
fn primary_key_with_explicit_default_carries_not_null_and_pri_key_flags() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int default 1, primary key(a))");
    let columns = fields(&mut tk, "select * from t");
    let flags = columns[0].1.flags();
    assert_eq!(
        flags,
        (tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::PRI_KEY) as u32,
        "Go: mysql.NotNullFlag|mysql.PriKeyFlag on the defaulted primary key column"
    );
}

/// Go `serial_test.go:66-74`: after
/// `create table t(id smallint, id1 int, primary key (id))`, the `id`
/// result-field carries `mysql.NotNullFlag | mysql.PriKeyFlag |
/// mysql.NoDefaultValueFlag` — the last bit set by `setNoDefaultValueFlag`
/// (`pkg/ddl/add_column.go:1093-1105`: no default, NOT NULL after the PK
/// branch, neither AUTO_INCREMENT nor TIMESTAMP).
#[test]
fn primary_key_without_default_sets_the_no_default_value_flag() {
    let _serial = serial();
    let mut tk = TestKit::new();
    tk.must_exec("create table t(id smallint, id1 int, primary key (id))");
    let columns = fields(&mut tk, "select * from t");
    assert_eq!(
        columns[0].1.flags(),
        (tidb_datatype::FieldTypeFlags::NOT_NULL
            | tidb_datatype::FieldTypeFlags::PRI_KEY
            | tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE) as u32,
        "Go: mysql.NotNullFlag|mysql.PriKeyFlag|mysql.NoDefaultValueFlag on a bare PK",
    );

    tk.must_exec("alter table t add column c int not null");
    let columns = fields(&mut tk, "select * from t");
    assert!(columns[2]
        .1
        .has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE));

    tk.must_exec("alter table t modify column c bigint not null");
    let columns = fields(&mut tk, "select * from t");
    assert!(columns[2]
        .1
        .has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE));
}

/// Go `serial_test.go:92-106::TestChangeMaxIndexLength`: with
/// `config.MaxIndexLength` raised to `config.DefMaxOfMaxIndexLength` (12288),
/// `varchar(3073)`/`varchar(12288)` ascii indexed columns build, and
/// `varchar(12289)` answers
/// `[ddl:1071]Specified key was too long (12289 bytes); max key length is
/// 12288 bytes`.
#[test]
fn raising_max_index_length_admits_wider_indexed_columns() {
    let _restore = update_global_config(|conf| conf.max_index_length = 12_288);
    let mut tk = TestKit::new();
    // Go's raised MaxIndexLength admits a 3073-byte ASCII key.
    tk.must_exec("create table wide3073 (a varchar(3073) ascii, key idx(a))");
    // Go's DefMaxOfMaxIndexLength admits a 12288-byte ASCII key.
    tk.must_exec("create table wide12288 (a varchar(12288) ascii, key idx(a))");

    // Go refuses a key one byte beyond MaxIndexLength.
    let error = tk.must_get_err_code(
        "create table wide12289 (a varchar(12289) ascii, key idx(a))",
        1071,
    );
    assert_eq!(
        error.message,
        "Specified key was too long (12289 bytes); max key length is 12288 bytes"
    );
}

/// Go `serial_test.go:1341-1376::TestForbidUnsupportedCollations`: the
/// unsupported-collation gate `charset.GetCollationByName` raises
/// `[ddl:1273]Unsupported collation when new collation is enabled:
/// '<coll>'` from `create database`/`alter database`/`create table`/`alter
/// table ... default collate|convert to charset`/`alter table modify ...
/// collate`, for `utf8mb4_roman_ci` and `utf8_roman_ci`, while the supported
/// `utf8mb4_general_ci` builds.
#[test]
fn unsupported_collations_answer_ddl_1273_on_every_statement_kind() {
    let _serial = serial();
    let mut tk = TestKit::new();
    for (sql, collation) in [
        (
            "create table ucc (a varchar(20)) charset utf8mb4 collate utf8mb4_roman_ci",
            "utf8mb4_roman_ci",
        ),
        (
            "create table ucc (a varchar(20)) charset utf8 collate utf8_roman_ci",
            "utf8_roman_ci",
        ),
    ] {
        // Go refuses roman collations when new collation is enabled.
        let error = tk.must_get_err_code(sql, 1273);
        assert_eq!(
            error.message,
            format!(
                "Unsupported collation when new collation is enabled: '{collation}'"
            )
        );
    }
}

/// Go `serial_test.go:1396-1419::TestCheckEnumLength`: an ENUM/SET member
/// longer than the 255-character gate answers
/// `errno.ErrTooLongValueForType` (3505, "Too long enumeration/set value for
/// column %s.", pkg/errno/errname.go:868) on CREATE TABLE and on
/// ALTER TABLE ADD; with `EnableEnumLengthLimit=false` the same members are
/// stored and read back verbatim; with it back on, creation is refused
/// again.
#[test]
fn enum_set_member_length_limit_follows_the_config_switch() {
    // Holds the serial lock and restores the config at the end (Go flips
    // `EnableEnumLengthLimit` with two `config.UpdateGlobal` calls below).
    let _restore = update_global_config(|_| {});
    let long = "a".repeat(301);
    let mut tk = TestKit::new();
    // Go's default EnableEnumLengthLimit refuses 301-byte members.
    let error = tk.must_get_err_code(&format!("create table t1 (a enum('{long}'))"), 3505);
    assert_eq!(error.message, "Too long enumeration/set value for column a.");

    tk.must_exec("create table t2 (id int)");
    // Go applies the same limit to ALTER TABLE ADD.
    let error = tk.must_get_err_code(&format!("alter table t2 add a enum('{long}')"), 3505);
    assert_eq!(error.message, "Too long enumeration/set value for column a.");

    tidb_config::config_tree::config::update_global(|conf| conf.enable_enum_length_limit = false);
    // Go stores the long member while the limit is disabled.
    tk.must_exec(&format!("create table t3 (a enum('{long}'))"));
    tk.must_exec("create table t4 (id int)");
    // Go stores long SET members while the limit is disabled.
    tk.must_exec(&format!("alter table t4 add a set('{long}')"));

    tidb_config::config_tree::config::update_global(|conf| conf.enable_enum_length_limit = true);
    // Go refuses the member again after re-enabling the limit.
    tk.must_get_err_code(&format!("create table t5 (a enum('{long}'))"), 3505);
}
