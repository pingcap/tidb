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

//! Port of the ported slice of the small `pkg/ddl/tests/serial` tests:
//! `TestIssue23872` (serial_test.go:64), `TestChangeMaxIndexLength` (:92),
//! `TestTableLocksDisable` (:1044), `TestForbidUnsupportedCollations`
//! (:1341), `TestCheckEnumLength` (:1396) and `TestGetReverseKey` (:1421).
//!
//! Each contract is re-derived from the Go source it exercises: the
//! result-field flag word from `setNoDefaultValueFlag`
//! (`pkg/ddl/add_column.go:1093`), the index-length gate from
//! `config.MaxIndexLength` (`pkg/ddl/index_prefix.rs`'s Go counterpart,
//! `checkIndexLength`), the collation gate from
//! `charset.GetCollationByName` via the DDL, and the enum/set limit from
//! `EnableEnumLengthLimit`. Where this tier has no carrier (a global config
//! switch, LOCK TABLES, `GetRangeEndKey`, the enum length gate), the Go test
//! is an `#[ignore]` gap test — never approximated.

use tidb_executor::{run_create_table_on, run_select_meta_on, Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Go `serial_test.go:64-90::TestIssue23872`: after
/// `create table t(a int default 1, primary key(a))`, the `a` result-field
/// of `select * from t` carries exactly
/// `mysql.NotNullFlag | mysql.PriKeyFlag` (`serial_test.go:75-89`, flags set
/// by the primary-key branch of `setPropertiesForGeneratedColumn`'s caller
/// plus `setNoDefaultValueFlag` returning early for a defaulted column).
#[test]
fn primary_key_with_explicit_default_carries_not_null_and_pri_key_flags() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(a int default 1, primary key(a))", &mut catalog)
        .expect("create table");
    let (columns, _) =
        run_select_meta_on("select * from t", &catalog, &ctx()).expect("select meta");
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
// go-parity-gap: this tier never sets NO_DEFAULT_VALUE on such a column
// (measured: the flag word is exactly NOT_NULL|PRI_KEY), so Go's full-flag
// equality cannot be pinned.
#[test]
#[ignore]
fn primary_key_without_default_sets_the_no_default_value_flag() {
}

/// Go `serial_test.go:92-102::TestChangeMaxIndexLength`: with
/// `config.MaxIndexLength` raised to `config.DefMaxOfMaxIndexLength` (12288),
/// `varchar(3073)`/`varchar(12288)` ascii indexed columns build, and
/// `varchar(12289)` answers
/// `[ddl:1071]Specified key was too long (12289 bytes); max key length is
/// 12288 bytes`.
// go-parity-gap: this tier has no MaxIndexLength config switch —
// `crate::ddl::index_prefix::MAX_INDEX_LENGTH` is fixed at Go's default
// 3072 bytes, so the raised-limit arms cannot be reproduced.
#[test]
#[ignore]
fn raising_max_index_length_admits_wider_indexed_columns() {
}

/// Go `serial_test.go:1044-1063::TestTableLocksDisable`: with
/// `enable-table-lock` off, `lock tables t1 write` and `unlock tables`
/// answer `Warning 1235 "LOCK TABLES is not supported. To enable this
/// experimental feature, set 'enable-table-lock' in the configuration
/// file."` (and the UNLOCK spelling), the table meta keeps `Lock == nil`.
// go-parity-gap: this tier has no LOCK TABLES statement carrier and no
// `enable-table-lock` config.
#[test]
#[ignore]
fn lock_tables_with_disabled_config_warns_1235_and_stores_no_lock() {
}

/// Go `serial_test.go:1341-1376::TestForbidUnsupportedCollations`: the
/// unsupported-collation gate `charset.GetCollationByName` raises
/// `[ddl:1273]Unsupported collation when new collation is enabled:
/// '<coll>'` from `create database`/`alter database`/`create table`/`alter
/// table ... default collate|convert to charset`/`alter table modify ...
/// collate`, for `utf8mb4_roman_ci` and `utf8_roman_ci`, while the supported
/// `utf8mb4_general_ci` builds.
// go-parity-gap: this tier answers `[1105] unknown collation` for the same
// statements (measured, `create table ucc ...`), i.e. the refusal exists but
// neither Go's code (1273) nor its message is carried, and this tier has no
// ALTER DATABASE / ALTER TABLE collate carrier to drive the remaining arms.
#[test]
#[ignore]
fn unsupported_collations_answer_ddl_1273_on_every_statement_kind() {
}

/// Go `serial_test.go:1396-1419::TestCheckEnumLength`: an ENUM/SET member
/// longer than the 255-character gate answers
/// `errno.ErrTooLongValueForType` (3505, "Too long enumeration/set value for
/// column %s.", pkg/errno/errname.go:868) on CREATE TABLE and on
/// ALTER TABLE ADD; with `EnableEnumLengthLimit=false` the same members are
/// stored and read back verbatim; with it back on, creation is refused
/// again.
// go-parity-gap: this tier has no enum/set member-length gate and no
// EnableEnumLengthLimit switch — a 301-character member builds fine
// (measured).
#[test]
#[ignore]
fn enum_set_member_length_limit_follows_the_config_switch() {
}

/// Go `serial_test.go:1421-1485::TestGetReverseKey`: over a split table with
/// rows at MinInt64..MaxInt64, `ddl.GetRangeEndKey`
/// (`pkg/ddl/reorg.go`, exported by `GetMaxRowID` at serial_test.go:60)
/// returns the LARGEST row key below the requested range end for
/// `[minInt64, minInt64+1]`, `[minInt64, 1<<61)`, `[1<<61, 2<<61)` and
/// `[3<<61, maxInt64]` — the reverse-scan bound the backfill range
/// calculator needs.
// go-parity-gap: this tier has no carrier of `GetRangeEndKey` and no mock
// cluster to split.
#[test]
#[ignore]
fn get_range_end_key_returns_the_largest_row_key_under_the_range_end() {
}
