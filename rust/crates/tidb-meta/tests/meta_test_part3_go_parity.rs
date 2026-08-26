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

//! Tail slice of Go `pkg/meta/meta_test.go` on `origin/master` (batch b041,
//! "pkg/meta.part3"): the two `IsTableInfoMustLoad` / `TableNameInfo`
//! benchmark bodies re-stated as correctness assertions over the same narrow /
//! wide table shapes, the four small scalar/system-database tests at the end
//! of the file, and the three InfoSchemaV2 bootstrap tests that stay
//! go-parity gaps because they need the session-bootstrap + InfoSchemaV2
//! pipeline that does not exist in this workspace.
//!
//! Earlier slices of the file are pinned by other modules in this directory
//! (see `rust/testport/receipts/b039.md` for that mapping).

use tidb_ast::CiString;
use tidb_meta::transaction::{
    fast_unmarshal_table_name_info, table_info_must_load, MemoryTransaction, Mutator,
    NextGenBootTableVersion, TtlTuneFactors, DdlTableVersion,
};
use tidb_meta::value;
use tidb_model::go_runtime::GoSharedPointerSlice;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::table_info::TableInfo;
use tidb_model::column::ColumnInfo;

/// Go `benchCases` (`pkg/meta/meta_test.go:1017`): the narrow and wide CREATE
/// TABLE shapes whose marshalled TableInfo feeds both benchmarks. Go builds
/// them through `ddl.MockTableInfo`; here the equivalent TableInfo values are
/// constructed directly and serialized with the Go-compatible marshaller.
fn bench_table_infos() -> Vec<TableInfo> {
    // narrow: `CREATE TABLE t (c INT PRIMARY KEY)`.
    let mut narrow = TableInfo {
        id: 1,
        name: CiString::new("t"),
        ..Default::default()
    };
    narrow.columns = vec![ColumnInfo {
        id: 1,
        name: CiString::new("c"),
        offset: 0,
        ..Default::default()
    }]
    .into();

    // wide: many typed columns plus five indexes (incl. a multi-column unique
    // index), which is what makes the JSON large.
    let mut wide = TableInfo {
        id: 1,
        name: CiString::new("t"),
        ..Default::default()
    };
    let names = [
        "c", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13",
    ];
    wide.columns = names
        .iter()
        .enumerate()
        .map(|(offset, name)| ColumnInfo {
            id: offset as i64 + 1,
            name: CiString::new(*name),
            offset: offset as i64,
            ..Default::default()
        })
        .collect::<Vec<_>>()
        .into();
    let index_columns = |offsets: &[i64]| -> GoSharedPointerSlice<IndexColumn> {
        offsets
            .iter()
            .map(|&offset| IndexColumn {
                name: CiString::new(names[offset as usize]),
                offset,
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into()
    };
    let index = |id: i64, name: &str, offsets: &[i64]| IndexInfo {
        id,
        name: CiString::new(name),
        columns: index_columns(offsets),
        ..Default::default()
    };
    wide.indices = vec![
        index(1, "idx", &[1]),
        index(2, "idx2", &[3, 4]),
        index(3, "idx3", &[5, 1]),
        index(4, "idx4", &[11]),
        index(5, "idx5", &[0, 1]),
    ]
    .into();
    vec![narrow, wide]
}

/// Go `BenchmarkIsTableInfoMustLoad` / `benchIsTableInfoMustLoad`
/// (`pkg/meta/meta_test.go:1042-1062`): for every bench-case TableInfo the
/// loop asserts `!IsTableInfoMustLoad(data)` — a fully-populated ordinary
/// table (columns and indexes only, none of the seven special attributes)
/// must keep taking the by-name fast path.
#[test]
fn is_table_info_must_load_is_false_for_bench_table_shapes() {
    for table in bench_table_infos() {
        let data = value::serialize_table_info(&table).unwrap();
        assert!(
            !table_info_must_load(&data),
            "ordinary table {} must not be flagged must-load",
            std::str::from_utf8(&data).unwrap()
        );
    }
}

/// Go `BenchmarkTableNameInfo` / `benchJSONTableNameInfo` +
/// `benchFastJSONTableNameInfo` (`pkg/meta/meta_test.go:1064-1106`): both the
/// full-JSON decode and the fast partial decoder read `ID == 1` and
/// `Name.L == "t"` from every bench-case payload; the two decoders agree.
#[test]
fn table_name_info_decoders_agree_on_bench_shapes() {
    for table in bench_table_infos() {
        let data = value::serialize_table_info(&table).unwrap();

        // Fast path (Go meta.FastUnmarshalTableNameInfo).
        let fast = fast_unmarshal_table_name_info(&data).unwrap();
        assert_eq!(fast.id, 1);
        assert_eq!(fast.name.lowercase(), "t");

        // Full decode path (Go json.Unmarshal into model.TableNameInfo):
        // round-trip the whole TableInfo through the crate's parser and
        // compare the identity fields.
        let full = value::parse_table_info(&data, /* db_id */ 7).unwrap();
        assert_eq!(full.id, 1);
        assert_eq!(full.name.lowercase(), "t");
        assert_eq!(full.name.original(), fast.name.original());
    }
}

/// Go `TestIsDatabaseExist` (`pkg/meta/meta_test.go:1375`): inside one
/// transaction, database 123 does not exist before
/// `CreateSysDatabaseByID("aaa", 123)` and exists after.
#[test]
fn is_database_exist_tracks_sys_database_creation() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert!(!meta.database_exists(123).unwrap());
    meta.create_sys_database_by_id("aaa", 123).unwrap();
    assert!(meta.database_exists(123).unwrap());
}

/// Go `TestBootTableVersion` (`pkg/meta/meta_test.go:1396`): a fresh cluster
/// reads `InitNextGenBootTableVersion`; setting `BaseNextGenBootTableVersion`
/// reads back; and the write went to the next-gen key specifically — the DDL
/// table version still reads `InitDDLTableVersion`.
#[test]
fn boot_table_version_writes_only_the_next_gen_key() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert_eq!(
        meta.next_gen_boot_table_version().unwrap(),
        NextGenBootTableVersion::INIT
    );
    meta.set_next_gen_boot_table_version(NextGenBootTableVersion::BASE)
        .unwrap();
    assert_eq!(
        meta.next_gen_boot_table_version().unwrap(),
        NextGenBootTableVersion::BASE
    );
    // Make sure we use the correct key.
    assert_eq!(meta.ddl_table_version().unwrap(), DdlTableVersion::INIT);
}

/// Go `TestCreateSysDatabaseByIDIfNotExists` (`pkg/meta/meta_test.go:1421`):
/// the first call creates database 123 (so it exists afterwards) and the
/// second call is a no-op success rather than an `ErrDatabaseExists`.
#[test]
fn create_sys_database_by_id_if_not_exists_is_idempotent() {
    let meta = Mutator::new(MemoryTransaction::default());
    meta.create_sys_database_by_id_if_not_exists("aaa", 123)
        .unwrap();
    assert!(meta.database_exists(123).unwrap());
    meta.create_sys_database_by_id_if_not_exists("aaa", 123)
        .unwrap();
    // The second creation is a no-op, and database 123 keeps its original
    // record (name "aaa").
    meta.create_sys_database_by_id_if_not_exists("other", 123)
        .unwrap();
    assert_eq!(meta.database(123).unwrap().unwrap().name.original(), "aaa");
}

/// Go `TestSetGetDXFScheduleTuneFactors` (`pkg/meta/meta_test.go:1442`, run
/// on next-gen kernels; skipped as classic here): before any set,
/// `GetDXFScheduleTuneFactors` returns nil; after storing
/// `TTLTuneFactors{TTL: time.Hour, TuneFactors{AmplifyFactor: 1.5}}` the same
/// value is returned.
///
/// The Rust crate has no kernel gate at runtime for this accessor, so unlike
/// Go the assertion runs unconditionally on the classic build too — the
/// storage format is kernel-independent (hash under `DXFScheduleTune`).
#[test]
fn set_get_dxf_schedule_tune_factors_round_trips() {
    let keyspace = ""; // Go store.GetKeyspace() is empty on unistore mocks.
    let factors = TtlTuneFactors {
        ttl_nanoseconds: 3_600_000_000_000, // time.Hour
        expire_time: Default::default(),
        amplify_factor: 1.5,
    };

    // Not set yet.
    let fresh = Mutator::new(MemoryTransaction::default());
    assert_eq!(fresh.dxf_schedule_tune_factors(keyspace).unwrap(), None);

    // Set it, then read it back (a `Mutator` handle shares the transaction,
    // matching Go's committed-value read).
    fresh
        .set_dxf_schedule_tune_factors(keyspace, &factors)
        .unwrap();
    let got = fresh
        .dxf_schedule_tune_factors(keyspace)
        .unwrap()
        .unwrap();
    assert_eq!(got.ttl_nanoseconds, factors.ttl_nanoseconds);
    assert_eq!(got.amplify_factor, factors.amplify_factor);
    assert_eq!(got.expire_time, factors.expire_time);
}
