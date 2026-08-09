// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The two `CREATE TABLE` metadata builders must agree about the flags a
//! column takes from its TYPE.
//!
//! Go stamps them once, in `processColumnFlags` (`pkg/ddl/add_column.go:1297`),
//! and this workspace now has one implementation of it too -- in
//! `tidb_executor::ddl::column_field_type`, called by
//! `tidb_exec::table_info_build`'s `TableInfo` builder and by the runnable
//! path's `column_types::field_type_of`. This test is the guard on the CALL:
//! sharing the function stops the two rule sets drifting, but not one builder
//! forgetting to run it, which is exactly the state this unit found -- the
//! `TableInfo` builder ran it and the live builder never had.
//!
//! The observable consequence of that omission, captured from real TiDB over
//! `create table y (a year, b bit(8), c int unsigned, d int zerofill,
//! e varchar(5))` with the row `(1990, 1, 1, 1, 'x')`:
//!
//! ```text
//! select a - 2000 from y   ERR   1690 BIGINT UNSIGNED value is out of range
//! select a + 0    from y   1990
//! ```
//!
//! while the live builder answered `-10`.

use tidb_datatype::FieldTypeFlags;
use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};

/// Every column shape whose flags come from the TYPE, from a DECLARED
/// modifier, or from neither -- so a builder that ran the pass on only some of
/// them, or that stamped flags on everything, fails here.
const FIXTURE: &str = "CREATE TABLE y (a YEAR, b BIT(8), c INT UNSIGNED, d INT ZEROFILL, \
                       e VARCHAR(5), f BLOB(100), g TEXT, h BIGINT UNSIGNED, i DECIMAL(5,2))";

#[test]
fn both_create_table_builders_stamp_the_same_type_flags() {
    let statement = tidb_parser::parse(FIXTURE).expect("the fixture parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("the fixture is a CREATE TABLE");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("the fixture is a CREATE TABLE");
    };

    let cluster = build_table_info(
        create,
        "utf8mb4",
        "utf8mb4_bin",
        ClusteredIndexDefMode::IntOnly,
    )
    .expect("the fixture is buildable as a TableInfo");

    let mut catalog = tidb_executor::Catalog::default();
    tidb_executor::run_create_table_on(FIXTURE, &mut catalog).expect("the fixture is runnable");
    let live = catalog
        .table_in("test", "y")
        .expect("the fixture created the table")
        .column_types();

    // The flag bits `processColumnFlags` owns. The key/nullability bits are
    // deliberately excluded: those come from the column OPTIONS, where the two
    // builders still differ (the `TableInfo` one stamps UNIQUE/MULTIPLE_KEY,
    // NO_DEFAULT_VALUE and ON_UPDATE_NOW and the live one does not), and
    // asserting them here would be asserting a divergence this unit did not
    // close.
    const TYPE_FLAGS: u32 =
        FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL | FieldTypeFlags::BINARY;

    assert_eq!(cluster.columns.len(), live.len());
    for (cluster_column, (live_name, live_type)) in cluster.columns.iter_deref().zip(live.iter()) {
        let cluster_column = cluster_column.read();
        let name = cluster_column.name.original();
        assert_eq!(name, live_name);
        assert_eq!(
            cluster_column.field_type.flags() & TYPE_FLAGS,
            live_type.flags() & TYPE_FLAGS,
            "column `{name}`: the TableInfo builder and the live builder \
             disagree about the flags Go's processColumnFlags stamps"
        );
        // The flen is asserted alongside because the flag and the width are
        // coupled: `build_field_type` narrows an unsigned integer's default
        // flen by one, and `YEAR` is one of Go's `IsTypeInteger` types, so a
        // builder that ran the pass too early would answer `year(3)` here
        // while still agreeing about every flag.
        assert_eq!(
            cluster_column.field_type.flen(),
            live_type.flen(),
            "column `{name}`: the two builders disagree about the flen"
        );
    }

    // Absolute values, so "both builders are wrong the same way" is not a
    // pass. `a` and `b` are the two the type alone decides.
    let flags = |name: &str| {
        live.iter()
            .find(|(column, _)| column == name)
            .unwrap_or_else(|| panic!("column `{name}`"))
            .1
            .flags()
            & TYPE_FLAGS
    };
    assert_eq!(
        flags("a"),
        FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL
    );
    assert_eq!(flags("b"), FieldTypeFlags::UNSIGNED);
    assert_eq!(flags("c"), FieldTypeFlags::UNSIGNED);
    assert_eq!(
        flags("d"),
        FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL
    );
    assert_eq!(flags("e"), 0);
    assert_eq!(flags("f"), FieldTypeFlags::BINARY);
    assert_eq!(flags("g"), 0);
    assert_eq!(flags("h"), FieldTypeFlags::UNSIGNED);
    assert_eq!(flags("i"), 0);
}
