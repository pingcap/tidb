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

#[allow(unused_imports)]
use tidb_table::partition_metadata::partition_spec_from_metadata;
#[allow(unused_imports)]
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
#[allow(unused_imports)]
use tidb_table::generated_column::TableColumnResolver;
#[allow(unused_imports)]
#[allow(unused_imports)]
use tidb_table::partition_routing::{
    PartitionDef, PartitionKind, PartitionSpec, RangeBound, RangeColumnBound,
};
#[allow(unused_imports)]
use tidb_error::mysql::MysqlError;
#[allow(unused_imports)]
use tidb_hack::GoCaseCompare;
#[allow(unused_imports)]
use tidb_hack::GoToLower;
#[allow(unused_imports)]
use tidb_ddl_build::table_partition::*;

/// The two halves of partitioning must agree. `build_partition_metadata`
/// renders a clause into the TEXT Go stores; `partition_spec_from_metadata`
/// reads that text back. A rendering that loses or reshapes a bound would
/// leave both halves looking right on their own while the table routed
/// rows to one partition and read them from another.
///
/// So every case here is run down BOTH paths and the results compared,
/// rather than each being checked against a written-out expectation.
/// A routing method rendered so that EQUAL specs render equally.
///
/// `PartitionKind::ListColumns` carries a `HashMap` of encoded keys, and
/// a `HashMap`'s `Debug` order is not stable between two maps holding the
/// same entries. Comparing the derived `Debug` text directly made this
/// check pass or fail on hash order rather than on the bounds, which is
/// exactly the sort of test that reports green while the thing it guards
/// is broken.
fn comparable(kind: &PartitionKind) -> String {
    match kind {
        PartitionKind::ListColumns {
            values,
            keys,
            default_partition,
            field_types,
        } => {
            let mut keys = keys
                .iter()
                .map(|(key, ordinal)| format!("{key:?}=>{ordinal}"))
                .collect::<Vec<_>>();
            keys.sort_unstable();
            format!("ListColumns {values:?} {keys:?} {default_partition:?} {field_types:?}")
        }
        other => format!("{other:?}"),
    }
}

fn routes_the_same_way(sql: &str) {
    let statement = tidb_parser::parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("{sql} is not DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
        panic!("{sql} is not CREATE TABLE");
    };
    let names = create
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let types = create
        .columns
        .iter()
        .map(|column| {
            tidb_ddl_build::column_field_type::build_field_type(
                &column.name,
                &column.ty,
                "utf8mb4",
                "utf8mb4_bin",
            )
            .unwrap_or_else(|error| panic!("{}: {error}", column.name))
        })
        .collect::<Vec<_>>();
    let ctx = crate::StmtContext::for_query();
    let mut next = 100;
    let built = build_partition_metadata(
        create,
        &names,
        &types,
        &[],
        &[],
        &mut || {
            next += 1;
            next
        },
        &crate::ddl::new_metabuild_context_with_sctx(&ctx),
    )
    .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
    .unwrap_or_else(|| panic!("{sql} declares partitioning"));
    let (stored, direct) = built;
    let mut definitions = stored.definitions.clone();
    for (ordinal, definition) in definitions.iter_mut().enumerate() {
        definition.id = direct.definitions[ordinal].id;
    }
    let reloaded = partition_spec_from_metadata(
        stored.kind,
        &stored.expr,
        &stored.columns,
        stored.is_empty_columns,
        &definitions,
        &[],
        &names,
        &types,
    )
    .unwrap_or_else(|error| {
        panic!(
            "{sql} reloads: {error:?}\n  kind={:?} expr={:?} columns={:?}\n  definitions={:?}",
            stored.kind, stored.expr, stored.columns, definitions
        )
    });
    assert_eq!(
        comparable(&direct.kind),
        comparable(&reloaded.kind),
        "{sql}: the stored text did not fold back to the bounds it came from"
    );
    assert_eq!(
        direct.expr_text, reloaded.expr_text,
        "{sql}: the reloaded spec prints a different clause"
    );
    assert_eq!(
        direct.dependencies, reloaded.dependencies,
        "{sql}: the reloaded spec reads different columns"
    );
    assert_eq!(
        direct.physical_ids(),
        reloaded.physical_ids(),
        "{sql}: the reloaded spec routes to different physical tables"
    );
}

/// Go exempts a GLOBAL unique index from the covering rule because its
/// logical-table keyspace enforces the constraint across partitions.
#[test]
fn a_global_unique_index_is_exempt_from_the_partition_covering_rule() {
    let sql = "CREATE TABLE t (id BIGINT, v BIGINT, UNIQUE KEY uv (v) GLOBAL) \
               PARTITION BY HASH (id) PARTITIONS 2";
    let statement = tidb_parser::parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("not DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
        panic!("not CREATE TABLE");
    };
    let names = vec!["id".to_owned(), "v".to_owned()];
    let types = vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    // Only `v`: the partition column `id` is NOT covered, which is what
    // brings the rule into play at all.
    let indexes = vec![index_info("uv", true, true, &["v"])];
    build_table_partitioning(
        create,
        &names,
        &types,
        &indexes,
        &[],
        &mut || 1,
        &crate::ddl::new_metabuild_context_with_sctx(&crate::StmtContext::for_query()),
    )
    .expect("Go accepts a GLOBAL key that does not cover the partition column");
}

/// Go's own golden matrix for `PARTITION BY KEY ()`
/// (`tests/integrationtest/r/ddl/partition.result`): which key the empty
/// column list resolves to, and when the statement is refused.
///
/// Reading only the clustered handle gave k2 and k3 an EMPTY column list,
/// and `key_partition_index` hashes the empty byte stream -- crc32 of
/// nothing is 0 -- so every row routed to partition 0 while `SHOW CREATE`
/// still printed `PARTITION BY KEY ()` and nothing surfaced the loss.
#[test]
fn an_empty_key_clause_resolves_the_key_go_resolves() {
    // (label, columns, not_null, handle_offsets, indexes, expected)
    let long = || FieldType::new(FieldTypeCode::LongLong);
    let not_null = || {
        let mut t = FieldType::new(FieldTypeCode::LongLong);
        t.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        t
    };
    let names = ["id", "id1"];
    let index = |name: &str, unique: bool, offsets: Vec<usize>| {
        let columns = offsets.iter().map(|offset| names[*offset]).collect::<Vec<_>>();
        index_info(name, unique, false, &columns)
    };

    // k1: the primary key IS the handle.
    assert_eq!(
        key_clause_dependencies(&[not_null(), long()], &[0], &[]).expect("k1 is legal"),
        vec!["id".to_owned()]
    );
    // k2: no handle; the implicit primary key is the first UNIQUE key
    // whose columns are all NOT NULL.
    assert_eq!(
        key_clause_dependencies(&[not_null(), long()], &[], &[index("id", true, vec![0])])
            .expect("k2 is legal"),
        vec!["id".to_owned()]
    );
    // k3: an explicit PRIMARY index wins even when it is NONCLUSTERED,
    // and even though a unique key precedes it.
    assert_eq!(
        key_clause_dependencies(
            &[not_null(), long()],
            &[],
            &[index("id", true, vec![0]), index("PRIMARY", true, vec![0])]
        )
        .expect("k3 is legal"),
        vec!["id".to_owned()]
    );
    // k4: the only unique key covers a NULLABLE column, so it cannot be
    // the implicit primary key and Go refuses the statement with 1105.
    let error = key_clause_dependencies(
        &[not_null(), long()],
        &[],
        &[index("id_id1", true, vec![0, 1])],
    )
    .expect_err("k4 has no key that can serve");
    assert_eq!(error.code, 1105);
    // A table with NO keys at all keeps the empty list: Go returns early
    // from `checkPartitioningKeysConstraints` and hashes the empty
    // stream, so this shape must stay legal.
    assert_eq!(
        key_clause_dependencies(&[long(), long()], &[], &[]).expect("a heap table is legal"),
        Vec::<String>::new()
    );
}

/// Builds `PARTITION BY KEY () PARTITIONS 2` over a two-column table and
/// returns the columns it routes by.
fn key_clause_dependencies(
    types: &[FieldType],
    handle_offsets: &[usize],
    indexes: &[tidb_model::index::IndexInfo],
) -> Result<Vec<String>, MysqlError> {
    let sql = "CREATE TABLE t (id BIGINT, id1 BIGINT) PARTITION BY KEY () PARTITIONS 2";
    let statement = tidb_parser::parse(sql).expect("the fixture parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("not DDL");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
        panic!("not CREATE TABLE");
    };
    let names = vec!["id".to_owned(), "id1".to_owned()];
    build_table_partitioning(
        create,
        &names,
        types,
        indexes,
        handle_offsets,
        &mut || 1,
        &crate::ddl::new_metabuild_context_with_sctx(&crate::StmtContext::for_query()),
    )
    .map(|spec| spec.expect("the clause is there").dependencies)
}

/// Go `checkPartitionFuncType` accepts a whitelisted partition function
/// only when its RESULT type is an integer (`ddl/partition.go:1895`), and
/// the whitelist is not uniform about that.
///
/// Answering "every whitelisted name returns an integer" admitted these
/// at CREATE, and routing then read a Real/Decimal/Time datum and put
/// every row in partition 0 -- the table was created and answered
/// wrongly rather than being refused.
#[test]
fn a_partition_function_is_admitted_on_its_result_type_not_its_name() {
    // Go asserts both halves of the FLOOR split itself, at
    // `ddl/tests/partition/db_partition_test.go:241-242`.
    assert_eq!(
        partition_clause_error("FLOOR(c2)"),
        Some(1491),
        "FLOOR over a float is a REAL"
    );
    assert_eq!(
        partition_clause_error("FLOOR(c1)"),
        None,
        "FLOOR over an int is an int"
    );
    assert_eq!(partition_clause_error("ABS(c2)"), Some(1491));
    assert_eq!(partition_clause_error("CEILING(c2)"), Some(1491));
    // FROM_DAYS returns a DATE whatever it is given -- but WHICH error
    // it gets depends on the order Go runs its checks in.
    // `checkPartitionExprArgs` runs at `buildTablePartitionInfo:615` and
    // wants a date-or-time argument, long before `checkPartitionFuncType`
    // (`create_table.go:530`) ever looks at the result type. So a BIGINT
    // argument is refused by the ARGUMENT rule with 1486 ...
    assert_eq!(partition_clause_error("FROM_DAYS(c1)"), Some(1486));
    // ... and only an argument that SATISFIES the argument rule survives
    // to be refused for its DATE result type with 1491.
    assert_eq!(partition_clause_error("FROM_DAYS(d)"), Some(1491));
    // The argument rules themselves: a function wanting a DATE will not
    // take an integer column, and the TIMESTAMP ban on ABS/FLOOR/MOD is
    // how Go keeps a partition expression timezone-independent.
    assert_eq!(partition_clause_error("YEAR(c1)"), Some(1486));
    assert_eq!(partition_clause_error("FLOOR(ts0)"), Some(1486));
    // A partition expression that reads no column at all is 1486
    // (`partition.go:1855`): it is constant, so every row would route to
    // the same partition.
    assert_eq!(partition_clause_error("1 + 1"), Some(1486));
    // UNIX_TIMESTAMP keeps ETInt only at fsp 0; a fractional-second
    // argument makes it a DECIMAL (`builtin_time.go:4398`).
    assert_eq!(partition_clause_error("UNIX_TIMESTAMP(ts0)"), None);
    assert_eq!(partition_clause_error("UNIX_TIMESTAMP(ts3)"), Some(1491));
    // Go INFERS these from the argument's flen/decimal, which a table of
    // verdicts cannot reproduce. Each is asserted by Go's own tests.
    //
    // `FLOOR` over a DECIMAL is an integer whenever the integer part fits
    // in `MaxIntWidth - 2` digits (`builtin_math.go:701`).
    assert_eq!(partition_clause_error("FLOOR(d2)"), None);
    // And the nested case Go accepts at `db_partition_test.go:311`:
    // `unix_timestamp(ts3)` is `decimal(15,3)`, and 15 - 3 = 12 <= 18, so
    // FLOOR of it is an integer even though the inner call is not.
    assert_eq!(partition_clause_error("FLOOR(UNIX_TIMESTAMP(ts3))"), None);
    // `MOD` reads its arguments in NUMERIC context, where a temporal with
    // no fractional seconds is integer arithmetic
    // (`builtin_arithmetic.go:80`) -- so it cannot share a predicate with
    // ABS and FLOOR, which reject the very same column.
    assert_eq!(partition_clause_error("MOD(dt0, 2)"), None);
    assert_eq!(partition_clause_error("ABS(dt0)"), Some(1491));
    assert_eq!(partition_clause_error("FLOOR(dt0)"), Some(1491));
    // The single most common date-partitioning form stays legal: YEAR
    // returns an integer even though its argument is a DATE.
    assert_eq!(partition_clause_error("YEAR(d)"), None);
    assert_eq!(partition_clause_error("TO_DAYS(d)"), None);
}

/// The MySQL error a `PARTITION BY RANGE (<expr>)` clause is refused
/// with, or `None` when it is accepted.
fn partition_clause_error(expr: &str) -> Option<u16> {
    let sql = format!(
        "CREATE TABLE t (c1 BIGINT, c2 FLOAT, d DATE, ts0 TIMESTAMP, ts3 TIMESTAMP(3), \
         d2 DECIMAL(10,2), dt0 DATETIME) \
         PARTITION BY RANGE ({expr}) (PARTITION p0 VALUES LESS THAN (100))"
    );
    let statement = tidb_parser::parse(&sql).expect("the fixture parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("not DDL")
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
        panic!("not CREATE TABLE")
    };
    // Derive the column types the way the DDL path does, so the fixture
    // cannot drift from what a real CREATE TABLE produces -- a plain
    // TIMESTAMP is fsp 0, and hand-building the FieldType left it
    // unspecified, which clamps to 6 and reads as a DECIMAL.
    let names = create
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let types = create
        .columns
        .iter()
        .map(|column| {
            tidb_ddl_build::column_field_type::build_field_type(
                &column.name,
                &column.ty,
                "utf8mb4",
                "utf8mb4_bin",
            )
            .unwrap_or_else(|error| panic!("{}: {error}", column.name))
        })
        .collect::<Vec<_>>();
    build_table_partitioning(
        create,
        &names,
        &types,
        &[],
        &[],
        &mut || 1,
        &crate::ddl::new_metabuild_context_with_sctx(&crate::StmtContext::for_query()),
    )
    .err()
    .map(|error| error.code)
}

#[test]
fn every_method_survives_the_store_and_load_round_trip() {
    for sql in [
        "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY HASH (id) PARTITIONS 4",
        "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY KEY (id) PARTITIONS 2",
        "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        "CREATE TABLE t (id BIGINT, v BIGINT) PARTITION BY LIST (id) \
         (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3, 4))",
        "CREATE TABLE t (id BIGINT, v VARCHAR(16)) PARTITION BY RANGE COLUMNS (v) \
         (PARTITION p0 VALUES LESS THAN ('m'), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        "CREATE TABLE t (id BIGINT, v VARCHAR(16)) PARTITION BY LIST COLUMNS (v) \
         (PARTITION p0 VALUES IN ('a', 'b'), PARTITION pd VALUES IN (DEFAULT))",
    ] {
        routes_the_same_way(sql);
    }
}

/// A value whose stored text needs ESCAPING is the case a naive renderer
/// gets wrong: Go doubles backslashes and then quotes
/// (`driver.WrapInSingleQuotes`), and the text is re-parsed on every load.
#[test]
fn a_bound_needing_escaping_survives_the_round_trip() {
    routes_the_same_way(
        "CREATE TABLE t (id BIGINT, v VARCHAR(16)) PARTITION BY LIST COLUMNS (v) \
         (PARTITION p0 VALUES IN ('it''s'), PARTITION p1 VALUES IN ('a\\\\b'))",
    );
}

/// A Go `IndexInfo` over the named columns, for the partition builders.
fn index_info(
    name: &str,
    unique: bool,
    global: bool,
    columns: &[&str],
) -> tidb_model::index::IndexInfo {
    tidb_model::index::IndexInfo {
        name: tidb_ast::CiString::new(name.to_owned()),
        unique,
        global,
        primary: name == "PRIMARY",
        columns: columns
            .iter()
            .map(|column| tidb_model::index::IndexColumn {
                name: tidb_ast::CiString::new((*column).to_owned()),
                length: tidb_datatype::UNSPECIFIED_LENGTH,
                ..tidb_model::index::IndexColumn::default()
            })
            .collect::<Vec<_>>()
            .into(),
        ..tidb_model::index::IndexInfo::default()
    }
}
