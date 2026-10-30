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

//! Go `ddl.CreateTable` and its job run in one process over the in-process
//! [`Catalog`]: the executor's test fixtures build their tables here
//! ([`run_create_table_on`]). The table is the DDL package's own --
//! `BuildTableInfoWithStmt` / `BuildTableInfoWithLike`
//! (`tidb_ddl_build::table_info_build`), checked by
//! `checkTableInfoValidWithStmt` and `checkTableInfoValidExtra` -- loaded
//! back with `TableFromMeta` (`tidb_table::table_from_meta::cluster_table`),
//! so a fixture's table is the one a node's DDL job builds from the same
//! statement. It goes with Stage 5's in-process catalog.
//!
//! The submodules are the executor's own: `hypo_index` (the index
//! advisor's hypothetical indexes) and `indexes` (an index backfill
//! failure's rendering).

mod hypo_index;
mod indexes;
#[cfg(test)]
mod tests_modify_column_type;
#[cfg(test)]
mod tests_table_partition;

pub use hypo_index::set_advisor_hypo_index;
pub use indexes::{index_backfill_error};


use crate::driver::DriverError;
use tidb_infoschema::Catalog;
use tidb_ast::{DdlStmt, Stmt};

/// Go `CREATE TABLE` through its job, in one process: the table built from
/// the statement (`BuildTableInfoWithStmt`), the ids the job draws
/// (`assignGIDsForJobs`, or a bootstrap table's reserved metadef id), and the
/// table loaded back from that metadata (`TableFromMeta`), registered in
/// `catalog`. Returns whether a table was created (`false` only for `IF NOT
/// EXISTS` over an existing name).
pub fn create_table_in_catalog(
    create: &tidb_ast::CreateTableStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    reserved_table_id: Option<i64>,
) -> Result<bool, DriverError> {
    let (database, name) = tidb_infoschema::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let Some(charset) = catalog.database_charset(&database) else {
        return Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[(database).into()]),
        ));
    };
    if catalog.contains_in(&database, &name) {
        let exists = DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
                .generate_with_stack_by_args(&[(format!("{database}.{name}")).into()]),
        );
        if create.if_not_exists {
            ctx.append_suppressed(&exists.clone().to_mysql_error());
            return Ok(false);
        }
        return Err(exists);
    }
    let refused = |error: tidb_ddl_build::table_info_build::DdlAdmissionError| {
        DriverError::Mysql(error.to_mysql_error())
    };
    let mb_ctx = new_metabuild_context_with_sctx(ctx);
    let mut info = match &create.like_table {
        // Go `CreateTable` with `ReferTable`: the preprocessor's checks on
        // the source, then `BuildTableInfoWithLike` over `referTbl.Meta()`.
        Some(source) => {
            let (source_database, source_name) =
                tidb_infoschema::split_table_path_pub(source, current_db)?;
            let not_exists = || {
                DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[
                            (&source_database).into(),
                            (&source_name).into(),
                        ]),
                )
            };
            let refer = match catalog.table_in(source_database, source_name) {
                Some(crate::TableEntry::Kv(table)) => table.meta(),
                // A view or sequence reaches `BuildTableInfoWithLike`'s
                // `ErrWrongObject`, which names the TARGET schema.
                Some(crate::TableEntry::View(_) | crate::TableEntry::Sequence(_)) => {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT
                            .generate_with_stack_by_args(&[
                                (&database).into(),
                                (&source_name).into(),
                                ("BASE TABLE").into(),
                            ]),
                    ));
                }
                Some(crate::TableEntry::Mem(_)) => {
                    return Err(DriverError::unsupported(
                        "CREATE TABLE ... LIKE a fixture table with no TableInfo",
                    ));
                }
                None => return Err(not_exists()),
            };
            let refer = refer.read();
            tidb_ddl_build::check_create_table_like_source(&refer, create.temporary)
                .map_err(DriverError::Mysql)?;
            tidb_ddl_build::table_info_build::build_table_info_with_like(
                &database,
                &name,
                &refer,
                create.temporary,
                create.on_commit_delete,
            )
            .map_err(refused)?
        }
        None => tidb_ddl_build::table_info_build::build_table_info_in_schema(
            create,
            &database,
            charset.charset.name(),
            charset.collation.name(),
            &mb_ctx,
            ctx.enable_check_constraint(),
        )
        .map_err(refused)?,
    };
    // Go `CreateTable`: `checkTableInfoValidWithStmt` and the region split
    // policies, then `createTableWithInfoJob`'s `checkTableInfoValidExtra`.
    tidb_ddl_build::table_info_build::check_table_info_valid_with_stmt(&mb_ctx, &info)
        .map_err(refused)?;
    tidb_ddl_build::table_info_build::set_create_table_split_policies(create, &mut info)
        .map_err(refused)?;
    tidb_ddl_build::table_info_build::check_table_info_valid_extra(
        &mut |warning| mb_ctx.append_warning(warning),
        &info,
    )
    .map_err(refused)?;
    info.id = reserved_table_id.unwrap_or_else(fixture_global_id);
    if let Some(partition) = info.partition.as_ref() {
        let count = partition.read().definitions.len();
        for index in 0..count {
            let id = fixture_global_id();
            partition
                .read()
                .definitions
                .update(index, |definition| definition.id = id);
        }
    }
    info.state = tidb_model::SchemaState::PUBLIC;
    let db_id = catalog.database_id(&database).unwrap_or_default();
    let auto_ids = tidb_table::table_from_meta::LocalTableAutoIds::default();
    let table = tidb_table::table_from_meta::cluster_table(
        &info,
        &tidb_kv::storage::MemTableStorage::new(),
        &tidb_table::table_from_meta::AutoIdSource::In { db_id, ids: &auto_ids },
    )
    .map_err(DriverError::unsupported)?;
    catalog.register_kv_in(&database, &name, table)?;
    Ok(true)
}

/// The ids a table built with no store behind it takes: Go's tests over
/// `MockInfoSchema` number their tables themselves, a stored table's come
/// from meta's `GenGlobalID`. One process-wide sequence, so two fixtures'
/// tables never share an id.
fn fixture_global_id() -> i64 {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
pub fn run_create_table_on(sql: &str, catalog: &mut Catalog) -> Result<bool, DriverError> {
    // A stock session's context: UTC, the SHIPPED strict `sql_mode` and
    // `tidb_enable_clustered_index` ON (`StmtContext`'s derived `Default`
    // is non-strict and leaves clustering at Go's zero value, OFF).
    run_create_table_on_with(
        sql,
        catalog,
        tidb_executor_default_database(),
        &crate::StmtContext::for_query(),
    )
}

/// [`run_create_table_on`] in `current_db` under `ctx`: the session settings
/// (SQL mode, time zone, clock, `tidb_enable_clustered_index`) the statement
/// is built with.
pub fn run_create_table_on_with(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let stmt = tidb_parser::parse(sql)
        .map_err(|e| DriverError::from(tidb_util::misc::syntax_error(format!("{e:?}"))))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    let DdlStmt::CreateTable(create) = &**ddl else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    // Go's statement path: `Compile`'s preprocessor (`checkCreateTableGrammar`)
    // before the DDL executor.
    tidb_planner::preprocess::check_ddl_grammar(&stmt, ctx.strict()).map_err(DriverError::Mysql)?;
    create_table_in_catalog(create, catalog, current_db, ctx, None)
}

/// The default schema an unqualified `CREATE TABLE` lands in.
fn tidb_executor_default_database() -> &'static str {
    tidb_infoschema::DEFAULT_DATABASE
}

use tidb_ddl_build::metabuild::new_metabuild_context_with_sctx;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{run_insert_on, run_select_on};
    use tidb_datatype::Datum;
    use tidb_charset::Charset;
use tidb_charset::Collation;
    use tidb_table::kv_table::TableCharset;

    /// CREATE TABLE -> INSERT -> SELECT, all from SQL strings, with rows as
    /// real TiKV-format bytes and metadata as tidb-model structs.
    #[test]
    fn create_insert_select_from_sql() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        assert!(run_create_table_on(
            "CREATE TABLE t (a BIGINT, b BIGINT UNSIGNED, s VARCHAR(10))",
            &mut catalog
        )
        .unwrap());

        assert_eq!(
            run_insert_on(
                "INSERT INTO t (a, s) VALUES (7, 'x')",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        let rows = run_select_on(
            "SELECT a, s FROM t",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Datum::Int(7));
        match &rows[0][1] {
            Datum::Bytes(b) => assert_eq!(b.as_slice(), b"x"),
            Datum::String(s) => assert_eq!(s.bytes(), b"x"),
            other => panic!("unexpected string datum {other:?}"),
        }
    }

    /// An index's visibility is stated by the DDL and read by the planner off
    /// `KvIndex::visible` -- one fact, two places. Every statement that
    /// creates an index resolves it at the single point that builds the
    /// `KvIndex`, so an `INVISIBLE` key is maintained (it is in `indexes()`)
    /// and hidden from every access path (it is not in `plan_indexes()`),
    /// which is Go's rule. Hardcoding `visible: true` -- as all three sites
    /// did -- made the planner choose an index Go never chooses. The
    /// `CREATE INDEX` and `ADD INDEX` sites are the session's
    /// (`tidb-session/tests/index_ddl_prefix_and_visibility_source.rs`).
    #[test]
    fn an_index_declared_invisible_is_maintained_but_never_planned() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on(
            "CREATE TABLE t (a INT, b INT, KEY idx_a (a) INVISIBLE, KEY idx_b (b))",
            &mut catalog,
        )
        .unwrap();
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t") else {
            panic!("expected a kv table");
        };
        let maintained: Vec<&str> = kv.indexes().iter().map(|i| i.name.as_str()).collect();
        let planned: Vec<&str> = kv.plan_indexes().map(|i| i.name.as_str()).collect();
        assert_eq!(
            maintained,
            vec!["idx_a", "idx_b"],
            "every declared index is maintained by writes"
        );
        assert_eq!(
            planned,
            vec!["idx_b"],
            "only the visible one is an access path"
        );
    }

    #[test]
    fn if_not_exists_and_duplicates() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap());
        assert!(run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).is_err());
        assert!(
            !run_create_table_on("CREATE TABLE IF NOT EXISTS t (a INT)", &mut catalog).unwrap()
        );
    }

    /// A single-column integer primary key sets Go's `PKIsHandle`; a
    /// non-integer one does not (`isIntCol`).
    #[test]
    fn primary_key_sets_the_handle_flag_only_for_an_integer_column() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on("CREATE TABLE t (a INT, PRIMARY KEY (a))", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE s (a VARCHAR(4) PRIMARY KEY)", &mut catalog).unwrap();
        run_create_table_on("CREATE TABLE h (a INT)", &mut catalog).unwrap();

        let handle_offset = |name: &str| match catalog.get_table_for_test(name) {
            Some(crate::TableEntry::Kv(kv)) => kv.pk_handle_offset(),
            _ => panic!("expected a kv table"),
        };
        assert_eq!(handle_offset("t"), Some(0));
        assert_eq!(handle_offset("s"), None, "a string PK is not a handle");
        assert_eq!(handle_offset("h"), None, "no PK, no handle column");
    }

    /// The `(charset, collation, flen)` a column resolves to, for the
    /// charset/collation captures below.
    fn resolved(catalog: &Catalog, table: &str, column: &str) -> (String, String, i64) {
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test(table) else {
            panic!("expected a kv table");
        };
        let column = kv
            .columns()
            .iter()
            .find(|c| c.name == column)
            .expect("column exists");
        (
            column.field_type.charset_name().to_owned(),
            column.field_type.collation_name().to_owned(),
            column.field_type.flen(),
        )
    }

    /// Captured from TiDB (`SHOW FULL COLUMNS` + `information_schema.columns`
    /// over one table carrying every string form): the BINARY/VARBINARY/BLOB
    /// family is charset `binary`, the CHAR/VARCHAR/TEXT family takes the
    /// table's default (`utf8mb4`/`utf8mb4_bin` -- NOT `utf8mb4_0900_ai_ci`),
    /// an explicit COLLATE wins, and the `BINARY` column attribute picks the
    /// charset's `_bin` collation.
    #[test]
    fn ddl_resolves_charset_and_collation_per_column() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on(
            "CREATE TABLE t1 (\
                 c_varchar VARCHAR(10), c_char CHAR(10), \
                 c_varbinary VARBINARY(10), c_binary BINARY(3), \
                 c_blob BLOB, c_text TEXT, c_tinytext TINYTEXT, c_longblob LONGBLOB, \
                 c_vc_cs VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, \
                 c_vc_bin VARCHAR(10) BINARY, \
                 c_enum ENUM('a','B'), c_set SET('a','B'), c_int INT)",
            &mut catalog,
        )
        .unwrap();

        let case = |column: &str| resolved(&catalog, "t1", column);
        assert_eq!(
            case("c_varchar"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 10)
        );
        assert_eq!(case("c_char"), ("utf8mb4".into(), "utf8mb4_bin".into(), 10));
        assert_eq!(case("c_varbinary"), ("binary".into(), "binary".into(), 10));
        assert_eq!(case("c_binary"), ("binary".into(), "binary".into(), 3));
        // The BLOB/TEXT family carries its type's fixed capacity as flen.
        assert_eq!(case("c_blob"), ("binary".into(), "binary".into(), 65535));
        assert_eq!(
            case("c_text"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 65535)
        );
        assert_eq!(
            case("c_tinytext"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 255)
        );
        assert_eq!(
            case("c_longblob"),
            ("binary".into(), "binary".into(), 4_294_967_295)
        );
        assert_eq!(
            case("c_vc_cs"),
            ("utf8mb4".into(), "utf8mb4_general_ci".into(), 10)
        );
        // `VARCHAR(10) BINARY` is the charset's `_bin` collation, NOT charset
        // `binary`: it still reports utf8mb4 (captured).
        assert_eq!(
            case("c_vc_bin"),
            ("utf8mb4".into(), "utf8mb4_bin".into(), 10)
        );
        // ENUM/SET take the table charset, and their flen is the display
        // length Go derives from the members.
        assert_eq!(case("c_enum"), ("utf8mb4".into(), "utf8mb4_bin".into(), 1));
        assert_eq!(case("c_set"), ("utf8mb4".into(), "utf8mb4_bin".into(), 3));

        // A binary-charset string column has no charset for `HasCharset`,
        // which is what makes SHOW/information_schema report NULL for it.
        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t1") else {
            panic!("expected a kv table");
        };
        let has_charset = |column: &str| {
            kv.columns()
                .iter()
                .find(|c| c.name == column)
                .unwrap()
                .field_type
                .has_charset()
        };
        assert!(has_charset("c_varchar") && has_charset("c_enum"));
        assert!(!has_charset("c_varbinary") && !has_charset("c_blob"));
        assert!(!has_charset("c_int"));
    }

    /// Captured from TiDB over `... DEFAULT CHARSET=latin1`: a column with no
    /// clause takes the table's charset AND collation, an explicit
    /// `CHARACTER SET utf8mb4` takes that charset's default collation, and
    /// `CHARACTER SET binary` turns a VARCHAR into a `varbinary`.
    #[test]
    fn ddl_column_charset_falls_back_to_the_table_default() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on(
            "CREATE TABLE t2 (a VARCHAR(10), b VARCHAR(10) CHARACTER SET utf8mb4, \
                 c VARCHAR(10) CHARACTER SET latin1, d VARCHAR(10) CHARACTER SET binary) \
                 DEFAULT CHARSET=latin1",
            &mut catalog,
        )
        .unwrap();
        let case = |column: &str| resolved(&catalog, "t2", column);
        assert_eq!(case("a"), ("latin1".into(), "latin1_bin".into(), 10));
        assert_eq!(case("b"), ("utf8mb4".into(), "utf8mb4_bin".into(), 10));
        assert_eq!(case("c"), ("latin1".into(), "latin1_bin".into(), 10));
        assert_eq!(case("d"), ("binary".into(), "binary".into(), 10));

        let Some(crate::TableEntry::Kv(kv)) = catalog.get_table_for_test("t2") else {
            panic!("expected a kv table");
        };
        assert_eq!(
            kv.charset(),
            TableCharset {
                charset: Charset::Latin1,
                collation: Collation::Latin1Bin,
            }
        );
    }

    /// A `COLLATE` alone determines the charset, and a `COLLATE` that does not
    /// belong to the written `CHARACTER SET` is rejected rather than silently
    /// producing a contradictory field type.
    #[test]
    fn ddl_collate_alone_picks_the_charset_and_a_mismatched_pair_is_rejected() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on(
            "CREATE TABLE t (a VARCHAR(10) COLLATE latin1_bin)",
            &mut catalog,
        )
        .unwrap();
        assert_eq!(
            resolved(&catalog, "t", "a"),
            ("latin1".into(), "latin1_bin".into(), 10)
        );
        assert!(run_create_table_on(
            "CREATE TABLE bad (a VARCHAR(10) CHARACTER SET latin1 COLLATE utf8mb4_bin)",
            &mut catalog,
        )
        .is_err());
    }

    /// Captured from TiDB: `INSERT INTO tb(b BINARY(3)) VALUES ('ab')` reads
    /// back as `0x616200` with `LENGTH` 3 -- a fixed-width binary column is
    /// zero-padded to its flen -- while `VARBINARY(3)` keeps the two bytes.
    #[test]
    fn binary_column_zero_pads_to_its_length() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on(
            "CREATE TABLE tb (b BINARY(3), vb VARBINARY(3))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO tb VALUES ('ab','ab')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let rows = run_select_on(
            "SELECT b, vb FROM tb",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let bytes = |value: &Datum| match value {
            Datum::Bytes(b) => b.clone(),
            Datum::String(s) => s.bytes().to_vec(),
            other => panic!("unexpected string datum {other:?}"),
        };
        assert_eq!(bytes(&rows[0][0]), vec![b'a', b'b', 0]);
        assert_eq!(bytes(&rows[0][1]), vec![b'a', b'b']);
    }
}


