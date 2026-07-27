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

//! `CREATE TABLE` execution: builds a `tidb_model::TableInfo` from the parsed
//! statement and registers a TiKV-byte-backed table for it -- tying the
//! transcreated metadata structs into the runnable path.
//!
//! This is the metadata slice of Go's `pkg/ddl` `buildTableInfo` /
//! `buildColumnAndConstraint`: column types map through `str_to_type` (Go
//! `types.StrToType`) with flen/decimal from the type arguments and the
//! unsigned flag. DEFERRED (documented): constraints/indexes (PK, UNIQUE,
//! FOREIGN KEY), column options (DEFAULT, NOT NULL, AUTO_INCREMENT, comments),
//! charset/collation resolution beyond the type's own defaults, TEMPORARY,
//! `CREATE TABLE ... LIKE`, partitioning, and the schema-version/DDL-job
//! machinery (the driver applies metadata directly; the DDL job queue is a
//! separate tier).

use crate::driver::{Catalog, DriverError};
use crate::kv_table::{KvColumn, KvIndex, KvTable};
use tidb_ast::CiString;
use tidb_ast::{ColumnDef, ColumnTypeArg, DdlStmt, Stmt};
use tidb_datatype::{
    str_to_type, Datum, FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags,
};
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;

/// Builds the column's `FieldType` from its parsed SQL type: code via
/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;
/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: u32 = 1 << 1;

/// Every index a `CREATE TABLE` declares, other than a primary key that
/// became the row handle.
///
/// Go's `buildTableInfo` turns each key constraint into an `IndexInfo` with
/// its own id, allocated in declaration order starting at 1.
///
/// DEFERRED (documented): FULLTEXT, VECTOR and COLUMNAR indexes, prefix
/// lengths, expression keys and index options, all rejected rather than
/// silently created as a plain index.
fn table_indexes(
    create: &tidb_ast::CreateTableStmt,
    columns: &[ColumnInfo],
    pk_is_handle: bool,
) -> Result<Vec<KvIndex>, DriverError> {
    let offset_of = |name: &str| -> Result<usize, DriverError> {
        columns
            .iter()
            .position(|col| col.name.original().eq_ignore_ascii_case(name))
            .ok_or(DriverError::Unsupported(
                "an index names a column the table does not define",
            ))
    };
    fn push(indexes: &mut Vec<KvIndex>, name: String, unique: bool, offsets: Vec<usize>) {
        indexes.push(KvIndex {
            id: (indexes.len() + 1) as i64,
            name,
            unique,
            column_offsets: offsets,
        });
    }
    let mut indexes: Vec<KvIndex> = Vec::new();

    for def in &create.columns {
        for option in &def.options {
            if let tidb_ast::ColumnOption::InlineKey(key) = option {
                match key.kind {
                    tidb_ast::InlineKeyKind::Unique => {
                        let offset = offset_of(&def.name)?;
                        push(&mut indexes, def.name.clone(), true, vec![offset]);
                    }
                    // A primary key that is not the row handle still needs an
                    // index to enforce its uniqueness.
                    tidb_ast::InlineKeyKind::Primary { .. } if !pk_is_handle => {
                        let offset = offset_of(&def.name)?;
                        push(&mut indexes, "PRIMARY".to_owned(), true, vec![offset]);
                    }
                    tidb_ast::InlineKeyKind::Primary { .. } => {}
                }
            }
        }
    }

    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::Index(index) = constraint else {
            continue;
        };
        match index.kind {
            tidb_ast::IndexConstraintKind::PrimaryKey if pk_is_handle => continue,
            tidb_ast::IndexConstraintKind::PrimaryKey
            | tidb_ast::IndexConstraintKind::Unique
            | tidb_ast::IndexConstraintKind::UniqueKey
            | tidb_ast::IndexConstraintKind::UniqueIndex
            | tidb_ast::IndexConstraintKind::Key
            | tidb_ast::IndexConstraintKind::Index => {}
            _ => {
                return Err(DriverError::Unsupported(
                    "FULLTEXT, VECTOR and COLUMNAR indexes are not supported yet",
                ))
            }
        }
        let unique = matches!(
            index.kind,
            tidb_ast::IndexConstraintKind::Unique
                | tidb_ast::IndexConstraintKind::UniqueKey
                | tidb_ast::IndexConstraintKind::UniqueIndex
                | tidb_ast::IndexConstraintKind::PrimaryKey
        );
        let mut offsets = Vec::with_capacity(index.parts.len());
        for part in &index.parts {
            let tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } = part
            else {
                return Err(DriverError::Unsupported(
                    "an expression index is not supported yet",
                ));
            };
            if prefix_len.is_some() {
                return Err(DriverError::Unsupported(
                    "a prefix-length index is not supported yet",
                ));
            }
            offsets.push(offset_of(name)?);
        }
        let name = match index.kind {
            tidb_ast::IndexConstraintKind::PrimaryKey => "PRIMARY".to_owned(),
            _ => index
                .name
                .clone()
                .unwrap_or_else(|| format!("idx_{}", indexes.len() + 1)),
        };
        push(&mut indexes, name, unique, offsets);
    }
    Ok(indexes)
}

/// Go `isIntCol`: whether the column's type can carry a handle.
fn is_int_column(column: &ColumnInfo) -> bool {
    matches!(
        column.field_type.code(),
        FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
    )
}

/// The single column a `PRIMARY KEY` names, whether written inline on the
/// column or as a table constraint.
///
/// DEFERRED (documented): a multi-column primary key, which Go turns into a
/// clustered common handle (`IsCommonHandle`); an expression or
/// prefix-length key; and `UNIQUE`/`KEY`/`FOREIGN KEY` constraints, which
/// need the index tier. All are rejected rather than silently dropped, so a
/// table never claims a constraint it does not enforce.
fn primary_key_column(create: &tidb_ast::CreateTableStmt) -> Result<Option<String>, DriverError> {
    let mut found: Option<String> = None;
    for def in &create.columns {
        for option in &def.options {
            if let tidb_ast::ColumnOption::InlineKey(key) = option {
                match key.kind {
                    tidb_ast::InlineKeyKind::Primary { .. } => {
                        if found.is_some() {
                            return Err(DriverError::Unsupported(
                                "a table may define only one primary key",
                            ));
                        }
                        found = Some(def.name.clone());
                    }
                    // A unique key is collected by `table_indexes`.
                    tidb_ast::InlineKeyKind::Unique => {}
                }
            }
        }
    }
    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::Index(index) = constraint else {
            return Err(DriverError::Unsupported(
                "only key table constraints are supported yet",
            ));
        };
        if index.kind != tidb_ast::IndexConstraintKind::PrimaryKey {
            // Unique and secondary keys are collected by `table_indexes`.
            continue;
        }
        if found.is_some() {
            return Err(DriverError::Unsupported(
                "a table may define only one primary key",
            ));
        }
        let [part] = index.parts.as_slice() else {
            return Err(DriverError::Unsupported(
                "a multi-column primary key is deferred (it needs the common-handle tier)",
            ));
        };
        let tidb_ast::IndexPart::Column {
            name, prefix_len, ..
        } = part
        else {
            return Err(DriverError::Unsupported(
                "an expression primary key is not supported yet",
            ));
        };
        if prefix_len.is_some() {
            return Err(DriverError::Unsupported(
                "a prefix-length primary key is not supported yet",
            ));
        }
        found = Some(name.clone());
    }
    Ok(found)
}

/// `str_to_type`, flen/decimal from numeric type arguments, unsigned flag.
fn field_type_of(def: &ColumnDef) -> Result<FieldType, DriverError> {
    let code = str_to_type(&def.ty.name.to_lowercase());
    if code == FieldTypeCode::Unspecified {
        return Err(DriverError::Unsupported("unsupported column type"));
    }
    let mut builder = FieldTypeBuilder::new().with_code(code);
    let mut numeric_args = def.ty.args.iter().filter_map(|arg| match arg {
        ColumnTypeArg::Text(text) => text.parse::<i64>().ok(),
        ColumnTypeArg::Bytes(_) => None,
    });
    if let Some(flen) = numeric_args.next() {
        builder = builder.flen_set(flen);
    }
    if let Some(decimal) = numeric_args.next() {
        builder = builder.decimal_set(decimal);
    }
    if def.ty.unsigned {
        builder = builder.add_flags(FieldTypeFlags::UNSIGNED);
    }
    Ok(builder.build())
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
pub fn run_create_table_on(sql: &str, catalog: &mut Catalog) -> Result<bool, DriverError> {
    run_create_table_in(sql, catalog, tidb_executor_default_database())
}

/// The default schema an unqualified `CREATE TABLE` lands in.
fn tidb_executor_default_database() -> &'static str {
    crate::driver::DEFAULT_DATABASE
}

/// [`run_create_table_on`] creating the table in `current_db`.
pub fn run_create_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<bool, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let create = match &stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::CreateTable(create) => create,
            _ => {
                return Err(DriverError::Unsupported(
                    "only CREATE TABLE is supported here",
                ))
            }
        },
        _ => {
            return Err(DriverError::Unsupported(
                "only CREATE TABLE is supported here",
            ))
        }
    };

    if create.like_table.is_some() {
        return Err(DriverError::Unsupported("CREATE TABLE LIKE is deferred"));
    }
    if create.columns.is_empty() {
        return Err(DriverError::Unsupported("a table needs columns"));
    }

    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name);
    if catalog.contains_in(&database, name) {
        if create.if_not_exists {
            return Ok(false);
        }
        return Err(DriverError::Unsupported("table already exists"));
    }

    // Build the ColumnInfos (ids 1..n, offsets in definition order).
    let mut columns = Vec::with_capacity(create.columns.len());
    for (i, def) in create.columns.iter().enumerate() {
        let field_type = field_type_of(def)?;
        let mut col = ColumnInfo::new((i + 1) as i64, &def.name, field_type);
        col.offset = i as i32;
        columns.push(col);
    }

    // The primary key, written either inline on a column or as a table
    // constraint.
    for (i, def) in create.columns.iter().enumerate() {
        if def
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull))
        {
            columns[i].add_flag(NOT_NULL_FLAG);
        }
    }

    let primary_key = primary_key_column(create)?;
    let pk_offset = match &primary_key {
        Some(pk_name) => {
            let offset = columns
                .iter()
                .position(|col| col.name.original().eq_ignore_ascii_case(pk_name))
                .ok_or(DriverError::Unsupported(
                    "the primary key names a column the table does not define",
                ))?;
            Some(offset)
        }
        None => None,
    };
    // Go `isSingleIntPK` + `ShouldBuildClusteredIndex`: a single-column
    // integer primary key becomes the row handle rather than a separate
    // index, which is what `TableInfo.PKIsHandle` records.
    let pk_is_handle = pk_offset.is_some_and(|offset| is_int_column(&columns[offset]));
    if let Some(offset) = pk_offset {
        // A primary key column is implicitly NOT NULL, as in MySQL.
        // Go marks a primary-key column NOT NULL and PRI (mysql.NotNullFlag,
        // mysql.PriKeyFlag).
        columns[offset].add_flag(NOT_NULL_FLAG | PRI_KEY_FLAG);
    }

    // Go evaluates a constant DEFAULT at DDL time and stores the value on the
    // ColumnInfo; a NOT NULL column with no DEFAULT keeps NoDefaultValueFlag,
    // which is the `None` case here.
    let mut defaults: Vec<Option<Datum>> = Vec::with_capacity(create.columns.len());
    for def in &create.columns {
        let mut default_value = None;
        for option in &def.options {
            match option {
                tidb_ast::ColumnOption::Default(expr) => {
                    let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
                        expr,
                        &tidb_expr::rewriter::NoResolver,
                    )
                    .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                    let tidb_expr::expression::Expression::Constant(constant) = rewritten else {
                        return Err(DriverError::Unsupported(
                            "an expression DEFAULT is not supported yet",
                        ));
                    };
                    let value = constant
                        .eval()
                        .map_err(|e| DriverError::Exec(crate::ExecError::Eval(e)))?;
                    default_value = Some(value);
                }
                // Go treats AUTO_INCREMENT and generated columns as their own
                // default sources; neither exists yet.
                tidb_ast::ColumnOption::AutoIncrement => {
                    return Err(DriverError::Unsupported(
                        "AUTO_INCREMENT is not supported yet",
                    ))
                }
                tidb_ast::ColumnOption::Generated { .. } => {
                    return Err(DriverError::Unsupported(
                        "generated columns are not supported yet",
                    ))
                }
                _ => {}
            }
        }
        defaults.push(default_value);
    }

    let info = TableInfo {
        id: catalog.allocate_table_id(),
        name: CiString::new(name),
        columns,
        pk_is_handle,
        ..TableInfo::default()
    };

    let kv_columns: Vec<KvColumn> = info
        .columns
        .iter()
        .map(|c| KvColumn {
            name: c.name.original().to_owned(),
            id: c.id,
            field_type: c.field_type.clone(),
            default_value: defaults[c.offset as usize].clone(),
        })
        .collect();
    let table = KvTable::new(info.id, kv_columns);
    let mut table = table;
    if pk_is_handle {
        if let Some(offset) = pk_offset {
            table.set_pk_handle_offset(offset);
        }
    }
    for index in table_indexes(create, &info.columns, pk_is_handle)? {
        table.add_index(index);
    }
    catalog.register_kv_in(&database, name, table);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{run_insert_on, run_select_on};
    use tidb_datatype::Datum;

    /// CREATE TABLE -> INSERT -> SELECT, all from SQL strings, with rows as
    /// real TiKV-format bytes and metadata as tidb-model structs.
    #[test]
    fn create_insert_select_from_sql() {
        let mut catalog = Catalog::default();
        assert!(run_create_table_on(
            "CREATE TABLE t (a BIGINT, b BIGINT UNSIGNED, s VARCHAR(10))",
            &mut catalog
        )
        .unwrap());

        assert_eq!(
            run_insert_on("INSERT INTO t (a, s) VALUES (7, 'x')", &mut catalog).unwrap(),
            1
        );
        let rows = run_select_on("SELECT a, s FROM t", &catalog).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Datum::Int(7));
        match &rows[0][1] {
            Datum::Bytes(b) => assert_eq!(b.as_slice(), b"x"),
            Datum::String(s) => assert_eq!(s.bytes(), b"x"),
            other => panic!("unexpected string datum {other:?}"),
        }
    }

    #[test]
    fn if_not_exists_and_duplicates() {
        let mut catalog = Catalog::default();
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
        let mut catalog = Catalog::default();
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
}
