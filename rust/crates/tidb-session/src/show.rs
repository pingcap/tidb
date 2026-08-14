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

//! The `SHOW` family: every `AdminStmt::Show*` (and `DESCRIBE`/`KILL`-adjacent
//! `ShowCreate`/`ShowColumns`/`ShowTables`) arm of
//! [`crate::Session::apply_schema_statement`], reached through
//! [`Session::dispatch_admin_stmt`], plus the `SHOW CREATE TABLE`/`VIEW` text
//! builders and column-description rows those arms share with
//! `run_information_schema_select` in `lib.rs`.
//!
//! `EXPLAIN`, account management (`CREATE`/`ALTER`/`DROP`/`RENAME USER`,
//! `GRANT`/`REVOKE`/`SHOW GRANTS`), and `KILL`/processlist live in their own
//! modules (`explain_arm`, `account`, `process_arm`) and are only delegated
//! to from [`Session::dispatch_admin_stmt`] here.

use crate::show_index::{show_index_rows, SHOW_INDEX_COLUMNS};
use crate::*;
use tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH;

/// The `Type` cell of a `SHOW COLUMNS`/`DESCRIBE` row: Go `NewColDesc`'s
/// `col.GetTypeDesc()`.
fn type_desc_cell(field_type: &tidb_datatype::FieldType) -> Datum {
    Datum::Bytes(
        field_type
            .type_desc(STRICT_INTEGER_DISPLAY_WIDTH)
            .into_bytes(),
    )
}

/// The text of a column's SETTLED `DEFAULT`, as every surface that prints one
/// renders it.
///
/// Go stores the default as a string and each printer -- `pkg/executor/show.go`
/// for both `SHOW CREATE TABLE` and `SHOW COLUMNS`, and
/// `pkg/infoschema`'s `COLUMN_DEFAULT` -- carries the SAME `TypeBit` branch:
/// a `BIT` column's stored bytes print through
/// `BinaryLiteral.ToBitLiteralString(true)`, so `DEFAULT 250` and
/// `DEFAULT b'11111010'` both read back as `b'11111010'`. Every other type
/// prints its stored text.
pub(crate) fn column_default_text(
    value: &Datum,
    field_type: &tidb_datatype::FieldType,
) -> Option<String> {
    if field_type.code() == tidb_datatype::FieldTypeCode::Bit {
        return match value {
            Datum::Null => None,
            Datum::String(_)
            | Datum::Bytes(_)
            | Datum::Raw(_)
            | Datum::BinaryLiteral(_)
            | Datum::Bit(_) => Some(
                tidb_datatype::BinaryLiteral::from(value.go_bytes()).to_bit_literal_string(true),
            ),
            // Anything else never settled into the column's own domain;
            // rendering it as bits would invent a value it does not hold.
            other => datum_text(other),
        };
    }
    datum_text(value)
}

/// The SQL-visible text of one LITERAL column default.
///
/// Go stores a version-1-and-later `TIMESTAMP` default as a UTC wall clock,
/// then `GetColDefaultValue` projects it into the reading session before any
/// metadata surface prints it. Version 0 uses the system zone as the source;
/// [`tidb_executor::column_default::materialize_stored_literal`] owns that
/// version boundary. Other types do not pass through this cast here: their
/// stored metadata spelling is the spelling SHOW and INFORMATION_SCHEMA
/// report.
pub(crate) fn literal_column_default_text(
    value: &Datum,
    column: &tidb_executor::KvColumn,
    flags: tidb_datatype::ConversionFlags,
    session_zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<String>, tidb_datatype::DatumValueError> {
    if column.field_type.code() != tidb_datatype::FieldTypeCode::Timestamp {
        return Ok(column_default_text(value, &column.field_type));
    }
    let converted = tidb_executor::column_default::materialize_stored_literal(
        value,
        &column.field_type,
        column.column_info_version,
        flags,
        session_zone,
    )?;
    Ok(match converted.value {
        Datum::Time(time) => Some(time.to_string()),
        other => column_default_text(&other, &column.field_type),
    })
}

/// Go `stringutil.Escape` with a non-ANSI_QUOTES sql_mode: backtick-quoted,
/// with an embedded backtick doubled.
fn escape_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go's `TABLE_TYPE` / `Table_type` value for an object.
fn table_type_of(is_view: bool) -> &'static str {
    if is_view {
        "VIEW"
    } else {
        "BASE TABLE"
    }
}

/// Go `ConstructResultOfShowCreateView`.
///
/// Go always prints the full preamble, including the defaults the statement
/// never wrote, and always prints an explicit column list even when the
/// `CREATE VIEW` had none -- the names come from the stored definition.
///
/// DIVERGENCE (documented): the definer is whatever the statement recorded,
/// which in this tier is the empty identity, printed as ``@``. A TiDB with
/// authentication prints the connected user there.
fn show_create_view_text(view: &tidb_executor::ViewDef) -> String {
    let mut out = format!(
        "CREATE ALGORITHM={} DEFINER={}@{} SQL SECURITY {} VIEW {} (",
        view.algorithm,
        escape_name(&view.definer_user),
        escape_name(&view.definer_host),
        view.security,
        escape_name(&view.name),
    );
    for (index, (name, _)) in view.columns.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&escape_name(name));
    }
    out.push_str(") AS ");
    out.push_str(&view.select_sql);
    out
}

/// How one index key part prints: a visible column by name, a hidden column
/// as the parenthesized expression it was built from, and a declared PREFIX
/// as the `(n)` after the name.
///
/// Captured from Go: `create index idx on t((a+1))` prints
/// ``KEY `idx` ((`a` + 1))``, and a mixed index `index idxe ((a+1), a)`
/// prints ``KEY `idxe` ((`a` + 1),`a`)`` -- the hidden column's own name is
/// never printed anywhere. Captured for the prefix:
/// `create table t (a char(255), b int, unique key idx(a(2), b))` prints
/// ``UNIQUE KEY `idx` (`a`(2),`b`)``, and a prefix covering the whole column
/// prints no `(n)` at all because the DDL stored none.
fn index_part_text(table: &tidb_executor::KvTable, offset: usize, prefix_length: i64) -> String {
    let column = &table.columns[offset];
    match column
        .generated
        .as_ref()
        .filter(|_| table.is_hidden(offset))
    {
        Some(generated) => format!("({})", generated.expr_text),
        None if prefix_length == tidb_executor::ddl::index_prefix::UNSPECIFIED_LENGTH => {
            escape_name(&column.name)
        }
        None => format!("{}({prefix_length})", escape_name(&column.name)),
    }
}

/// Go `constructResultOfShowCreateTable`, over the metadata this seed keeps.
///
/// The shape is Go's line for line: the header, two-space-indented column
/// clauses separated by ",\n", the clustered primary key when the handle is
/// one, then the indexes, then the closing paren with the engine and charset.
///
/// A column prints its own charset/collation only where it differs from the
/// table's, which is Go's rule and what the capture shows: a column whose
/// charset differs prints `CHARACTER SET <cs> COLLATE <coll>`, one that only
/// differs in collation prints `COLLATE <coll>`, and a binary-charset column
/// (`varbinary`, `blob`) prints neither because its type name already says so.
fn show_create_table_text(
    database: &str,
    name: &str,
    table: &tidb_executor::KvTable,
    ctx: &tidb_executor::StmtContext,
) -> Result<String, DriverError> {
    let mut out = format!("CREATE TABLE {} (\n", escape_name(name));
    let mut clauses: Vec<String> = Vec::with_capacity(table.columns.len() + 1);

    let table_charset = table.charset();
    // Only the VISIBLE columns get a definition line: the hidden column an
    // expression index was rewritten into is printed as the index's
    // expression instead, below.
    for (offset, column) in table.visible_columns().iter().enumerate() {
        let mut clause = format!(
            "  {} {}",
            escape_name(&column.name),
            column.field_type.type_desc(STRICT_INTEGER_DISPLAY_WIDTH)
        );
        clause.push_str(&column_charset_clause(&column.field_type, table_charset));
        let not_null = column.field_type.flags() & NOT_NULL_FLAG != 0;
        if table.auto_increment_offset() == Some(offset) {
            // Go writes the pair together for an auto column and prints no
            // default for it.
            clause.push_str(" NOT NULL AUTO_INCREMENT");
            clauses.push(clause);
            continue;
        }
        // A generated column prints its expression where an ordinary column
        // prints its DEFAULT, and never prints a DEFAULT of its own -- its
        // value has one source. Captured from Go: `` `b` int(11) GENERATED
        // ALWAYS AS (`a` + 1) VIRTUAL`` , with `NOT NULL` still trailing it.
        if let Some(generated) = &column.generated {
            clause.push_str(&format!(
                " GENERATED ALWAYS AS ({}) {}",
                generated.expr_text,
                if generated.stored {
                    "STORED"
                } else {
                    "VIRTUAL"
                }
            ));
            if not_null {
                clause.push_str(" NOT NULL");
            }
            clauses.push(clause);
            continue;
        }
        if not_null {
            clause.push_str(" NOT NULL");
        }
        // Go prints nothing for a column carrying NoDefaultValueFlag; absent
        // that flag, a nullable column with no stored default reports NULL.
        if !column
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE)
        {
            match &column.default_value {
                Some(tidb_executor::column_default::ColumnDefault::Value(Datum::Null)) => {
                    if column.field_type.code() == tidb_datatype::FieldTypeCode::Timestamp {
                        clause.push_str(" NULL");
                    }
                    clause.push_str(" DEFAULT NULL")
                }
                Some(default) => {
                    // Go quotes every non-bit LITERAL default, integers included,
                    // and prints the computed forms unquoted -- see
                    // `ColumnDefault::show_create_clause` for which is which.
                    let literal = match default {
                        tidb_executor::column_default::ColumnDefault::Value(value) => {
                            literal_column_default_text(
                                value,
                                column,
                                ctx.show_default_conversion_flags(),
                                &ctx.session_zone(),
                            )
                            .map_err(|_| DriverError::FieldGetDefaultFailed(column.name.clone()))?
                            .unwrap_or_default()
                        }
                        _ => String::new(),
                    };
                    clause.push_str(&format!(
                        " DEFAULT {}",
                        default.show_create_clause(&column.field_type, &literal)
                    ));
                }
                None if !not_null => {
                    if column.field_type.code() == tidb_datatype::FieldTypeCode::Timestamp {
                        clause.push_str(" NULL");
                    }
                    clause.push_str(" DEFAULT NULL");
                }
                None => {}
            }
        }
        if column
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW)
        {
            clause.push_str(" ON UPDATE CURRENT_TIMESTAMP");
            let fsp = column.field_type.decimal();
            if fsp > 0 {
                clause.push('(');
                clause.push_str(&fsp.to_string());
                clause.push(')');
            }
        }
        if let Some(spec) = table.auto_random().filter(|spec| spec.offset == offset) {
            if spec.range_bits == 64 {
                clause.push_str(&format!(
                    " /*T![auto_rand] AUTO_RANDOM({}) */",
                    spec.shard_bits
                ));
            } else {
                clause.push_str(&format!(
                    " /*T![auto_rand] AUTO_RANDOM({}, {}) */",
                    spec.shard_bits, spec.range_bits
                ));
            }
        }
        clauses.push(clause);
    }

    // Go emits a clustered primary key here, because a clustered key -- an
    // int handle or a common handle -- is not in the index list.
    let clustered: Vec<usize> = match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None => table.common_handle_offsets().to_vec(),
    };
    if !clustered.is_empty() {
        let columns = clustered
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        clauses.push(format!(
            "  PRIMARY KEY ({columns}) /*T![clustered_index] CLUSTERED */"
        ));
    }

    for index in table.indexes() {
        let columns = index
            .column_offsets
            .iter()
            .enumerate()
            .map(|(position, offset)| {
                index_part_text(table, *offset, index.prefix_length(position))
            })
            .collect::<Vec<_>>()
            .join(",");
        let mut clause = if index.name.eq_ignore_ascii_case("PRIMARY") {
            // A primary key that is not the handle is non-clustered here,
            // since this seed builds no clustered common handle.
            format!("  PRIMARY KEY ({columns}) /*T![clustered_index] NONCLUSTERED */")
        } else if index.unique {
            format!("  UNIQUE KEY {} ({columns})", escape_name(&index.name))
        } else {
            format!("  KEY {} ({columns})", escape_name(&index.name))
        };
        if !index.visible {
            clause.push_str(" /*!80000 INVISIBLE */");
        }
        if !index.comment.is_empty() {
            clause.push_str(" COMMENT '");
            clause.push_str(&tidb_util::format::output_format(&index.comment));
            clause.push('\'');
        }
        clauses.push(clause);
    }

    // Go prints the referential constraints after every key, each on its own
    // line, with the `ON DELETE`/`ON UPDATE` clause only when one was
    // written. `RESTRICT` is the stored form of NO ACTION/SET DEFAULT/no
    // clause, so the three are indistinguishable here -- the one place this
    // engine's collapse of them is visible.
    for foreign_key in table.foreign_keys() {
        let columns = foreign_key
            .cols
            .iter()
            .map(|name| escape_name(name))
            .collect::<Vec<_>>()
            .join(",");
        let referenced = foreign_key
            .ref_cols
            .iter()
            .map(|name| escape_name(name))
            .collect::<Vec<_>>()
            .join(",");
        // Go `pkg/executor/show.go`: the referenced table is qualified with
        // its SCHEMA only when that schema differs from the one holding this
        // table (`fk.RefSchema.L != "" && fk.RefSchema.L != dbName.L`).
        // Without this a cross-schema constraint printed back as a
        // same-schema one, which is a `SHOW CREATE TABLE` output no server
        // could replay into the table it came from.
        let target = if foreign_key.ref_schema.is_empty()
            || foreign_key.ref_schema.eq_ignore_ascii_case(database)
        {
            escape_name(&foreign_key.ref_table)
        } else {
            format!(
                "{}.{}",
                escape_name(&foreign_key.ref_schema),
                escape_name(&foreign_key.ref_table)
            )
        };
        let mut clause = format!(
            "  CONSTRAINT {} FOREIGN KEY ({columns}) REFERENCES {target} ({referenced})",
            escape_name(&foreign_key.name),
        );
        if let Some(action) = referential_action_sql(foreign_key.on_delete) {
            clause.push_str(&format!(" ON DELETE {action}"));
        }
        if let Some(action) = referential_action_sql(foreign_key.on_update) {
            clause.push_str(&format!(" ON UPDATE {action}"));
        }
        clauses.push(clause);
    }

    out.push_str(&clauses.join(",\n"));
    out.push_str(&format!(
        "\n) ENGINE=InnoDB DEFAULT CHARSET={} COLLATE={}",
        table_charset.charset.name(),
        table_charset.collation.name()
    ));
    if !table.comment().is_empty() {
        out.push_str(&format!(
            " COMMENT='{}'",
            tidb_util::format::output_format(table.comment())
        ));
    }
    if let Some(base) = table.next_auto_random().filter(|base| *base > 1) {
        out.push_str(&format!(" /*T![auto_rand_base] AUTO_RANDOM_BASE={base} */"));
    }
    if table.is_cached() {
        out.push_str(" /* CACHED ON */");
    }
    out.push_str(&partition_clause_text(table));
    Ok(out)
}

/// Go `ddl.AppendPartitionInfo`: the `PARTITION BY ...` tail, or the empty
/// string for an unpartitioned table.
///
/// The tail starts with a NEWLINE and no comma -- it follows the closing
/// paren's `COLLATE=...`, not the column list. For HASH, Go prints the
/// partition COUNT rather than the definitions whenever every partition still
/// carries its default name `p{i}` and no comment or placement policy, which
/// is the only HASH shape this tier can build. RANGE always prints the
/// DEFINITION LIST instead, because its bounds ARE the partitioning. Both
/// captured verbatim:
///
/// ```text
/// PARTITION BY HASH (`a`) PARTITIONS 4
/// ```
///
/// ```text
/// PARTITION BY RANGE (`a`)
/// (PARTITION `p0` VALUES LESS THAN (10),
///  PARTITION `p1` VALUES LESS THAN (20),
///  PARTITION `pm` VALUES LESS THAN (MAXVALUE))
/// ```
fn partition_clause_text(table: &tidb_executor::KvTable) -> String {
    let Some(partition) = table.partition() else {
        return String::new();
    };
    let head = format!(
        "\nPARTITION BY {} ({})",
        partition.kind.sql(),
        partition.expr_text
    );
    match &partition.kind {
        tidb_executor::PartitionKind::Hash => format!("{head} PARTITIONS {}", partition.num()),
        tidb_executor::PartitionKind::Key => format!("{head} PARTITIONS {}", partition.num()),
        tidb_executor::PartitionKind::Range {
            less_than,
            unsigned,
        } => format!(
            "{head}{}",
            tidb_executor::ddl::table_partition_range::range_definitions_text(
                &partition.definitions,
                less_than,
                *unsigned
            )
        ),
        tidb_executor::PartitionKind::RangeColumns {
            less_than,
            field_types: _,
        } => format!(
            "\nPARTITION BY RANGE COLUMNS({}){}",
            partition.expr_text,
            tidb_executor::ddl::table_partition_range::range_columns_definitions_text(
                &partition.definitions,
                less_than,
            )
        ),
        tidb_executor::PartitionKind::List {
            values,
            null_partition,
            default_partition,
            unsigned,
        } => format!(
            "{head}{}",
            tidb_executor::ddl::table_partition_list::list_definitions_text(
                &partition.definitions,
                values,
                *null_partition,
                *default_partition,
                *unsigned
            )
        ),
        tidb_executor::PartitionKind::ListColumns {
            values,
            default_partition,
            ..
        } => format!(
            "\nPARTITION BY LIST COLUMNS({}){}",
            partition.expr_text,
            tidb_executor::ddl::table_partition_list::list_columns_definitions_text(
                &partition.definitions,
                values,
                *default_partition
            )
        ),
    }
}

/// The `ON DELETE`/`ON UPDATE` spelling `SHOW CREATE TABLE` prints, or
/// `None` for an omitted clause.
fn referential_action_sql(action: tidb_executor::FkAction) -> Option<&'static str> {
    match action {
        tidb_executor::FkAction::NoOption => None,
        tidb_executor::FkAction::Restrict => Some("RESTRICT"),
        tidb_executor::FkAction::Cascade => Some("CASCADE"),
        tidb_executor::FkAction::SetNull => Some("SET NULL"),
        tidb_executor::FkAction::NoAction => Some("NO ACTION"),
        tidb_executor::FkAction::SetDefault => Some("SET DEFAULT"),
    }
}

/// The ` CHARACTER SET x COLLATE y` tail a column clause carries when its own
/// charset/collation differs from the table's default.
///
/// Matching the table is not on its own enough to omit the collation. Go
/// `pkg/executor/show.go` has a second reason to print it, in the `else` of
/// the table comparison: when the column collation equals the table's but is
/// NOT the charset's own default (`charset.GetDefaultCollation`), the name is
/// printed anyway, because re-reading the clause without it would resolve to
/// that default and give a DIFFERENT column. `utf8mb4` defaults to
/// `utf8mb4_bin` here, so every `COLLATE=utf8mb4_general_ci` table used to
/// print columns whose comparison semantics the printed statement did not
/// reproduce.
fn column_charset_clause(
    field_type: &tidb_datatype::FieldType,
    table: tidb_executor::TableCharset,
) -> String {
    if !field_type.has_charset() {
        return String::new();
    }
    let charset = field_type.charset_name();
    let collation = field_type.collation_name();
    if charset != table.charset.name() {
        format!(" CHARACTER SET {charset} COLLATE {collation}")
    } else if collation != table.collation.name()
        || collation != field_type.charset().default_collation().name()
    {
        format!(" COLLATE {collation}")
    } else {
        String::new()
    }
}

/// Go `NewColDesc`'s `Collation` cell: the column's own collation name, and
/// NULL for anything with no character set -- a numeric or temporal column,
/// and a binary-charset string column alike (captured: `varbinary`, `binary`,
/// `blob` and `longblob` all report NULL).
fn column_collation_cell(field_type: &tidb_datatype::FieldType) -> Datum {
    if field_type.has_charset() {
        Datum::Bytes(field_type.collation_name().as_bytes().to_vec())
    } else {
        Datum::Null
    }
}

/// Go `table.ColDescFieldNames(false)`: the columns `SHOW COLUMNS` and
/// `DESCRIBE` produce.
const COL_DESC_FIELD_NAMES: &[&str] = &["Field", "Type", "Null", "Key", "Default", "Extra"];

/// Go `table.ColDescFieldNames(true)`: the extra columns `SHOW FULL COLUMNS`
/// inserts between `Type` and `Null`, plus the trailing `Privileges` and
/// `Comment` columns.
const FULL_COL_DESC_FIELD_NAMES: &[&str] = &[
    "Field",
    "Type",
    "Collation",
    "Null",
    "Key",
    "Default",
    "Extra",
    "Privileges",
    "Comment",
];

/// Go's mock session's fixed grant string for every column of every table
/// (`fetchShowColumns`): this tier grants no per-column privileges of its
/// own, so it reports the same static capture MySQL/TiDB print for a column
/// the current user can select, insert, update, and reference.
const FULL_COL_DESC_PRIVILEGES: &str = "select,insert,update,references";

/// Go `table.NewColDesc`, restricted to the facts this seed's metadata holds.
///
/// `Null` is NO when the column carries `NotNullFlag`; `Key` is PRI for a
/// primary-key column, UNI for a column that is the whole of a unique index,
/// and MUL for one that leads a non-unique index -- Go reads those from the
/// column's key flags, which the DDL sets from the same index definitions.
///
/// `Default` is the column's stored `DEFAULT`, or NULL when none was written.
///
/// `Extra` follows Go `NewColDesc`'s ordered precedence: auto increment,
/// ON UPDATE CURRENT_TIMESTAMP, generated-column kind, then expression
/// default.
fn column_description(
    column: &tidb_executor::KvColumn,
    offset: usize,
    table: &tidb_executor::KvTable,
    full: bool,
    ctx: &tidb_executor::StmtContext,
) -> Result<Vec<Datum>, DriverError> {
    let null_flag = if column.field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    let extra = column_extra(
        &column.field_type,
        table.auto_increment_offset() == Some(offset),
        column.generated.as_ref().map(|generated| generated.stored),
        column
            .default_value
            .as_ref()
            .is_some_and(tidb_executor::column_default::ColumnDefault::is_default_generated),
    );
    let key_flag = column_key_flag(table, offset);
    let default = match &column.default_value {
        Some(tidb_executor::column_default::ColumnDefault::Value(value)) => {
            match literal_column_default_text(
                value,
                column,
                ctx.show_default_conversion_flags(),
                &ctx.session_zone(),
            )
            .map_err(|_| DriverError::FieldGetDefaultFailed(column.name.clone()))?
            {
                Some(text) => Datum::Bytes(text.into_bytes()),
                None => Datum::Null,
            }
        }
        // Go `NewColDesc` reports a computed default's STORED string here,
        // which is not the parenthesised form `SHOW CREATE TABLE` prints.
        Some(computed) => match computed.column_desc_text(&column.field_type) {
            Some(text) => Datum::Bytes(text.into_bytes()),
            None => Datum::Null,
        },
        None => Datum::Null,
    };
    if !full {
        return Ok(vec![
            Datum::Bytes(column.name.clone().into_bytes()),
            type_desc_cell(&column.field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.into_bytes()),
            default,
            Datum::Bytes(extra.clone().into_bytes()),
        ]);
    }
    let collation = column_collation_cell(&column.field_type);
    Ok(vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        type_desc_cell(&column.field_type),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.into_bytes()),
        default,
        Datum::Bytes(extra.into_bytes()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()), // Comment: no per-column comments modelled.
    ])
}

/// Go `table.NewColDesc`'s `Extra` field, kept as a pure formatter so every
/// metadata producer supplies the same four source facts in the same order.
pub(crate) fn column_extra(
    field_type: &tidb_datatype::FieldType,
    auto_increment: bool,
    generated_stored: Option<bool>,
    default_is_expr: bool,
) -> String {
    if auto_increment {
        return "auto_increment".to_owned();
    }
    if field_type.has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW) {
        let fsp = field_type.decimal();
        return if fsp > 0 {
            format!("DEFAULT_GENERATED on update CURRENT_TIMESTAMP({fsp})")
        } else {
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP".to_owned()
        };
    }
    if let Some(stored) = generated_stored {
        return if stored {
            "STORED GENERATED".to_owned()
        } else {
            "VIRTUAL GENERATED".to_owned()
        };
    }
    if default_is_expr {
        return "DEFAULT_GENERATED".to_owned();
    }
    String::new()
}

/// A view column's `SHOW COLUMNS` row.
///
/// A view carries no storage metadata, so Go reports no key, no default and
/// no extra for every one of its columns; only the name, the type the body
/// produced, and nullability come from the definition.
///
/// # Why this disagrees with `information_schema.columns`, on purpose
///
/// Go's `tryFillViewColumnType` (`pkg/executor/show.go`) OVERWRITES the stored
/// column's `FieldType` with the re-planned one and then rewrites `VarString`
/// to `Varchar` in place, so every cell this row prints -- type text, charset,
/// collation, nullability -- is read off the PLAN's type.
/// `dataForColumnsInTable` does not: it keeps the re-planned type for
/// `COLUMN_TYPE`/`DATA_TYPE` only and builds the rest from the STORED column,
/// and it does the `VarString` remap for `DATA_TYPE` alone.
///
/// One captured view makes both halves visible at once:
///
/// ```text
/// desc v                            ->  event_id | varchar(32)    | NO
/// information_schema.columns for v  ->  event_id | var_string(32) | YES
/// ```
///
/// Making the two surfaces agree would be the regression, not the fix.
fn view_column_description(
    name: &str,
    field_type: &tidb_datatype::FieldType,
    full: bool,
) -> Vec<Datum> {
    let field_type = &show_columns_view_type(field_type);
    let null_flag = if field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    if !full {
        return vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            type_desc_cell(field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(Vec::new()),
            Datum::Null,
            Datum::Bytes(Vec::new()),
        ];
    }
    let collation = column_collation_cell(field_type);
    vec![
        Datum::Bytes(name.as_bytes().to_vec()),
        type_desc_cell(field_type),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
        Datum::Null,
        Datum::Bytes(Vec::new()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
    ]
}

/// Go `tryFillViewColumnType`'s closing rewrite: a re-planned view column
/// whose type came back `VarString` is reported as a `VARCHAR`.
///
/// `CAST(... AS CHAR(32))` yields a `VarString` in the plan, so without this a
/// view over one describes itself with a type name no `CREATE TABLE` can
/// spell. The rewrite is confined to the `SHOW` surface; see
/// [`view_column_description`] for the surface that deliberately does not.
fn show_columns_view_type(field_type: &tidb_datatype::FieldType) -> tidb_datatype::FieldType {
    let mut field_type = field_type.clone();
    if field_type.code() == tidb_datatype::FieldTypeCode::VarString {
        field_type.set_code(tidb_datatype::FieldTypeCode::Varchar);
    }
    field_type
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

/// Go `NewColDesc`'s key flag, shared by `SHOW COLUMNS` and
/// `information_schema.COLUMNS`: PRI for a primary key, UNI for a column that
/// is the whole of a unique index, MUL for one that leads a non-unique index.
pub(crate) fn column_key_flag(table: &tidb_executor::KvTable, offset: usize) -> String {
    if table.columns[offset]
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    {
        "PRI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
    {
        "UNI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.first() == Some(&offset))
    {
        "MUL".to_owned()
    } else {
        String::new()
    }
}

/// A one-column result set of strings, the shape SHOW DATABASES and SHOW
/// TABLES produce.
pub(crate) fn string_column_output(column: &str, values: Vec<String>) -> StmtOutput {
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    StmtOutput::Rows {
        columns: vec![(column.to_owned(), field_type)],
        rows: values
            .into_iter()
            .map(|value| vec![Datum::Bytes(value.into_bytes())])
            .collect(),
    }
}

/// The `SHOW TABLE STATUS` header, with the columns Go reports as numbers
/// marked.
const SHOW_TABLE_STATUS_COLUMNS: &[(&str, bool)] = &[
    ("Name", false),
    ("Engine", false),
    ("Version", true),
    ("Row_format", false),
    ("Rows", true),
    ("Avg_row_length", true),
    ("Data_length", true),
    ("Max_data_length", true),
    ("Index_length", true),
    ("Data_free", true),
    ("Auto_increment", true),
    ("Create_time", false),
    ("Update_time", false),
    ("Check_time", false),
    ("Collation", false),
    ("Checksum", false),
    ("Create_options", false),
    ("Comment", false),
];

fn show_table_status_row(
    name: &str,
    auto_increment: Option<i64>,
    charset: tidb_executor::TableCharset,
    comment: &str,
) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    vec![
        text(name),
        text("InnoDB"),
        Datum::Int(10),
        text("Compact"),
        Datum::Int(0), // Rows
        Datum::Int(0), // Avg_row_length
        Datum::Int(0), // Data_length
        Datum::Int(0), // Max_data_length
        Datum::Int(0), // Index_length
        Datum::Int(0), // Data_free
        match auto_increment {
            Some(next) => Datum::Int(next),
            None => Datum::Null,
        },
        Datum::Null, // Create_time: no per-table creation timestamp here.
        Datum::Null, // Update_time
        Datum::Null, // Check_time
        text(charset.collation.name()),
        text(""),      // Checksum
        text(""),      // Create_options
        text(comment), // Comment
    ]
}

/// One `SHOW TABLE STATUS` row for a view. Captured from Go: a view answers
/// its name, NULL for every storage cell -- engine, version, row format,
/// counts, sizes, collation and create options alike -- an empty `Checksum`,
/// and the literal `VIEW` as its comment, which is how the two kinds of
/// object are told apart in this output.
fn show_table_status_view_row(name: &str) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut row = vec![text(name)];
    // Engine through Auto_increment: ten cells a view has no value for.
    row.extend(std::iter::repeat_n(Datum::Null, 10));
    // Create_time, which Go fills and this tier has no source for, then
    // Update_time, Check_time and Collation, which are NULL for a view in Go
    // too.
    row.extend(std::iter::repeat_n(Datum::Null, 4));
    row.push(text("")); // Checksum
    row.push(Datum::Null); // Create_options
    row.push(text("VIEW")); // Comment
    row
}

/// The collations `SHOW COLLATION` reports, in mock TiDB's own capture order
/// (alphabetical by collation name). `Utf8Mb4ZhPinyinTiDbAsCs` is
/// deliberately excluded: it is a reserved stub collation, and Go's own
/// `SHOW COLLATION` capture omits it too.
const SHOW_COLLATION_ROWS: &[tidb_datatype::Collation] = &[
    tidb_datatype::Collation::AsciiBin,
    tidb_datatype::Collation::Binary,
    tidb_datatype::Collation::Gb18030Bin,
    tidb_datatype::Collation::Gb18030ChineseCi,
    tidb_datatype::Collation::GbkBin,
    tidb_datatype::Collation::GbkChineseCi,
    tidb_datatype::Collation::Latin1Bin,
    tidb_datatype::Collation::Utf8Bin,
    tidb_datatype::Collation::Utf8GeneralCi,
    tidb_datatype::Collation::Utf8UnicodeCi,
    tidb_datatype::Collation::Utf8Mb40900AiCi,
    tidb_datatype::Collation::Utf8Mb40900Bin,
    tidb_datatype::Collation::Utf8Mb4Bin,
    tidb_datatype::Collation::Utf8Mb4GeneralCi,
    tidb_datatype::Collation::Utf8Mb4UnicodeCi,
];

/// Whether `collation` is the one `SHOW COLLATION` marks `Default`.
///
/// Go reads `Collation.IsDefault`, which `switchDefaultCollation` keeps in
/// step with the owning charset's default collation, so this is exactly
/// "`collation` is its charset's default" and is derived rather than listed.
/// An explicit list here was a second place for the `gbk`/`gb18030` default to
/// be spelled, and it carried a doc claim -- that
/// [`tidb_datatype::Charset::default_collation`] returns the `_bin`
/// collations for those charsets -- that was already untrue when read.
fn is_default_show_collation(collation: tidb_datatype::Collation) -> bool {
    collation.charset().default_collation() == collation
}

/// The column names a `SHOW VARIABLES` row carries, which its `WHERE` filter
/// resolves against.
const SHOW_VARIABLE_COLUMNS: &[&str; 2] = &["Variable_name", "Value"];

/// The virtual column names available to `SHOW CHARSET WHERE`.
const SHOW_CHARSET_COLUMNS: &[&str; 4] = &["Charset", "Description", "Default collation", "Maxlen"];

/// The virtual column names available to `SHOW ENGINES WHERE`.
const SHOW_ENGINES_COLUMNS: &[&str; 6] = &[
    "Engine",
    "Support",
    "Comment",
    "Transactions",
    "XA",
    "Savepoints",
];

/// The virtual column names available to `SHOW COLLATION WHERE`.
const SHOW_COLLATION_COLUMNS: &[&str; 7] = &[
    "Collation",
    "Charset",
    "Id",
    "Default",
    "Compiled",
    "Sortlen",
    "Pad_attribute",
];

/// The status variables this tier truthfully reports for `SHOW STATUS`, as
/// `(name, value, session_only)`, in row order.
///
/// The values are Go's captured defaults for a plain (no-TLS, no-compression)
/// connection, which is exactly what this tier is: no wire compression, so
/// `Compression` is `OFF`, and no TLS, so the `Ssl_*` family is empty/`0`.
/// The `session_only` flag mirrors Go's `vardef.ScopeSession`, which
/// `fetchShowStatus` uses to drop rows from `SHOW GLOBAL STATUS`.
///
/// NOT modelled (this tier has no metrics/server tier to read them from):
/// the `Performance_schema_session_connect_attrs_*` counters,
/// `ddl_schema_version`, `server_id`, `last_plan_binding_update_time`, and
/// `tidb_keys_examined`.
const SHOW_STATUS_VARS: &[(&str, &str, bool)] = &[
    ("Compression", "OFF", true),
    ("Compression_algorithm", "", true),
    ("Compression_level", "0", true),
    ("Ssl_cipher", "", false),
    ("Ssl_cipher_list", "", false),
    ("Ssl_verify_mode", "0", false),
    ("Ssl_version", "", false),
];

/// A resolver over one row of a virtual `SHOW` result, so the statement's own
/// `WHERE` can be evaluated against it.
///
/// Go builds the same thing as a real selection over the show output; this
/// tier evaluates the predicate per row instead, which is the same filter
/// without a plan to carry it.
struct ShowRowResolver<'a> {
    columns: &'a [&'a str],
    row: &'a [Datum],
}

impl tidb_executor::Columns for ShowRowResolver<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        let name = path.last()?;
        let index = self
            .columns
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))?;
        self.row.get(index).cloned()
    }
}

/// Whether one virtual `SHOW` row satisfies the statement's `WHERE`.
fn show_row_matches(
    predicate: &tidb_ast::Expr,
    columns: &[&str],
    row: &[Datum],
) -> Result<bool, DriverError> {
    let resolver = ShowRowResolver { columns, row };
    let value = tidb_executor::eval_in(predicate, &resolver)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    let truthy = tidb_executor::truthy_of(&value)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    Ok(truthy.unwrap_or(false))
}

/// An evaluated SHOW LIKE operand, including whether Go's predicate extractor
/// lower-cases both the metadata name and a literal pattern.
struct ShowLikePattern {
    value: Option<String>,
    fold_lowercase: bool,
    literal_name: Option<String>,
}

impl ShowLikePattern {
    fn from_expr(expr: &tidb_ast::Expr, value: Option<String>, has_extractor: bool) -> Self {
        let extracted_literal = has_extractor
            && matches!(
                expr,
                tidb_ast::Expr::Null
                    | tidb_ast::Expr::Int(_)
                    | tidb_ast::Expr::Decimal(_)
                    | tidb_ast::Expr::Float(_)
                    | tidb_ast::Expr::Hex(_)
                    | tidb_ast::Expr::Bit(_)
                    | tidb_ast::Expr::String(_)
                    | tidb_ast::Expr::RawString(_)
                    | tidb_ast::Expr::Bool(_)
            );
        let literal_name = if extracted_literal {
            value.clone().filter(|name| !name.is_empty())
        } else {
            None
        };
        Self {
            value: if extracted_literal {
                value.map(|pattern| pattern.to_lowercase())
            } else {
                value
            },
            fold_lowercase: extracted_literal,
            literal_name,
        }
    }

    fn column_name(&self, base: &str) -> String {
        self.literal_name
            .as_ref()
            .map_or_else(|| base.to_owned(), |pattern| format!("{base} ({pattern})"))
    }

    fn matches(&self, text: &str) -> bool {
        let Some(pattern) = &self.value else {
            return false;
        };
        if self.fold_lowercase {
            tidb_executor::like_match_with_collation(
                text.to_lowercase(),
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        } else {
            tidb_executor::like_match_with_collation(
                text,
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        }
    }
}

/// Applies the `LIKE`/`WHERE` layer Go builds over a virtual SHOW result.
fn filter_show_output(
    output: StmtOutput,
    like_pattern: Option<ShowLikePattern>,
    where_clause: Option<&tidb_ast::Expr>,
) -> Result<StmtOutput, DriverError> {
    let StmtOutput::Rows { columns, rows } = output else {
        return Ok(output);
    };
    let column_names: Vec<&str> = columns.iter().map(|(name, _)| name.as_str()).collect();
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows {
        let matches_like = match &like_pattern {
            None => true,
            Some(pattern) => row
                .first()
                .and_then(datum_text)
                .is_some_and(|text| pattern.matches(&text)),
        };
        if !matches_like {
            continue;
        }
        if let Some(predicate) = where_clause {
            if !show_row_matches(predicate, &column_names, &row)? {
                continue;
            }
        }
        filtered.push(row);
    }
    Ok(StmtOutput::Rows {
        columns,
        rows: filtered,
    })
}

impl Session {
    /// The `SHOW COLUMNS` / `DESCRIBE` result for one table, optionally
    /// narrowed to a single column as Go's `DESCRIBE tbl col` narrows it.
    fn show_columns(
        &mut self,
        database: &str,
        table_path: &[String],
        column: Option<&str>,
        full: bool,
    ) -> Result<StmtOutput, DriverError> {
        // A `db.tbl` path names its own schema, as everywhere else.
        let (database, table_name) = match table_path {
            [name] => (database.to_owned(), name.clone()),
            [db, name] => (db.clone(), name.clone()),
            _ => return Err(DriverError::unsupported("empty table name")),
        };
        let ctx = self.statement_context(false);
        let rows = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.table_in(&database, &table_name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{database}.{table_name}"
                ))));
            };
            if let tidb_executor::TableEntry::View(view) = entry {
                // Go re-plans the body here (`tryFillViewColumnType`), so the
                // types reported are the ones the base tables have now, and a
                // body that no longer resolves fails the statement with its
                // own error rather than with ErrViewInvalid.
                let view = view.clone();
                let columns = tidb_executor::view_column_list(&view, &database, catalog, &ctx)?;
                return Ok(columns
                    .iter()
                    .filter(|(candidate, _)| {
                        column.is_none_or(|name| candidate.eq_ignore_ascii_case(name))
                    })
                    .map(|(name, field_type)| view_column_description(name, field_type, full))
                    .collect::<Vec<_>>());
            }
            let tidb_executor::TableEntry::Kv(table) = entry else {
                return Err(DriverError::unsupported(
                    "SHOW COLUMNS needs a storage-backed table",
                ));
            };
            table
                .visible_columns()
                .iter()
                .enumerate()
                .filter(|(_, candidate)| {
                    column.is_none_or(|name| candidate.name.eq_ignore_ascii_case(name))
                })
                .map(|(offset, candidate)| column_description(candidate, offset, table, full, &ctx))
                .collect::<Result<Vec<_>, _>>()
        })?;
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let field_names = if full {
            FULL_COL_DESC_FIELD_NAMES
        } else {
            COL_DESC_FIELD_NAMES
        };
        Ok(StmtOutput::Rows {
            columns: field_names
                .iter()
                .map(|name| ((*name).to_owned(), field_type.clone()))
                .collect(),
            rows,
        })
    }

    /// The `AdminStmt` dispatch [`Session::apply_schema_statement`] reaches
    /// for the whole admin/inspection surface: the `SHOW` family here, plus
    /// `EXPLAIN`, `GRANT`/`REVOKE`/`SHOW GRANTS`, and `KILL` delegated to
    /// their own modules.
    pub(crate) fn dispatch_admin_stmt(
        &mut self,
        admin: &tidb_ast::AdminStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        match admin {
            // `EXPLAIN <select>`: plan the statement and report the plan,
            // running nothing. Go's EXPLAIN plans without executing (an
            // `EXPLAIN INSERT` inserts no row, captured), and so does
            // this: `tidb_executor::explain_select_stmt` re-runs the
            // driver's own read-path decisions without touching storage.
            //
            // See `tidb_executor::explain`'s module doc for every place
            // this tier's plan text diverges from Go's and why.
            tidb_ast::AdminStmt::Explain(explain) => self.explain_stmt(explain),
            // SQL bindings. See `crate::binding_arm`, and `crate::binding`
            // for the normalization and hint-transfer they are built on.
            tidb_ast::AdminStmt::CreateBinding(create) => {
                Ok(Some(self.create_binding_stmt(create)?))
            }
            tidb_ast::AdminStmt::DropBinding(drop) => Ok(Some(self.drop_binding_stmt(drop)?)),
            tidb_ast::AdminStmt::ShowBindings(show) => Ok(Some(self.show_bindings_stmt(show)?)),
            // `ANALYZE TABLE`, over this session's own catalog. See
            // `crate::analyze_arm` for why an in-process session runs it here
            // rather than routing it at a cluster node that can write
            // `mysql.stats_*`.
            tidb_ast::AdminStmt::AnalyzeTable(_) | tidb_ast::AdminStmt::AnalyzeIncremental(_) => {
                self.analyze_stmt(admin)
            }
            tidb_ast::AdminStmt::Grant(grant) => Ok(Some(self.grant_stmt(grant)?)),
            tidb_ast::AdminStmt::Revoke(revoke) => Ok(Some(self.revoke_stmt(revoke)?)),
            tidb_ast::AdminStmt::ShowGrants(show) => Ok(Some(self.show_grants_stmt(show)?)),
            tidb_ast::AdminStmt::ShowCreateUser(spec) => {
                Ok(Some(self.show_create_user_stmt(spec)?))
            }
            tidb_ast::AdminStmt::GrantRole(grant) => Ok(Some(self.grant_role_stmt(grant)?)),
            tidb_ast::AdminStmt::RevokeRole(revoke) => Ok(Some(self.revoke_role_stmt(revoke)?)),
            tidb_ast::AdminStmt::ShowDatabases(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowDatabasesFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowDatabasesFilter::Where(expr)) => (None, Some(expr)),
                };
                let names = self.with_catalog_mut(|catalog| Ok(catalog.database_names()))?;
                // Go `fetchShowDatabases` (`executor/show.go` around line
                // 462): one `DBIsVisible` per schema, so an account sees
                // only what it holds some evidence for -- plus
                // `information_schema`, which is visible to everyone and is
                // already first in `database_names`.
                let names = names
                    .into_iter()
                    .filter(|name| self.database_is_visible(name))
                    .collect();
                let column_name = like_pattern.as_ref().map_or_else(
                    || "Database".to_owned(),
                    |pattern| pattern.column_name("Database"),
                );
                let output = string_column_output(&column_name, names);
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go `fetchShowTableStatus`: one row per table in the
            // schema, with the columns MySQL's own SHOW TABLE STATUS
            // reports.
            //
            // NOT MODELLED, and each reported the way Go reports an
            // absent value rather than invented: every size and count
            // (Rows, Data_length, Index_length and friends) is 0, which
            // is also what TiDB itself answers without a statistics tier;
            // Create_time is NULL because this tier stores no per-table
            // creation timestamp; Update_time, Check_time and Checksum
            // are NULL or empty for the same reason.
            tidb_ast::AdminStmt::ShowTableStatus(show) => {
                let database = match &show.database {
                    Some(database) => database.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                // Go `fetchShowTableStatus` (`executor/show.go` around line
                // 639) applies the same pre-lookup 1044 gate `SHOW TABLES`
                // does.
                self.require_visible_database(&database)?;
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowTableStatusFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowTableStatusFilter::Where(expr)) => (None, Some(expr)),
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let mut rows = Vec::new();
                    let names = catalog.table_names(&database).ok_or_else(|| {
                        DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                    })?;
                    for name in names {
                        let entry = catalog.table_in(&database, &name);
                        let (auto_increment, table_charset, comment) = match entry {
                            Some(tidb_executor::TableEntry::Kv(table)) => (
                                table.next_auto_increment(),
                                table.charset(),
                                table.comment(),
                            ),
                            _ => (None, tidb_executor::TableCharset::default(), ""),
                        };
                        let row = if entry.is_some_and(tidb_executor::TableEntry::is_view) {
                            show_table_status_view_row(&name)
                        } else {
                            show_table_status_row(&name, auto_increment, table_charset, comment)
                        };
                        rows.push(row);
                    }
                    Ok(rows)
                })?;
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let columns = SHOW_TABLE_STATUS_COLUMNS
                    .iter()
                    .map(|(name, numeric)| {
                        ((*name).to_owned(), if *numeric { number() } else { text() })
                    })
                    .collect();
                let output = StmtOutput::Rows { columns, rows };
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go `fetchShowIndex`: one row per index COLUMN, ordered
            // with the clustered primary key first, then the table's own
            // indexes in definition order.
            //
            // NOT MODELLED, and each reported the way Go reports an
            // absent value rather than invented: Cardinality is 0 (no
            // statistics tier), Sub_part and Packed are NULL (no prefix
            // or packed indexes here), Comment/Index_comment are empty,
            // Expression is NULL (no expression indexes), and Global is
            // NO (no partitioned global indexes).
            tidb_ast::AdminStmt::ShowIndex(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowIndexFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, false)), None)
                    }
                    Some(tidb_ast::ShowIndexFilter::Where(expr)) => (None, Some(expr)),
                };
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match show.table.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    let tidb_executor::TableEntry::Kv(table) = entry else {
                        return Err(DriverError::unsupported(
                            "SHOW INDEX needs a storage-backed table",
                        ));
                    };
                    Ok(show_index_rows(&table_name, table))
                })?;
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let columns = SHOW_INDEX_COLUMNS
                    .iter()
                    .map(|(name, numeric)| {
                        ((*name).to_owned(), if *numeric { number() } else { text() })
                    })
                    .collect();
                let output = StmtOutput::Rows { columns, rows };
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go `ShowExec` with `ShowVariables`: one row per variable,
            // as `Variable_name` and `Value`, filtered by LIKE.
            //
            // `GLOBAL` reads the shared table live (a variable with no
            // GLOBAL scope at all falls back to its registry default, as Go
            // reports SOMETHING for every name `SHOW GLOBAL VARIABLES`
            // lists rather than erroring); `SESSION`/unqualified reads this
            // session's own copy, same as a plain `@@x`.
            tidb_ast::AdminStmt::ShowVariables(show) => {
                let pattern = match &show.like {
                    Some(tidb_ast::Expr::String(text)) => Some(text.clone()),
                    Some(_) => {
                        return Err(DriverError::unsupported(
                            "SHOW VARIABLES LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let mut rows = Vec::new();
                for definition in sysvar::SYS_VARS {
                    if self.sem_hides_sysvar(definition.name) {
                        continue;
                    }
                    let matches = match &pattern {
                        Some(pattern) => tidb_executor::like_match_with_collation(
                            definition.name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ),
                        None => true,
                    };
                    if !matches {
                        continue;
                    }
                    let value = if show.global {
                        self.vars
                            .get_global(definition.name)
                            .unwrap_or_else(|_| sysvar::effective_default(definition))
                    } else {
                        self.vars
                            .get_system(definition.name)
                            .unwrap_or_else(|_| sysvar::effective_default(definition))
                    };
                    let row = vec![
                        Datum::Bytes(definition.name.as_bytes().to_vec()),
                        Datum::Bytes(value.into_bytes()),
                    ];
                    // Go plans the WHERE as a selection over the same
                    // virtual rows, which is what this filter is.
                    if let Some(predicate) = &show.where_clause {
                        if !show_row_matches(predicate, SHOW_VARIABLE_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                        (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowStatus`: one `Variable_name | Value` row per
            // status variable that `variable.GetStatusVars` collects from
            // the registered `Statistics` providers, with `GLOBAL` scope
            // skipping session-only variables.
            //
            // This tier serves only `SHOW_STATUS_VARS` (see its doc
            // comment for what is not modelled). As with the
            // `ShowVariables` arm above, GLOBAL and SESSION read the same
            // values here because this tier keeps no persisted global
            // tier; GLOBAL still drops session-only rows, which the Go
            // capture confirms (`SHOW GLOBAL STATUS` omits the
            // `Compression*` family).
            tidb_ast::AdminStmt::ShowStatus(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowStatusFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowStatusFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW STATUS LIKE takes a string pattern",
                        ))
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowStatusFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let mut rows = Vec::new();
                for &(name, value, session_only) in SHOW_STATUS_VARS {
                    if self.sem_hides_status_var(name) {
                        continue;
                    }
                    if show.global && session_only {
                        continue;
                    }
                    if let Some(pattern) = &pattern {
                        if !tidb_executor::like_match_with_collation(
                            name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ) {
                            continue;
                        }
                    }
                    let row = vec![
                        Datum::Bytes(name.as_bytes().to_vec()),
                        Datum::Bytes(value.as_bytes().to_vec()),
                    ];
                    // Go plans the WHERE as a selection over the same
                    // virtual rows, which is what this filter is.
                    if let Some(predicate) = predicate {
                        if !show_row_matches(predicate, SHOW_VARIABLE_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                        (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowCharset`: one row per charset in the parser's
            // registry, captured from mock TiDB (`Charset | Description |
            // Default collation | Maxlen`).
            tidb_ast::AdminStmt::ShowCharset(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowCharsetFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW CHARSET LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                    Some(tidb_ast::ShowCharsetFilter::Where(_)) => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                // Go's `SHOW CHARSET` is `charset.GetSupportedCharsets`, whose
                // rows carry whatever default collation
                // `switchDefaultCollation` last wrote. Reading the registry
                // here rather than a table copied out of it is what keeps the
                // `gbk`/`gb18030` default from having a second spelling.
                let mut rows = Vec::new();
                for info in tidb_datatype::get_supported_charsets() {
                    if let Some(pattern) = &pattern {
                        if !tidb_executor::like_match_with_collation(
                            &info.name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ) {
                            continue;
                        }
                    }
                    let row = vec![
                        Datum::Bytes(info.name.into_bytes()),
                        Datum::Bytes(info.description.into_bytes()),
                        Datum::Bytes(info.default_collation.into_bytes()),
                        Datum::Int(info.maxlen as i64),
                    ];
                    if let Some(predicate) = predicate {
                        if !show_row_matches(predicate, SHOW_CHARSET_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Charset".to_owned(), text()),
                        ("Description".to_owned(), text()),
                        ("Default collation".to_owned(), text()),
                        ("Maxlen".to_owned(), number()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowEngines`: this tier is the mock/embedded
            // single-engine server, so the table is always the single
            // `InnoDB` row Go's mock session reports.
            tidb_ast::AdminStmt::ShowEngines(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.as_str())
                    }
                    Some(tidb_ast::ShowEnginesFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW ENGINES LIKE takes a string pattern",
                        ));
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let row = vec![
                    Datum::Bytes(b"InnoDB".to_vec()),
                    Datum::Bytes(b"DEFAULT".to_vec()),
                    Datum::Bytes(
                        b"Supports transactions, row-level locking, and foreign keys".to_vec(),
                    ),
                    Datum::Bytes(b"YES".to_vec()),
                    Datum::Bytes(b"YES".to_vec()),
                    Datum::Bytes(b"YES".to_vec()),
                ];
                let included = pattern.is_none_or(|pattern| {
                    tidb_executor::like_match_with_collation(
                        "InnoDB",
                        pattern,
                        None,
                        tidb_datatype::Collation::Utf8Mb4Bin,
                    )
                }) && predicate
                    .map(|predicate| show_row_matches(predicate, SHOW_ENGINES_COLUMNS, &row))
                    .transpose()?
                    .unwrap_or(true);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Engine".to_owned(), text()),
                        ("Support".to_owned(), text()),
                        ("Comment".to_owned(), text()),
                        ("Transactions".to_owned(), text()),
                        ("XA".to_owned(), text()),
                        ("Savepoints".to_owned(), text()),
                    ],
                    rows: included.then_some(row).into_iter().collect(),
                }))
            }
            // Go `fetchShowCollation`: one row per collation in the
            // parser's registry (`Collation | Charset | Id | Default |
            // Compiled | Sortlen | Pad_attribute`).
            //
            // NOT MODELLED (documented): `Utf8Mb4ZhPinyinTiDbAsCs`, TiDB's
            // reserved pinyin collation stub -- mock TiDB's own `SHOW
            // COLLATION` capture omits it too, so this table matches the
            // 15 collations Go actually lists rather than this crate's
            // full 16-variant registry.
            tidb_ast::AdminStmt::ShowCollation(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowCollationFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW COLLATION LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                    Some(tidb_ast::ShowCollationFilter::Where(_)) => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let mut rows = Vec::new();
                for &collation in SHOW_COLLATION_ROWS {
                    let name = collation.name();
                    if let Some(pattern) = &pattern {
                        if !tidb_executor::like_match_with_collation(
                            name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ) {
                            continue;
                        }
                    }
                    let (sortlen, pad_attribute): (i64, &str) = match collation {
                        tidb_datatype::Collation::Utf8UnicodeCi
                        | tidb_datatype::Collation::Utf8Mb4UnicodeCi => (8, "PAD SPACE"),
                        tidb_datatype::Collation::Utf8Mb40900AiCi => (0, "NO PAD"),
                        tidb_datatype::Collation::Binary
                        | tidb_datatype::Collation::Utf8Mb40900Bin => (1, "NO PAD"),
                        _ => (1, "PAD SPACE"),
                    };
                    let row = vec![
                        Datum::Bytes(name.as_bytes().to_vec()),
                        Datum::Bytes(collation.charset().name().as_bytes().to_vec()),
                        Datum::Int(i64::from(collation.id())),
                        Datum::Bytes(if is_default_show_collation(collation) {
                            b"Yes".to_vec()
                        } else {
                            Vec::new()
                        }),
                        Datum::Bytes(b"Yes".to_vec()),
                        Datum::Int(sortlen),
                        Datum::Bytes(pad_attribute.as_bytes().to_vec()),
                    ];
                    if let Some(predicate) = predicate {
                        if !show_row_matches(predicate, SHOW_COLLATION_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Collation".to_owned(), text()),
                        ("Charset".to_owned(), text()),
                        ("Id".to_owned(), number()),
                        ("Default".to_owned(), text()),
                        ("Compiled".to_owned(), text()),
                        ("Sortlen".to_owned(), number()),
                        ("Pad_attribute".to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `ShowExec` with `ShowWarnings`/`ShowErrors`: the rows are
            // the statement-context warnings, whose `Level` column is
            // `Warning` or `Error`.
            //
            // DEFERRED (documented, and refused rather than ignored): the
            // optional filter Go's shared SHOW grammar accepts here.
            tidb_ast::AdminStmt::ShowWarnings(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::unsupported(
                        "SHOW WARNINGS filters are not supported yet",
                    ));
                }
                Ok(Some(self.warning_output(show.count_only, false)))
            }
            tidb_ast::AdminStmt::ShowErrors(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::unsupported(
                        "SHOW ERRORS filters are not supported yet",
                    ));
                }
                Ok(Some(self.warning_output(show.count_only, true)))
            }
            // Go `ShowExec.fetchShowProcessList`: one row per live
            // connection of this server, read from the session manager.
            tidb_ast::AdminStmt::ShowInspection(show) => {
                if show.kind != tidb_ast::ShowInspectionKind::ProcessList {
                    return Ok(None);
                }
                if show.filter.is_some() || show.database.is_some() {
                    return Err(DriverError::unsupported(
                        "SHOW PROCESSLIST filters are not supported yet",
                    ));
                }
                Ok(Some(self.process_list_output(show.full)))
            }
            // Go `SimpleExec.executeKillStmt`.
            tidb_ast::AdminStmt::Kill(kill) => self.kill_stmt(kill),
            // Go `CheckTableExec` / `CheckIndexRangeExec`. See
            // [`crate::admin_check_arm`] for the shapes and the refusals.
            tidb_ast::AdminStmt::AdminCheck(check) => {
                self.admin_check_stmt(check.as_ref()).map(Some)
            }
            tidb_ast::AdminStmt::ShowCreate {
                kind,
                name,
                if_not_exists,
            } => {
                if *kind == tidb_ast::ShowCreateKind::Database {
                    let [database] = name.as_slice() else {
                        return Err(DriverError::unsupported("empty database name"));
                    };
                    let (reported, charset) = self.with_catalog_mut(|catalog| {
                        catalog.database_definition(database).ok_or_else(|| {
                            DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                        })
                    })?;
                    return Ok(Some(crate::show_create_database::output(
                        reported,
                        charset,
                        *if_not_exists,
                    )));
                }
                // `SHOW CREATE SEQUENCE` and `SHOW CREATE TABLE` take the
                // SAME path: Go's `buildShow` picks the column names from
                // whether the object IS a sequence, not from the keyword
                // written (captured: `show create table s1` over a sequence
                // answers `Sequence | Create Sequence` with the
                // `CREATE SEQUENCE` text).
                let want_view = match kind {
                    tidb_ast::ShowCreateKind::Table | tidb_ast::ShowCreateKind::Sequence => false,
                    tidb_ast::ShowCreateKind::View => true,
                    _ => return Ok(None),
                };
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match name.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let ctx = self.statement_context(false);
                // A view answers either spelling with the same row, which
                // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
                // table is refused.
                let shown = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    match entry {
                        tidb_executor::TableEntry::View(view) => Ok((
                            show_create_view_text(view),
                            table_name.clone(),
                            Some((
                                view.character_set_client.clone(),
                                view.collation_connection.clone(),
                            )),
                            false,
                        )),
                        _ if want_view => Err(DriverError::Schema(SchemaErrorKind::WrongObject {
                            name: format!("{database}.{table_name}"),
                            expected: "VIEW",
                        })),
                        tidb_executor::TableEntry::Sequence(sequence) => Ok((
                            tidb_executor::show_create_sequence(sequence),
                            sequence.name.clone(),
                            None,
                            true,
                        )),
                        tidb_executor::TableEntry::Kv(table) => Ok((
                            show_create_table_text(&database, &table_name, table, &ctx)?,
                            table_name.clone(),
                            None,
                            false,
                        )),
                        tidb_executor::TableEntry::Mem(_) | tidb_executor::TableEntry::Cte(_) => {
                            Err(DriverError::unsupported(
                                "SHOW CREATE TABLE needs a storage-backed table",
                            ))
                        }
                    }
                });
                let (text, reported, view_charset, is_sequence) = shown?;
                let field_type =
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                // Go's view form carries its own header and the session's
                // character set and collation.
                if let Some((character_set_client, collation_connection)) = view_charset {
                    return Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("View".to_owned(), field_type.clone()),
                            ("Create View".to_owned(), field_type.clone()),
                            ("character_set_client".to_owned(), field_type.clone()),
                            ("collation_connection".to_owned(), field_type),
                        ],
                        rows: vec![vec![
                            Datum::Bytes(reported.into_bytes()),
                            Datum::Bytes(text.into_bytes()),
                            Datum::Bytes(character_set_client.into_bytes()),
                            Datum::Bytes(collation_connection.into_bytes()),
                        ]],
                    }));
                }
                // Go `buildShow` names these columns from `isSequence`, so a
                // sequence reports `Sequence | Create Sequence` whichever
                // keyword was written.
                let (name_column, text_column) = if is_sequence {
                    ("Sequence", "Create Sequence")
                } else {
                    ("Table", "Create Table")
                };
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (name_column.to_owned(), field_type.clone()),
                        (text_column.to_owned(), field_type),
                    ],
                    rows: vec![vec![
                        Datum::Bytes(reported.into_bytes()),
                        Datum::Bytes(text.into_bytes()),
                    ]],
                }))
            }
            tidb_ast::AdminStmt::ShowTableNextRowId(show) => {
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match show.table.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let ids = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    let tidb_executor::TableEntry::Kv(table) = entry else {
                        return Err(DriverError::unsupported(
                            "SHOW TABLE NEXT_ROW_ID needs a storage-backed table",
                        ));
                    };
                    table
                        .next_global_row_ids()
                        .map_err(|error| DriverError::AutoIdUnavailable(error.0))
                })?;
                let rows = ids
                    .into_iter()
                    .map(|(column, next, id_type)| {
                        vec![
                            Datum::Bytes(database.as_bytes().to_vec()),
                            Datum::Bytes(table_name.as_bytes().to_vec()),
                            Datum::Bytes(column.into_bytes()),
                            Datum::Int(next),
                            Datum::Bytes(id_type.as_bytes().to_vec()),
                        ]
                    })
                    .collect();
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("DB_NAME".to_owned(), text()),
                        ("TABLE_NAME".to_owned(), text()),
                        ("COLUMN_NAME".to_owned(), text()),
                        ("NEXT_GLOBAL_ROW_ID".to_owned(), number()),
                        ("ID_TYPE".to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowColumns`.
            tidb_ast::AdminStmt::ShowColumns(show) => {
                if show.extended {
                    return Err(DriverError::unsupported(
                        "SHOW EXTENDED COLUMNS is not supported yet",
                    ));
                }
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowColumnsFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowColumnsFilter::Where(expr)) => (None, Some(expr)),
                };
                let database = match &show.database {
                    Some(name) => name.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                let output = self.show_columns(&database, &show.table, None, show.full)?;
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go's parser rewrites `DESCRIBE tbl [col]` into a SHOW
            // COLUMNS statement; this parser keeps a node of its own, so
            // the same output is produced from it here.
            tidb_ast::AdminStmt::DescribeTable(describe) => {
                let database = self.require_current_database()?.to_owned();
                let column = describe.column.as_ref().and_then(|path| path.last());
                self.show_columns(
                    &database,
                    &describe.table,
                    column.map(String::as_str),
                    false,
                )
                .map(Some)
            }
            tidb_ast::AdminStmt::ShowTables(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowTablesFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowTablesFilter::Where(expr)) => (None, Some(expr)),
                };
                let database = match &show.database {
                    Some(name) => name.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                // Go `fetchShowTables` (`executor/show.go` around line 576)
                // asks `DBIsVisible` BEFORE `SchemaExists`, so a schema this
                // account could not see reports 1044 whether or not it
                // exists.
                self.require_visible_database(&database)?;
                let full = show.full;
                let listed = self.with_catalog_mut(|catalog| {
                    Ok(catalog.table_names(&database).map(|names| {
                        names
                            .into_iter()
                            .map(|name| {
                                let is_view = catalog.is_view_in(&database, &name);
                                (name, is_view)
                            })
                            .collect::<Vec<_>>()
                    }))
                })?;
                let listed = listed.ok_or_else(|| {
                    DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                })?;
                // Go filters each listed table by "any privilege at all"
                // (`show.go` around line 613), with `CREATE TEMPORARY
                // TABLES` excluded from the mask. Column-scope grants are
                // deliberately not consulted, which is Go's own standing
                // TODO there and is measured: a `SELECT(a)` grant makes the
                // SCHEMA visible but lists no table.
                let listed: Vec<_> = listed
                    .into_iter()
                    .filter(|(name, _)| {
                        self.has_any_scoped_privilege(
                            &database,
                            name,
                            privilege::show_tables_priv_mask(),
                        )
                    })
                    .filter(|(name, _)| {
                        like_pattern
                            .as_ref()
                            .is_none_or(|pattern| pattern.matches(name))
                    })
                    .collect();
                // Go names the column after the schema being listed.
                let base_name = format!("Tables_in_{database}");
                let name_column = match &like_pattern {
                    Some(pattern) => pattern.column_name(&base_name),
                    None => base_name,
                };
                let field_type =
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let column_names: Vec<&str> = if full {
                    vec![name_column.as_str(), "Table_type"]
                } else {
                    vec![name_column.as_str()]
                };
                let mut rows = Vec::with_capacity(listed.len());
                for (name, is_view) in listed {
                    let mut row = vec![Datum::Bytes(name.into_bytes())];
                    if full {
                        row.push(Datum::Bytes(table_type_of(is_view).as_bytes().to_vec()));
                    }
                    if let Some(predicate) = where_clause {
                        if !show_row_matches(predicate, &column_names, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                let columns = if full {
                    vec![
                        (name_column, field_type.clone()),
                        ("Table_type".to_owned(), field_type),
                    ]
                } else {
                    vec![(name_column, field_type)]
                };
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod column_description_source_tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

    #[test]
    fn test_desc() {
        // Direct port of pkg/table/column_test.go::TestDesc. The Go test
        // drives NewColDesc by replacing the column flags, then toggles the
        // generated-column storage bit and asks for both result schemas.
        let auto = FieldType::new(FieldTypeCode::Long).with_flags(
            FieldTypeFlags::AUTO_INCREMENT | FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY,
        );
        assert_eq!(column_extra(&auto, true, None, false), "auto_increment");

        let multiple = FieldType::new(FieldTypeCode::Long).with_flags(FieldTypeFlags::MULTIPLE_KEY);
        assert_eq!(column_extra(&multiple, false, None, false), "");

        let on_update = FieldType::new(FieldTypeCode::Timestamp)
            .with_flags(FieldTypeFlags::UNIQUE_KEY | FieldTypeFlags::ON_UPDATE_NOW);
        assert_eq!(
            column_extra(&on_update, false, None, false),
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
        );

        let ordinary = FieldType::new(FieldTypeCode::Long);
        assert_eq!(
            column_extra(&ordinary, false, Some(true), false),
            "STORED GENERATED"
        );
        assert_eq!(
            column_extra(&ordinary, false, Some(false), false),
            "VIRTUAL GENERATED"
        );
        assert_eq!(
            column_extra(&ordinary, false, None, true),
            "DEFAULT_GENERATED"
        );

        assert_eq!(
            COL_DESC_FIELD_NAMES,
            ["Field", "Type", "Null", "Key", "Default", "Extra"]
        );
        assert_eq!(
            FULL_COL_DESC_FIELD_NAMES,
            [
                "Field",
                "Type",
                "Collation",
                "Null",
                "Key",
                "Default",
                "Extra",
                "Privileges",
                "Comment",
            ]
        );
    }
}
