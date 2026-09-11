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

//! SHOW CREATE table/view text and stored definition formatting.

use super::*;

/// Go prints the column comment last, after AUTO_RANDOM and before the next
/// column's line, for every column that carries one.
fn push_column_comment(clause: &mut String, column: &tidb_executor::KvColumn) {
    if column.comment.is_empty() {
        return;
    }
    clause.push_str(" COMMENT '");
    clause.push_str(&tidb_util::format::output_format(&column.comment));
    clause.push('\'');
}

/// Go `stringutil.Escape` with a non-ANSI_QUOTES sql_mode: backtick-quoted,
/// with an embedded backtick doubled.
fn escape_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go's `TABLE_TYPE` / `Table_type` value for an object — `getTableType`
/// (`executor/show.go:519-528`): a view is VIEW, a sequence is SEQUENCE,
/// everything this tier lists is BASE TABLE.
pub(super) fn table_type_of(is_view: bool, is_sequence: bool) -> &'static str {
    if is_view {
        "VIEW"
    } else if is_sequence {
        "SEQUENCE"
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
/// The definer is whatever the statement recorded, which
/// [`tidb_executor::view`] settles at CREATE time: an explicit
/// `DEFINER = u@h` is kept verbatim, and `DEFINER = CURRENT_USER` (the
/// default) takes the connection's authenticated identity. It prints as
/// ``@`` only for a session with no authenticated identity at all, which
/// is an in-process session rather than a served connection -- a served
/// one prints its user, as the wire test in `tidb-server` captures
/// (`DEFINER=\`alice\`@\`%\``).
pub(super) fn show_create_view_text(view: &tidb_executor::ViewDef) -> String {
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
pub(super) fn show_create_table_text(
    database: &str,
    name: &str,
    table: &tidb_executor::KvTable,
    ctx: &tidb_executor::StmtContext,
) -> Result<String, DriverError> {
    // Go `ConstructResultOfShowCreateTable` (`executor/show.go:1073`): the
    // header names the table's kind, and the two temporary spellings are NOT
    // symmetric -- a global one prints `GLOBAL TEMPORARY` while a local one
    // prints plain `TEMPORARY`, which is the syntax each was created with.
    let mut out = match table.temp_table_type() {
        tidb_model::TempTableType::GLOBAL => {
            format!("CREATE GLOBAL TEMPORARY TABLE {} (\n", escape_name(name))
        }
        tidb_model::TempTableType::LOCAL => {
            format!("CREATE TEMPORARY TABLE {} (\n", escape_name(name))
        }
        _ => format!("CREATE TABLE {} (\n", escape_name(name)),
    };
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
            // Go does NOT stop here: its column loop falls through to the
            // comment, which an auto-increment column can carry like any
            // other. Everything between is inapplicable to one -- it has no
            // DEFAULT, no generation expression, and no ON UPDATE.
            push_column_comment(&mut clause, column);
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
        push_column_comment(&mut clause, column);
        clauses.push(clause);
    }

    // Go `ShowCreateTable` (`show.go:1193`) emits the primary key from the
    // COLUMN only when `PKIsHandle`, because that key alone is absent from
    // `tb.Indices()`; every other primary key is printed once by the index
    // loop below.
    //
    // A common handle reaches this function from two tiers that disagree
    // about where it lives: a table loaded from a stored `TableInfo` keeps
    // a PRIMARY entry in its index list, while one configured in-process
    // records only the handle offsets. Emitting from the handle offsets
    // unconditionally therefore printed the key TWICE for the first tier --
    // a `SHOW CREATE TABLE` that is not valid SQL. Print it here only when
    // the index list does not already carry it.
    let index_carries_primary = table
        .indexes()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("PRIMARY"));
    let handle_columns: Vec<usize> = match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None if index_carries_primary => Vec::new(),
        None => table.common_handle_offsets().to_vec(),
    };
    let primary_emitted_from_handles = !handle_columns.is_empty();
    if primary_emitted_from_handles {
        let columns = handle_columns
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        clauses.push(format!(
            "  PRIMARY KEY ({columns}) /*T![clustered_index] CLUSTERED */"
        ));
    }

    for index in table.indexes() {
        // The PRIMARY is emitted ONCE, and the block above decided which of
        // the two places does it: it prints from the handle offsets only when
        // the index list carries no PRIMARY, and otherwise leaves it to this
        // loop. So the loop skips the PRIMARY exactly when that block already
        // printed it -- and no longer on its own separate test.
        //
        // The two guards used to be written against each other rather than
        // against one fact: the block deferred whenever the index list held a
        // PRIMARY, while this loop skipped whenever the index's columns were
        // the common handle's. For a cluster-loaded common-handle table BOTH
        // were true, so neither printed and `SHOW CREATE TABLE` lost the key
        // altogether. A `pk_is_handle` table whose index list also held a
        // PRIMARY was the mirror case: neither test fired and it printed
        // TWICE.
        if index.name.eq_ignore_ascii_case("PRIMARY") && primary_emitted_from_handles {
            continue;
        }
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
            // Go `idxInfo.Primary`: the comment follows
            // `tableInfo.HasClusteredIndex()`, which a common handle
            // satisfies.
            let clustered = if table.common_handle_offsets().is_empty() {
                "NONCLUSTERED"
            } else {
                "CLUSTERED"
            };
            format!("  PRIMARY KEY ({columns}) /*T![clustered_index] {clustered} */")
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

    // Go emits public CHECK constraints after every foreign key. The stored
    // expression is already normalized by DDL; SHOW adds one parenthesis
    // pair for CHECK and another around the expression itself.
    for constraint in table
        .check_constraint_infos()
        .iter()
        .filter(|constraint| constraint.state == tidb_model::SchemaState::PUBLIC)
    {
        let mut clause = format!(
            "  CONSTRAINT {} CHECK (({}))",
            escape_name(constraint.name.original()),
            constraint.expr_string
        );
        if !constraint.enforced {
            clause.push_str(" /*!80016 NOT ENFORCED */");
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
    // Go `ShowCreateTable` (`executor/show.go:1373-1375`): the compression
    // setting, printed before the auto-increment option when non-empty.
    if !table.compression().is_empty() {
        out.push_str(&format!(" COMPRESSION='{}'", table.compression()));
    }
    // Go `ShowCreateTable` (`executor/show.go:1383-1387`): the MySQL-compatible
    // ungated `AUTO_INCREMENT=%d`, printed when the table has an auto-increment
    // column and the allocator's next value exceeds 1 (fresh tables with no
    // inserts print nothing).
    if let Some(next) = table.next_auto_increment().filter(|next| *next > 1) {
        out.push_str(&format!(" AUTO_INCREMENT={next}"));
    }
    // Go `ShowCreateTable`: printed only when the table set one.
    if table.auto_id_cache() != 0 {
        out.push_str(&format!(
            " /*T![auto_id_cache] AUTO_ID_CACHE={} */",
            table.auto_id_cache()
        ));
    }
    if let Some(base) = table.next_auto_random().filter(|base| *base > 1) {
        out.push_str(&format!(" /*T![auto_rand_base] AUTO_RANDOM_BASE={base} */"));
    }
    // Go `ShowCreateTable` (`executor/show.go:1405`), in this position:
    // after `AUTO_RANDOM_BASE` and before the placement policy.
    // `PRE_SPLIT_REGIONS` shares the ONE comment rather than opening a second
    // -- it gets its own only for an `AUTO_RANDOM` table, which has no shard
    // clause to join.
    if table.shard_row_id_bits() > 0 {
        out.push_str(&format!(
            " /*T! SHARD_ROW_ID_BITS={} ",
            table.shard_row_id_bits()
        ));
        if table.pre_split_regions() > 0 {
            out.push_str(&format!("PRE_SPLIT_REGIONS={} ", table.pre_split_regions()));
        }
        out.push_str("*/");
    }
    // Go `ConstructResultOfShowCreateTable` (`executor/show.go:1421`) prints
    // the clause for every GLOBAL temporary table UNCONDITIONALLY, after the
    // comment and before the placement policy. It is unconditional because
    // nothing stores the mode: `TableInfo` has no `OnCommitDelete` field, and
    // `ON COMMIT PRESERVE ROWS` is refused at CREATE, so a global temporary
    // table in the catalog can only be a DELETE ROWS one. A local temporary
    // table prints no ON COMMIT clause at all.
    if table.temp_table_type() == tidb_model::TempTableType::GLOBAL {
        out.push_str(" ON COMMIT DELETE ROWS");
    }
    // Go `ShowCreateTable` (`executor/show.go:1425`) prints the table's own
    // policy after the comment and BEFORE the cached marker, under the same
    // `/*T![placement] ... */` feature gate a partition's uses -- so a dump
    // carrying placement still loads on a parser that does not know it.
    out.push_str(&tidb_executor::partition_placement_text(
        table.placement_policy(),
    ));
    if table.is_cached() {
        out.push_str(" /* CACHED ON */");
    }
    out.push_str(&ttl_clause_text(table));
    out.push_str(&partition_clause_text(table));
    Ok(out)
}

/// Go `ShowCreateTable`'s `TTLInfo` block (`executor/show.go:1510`): three
/// separately gated clauses, each behind the `ttl` feature comment so a
/// dumped definition still loads on a parser that does not know the syntax.
///
/// The three are always printed together and always all three -- Go writes
/// `TTL_ENABLE` and `TTL_JOB_INTERVAL` unconditionally once there is a
/// `TTLInfo`, rather than only when they were written -- which is why a table
/// created with `TTL=` alone prints `TTL_ENABLE='ON'` and the default job
/// interval back.
fn ttl_clause_text(table: &tidb_executor::KvTable) -> String {
    let Some(info) = table.ttl_info() else {
        return String::new();
    };
    let unit = tidb_model::time_unit_type_keyword(info.interval_time_unit).unwrap_or_default();
    // Go's "this only happens for a table created in 6.5" fallback, where the
    // job interval had not been introduced yet.
    let job_interval = if info.job_interval.is_empty() {
        tidb_model::OLD_DEFAULT_TTL_JOB_INTERVAL
    } else {
        info.job_interval.as_str()
    };
    let mut text = format!(
        " /*T![ttl] TTL=`{}` + INTERVAL {} {} */",
        info.column_name.original().replace('`', "``"),
        info.interval_expr_str,
        unit,
    );
    text.push_str(&format!(
        " /*T![ttl] TTL_ENABLE='{}' */",
        if info.enable { "ON" } else { "OFF" }
    ));
    text.push_str(&format!(" /*T![ttl] TTL_JOB_INTERVAL='{job_interval}' */"));
    text
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
/// Go `AppendPartitionInfo`'s `defaultPartitionDefinitions`
/// (`ddl/partition.go:5147-5159`): whether a HASH/KEY table's partitions are
/// still exactly the ones `PARTITIONS n` would generate.
fn hash_definitions_are_default(definitions: &[tidb_executor::PartitionDef]) -> bool {
    definitions.iter().enumerate().all(|(ordinal, definition)| {
        definition.name == format!("p{ordinal}")
            && definition.comment.is_empty()
            // Go's third condition: a partition carrying a PLACEMENT POLICY
            // is not default-shaped either (`AppendPartitionInfo`), so the
            // compact `PARTITIONS n` form would lose the policy.
            && definition.placement_policy.is_none()
    })
}

fn partition_clause_text(table: &tidb_executor::KvTable) -> String {
    let Some(partition) = table.partition() else {
        return String::new();
    };
    let head = format!(
        "\nPARTITION BY {} ({})",
        partition.kind.sql(),
        partition.expr_text
    );
    let defs = || tidb_executor::append_partition_defs(&partition.definitions, &partition.kind);
    match &partition.kind {
        // Go `AppendPartitionInfo` (`ddl/partition.go:5147-5171`): HASH and
        // KEY print the COMPACT `PARTITIONS n` form only when every partition
        // is still the one Go would have generated -- named `p<i>`, with no
        // comment and no placement ref. As soon as one partition was named or
        // commented, the whole definition list is printed instead, because the
        // compact form cannot express it.
        //
        // Printing `PARTITIONS n` unconditionally lost the written names and
        // any comments from `SHOW CREATE TABLE`, so the DDL it produced built
        // a DIFFERENT table from the one it described.
        tidb_executor::PartitionKind::Hash | tidb_executor::PartitionKind::Key => {
            let head = if matches!(partition.kind, tidb_executor::PartitionKind::Key) {
                // Go `writeColumnListToBuffer` (`ddl/partition.go:5125`)
                // emits NOTHING when the column list was filled in from the
                // primary key, so `PARTITION BY KEY ()` reads back as
                // written and re-creating it resolves the key again.
                let columns = if partition.is_empty_columns {
                    String::new()
                } else {
                    partition.expr_text.clone()
                };
                format!("\nPARTITION BY KEY ({columns})")
            } else {
                head.clone()
            };
            if hash_definitions_are_default(&partition.definitions) {
                format!("{head} PARTITIONS {}", partition.num())
            } else {
                format!("{head}{}", defs())
            }
        }
        tidb_executor::PartitionKind::Range { .. } | tidb_executor::PartitionKind::List { .. } => {
            format!("{head}{}", defs())
        }
        // Go prints ` COLUMNS(` for the typed forms, with MySQL's two spaces
        // after LIST (`ddl/partition.go:5185`).
        tidb_executor::PartitionKind::RangeColumns { .. }
        | tidb_executor::PartitionKind::ListColumns { .. } => format!(
            "\nPARTITION BY {} COLUMNS({}){}",
            partition.kind.sql(),
            partition.expr_text,
            defs()
        ),
        // Go `AppendPartitionInfo` has no NONE special case: `Columns` is
        // empty, so it takes the `else` branch and prints
        // `PARTITION BY NONE (<Expr>)` with the definition list.
        tidb_executor::PartitionKind::None => format!("{head}{}", defs()),
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
