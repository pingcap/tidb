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

//! Turning the constraints a `CREATE TABLE` declares into table metadata:
//! keys, foreign keys, and the primary key that becomes the row handle.
//!
//! Inside: [`table_indexes`], which gives every declared key its own
//! `IndexInfo` in TiDB's observed order (table-level constraints first, then
//! inline column constraints); [`table_foreign_keys`] with [`fk_action`],
//! which collapse `NO ACTION`/`SET DEFAULT`/no clause onto `RESTRICT` for the
//! reason [`FkAction`] documents; [`primary_key_column`] and
//! [`is_int_column`], which decide whether a primary key is clustered ONTO
//! the row handle; and the two column flags that decision stamps.
//!
//! Mirrors the constraint half of Go `pkg/ddl`'s `buildTableInfo` --
//! `buildIndexInfo`, `buildFKInfo`, and the `PKIsHandle` test. The type and
//! charset half is in the sibling `column_types` module.

use super::{
    index_part_names, is_visible, Catalog, ColumnInfo, DriverError, FkAction, KvForeignKey,
    KvIndex, SchemaErrorKind,
};
use crate::expression_index::HiddenIndexColumn;
use tidb_datatype::FieldTypeCode;

/// Go `mysql.AutoIncrementFlag`.
pub(crate) const AUTO_INCREMENT_FLAG: u32 = 1 << 9;

/// Go `mysql.PriKeyFlag`.
pub(crate) const PRI_KEY_FLAG: u32 = 1 << 1;

/// Every index a `CREATE TABLE` declares, other than a primary key that
/// became the row handle.
///
/// Go's `buildTableInfo` turns each key constraint into an `IndexInfo` with
/// its own id. The order is table-level constraints first, in written order,
/// then the inline column constraints in column order -- captured from real
/// TiDB's `SHOW CREATE TABLE`, which lists them in index order:
/// `create table x (a bigint unique, b bigint unique, key kb (b))` reports
/// `KEY kb` before `UNIQUE KEY a` and `UNIQUE KEY b`.
///
/// An EXPRESSION key part becomes a hidden generated column appended after
/// the table's declared ones, and the index points at that column -- Go's own
/// structure, see [`crate::expression_index`]. Those columns come back
/// alongside the indexes because the caller is what owns the column vector.
///
/// DEFERRED (documented): FULLTEXT, VECTOR and COLUMNAR indexes, prefix
/// lengths and index options, all rejected rather than silently created as a
/// plain index.
pub(crate) fn table_indexes(
    create: &tidb_ast::CreateTableStmt,
    columns: &[ColumnInfo],
    pk_is_handle: bool,
) -> Result<(Vec<KvIndex>, Vec<HiddenIndexColumn>), DriverError> {
    let column_names: Vec<String> = columns
        .iter()
        .map(|c| c.name.original().to_owned())
        .collect();
    let column_types: Vec<tidb_datatype::FieldType> =
        columns.iter().map(|c| c.field_type.clone()).collect();
    let mut hidden: Vec<HiddenIndexColumn> = Vec::new();
    // Go `checkIndexColumn`: a JSON column can never be an index column, in
    // any position of any index kind -- checked here, where every index part
    // resolves its column, so the rule has exactly one home.
    let offset_of = |name: &str| -> Result<usize, DriverError> {
        let offset = columns
            .iter()
            .position(|col| col.name.original().eq_ignore_ascii_case(name))
            .ok_or(DriverError::Unsupported(
                "an index names a column the table does not define",
            ))?;
        if columns[offset].field_type.code() == FieldTypeCode::Json {
            return Err(DriverError::JsonUsedInKey(
                columns[offset].name.original().to_owned(),
            ));
        }
        Ok(offset)
    };
    fn push(
        indexes: &mut Vec<KvIndex>,
        name: String,
        unique: bool,
        offsets: Vec<usize>,
        visible: bool,
    ) {
        indexes.push(KvIndex {
            id: (indexes.len() + 1) as i64,
            name,
            unique,
            column_offsets: offsets,
            visible,
        });
    }
    /// Go `GetName4AnonymousIndex` (`pkg/ddl/executor.go`): an index written
    /// without a name is named after its FIRST column, and a collision with an
    /// index already on the table appends `_2`, `_3`, ... until it is free.
    /// `PRIMARY` is never taken as an anonymous name, so a column called
    /// `primary` yields `primary_2`.
    ///
    /// Captured: `create table n3 (a int, b int, unique key (a), key (a))`
    /// prints `UNIQUE KEY \`a\` (\`a\`)` and `KEY \`a_2\` (\`a\`)`.
    fn anonymous_index_name(indexes: &[KvIndex], first_column: &str) -> String {
        let mut id = 2;
        let mut name = if first_column.eq_ignore_ascii_case("primary") {
            id = 3;
            format!("{first_column}_2")
        } else {
            first_column.to_owned()
        };
        while indexes
            .iter()
            .any(|index| index.name.eq_ignore_ascii_case(&name))
        {
            name = format!("{first_column}_{id}");
            id += 1;
        }
        name
    }
    let mut indexes: Vec<KvIndex> = Vec::new();

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
        crate::ddl::indexes::reject_partial_index(&index.options)?;
        let unique = matches!(
            index.kind,
            tidb_ast::IndexConstraintKind::Unique
                | tidb_ast::IndexConstraintKind::UniqueKey
                | tidb_ast::IndexConstraintKind::UniqueIndex
                | tidb_ast::IndexConstraintKind::PrimaryKey
        );
        // The name has to be settled BEFORE the expression parts are built,
        // because a hidden column is named `_V$_<index name>_<part>`.
        let name = match index.kind {
            tidb_ast::IndexConstraintKind::PrimaryKey => "PRIMARY".to_owned(),
            _ => match index.name.clone() {
                Some(given) => given,
                None => match index.parts.first() {
                    Some(tidb_ast::IndexPart::Column { name, .. }) => {
                        anonymous_index_name(&indexes, name)
                    }
                    // Go `getAnonymousIndexPrefix`: an expression key part has
                    // no column name to be named after. Captured: two unnamed
                    // expression indexes become `expression_index` and
                    // `expression_index_2`.
                    Some(tidb_ast::IndexPart::Expr { .. }) => {
                        anonymous_index_name(&indexes, "expression_index")
                    }
                    None => return Err(DriverError::Unsupported("an index names no column")),
                },
            },
        };
        let built = crate::expression_index::build_hidden_columns(
            &name,
            &index.parts,
            &column_names,
            &column_types,
        )?;
        let mut offsets = Vec::with_capacity(index.parts.len());
        for (position, part) in index.parts.iter().enumerate() {
            match part {
                tidb_ast::IndexPart::Column {
                    name, prefix_len, ..
                } => {
                    if prefix_len.is_some() {
                        return Err(DriverError::Unsupported(
                            "a prefix-length index is not supported yet",
                        ));
                    }
                    offsets.push(offset_of(name)?);
                }
                // The hidden columns are appended after the declared ones, in
                // the order they were built, so this part's offset is where
                // it will land in the final column vector.
                tidb_ast::IndexPart::Expr { .. } => {
                    let index_in_built = built
                        .iter()
                        .position(|(at, _)| *at == position)
                        .expect("every expression part builds a hidden column");
                    offsets.push(columns.len() + hidden.len() + index_in_built);
                }
            }
        }
        hidden.extend(built.into_iter().map(|(_, column)| column));
        push(
            &mut indexes,
            name,
            unique,
            offsets,
            is_visible(&index.options),
        );
    }
    for def in &create.columns {
        for option in &def.options {
            if let tidb_ast::ColumnOption::InlineKey(key) = option {
                match key.kind {
                    tidb_ast::InlineKeyKind::Unique => {
                        let offset = offset_of(&def.name)?;
                        push(&mut indexes, def.name.clone(), true, vec![offset], true);
                    }
                    // A primary key that is not the row handle still needs an
                    // index to enforce its uniqueness.
                    tidb_ast::InlineKeyKind::Primary { .. } if !pk_is_handle => {
                        let offset = offset_of(&def.name)?;
                        push(&mut indexes, "PRIMARY".to_owned(), true, vec![offset], true);
                    }
                    tidb_ast::InlineKeyKind::Primary { .. } => {}
                }
            }
        }
    }

    Ok((indexes, hidden))
}

/// Go `ddl.buildFKInfo`: the `FOREIGN KEY` table constraints, resolved
/// against the table being created and against the referenced table.
///
/// `foreign_key_checks` is the session switch: with it OFF, Go skips the
/// reference-resolution checks entirely and stores the constraint as written
/// (`ddl.checkTableInfoValid` -> `checkAndCreateForeignKey`), which is what
/// lets a child table be created before its parent.
///
/// NOT MODELLED (documented): a column-level `REFERENCES` clause, a `MATCH
/// PARTIAL` mode, and a prefix-length or expression key part -- each is
/// refused rather than silently dropped, so a table never claims a
/// constraint it does not enforce.
pub(crate) fn table_foreign_keys(
    create: &tidb_ast::CreateTableStmt,
    columns: &[ColumnInfo],
    catalog: &Catalog,
    database: &str,
    foreign_key_checks: bool,
) -> Result<Vec<KvForeignKey>, DriverError> {
    let mut keys = Vec::new();
    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::ForeignKey(definition) = constraint else {
            continue;
        };
        if definition.reference.match_type == tidb_ast::ForeignKeyMatch::Partial {
            return Err(DriverError::Unsupported(
                "MATCH PARTIAL is not supported yet",
            ));
        }
        let mut cols = Vec::with_capacity(definition.parts.len());
        for name in index_part_names(&definition.parts)? {
            let offset = columns
                .iter()
                .position(|column| column.name.original().eq_ignore_ascii_case(&name))
                .ok_or(DriverError::Unsupported(
                    "a foreign key names a column the table does not define",
                ))?;
            cols.push(offset);
        }
        let Some(path) = &definition.reference.table else {
            return Err(DriverError::Unsupported(
                "a foreign key needs a referenced table",
            ));
        };
        let (ref_schema, ref_table) = match path.as_slice() {
            [name] => (database.to_owned(), name.clone()),
            [schema, name] => (schema.clone(), name.clone()),
            _ => return Err(DriverError::Unsupported("empty referenced table name")),
        };
        let ref_cols = match &definition.reference.parts {
            Some(parts) => index_part_names(parts)?,
            None => Vec::new(),
        };
        if ref_cols.len() != cols.len() {
            return Err(DriverError::WrongFkDef {
                name: definition.name.clone().unwrap_or_default(),
                reason: "Key reference and table reference don't match".to_owned(),
            });
        }
        let fk_name = definition
            .name
            .clone()
            .unwrap_or_else(|| format!("fk_{}", keys.len() + 1));
        child_generated_column_rules(create, &cols, definition, &fk_name)?;
        if foreign_key_checks {
            // Go `checkTableInfoValid`: an unresolvable reference is
            // `ErrNoReferencedRow`-adjacent at DDL time, not at write time.
            let parent = catalog.get_in(&ref_schema, &ref_table).ok_or_else(|| {
                DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{ref_schema}.{ref_table}"
                )))
            })?;
            let parent_columns = parent.column_names();
            for name in &ref_cols {
                if !parent_columns
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(name))
                {
                    return Err(DriverError::UnknownColumnInTable {
                        column: name.clone(),
                        table: ref_table.clone(),
                    });
                }
            }
            // Go `checkTableForeignKey`: the REFERENCED column may not be
            // virtual either. Unlike the child-side rule above this one sits
            // behind the switch, because it is the parent lookup that reaches
            // it at all.
            let crate::TableEntry::Kv(parent) = parent else {
                return Err(DriverError::Unsupported(
                    "a foreign key may not reference a view",
                ));
            };
            for name in &ref_cols {
                let virtual_generated = parent
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(name))
                    .and_then(|column| column.generated.as_ref())
                    .is_some_and(|generated| !generated.stored);
                if virtual_generated {
                    return Err(DriverError::ForeignKeyUsesVirtualColumn {
                        foreign_key: fk_name.clone(),
                        column: name.clone(),
                    });
                }
            }
        }
        keys.push(KvForeignKey {
            name: fk_name,
            cols,
            ref_schema,
            ref_table,
            ref_cols,
            on_delete: fk_action(definition.reference.on_delete),
            on_update: fk_action(definition.reference.on_update),
        });
    }
    Ok(keys)
}

/// Go `buildFKInfo`'s generated-column arm: what a constraint may name on the
/// CHILD side.
///
/// * A VIRTUAL referencing column is 3733. There is no stored value to key
///   the constraint on.
/// * A STORED referencing column is legal, but only under actions that never
///   WRITE it: `ON UPDATE CASCADE`, `ON UPDATE SET NULL` and
///   `ON DELETE SET NULL` are 3104, because each would assign a column whose
///   value the table computes. `ON DELETE CASCADE` removes the row instead of
///   writing the column, so it is accepted -- captured, not inferred.
///
/// NOT gated on `foreign_key_checks`: captured, `create table t2 (a int,
/// c int as (a+1) virtual, constraint fk foreign key(c) references t1(a))` is
/// 3733 with the switch at 0, because Go reaches this from `buildFKInfo`,
/// which runs unconditionally, rather than from the reference resolution the
/// switch guards.
fn child_generated_column_rules(
    create: &tidb_ast::CreateTableStmt,
    cols: &[usize],
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    fk_name: &str,
) -> Result<(), DriverError> {
    for offset in cols {
        let Some(column) = create.columns.get(*offset) else {
            continue;
        };
        let Some((_, stored)) = column.options.iter().find_map(|option| match option {
            tidb_ast::ColumnOption::Generated {
                expression_text,
                stored,
                ..
            } => Some((expression_text, *stored)),
            _ => None,
        }) else {
            continue;
        };
        if !stored {
            return Err(DriverError::ForeignKeyUsesVirtualColumn {
                foreign_key: fk_name.to_owned(),
                column: column.name.clone(),
            });
        }
        // Go spells the clause back verbatim in the message, which is why the
        // AST action is read here rather than the `FkAction` it collapses to:
        // `SET DEFAULT` behaves as `RESTRICT` at run time but is still named
        // as itself by this refusal.
        let writes = |action: Option<tidb_ast::ReferentialAction>| -> Option<&'static str> {
            match action? {
                tidb_ast::ReferentialAction::Cascade => Some("CASCADE"),
                tidb_ast::ReferentialAction::SetNull => Some("SET NULL"),
                tidb_ast::ReferentialAction::SetDefault => Some("SET DEFAULT"),
                _ => None,
            }
        };
        if let Some(action) = writes(definition.reference.on_update) {
            return Err(DriverError::WrongFkOptionForGeneratedColumn {
                clause: format!("ON UPDATE {action}"),
            });
        }
        // ON DELETE CASCADE is absent here on purpose: it deletes the row
        // rather than writing the generated column, and TiDB accepts it.
        if let Some(action) =
            writes(definition.reference.on_delete).filter(|action| *action != "CASCADE")
        {
            return Err(DriverError::WrongFkOptionForGeneratedColumn {
                clause: format!("ON DELETE {action}"),
            });
        }
    }
    Ok(())
}

/// Go `ast.ReferOptionType` -> the behaviour it actually produces. See
/// [`FkAction`] for why three of the six spellings collapse into `Restrict`.
fn fk_action(action: Option<tidb_ast::ReferentialAction>) -> FkAction {
    match action.unwrap_or(tidb_ast::ReferentialAction::NoOption) {
        tidb_ast::ReferentialAction::Cascade => FkAction::Cascade,
        tidb_ast::ReferentialAction::SetNull => FkAction::SetNull,
        tidb_ast::ReferentialAction::NoOption
        | tidb_ast::ReferentialAction::Restrict
        | tidb_ast::ReferentialAction::NoAction
        | tidb_ast::ReferentialAction::SetDefault => FkAction::Restrict,
    }
}

/// Go `isIntCol`: whether the column's type can carry a handle.
pub(crate) fn is_int_column(column: &ColumnInfo) -> bool {
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
pub(crate) fn primary_key_column(
    create: &tidb_ast::CreateTableStmt,
) -> Result<Option<Vec<String>>, DriverError> {
    let mut found: Option<Vec<String>> = None;
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
                        found = Some(vec![def.name.clone()]);
                    }
                    // A unique key is collected by `table_indexes`.
                    tidb_ast::InlineKeyKind::Unique => {}
                }
            }
        }
    }
    for constraint in &create.table_constraints {
        let index = match constraint {
            tidb_ast::TableConstraint::Index(index) => index,
            // A foreign key is collected by `table_foreign_keys`, and never
            // contributes a primary key.
            tidb_ast::TableConstraint::ForeignKey(_) => continue,
            // A CHECK constraint is discarded -- which is what real TiDB
            // does with `tidb_enable_check_constraint` off, the only mode
            // `run_create_table_in` accepts one in. See its doc comment for
            // the captured `SHOW CREATE TABLE` evidence.
            tidb_ast::TableConstraint::Check(_) => continue,
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
        let mut names = Vec::with_capacity(index.parts.len());
        for part in &index.parts {
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
            names.push(name.clone());
        }
        found = Some(names);
    }
    Ok(found)
}
