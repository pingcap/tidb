//! Foreign-key metadata construction and catalog-aware validation for the
//! cluster DDL path, over the persisted `TableInfo` shape.
//!
//! Go splits this across three stages, and so does this module:
//! * [`build_fk_infos`] -- `buildTableInfo`'s `ConstraintForeignKey` arm
//!   (`create_table.go:1452`, `buildFKInfo` at `executor.go:5897`) plus
//!   `addIndexForForeignKey` (`create_table.go:1740`). Catalog-free.
//! * [`check_table_foreign_keys_valid`] -- `checkTableForeignKeysValid`
//!   (`foreign_key.go:151`), the submitter-side check against InfoSchema.
//! * [`check_table_foreign_keys_valid_in_owner`] -- the owner-side
//!   `checkTableForeignKeyValidInOwner` (`foreign_key.go:214`) followed by
//!   `allocateFKIndexID` (`create_table.go:95`).

#[allow(unused_imports)]
use tidb_ddl_build::foreign_key::*;
use tidb_infoschema::cluster_catalog::ClusterCatalog;
use tidb_ast::CiString;
use tidb_datatype::FieldTypeFlags;
use tidb_hack::GoToLower;
use tidb_model::column::find_column_info;
use tidb_model::index::{is_index_prefix_covered_for_foreign_key, IndexInfo};
use tidb_model::schema_state::SchemaState;
use tidb_model::table::{FKInfo, ReferredFKInfo, FK_VERSION1};
use tidb_model::table_info::TableInfo;

/// Go `checkTableForeignKeyValid` (submitter) and, with `in_owner`,
/// `checkTableForeignKeyValidInOwner` for one key of `table`: the key against
/// the table it references, which may be `table` itself.
pub(crate) fn check_foreign_key_valid(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &TableInfo,
    fk: &FKInfo,
    fk_check: bool,
    in_owner: bool,
) -> Refusal<()> {
    if fk.version < FK_VERSION1 {
        return Ok(());
    }
    let refer = if fk.ref_schema.lowercase() == schema.go_to_lower()
        && fk.ref_table.lowercase() == table.name.lowercase()
    {
        if !in_owner {
            let cols = fk.cols.snapshot();
            let ref_cols = fk.ref_cols.snapshot();
            if cols
                .iter()
                .zip(&ref_cols)
                .all(|(a, b)| a.lowercase() == b.lowercase())
            {
                return Err(cannot_add());
            }
        }
        table
    } else {
        match lookup(catalog, fk.ref_schema.original(), fk.ref_table.original()) {
            Some(refer) => refer,
            None if !fk_check => return Ok(()),
            None if in_owner => {
                return Err(coded(
                    &tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS,
                    format!(
                        "Table '{}.{}' doesn't exist",
                        fk.ref_schema.original(),
                        fk.ref_table.original()
                    ),
                ))
            }
            None => {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT
                        .generate_with_stack_by_args(&[(fk.ref_table.original())
                            .to_string()
                            .into()]),
                )))
            }
        }
    };
    check_table_foreign_key(refer, table, fk)
}

fn lookup<'c>(catalog: &'c ClusterCatalog, schema: &str, table: &str) -> Option<&'c TableInfo> {
    catalog
        .find_table(schema, table)
        .map(|(_, table)| table)
        .filter(|table| table.state == SchemaState::PUBLIC)
}

/// Go `checkTableForeignKeysValid` (`foreign_key.go:151`) and, with
/// `in_owner`, `checkTableForeignKeyValidInOwner` (`foreign_key.go:214`).
/// They differ only in the self-reference same-columns refusal (submitter
/// only) and the error an unopenable parent reports.
fn check_valid(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &TableInfo,
    fk_check: bool,
    in_owner: bool,
) -> Refusal<()> {
    if !tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    let schema_l = schema.go_to_lower();
    for fk in table.foreign_keys.iter_deref() {
        let fk = fk.read();
        check_foreign_key_valid(catalog, schema, table, &fk, fk_check, in_owner)?;
    }
    // Go `is.GetTableReferredForeignKeys`: children created earlier with
    // `foreign_key_checks=0` that reference this (new) table.
    for database in &catalog.databases {
        for child in &database.tables {
            for fk in child.foreign_keys.iter_deref() {
                let fk = fk.read();
                if fk.version >= FK_VERSION1
                    && fk.ref_schema.lowercase() == schema_l
                    && fk.ref_table.lowercase() == table.name.lowercase()
                {
                    check_table_foreign_key(table, child, &fk)?;
                }
            }
        }
    }
    Ok(())
}

/// Go `checkTableForeignKeysValid`: the submitter-side check.
pub fn check_table_foreign_keys_valid(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &TableInfo,
    fk_check: bool,
) -> Refusal<()> {
    check_valid(catalog, schema, table, fk_check, false)
}

/// Go `checkTableForeignKeyValidInOwner` then `allocateFKIndexID` for every
/// key (`create_table.go:87-98`).
pub fn check_table_foreign_keys_valid_in_owner(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &mut TableInfo,
    fk_check: bool,
) -> Refusal<()> {
    check_valid(catalog, schema, table, fk_check, true)?;
    for fk in table.foreign_keys.iter_deref() {
        table.max_foreign_key_id += 1;
        let mut fk = fk.write();
        fk.id = table.max_foreign_key_id;
        fk.state = SchemaState::PUBLIC;
    }
    Ok(())
}

/// Go `InfoSchema.GetTableReferredForeignKeys`: the foreign keys of every
/// table that cite `schema`.`table`, as `addReferredForeignKeys` builds the
/// `referredForeignKeyMap` entry -- version-1 keys only, ordered by child
/// schema, child table and key name.
pub(crate) fn get_table_referred_foreign_keys(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
) -> Vec<ReferredFKInfo> {
    let (schema, table) = (
        tidb_hack::go_to_lower(schema),
        tidb_hack::go_to_lower(table),
    );
    let mut referred = Vec::new();
    for database in &catalog.databases {
        for child in &database.tables {
            for fk in child.foreign_keys.iter_deref() {
                let fk = fk.read();
                if fk.version < FK_VERSION1
                    || fk.ref_schema.lowercase() != schema
                    || fk.ref_table.lowercase() != table
                {
                    continue;
                }
                referred.push(ReferredFKInfo {
                    cols: tidb_model::serde_helpers::GoValueSlice::allocated(
                        fk.ref_cols.snapshot(),
                    ),
                    child_schema: database.info.name.clone(),
                    child_table: child.name.clone(),
                    child_fk_name: fk.name.clone(),
                });
            }
        }
    }
    referred.sort_by(|left, right| {
        (
            left.child_schema.lowercase(),
            left.child_table.lowercase(),
            left.child_fk_name.lowercase(),
        )
            .cmp(&(
                right.child_schema.lowercase(),
                right.child_table.lowercase(),
                right.child_fk_name.lowercase(),
            ))
    });
    referred
}

/// Go `checkTableHasForeignKeyReferred`: the first key, from a table that
/// still exists and is not among `ignored` (schema, table) pairs, that cites
/// `schema`.`table`. `None` when `fk_check` is off.
pub(crate) fn check_table_has_foreign_key_referred(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
    ignored: &[(String, String)],
    fk_check: bool,
) -> Option<ReferredFKInfo> {
    if !fk_check {
        return None;
    }
    get_table_referred_foreign_keys(catalog, schema, table)
        .into_iter()
        .filter(|referred| {
            !ignored.iter().any(|(ignored_schema, ignored_table)| {
                tidb_hack::go_to_lower(ignored_schema) == referred.child_schema.lowercase()
                    && tidb_hack::go_to_lower(ignored_table) == referred.child_table.lowercase()
            })
        })
        .find(|referred| {
            catalog
                .find_table(
                    referred.child_schema.lowercase(),
                    referred.child_table.lowercase(),
                )
                .is_some()
        })
}

/// Go `checkIndexNeededInForeignKey` (`foreign_key.go:443`): an index a key
/// of this table, or a key citing it, needs cannot be dropped unless another
/// index (or the integer primary key) still covers the key's columns.
pub(crate) fn check_index_needed_in_foreign_key(
    catalog: &ClusterCatalog,
    db_name: &str,
    tb_info: &TableInfo,
    idx_info: &IndexInfo,
) -> Refusal<()> {
    let referred_fks = get_table_referred_foreign_keys(catalog, db_name, tb_info.name.original());
    if tb_info.foreign_keys.is_empty() && referred_fks.is_empty() {
        return Ok(());
    }
    let check_fn = |cols: &[CiString]| -> Refusal<()> {
        if !is_index_prefix_covered_for_foreign_key(tb_info, idx_info, cols) {
            return Ok(());
        }
        if tb_info.pk_is_handle && cols.len() == 1 {
            if let Some(ref_col) = find_column_info(&tb_info.columns, cols[0].lowercase()) {
                if ref_col.read().get_flag() & u64::from(FieldTypeFlags::PRI_KEY) != 0 {
                    return Ok(());
                }
            }
        }
        let remains_covered = tb_info.indices.iter_deref().any(|index| {
            let index = index.read();
            index.id != idx_info.id
                && is_index_prefix_covered_for_foreign_key(tb_info, &index, cols)
        });
        if remains_covered {
            return Ok(());
        }
        Err(coded(
            &tidb_util::dbterror::ERR_DROP_INDEX_NEEDED_IN_FOREIGN_KEY,
            format!(
                "Cannot drop index '{}': needed in a foreign key constraint",
                idx_info.name
            ),
        ))
    };
    for fk in tb_info.foreign_keys.iter_deref() {
        let fk = fk.read();
        if fk.version < FK_VERSION1 {
            continue;
        }
        check_fn(&fk.cols.snapshot())?;
    }
    for referred_fk in &referred_fks {
        check_fn(&referred_fk.cols.iter().cloned().collect::<Vec<_>>())?;
    }
    Ok(())
}

/// Go `checkDropColumnWithForeignKeyConstraint` (`foreign_key.go:503`):
/// `col_name` is lowercase, as Go passes `colName.L`.
pub(crate) fn check_drop_column_with_foreign_key_constraint(
    catalog: &ClusterCatalog,
    db_name: &str,
    tb_info: &TableInfo,
    col_name: &str,
) -> Refusal<()> {
    for fk in tb_info.foreign_keys.iter_deref() {
        let fk = fk.read();
        if fk
            .cols
            .snapshot()
            .iter()
            .any(|col| col.lowercase() == col_name)
        {
            return Err(coded(
                &tidb_util::dbterror::ERR_FK_COLUMN_CANNOT_DROP,
                format!(
                    "Cannot drop column '{col_name}': needed in a foreign key constraint '{}'",
                    fk.name
                ),
            ));
        }
    }
    for referred_fk in get_table_referred_foreign_keys(catalog, db_name, tb_info.name.original()) {
        if referred_fk
            .cols
            .iter()
            .any(|col| col.lowercase() == col_name)
        {
            return Err(coded(
                &tidb_util::dbterror::ERR_FK_COLUMN_CANNOT_DROP_CHILD,
                format!(
                    "Cannot drop column '{col_name}': needed in a foreign key constraint '{}' \
                     of table '{}'",
                    referred_fk.child_fk_name, referred_fk.child_table
                ),
            ));
        }
    }
    Ok(())
}

/// Go `checkModifyColumnWithForeignKeyConstraint` (`foreign_key.go:301`): a
/// MODIFY / CHANGE that changes the column's type, length or scale must keep
/// it compatible with the column on the other side of every foreign key it
/// takes part in, as a child (this table's keys) and as a parent (the keys
/// citing this table).
pub(crate) fn check_modify_column_with_foreign_key_constraint(
    catalog: &ClusterCatalog,
    db_name: &str,
    tb_info: &TableInfo,
    original_col: &tidb_model::ColumnInfo,
    new_col: &tidb_model::ColumnInfo,
) -> Refusal<()> {
    let (new, original) = (&new_col.field_type, &original_col.field_type);
    if new.code() == original.code()
        && new.flen() == original.flen()
        && new.decimal() == original.decimal()
    {
        return Ok(());
    }
    let original_name = original_col.name.lowercase();
    for fk in tb_info.foreign_keys.iter_deref() {
        let fk = fk.read();
        let ref_cols = fk.ref_cols.snapshot();
        for (i, col) in fk.cols.snapshot().iter().enumerate() {
            if col.lowercase() != original_name {
                continue;
            }
            // Go `is.TableByName`, whose error it returns as is.
            let Some((_, refer_table)) =
                catalog.find_table(fk.ref_schema.original(), fk.ref_table.original())
            else {
                return Err(coded(
                    &tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS,
                    format!(
                        "Table '{}.{}' doesn't exist",
                        fk.ref_schema.original(),
                        fk.ref_table.original()
                    ),
                ));
            };
            let Some(refer_col) = find_column_info(&refer_table.columns, ref_cols[i].lowercase())
            else {
                return Err(coded(
                    &tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS,
                    format!(
                        "Unknown column '{}' in '{}'",
                        ref_cols[i].lowercase(),
                        refer_table.name.lowercase()
                    ),
                ));
            };
            let refer_col = refer_col.read();
            if new.code() != refer_col.field_type.code() {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_FK_INCOMPATIBLE_COLUMNS.generate_with_stack_by_args(
                        &[
                            (new_col.name.original()).to_string().into(),
                            (ref_cols[i].original()).to_string().into(),
                            (fk.name.original()).to_string().into(),
                        ],
                    ),
                )));
            }
            if !is_acceptable_foreign_key_column_change(new_col, original_col, &refer_col) {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE
                        .generate_with_stack_by_args(&[
                            (original_col.name.original()).to_string().into(),
                            (fk.name.original()).to_string().into(),
                        ]),
                )));
            }
        }
    }
    for referred_fk in get_table_referred_foreign_keys(catalog, db_name, tb_info.name.original()) {
        for (i, col) in referred_fk.cols.iter().enumerate() {
            if col.lowercase() != original_name {
                continue;
            }
            let Some((_, child_tbl_info)) = catalog.find_table(
                referred_fk.child_schema.original(),
                referred_fk.child_table.original(),
            ) else {
                continue;
            };
            // Go `model.FindFKInfoByName`.
            let Some(fk) = child_tbl_info.foreign_keys.iter_deref().find(|fk| {
                fk.read().name.lowercase() == referred_fk.child_fk_name.lowercase()
            }) else {
                continue;
            };
            let fk_cols = fk.read().cols.snapshot();
            let Some(child_col) = fk_cols
                .get(i)
                .and_then(|name| find_column_info(&child_tbl_info.columns, name.lowercase()))
            else {
                continue;
            };
            let child_col = child_col.read();
            if new.code() != child_col.field_type.code() {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_FK_INCOMPATIBLE_COLUMNS.generate_with_stack_by_args(
                        &[
                            (child_col.name.original()).to_string().into(),
                            (original_col.name.original()).to_string().into(),
                            (referred_fk.child_fk_name.original()).to_string().into(),
                        ],
                    ),
                )));
            }
            if !is_acceptable_foreign_key_column_change(new_col, original_col, &child_col) {
                return Err(refusal(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE_CHILD
                        .generate_with_stack_by_args(&[
                            (original_col.name.original()).to_string().into(),
                            (referred_fk.child_fk_name.original()).to_string().into(),
                            (format!(
                                "{}.{}",
                                referred_fk.child_schema.lowercase(),
                                referred_fk.child_table.lowercase()
                            ))
                            .to_string()
                            .into(),
                        ]),
                )));
            }
        }
    }
    Ok(())
}

/// Go `isAcceptableForeignKeyColumnChange` (`foreign_key.go`): an integer's
/// display width says nothing about its range; any other column may not
/// shrink below either side, and a decimal may not move its precision or
/// scale at all.
fn is_acceptable_foreign_key_column_change(
    new_col: &tidb_model::ColumnInfo,
    original_col: &tidb_model::ColumnInfo,
    related_col: &tidb_model::ColumnInfo,
) -> bool {
    use tidb_datatype::FieldTypeCode::{Int24, Long, LongLong, NewDecimal, Short, Tiny};
    let (new, original) = (&new_col.field_type, &original_col.field_type);
    if matches!(new.code(), Tiny | Short | Int24 | Long | LongLong) {
        return true;
    }
    if new.flen() < related_col.field_type.flen() {
        return false;
    }
    if new.flen() < original.flen() {
        return false;
    }
    if new.code() == NewDecimal
        && (new.flen() != original.flen() || new.decimal() != original.decimal())
    {
        return false;
    }
    true
}
