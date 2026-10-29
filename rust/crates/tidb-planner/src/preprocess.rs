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

//! Go `pkg/planner/core/preprocess.go`: the preprocessor's name
//! qualification (`handleTableName` and the CTE scope, `preprocessWith`, it
//! consults) and its grammar checks over DDL statements (`checkDDLGrammar`
//! below).

use std::any::Any;

use tidb_ast::{Visitable, Visitor};
use tidb_hack::go_to_lower;

use crate::plan_base::PlanError;

/// Query-local CTE visibility shared by physical-table admission and PREPARE
/// database pinning. Go's preprocessWith publishes ordinary names after their
/// definition, recursive names before it, and restores the enclosing scope.
#[derive(Default)]
pub struct CteScope {
    visible: Vec<String>,
    offsets: Vec<usize>,
    recursive: Vec<bool>,
}

impl CteScope {
    /// Tracks the scope a visitor enters; call from `Visitor::enter`.
    pub fn enter(&mut self, node: &mut dyn Any) {
        if node.is::<tidb_ast::SelectStmt>() || node.is::<tidb_ast::SetOprStmt>() {
            self.offsets.push(self.visible.len());
        } else if let Some(with) = node.downcast_mut::<tidb_ast::WithClause>() {
            self.recursive.push(with.recursive);
        } else if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
            self.offsets.push(self.visible.len());
            if self.recursive.last().copied().unwrap_or(false) {
                self.visible.push(go_to_lower(cte.name.clone()));
            }
        }
    }

    /// Tracks the scope a visitor leaves; call from `Visitor::leave`.
    pub fn leave(&mut self, node: &mut dyn Any) {
        if node.is::<tidb_ast::SelectStmt>() || node.is::<tidb_ast::SetOprStmt>() {
            self.visible
                .truncate(self.offsets.pop().expect("query scope entered"));
        } else if node.is::<tidb_ast::WithClause>() {
            self.recursive.pop();
        } else if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
            self.visible
                .truncate(self.offsets.pop().expect("CTE scope entered"));
            self.visible.push(go_to_lower(cte.name.clone()));
        }
    }

    /// Whether `path` is an unqualified name of a CTE visible here.
    #[must_use]
    pub fn contains(&self, path: &[String]) -> bool {
        matches!(path, [name] if self.visible.contains(&go_to_lower(name.clone())))
    }
}

/// Go `preprocessor.handleTableName` (`pkg/planner/core/preprocess.go:1758`):
/// every unqualified table name that does not name a CTE takes the current
/// database. PREPARE runs it on the statement it retains, so the stored
/// `VisitInfos` and every later EXECUTE -- after any `USE` -- resolve against
/// the database current at PREPARE (`PlanCacheStmt.StmtDB`); an unqualified
/// name with no current database is Go's `ErrNoDB`.
///
/// # Errors
///
/// `ErrNoDB` when an unqualified name has no current database to take.
pub fn handle_table_name<N: Visitable>(node: &mut N, current_db: &str) -> Result<(), PlanError> {
    struct Pinner<'a> {
        current_db: &'a str,
        scope: CteScope,
        unresolved: bool,
    }
    impl Pinner<'_> {
        fn pin(&mut self, path: &mut Vec<String>) {
            if path.len() != 1 {
                return;
            }
            if self.current_db.is_empty() {
                self.unresolved = true;
                return;
            }
            path.insert(0, self.current_db.to_owned());
        }
    }
    impl Visitor for Pinner<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            self.scope.enter(node);
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                if !self.scope.contains(&table_ref.name) {
                    self.pin(&mut table_ref.name);
                }
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.pin(&mut insert.table);
            } else if let Some(import) = node.downcast_mut::<tidb_ast::ImportIntoStmt>() {
                self.pin(&mut import.table);
            } else if let Some(distribute) = node.downcast_mut::<tidb_ast::DistributeTableStmt>() {
                self.pin(&mut distribute.table);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateTableStmt>() {
                self.pin(&mut create.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateViewStmt>() {
                self.pin(&mut create.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateSequenceStmt>() {
                self.pin(&mut create.name);
            } else if let Some(drop) = node.downcast_mut::<tidb_ast::DropTableStmt>() {
                for name in &mut drop.names {
                    self.pin(name);
                }
            } else if let Some(alter) = node.downcast_mut::<tidb_ast::AlterTableStmt>() {
                self.pin(&mut alter.name);
            } else if let Some(create) = node.downcast_mut::<tidb_ast::CreateIndexStmt>() {
                self.pin(&mut create.table);
            } else if let Some(drop) = node.downcast_mut::<tidb_ast::DropIndexStmt>() {
                self.pin(&mut drop.table);
            } else if let Some(split) = node.downcast_mut::<tidb_ast::SplitRegionStmt>() {
                self.pin(&mut split.table);
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            self.scope.leave(node);
            true
        }
    }
    let mut pinner = Pinner {
        current_db,
        scope: CteScope::default(),
        unresolved: false,
    };
    node.accept(&mut pinner);
    if pinner.unresolved {
        return Err(PlanError::from(
            tidb_error::plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

/// Go `preprocessor.resolveShowStmt` (`pkg/planner/core/preprocess.go:2028`)
/// and the `handleTableName` the preprocessor then runs on `Table`: a SHOW
/// without a database takes its table's schema, else the current database;
/// one with a database qualifies an unqualified table with it;
/// `CURRENT_USER` becomes the session's authenticated account
/// (`auth_user`), when the session has one; and a table still unqualified
/// takes the current database.
///
/// # Errors
///
/// `ErrNoDB` for an unqualified table with no current database.
pub fn resolve_show_stmt(
    node: &mut tidb_ast::ShowStmt,
    current_db: &str,
    auth_user: Option<(&str, &str)>,
) -> Result<(), PlanError> {
    if node.db_name.is_empty() {
        node.db_name = match node.table.as_deref() {
            Some([schema, _]) if !schema.is_empty() => schema.clone(),
            _ => current_db.to_owned(),
        };
    } else if let Some(table) = node.table.as_mut() {
        if let [name] = table.as_slice() {
            *table = vec![node.db_name.clone(), name.clone()];
        }
    }
    if let (Some(user), Some((auth_username, auth_hostname))) = (node.user.as_mut(), auth_user) {
        if user.current_user {
            user.user = auth_username.to_owned();
            user.host = auth_hostname.to_owned();
        }
    }
    if let Some(table) = node.table.as_mut() {
        if let [name] = table.as_slice() {
            if current_db.is_empty() {
                return Err(PlanError::from(
                    tidb_error::plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
                ));
            }
            *table = vec![current_db.to_owned(), name.clone()];
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// DDL grammar checks.
//
// Go `planner/core/preprocess.go`'s grammar checks over DDL statements.
//
// Go's preprocessor walks every statement before it is planned, privilege
// checked or run, and refuses the DDL whose names or declarations are wrong
// on their face: an empty or space-terminated identifier, a second primary
// key, an index named `PRIMARY`, a zero key length, a column type out of
// range. Its order is the walk's: the statement's own checks on entry, the
// constraint nodes beneath it, then the checks Go runs on leaving a CREATE
// TABLE (`checkAutoIncrement`, `checkContainDotColumn`). The first error
// stops the walk. The checks that need the infoschema (a LIKE source, a
// DROP TEMPORARY target) stay with the session, which owns the catalog.

use tidb_ast::{
    AlterPartitionAction, AlterTableAction, ColumnDef, ColumnOption, ColumnTypeArg,
    CreateTableStmt, CreateTableTemporary, DdlStmt, IndexConstraintKind, IndexPart, Stmt,
    TableConstraint, TableOption,
};
use tidb_datatype::FieldTypeCode;

use tidb_error::mysql::MysqlError;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// `mysql.MaxKeyParts`.
const MAX_KEY_PARTS: usize = 16;
/// `mysql.MaxFieldCharLength`.
const MAX_FIELD_CHAR_LENGTH: i64 = 255;
/// `mysql.MaxFieldVarCharLength`.
const MAX_FIELD_VARCHAR_LENGTH: i64 = 65535;
/// `mysql.MaxDoublePrecisionLength`.
const MAX_DOUBLE_PRECISION_LENGTH: i64 = 53;
/// `mysql.MaxFloatingTypeScale`.
const MAX_FLOATING_TYPE_SCALE: i64 = 30;
/// `mysql.MaxFloatingTypeWidth`.
const MAX_FLOATING_TYPE_WIDTH: i64 = 255;
/// `mysql.MaxDecimalScale`.
const MAX_DECIMAL_SCALE: i64 = 30;
/// `mysql.MaxDecimalWidth`.
const MAX_DECIMAL_WIDTH: i64 = 65;
/// `mysql.MaxBitDisplayWidth`.
const MAX_BIT_DISPLAY_WIDTH: i64 = 64;
/// `mysql.MaxTypeSetMembers`.
const MAX_TYPE_SET_MEMBERS: usize = 64;
/// `auth.UserNameMaxLength`.
const USER_NAME_MAX_LENGTH: usize = 32;
/// `auth.HostNameMaxLength`.
const HOST_NAME_MAX_LENGTH: usize = 255;

/// Go's preprocessor grammar checks over one DDL statement. `strict` is the
/// session's strict SQL mode, which decides whether an over-long VARCHAR is
/// an error or is left to be converted to TEXT/BLOB (`hasAutoConvertWarning`).
pub fn check_ddl_grammar(stmt: &Stmt, strict: bool) -> Result<(), MysqlError> {
    let Stmt::Ddl(ddl) = stmt else {
        return Ok(());
    };
    match ddl.as_ref() {
        DdlStmt::CreateDatabase { name, .. } | DdlStmt::DropDatabase { name, .. } => {
            check_database_name(name)
        }
        // `ALTER DATABASE` with no name alters the current database.
        DdlStmt::AlterDatabase { name, .. } => name.as_deref().map_or(Ok(()), check_database_name),
        DdlStmt::CreateTable(create) => check_create_table(create, strict),
        DdlStmt::CreateView(create) => {
            check_table_name(path_name(&create.name))?;
            for column in &create.columns {
                if is_incorrect_identifier_name(column) {
                    return Err(MysqlError::from_terror(
                        &tidb_dbterror::ERR_WRONG_COLUMN_NAME
                            .generate_with_stack_by_args(&[(column.clone()).to_string().into()]),
                    ));
                }
            }
            if !create.definer.current_user {
                check_definer_length(&create.definer.user, "user name", USER_NAME_MAX_LENGTH)?;
                check_definer_length(&create.definer.host, "host name", HOST_NAME_MAX_LENGTH)?;
            }
            Ok(())
        }
        DdlStmt::DropTable(drop) => {
            for path in &drop.names {
                check_table_name(path_name(path))?;
            }
            Ok(())
        }
        DdlStmt::DropSequence(drop) => {
            for path in &drop.names {
                check_table_name(path_name(path))?;
            }
            Ok(())
        }
        // Go checks the first pair only (`checkRenameTableGrammar`).
        DdlStmt::RenameTable(rename) => match rename.pairs.first() {
            Some((from, to)) => {
                check_table_name(path_name(from))?;
                check_table_name(path_name(to))
            }
            None => Ok(()),
        },
        DdlStmt::CreateIndex(create) => {
            check_table_name(path_name(&create.table))?;
            if create.name.is_empty() {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_WRONG_NAME_FOR_INDEX
                        .generate_with_stack_by_args(&[(&create.name).into()]),
                ));
            }
            check_index_info(&create.name, &create.parts)
        }
        DdlStmt::AlterTable(alter) => check_alter_table(alter),
        _ => Ok(()),
    }
}

/// Go `preprocessor.checkRepairTableGrammar` (`preprocess.go:1407`), which
/// the walk runs on entering an `ADMIN REPAIR TABLE`, before its CREATE
/// TABLE and before the planner's SUPER check: refused outside REPAIR MODE
/// and with an empty repair list (`domainutil.RepairInfo`, read by the
/// caller), then both names checked as a RENAME checks them.
pub fn check_repair_table_grammar(
    stmt: &tidb_ast::RepairTableStmt,
    in_repair_mode: bool,
    repair_table_list_empty: bool,
) -> Result<(), MysqlError> {
    let repair_table_fail = |reason: &str| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_REPAIR_TABLE_FAIL
                .generate_with_stack_by_args(&[reason.into()]),
        )
    };
    if !in_repair_mode {
        return Err(repair_table_fail("TiDB is not in REPAIR MODE"));
    }
    if repair_table_list_empty {
        return Err(repair_table_fail("repair list is empty"));
    }
    check_table_name(path_name(&stmt.table))?;
    check_table_name(path_name(&stmt.create.name))
}

/// Go `util.IsInCorrectIdentifierName`: an empty name, or one ending in a
/// space.
fn is_incorrect_identifier_name(name: &str) -> bool {
    name.is_empty() || name.ends_with(' ')
}

/// The last segment of a written object path: the object's own name.
fn path_name(path: &[String]) -> &str {
    path.last().map_or("", String::as_str)
}

fn check_database_name(name: &str) -> Result<(), MysqlError> {
    if is_incorrect_identifier_name(name) {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_DB_NAME.generate_with_stack_by_args(&[name.into()]),
        ));
    }
    Ok(())
}

fn check_table_name(name: &str) -> Result<(), MysqlError> {
    if is_incorrect_identifier_name(name) {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TABLE_NAME
                .generate_with_stack_by_args(&[name.to_owned().into()]),
        ));
    }
    Ok(())
}

/// `dbterror.ErrWrongStringLength` (1470): `String '%-.70s' is too long for
/// %s (should be no longer than %d)`. The `%-.70s` keeps the first 70 runes of
/// the offending value, so a 255-character host prints as its first 70.
#[must_use]
pub fn wrong_string_length_error(value: &str, kind: &str, maximum: usize) -> MysqlError {
    let shown: String = value.chars().take(70).collect();
    MysqlError::from_terror(
        &tidb_dbterror::ERR_WRONG_STRING_LENGTH.generate_with_stack_by_args(&[
            shown.into(),
            kind.into(),
            maximum.into(),
        ]),
    )
}

/// `dbterror.ErrWrongStringLength` (1470) over a view's DEFINER.
fn check_definer_length(value: &str, kind: &str, maximum: usize) -> Result<(), MysqlError> {
    if value.len() > maximum {
        return Err(wrong_string_length_error(value, kind, maximum));
    }
    Ok(())
}

/// Go `checkCreateTableGrammar`, then the CREATE TABLE's constraint nodes,
/// then `checkAutoIncrement` and `checkContainDotColumn` on leaving it.
fn check_create_table(create: &CreateTableStmt, strict: bool) -> Result<(), MysqlError> {
    let temporary = create.temporary != CreateTableTemporary::None;
    if temporary {
        for option in &create.table_options {
            match option {
                TableOption::ShardRowIdBits(_) => {
                    return Err(MysqlError::from_terror(
                        &tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                            .generate_with_stack_by_args(&["shard_row_id_bits".into()]),
                    ))
                }
                TableOption::PlacementPolicy(_) => {
                    return Err(MysqlError::from_terror(
                        &tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                            .generate_with_stack_by_args(&["PLACEMENT".into()]),
                    ))
                }
                _ => {}
            }
        }
    }
    check_table_name(path_name(&create.name))?;
    let mut primary_keys = 0;
    for column in &create.columns {
        if let Err(error) = check_column(column) {
            // Go issue #30328: outside strict mode an over-long VARCHAR is
            // converted to TEXT/BLOB with a warning instead
            // (`hasAutoConvertWarning`).
            let converts = !strict
                && matches!(error, ColumnCheckError::TooBigFieldLength)
                && is_varchar(column);
            if !converts {
                return Err(error.into_driver_error(column));
            }
        }
        primary_keys += check_column_options(temporary, &column.options)?;
        if primary_keys > 1 {
            return Err(MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_MULTIPLE_PRI_KEY
                    .generate_with_stack_by_args(&[]),
            ));
        }
    }
    for constraint in &create.table_constraints {
        match constraint {
            TableConstraint::Index(index) => match index.kind {
                IndexConstraintKind::PrimaryKey => {
                    if primary_keys > 0 {
                        return Err(MysqlError::from_terror(
                            &tidb_error::catalog::infoschema::ERR_MULTIPLE_PRI_KEY
                                .generate_with_stack_by_args(&[]),
                        ));
                    }
                    primary_keys += 1;
                    check_index_info(index.name.as_deref().unwrap_or(""), &index.parts)?;
                }
                IndexConstraintKind::Key
                | IndexConstraintKind::Index
                | IndexConstraintKind::Unique
                | IndexConstraintKind::UniqueKey
                | IndexConstraintKind::UniqueIndex => {
                    let name = index.name.as_deref().unwrap_or("");
                    check_index_info(name, &index.parts)?;
                    if index.is_empty_index {
                        return Err(MysqlError::from_terror(
                            &tidb_dbterror::ERR_WRONG_NAME_FOR_INDEX
                                .generate_with_stack_by_args(&[name.into()]),
                        ));
                    }
                }
                _ => {}
            },
            TableConstraint::ForeignKey(foreign_key) => {
                let name = foreign_key.name.as_deref().unwrap_or("");
                check_index_info(name, &foreign_key.parts)?;
            }
            _ => {}
        }
    }
    check_unsupported_table_options(&create.table_options)?;
    if create.ctas.is_some() {
        return Err(MysqlError::unknown(
            "'CREATE TABLE ... SELECT' is not implemented yet",
        ));
    }
    if create.columns.is_empty() && create.like_table.is_none() {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_TABLE_MUST_HAVE_COLUMNS.generate_with_stack_by_args(&[]),
        ));
    }
    if let Some(partitioning) = &create.partitioning {
        if partitioning
            .definitions
            .iter()
            .any(|definition| is_incorrect_identifier_name(&definition.name))
        {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_PARTITION_NAME.generate_with_stack_by_args(&[]),
            ));
        }
    }
    check_auto_increment(create)?;
    check_contain_dot_column(create)
}

/// Go `checkUnsupportedTableOptions`: reject the two unsupported compatibility
/// clauses (`UNION`, `INSERT_METHOD`) and storage engines outside TiDB's
/// MySQL/MariaDB allowlist (`checkTableEngine`).
pub fn check_unsupported_table_options(options: &[TableOption]) -> Result<(), MysqlError> {
    /// Go `mysqlValidTableEngineNames`.
    const VALID_ENGINES: &[&str] = &[
        "archive",
        "blackhole",
        "csv",
        "example",
        "federated",
        "innodb",
        "memory",
        "merge",
        "mgr_myisam",
        "myisam",
        "ndb",
        "heap",
        "aria",
        "myrocks",
        "tokudb",
    ];

    for option in options {
        match option {
            TableOption::Union(_) => {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_TABLE_OPTION_UNION_UNSUPPORTED,
                ))
            }
            TableOption::InsertMethod(_) => {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_TABLE_OPTION_INSERT_METHOD_UNSUPPORTED,
                ))
            }
            TableOption::Engine(engine)
                if !VALID_ENGINES
                    .iter()
                    .any(|valid| engine.eq_ignore_ascii_case(valid)) =>
            {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNKNOWN_ENGINE
                        .generate_with_stack_by_args(&[(engine.clone()).to_string().into()]),
                ))
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `checkAlterTableGrammar`.
fn check_alter_table(alter: &tidb_ast::AlterTableStmt) -> Result<(), MysqlError> {
    check_table_name(path_name(&alter.name))?;
    for action in &alter.actions {
        if let AlterTableAction::RenameTable { new_name } = action {
            check_table_name(path_name(new_name))?;
        }
        let new_columns: &[ColumnDef] = match action {
            AlterTableAction::AddColumn { column, .. }
            | AlterTableAction::ModifyColumn { column, .. }
            | AlterTableAction::ChangeColumn { column, .. } => std::slice::from_ref(column),
            AlterTableAction::AddColumns { columns, .. } => columns,
            _ => &[],
        };
        for column in new_columns {
            check_column(column).map_err(|error| error.into_driver_error(column))?;
        }
        if let AlterTableAction::SetTableOptions { options } = action {
            check_unsupported_table_options(options)?;
        }
        match action {
            AlterTableAction::AddIndexConstraint(index)
                if matches!(
                    index.kind,
                    IndexConstraintKind::Key
                        | IndexConstraintKind::Index
                        | IndexConstraintKind::Unique
                        | IndexConstraintKind::UniqueIndex
                        | IndexConstraintKind::UniqueKey
                        | IndexConstraintKind::PrimaryKey
                ) =>
            {
                check_index_info(index.name.as_deref().unwrap_or(""), &index.parts)?;
            }
            AlterTableAction::AddStatistics { name, .. }
            | AlterTableAction::DropStatistics { name, .. }
                if is_incorrect_identifier_name(name) =>
            {
                return Err(MysqlError::unknown(format!(
                    "Incorrect statistics name: {name}"
                )));
            }
            AlterTableAction::Partition(AlterPartitionAction::Add {
                spec: tidb_ast::AddPartitionSpec::Definitions(definitions),
                ..
            }) if definitions
                .iter()
                .any(|definition| is_incorrect_identifier_name(&definition.name)) =>
            {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_WRONG_PARTITION_NAME.generate_with_stack_by_args(&[]),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `checkIndexInfo`: the index name, the number of key parts, zero
/// prefix lengths and repeated columns.
fn check_index_info(name: &str, parts: &[IndexPart]) -> Result<(), MysqlError> {
    if name.go_equal_fold("PRIMARY") {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_NAME_FOR_INDEX
                .generate_with_stack_by_args(&[name.into()]),
        ));
    }
    if parts.len() > MAX_KEY_PARTS {
        return Err(MysqlError::from_terror(
            &tidb_error::catalog::infoschema::ERR_TOO_MANY_KEY_PARTS
                .generate_with_stack_by_args(&[MAX_KEY_PARTS.into()]),
        ));
    }
    for part in parts {
        if let IndexPart::Column {
            name,
            prefix_len: Some(0),
            ..
        } = part
        {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_KEY_PART0
                    .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ));
        }
    }
    // Go `checkDuplicateColumnName`.
    let mut seen: Vec<String> = Vec::with_capacity(parts.len());
    for part in parts {
        if let IndexPart::Column { name, .. } = part {
            let folded = name.go_to_lower();
            if seen.contains(&folded) {
                return Err(MysqlError::from_terror(
                    &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                        .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
                ));
            }
            seen.push(folded);
        }
    }
    Ok(())
}

/// Go `checkColumnOptions`: whether the column declares an inline primary
/// key, refusing a virtual generated one and AUTO_RANDOM on a temporary
/// table.
fn check_column_options(temporary: bool, options: &[ColumnOption]) -> Result<usize, MysqlError> {
    let mut primary = 0;
    let mut generated = false;
    let mut stored = false;
    for option in options {
        match option {
            ColumnOption::InlineKey(key)
                if matches!(key.kind, tidb_ast::InlineKeyKind::Primary { .. }) =>
            {
                primary = 1;
            }
            ColumnOption::Generated {
                stored: is_stored, ..
            } => {
                generated = true;
                stored = *is_stored;
            }
            ColumnOption::AutoRandom(_) if temporary => {
                return Err(MysqlError::from_terror(
                    &tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                        .generate_with_stack_by_args(&["auto_random".into()]),
                ));
            }
            _ => {}
        }
    }
    if primary > 0 && generated && !stored {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN.generate_with_stack_by_args(
                &[
                    ("Defining a virtual generated column as primary key".to_owned())
                        .to_string()
                        .into(),
                ],
            ),
        ));
    }
    Ok(primary)
}

/// Why Go's `checkColumn` refuses a column definition.
enum ColumnCheckError {
    WrongColumnName,
    InvalidDefault,
    DisplayWidthOutOfRange,
    TooBigFieldLength,
    WrongFieldSpec,
    TooBigScale { scale: i64, maximum: i64 },
    TooBigDisplayWidth { maximum: i64 },
    MBiggerThanD,
    TooBigSet,
    IllegalSetValue(String),
    TooBigPrecision { precision: i64 },
    InvalidFieldSize,
}

impl ColumnCheckError {
    fn into_driver_error(self, column: &ColumnDef) -> MysqlError {
        let name = &column.name;
        let coded = |proto: &'static tidb_error::terror::TerrorError, message: String| {
            MysqlError::from_terror(&proto.generate_with_stack(message))
        };
        match self {
            Self::WrongColumnName => MysqlError::from_terror(
        &tidb_dbterror::ERR_WRONG_COLUMN_NAME
            .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
    ),
            Self::InvalidDefault => coded(
                &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE,
                format!("Invalid default value for '{name}'"),
            ),
            Self::DisplayWidthOutOfRange => coded(
                &tidb_error::catalog::types::ERR_TOO_BIG_DISPLAY_WIDTH,
                format!("Display width out of range for column '{name}' (max = {})", u32::MAX),
            ),
            Self::TooBigFieldLength => {
                let maximum = if is_varchar(column) {
                    MAX_FIELD_VARCHAR_LENGTH
                } else {
                    MAX_FIELD_CHAR_LENGTH
                };
                coded(
                    &tidb_error::catalog::types::ERR_TOO_BIG_FIELD_LENGTH,
                    format!(
                        "Column length too big for column '{name}' (max = {maximum}); use BLOB or TEXT instead"
                    ),
                )
            }
            Self::WrongFieldSpec => coded(
                &tidb_error::catalog::types::ERR_WRONG_FIELD_SPEC,
                format!("Incorrect column specifier for column '{name}'"),
            ),
            Self::TooBigScale { scale, maximum } => coded(
                &tidb_error::catalog::types::ERR_TOO_BIG_SCALE,
                format!("Too big scale {scale} specified for column '{name}'. Maximum is {maximum}."),
            ),
            Self::TooBigDisplayWidth { maximum } => coded(
                &tidb_error::catalog::types::ERR_TOO_BIG_DISPLAY_WIDTH,
                format!("Display width out of range for column '{name}' (max = {maximum})"),
            ),
            Self::MBiggerThanD => MysqlError::from_terror(
        &tidb_error::catalog::types::ERR_M_BIGGER_THAN_D
            .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
    ),
            Self::TooBigSet => coded(
                &tidb_error::catalog::types::ERR_TOO_BIG_SET,
                format!("Too many strings for column {name} and SET"),
            ),
            Self::IllegalSetValue(value) => coded(
                &tidb_error::catalog::types::ERR_ILLEGAL_VALUE_FOR_TYPE,
                format!("Illegal set '{value}' value found during parsing"),
            ),
            Self::TooBigPrecision { precision } => coded(
                &tidb_error::catalog::types::ERR_TOO_BIG_PRECISION,
                format!(
                    "Too-big precision {precision} specified for '{name}'. Maximum is {MAX_DECIMAL_WIDTH}."
                ),
            ),
            Self::InvalidFieldSize => coded(
                &tidb_error::catalog::types::ERR_INVALID_FIELD_SIZE,
                format!("Invalid size for column '{name}'."),
            ),
        }
    }
}

/// Whether the declared type is VARCHAR or VARBINARY (Go `TypeVarchar`).
fn is_varchar(column: &ColumnDef) -> bool {
    column.ty.type_code()
        .is_some_and(|code| code == FieldTypeCode::Varchar)
}

/// One numeric type argument as Go's parser stores it.
fn type_arg(argument: &ColumnTypeArg) -> Option<i64> {
    match argument {
        ColumnTypeArg::Text(text) => text.parse().ok(),
        ColumnTypeArg::Bytes(_) => None,
    }
}

/// Go `checkColumn`: the column's name, a NOW() default on a type that is
/// not a timestamp, and the declared type's range limits. A type this
/// preprocessor cannot read is left to the DDL builder, which reports it.
fn check_column(column: &ColumnDef) -> Result<(), ColumnCheckError> {
    if is_incorrect_identifier_name(&column.name) {
        return Err(ColumnCheckError::WrongColumnName);
    }
    let Some(code) = column.ty.type_code() else {
        return Ok(());
    };
    if is_invalid_default_value(column, code) {
        return Err(ColumnCheckError::InvalidDefault);
    }
    let args = &column.ty.args;
    let numeric = |index: usize| args.get(index).and_then(type_arg);
    let flen = if matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        None
    } else {
        numeric(0)
    };
    if flen.is_some_and(|flen| flen > i64::from(u32::MAX)) {
        return Err(ColumnCheckError::DisplayWidthOutOfRange);
    }
    match code {
        FieldTypeCode::String => {
            if flen.is_some_and(|flen| flen > MAX_FIELD_CHAR_LENGTH) {
                return Err(ColumnCheckError::TooBigFieldLength);
            }
        }
        // Go checks a VARCHAR here only when its charset is already known
        // (`tp.GetCharset()` is set by the parser for VARBINARY and an
        // explicit CHARACTER SET); a binary charset admits 65535 bytes. A
        // multi-byte charset's limit is left to the DDL builder.
        FieldTypeCode::Varchar => {
            let binary = tidb_ast::is_intrinsically_binary(&column.ty.name)
                || column
                    .ty
                    .charset
                    .as_deref()
                    .is_some_and(|charset| charset.eq_ignore_ascii_case("binary"));
            if binary && flen.is_some_and(|flen| flen > MAX_FIELD_VARCHAR_LENGTH) {
                return Err(ColumnCheckError::TooBigFieldLength);
            }
        }
        FieldTypeCode::Float | FieldTypeCode::Double => match (flen, numeric(1)) {
            (Some(flen), None) => {
                if code == FieldTypeCode::Float && flen > MAX_DOUBLE_PRECISION_LENGTH {
                    return Err(ColumnCheckError::WrongFieldSpec);
                }
            }
            (Some(flen), Some(decimal)) => {
                if decimal > MAX_FLOATING_TYPE_SCALE {
                    return Err(ColumnCheckError::TooBigScale {
                        scale: decimal,
                        maximum: MAX_FLOATING_TYPE_SCALE,
                    });
                }
                if flen > MAX_FLOATING_TYPE_WIDTH || flen == 0 {
                    return Err(ColumnCheckError::TooBigDisplayWidth {
                        maximum: MAX_FLOATING_TYPE_WIDTH,
                    });
                }
                if flen < decimal {
                    return Err(ColumnCheckError::MBiggerThanD);
                }
            }
            _ => {}
        },
        FieldTypeCode::Set => {
            if args.len() > MAX_TYPE_SET_MEMBERS {
                return Err(ColumnCheckError::TooBigSet);
            }
            for argument in args {
                if let ColumnTypeArg::Text(member) = argument {
                    if member.contains(',') {
                        return Err(ColumnCheckError::IllegalSetValue(member.clone()));
                    }
                }
            }
        }
        FieldTypeCode::NewDecimal => {
            let decimal = numeric(1);
            if let Some(decimal) = decimal.filter(|decimal| *decimal > MAX_DECIMAL_SCALE) {
                return Err(ColumnCheckError::TooBigScale {
                    scale: decimal,
                    maximum: MAX_DECIMAL_SCALE,
                });
            }
            if let Some(flen) = flen.filter(|flen| *flen > MAX_DECIMAL_WIDTH) {
                return Err(ColumnCheckError::TooBigPrecision { precision: flen });
            }
            if let (Some(flen), Some(decimal)) = (flen, decimal) {
                if flen < decimal {
                    return Err(ColumnCheckError::MBiggerThanD);
                }
            }
        }
        FieldTypeCode::Bit => {
            if let Some(flen) = flen {
                if flen <= 0 {
                    return Err(ColumnCheckError::InvalidFieldSize);
                }
                if flen > MAX_BIT_DISPLAY_WIDTH {
                    return Err(ColumnCheckError::TooBigDisplayWidth {
                        maximum: MAX_BIT_DISPLAY_WIDTH,
                    });
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Go `isInvalidDefaultValue`: the LAST default is `NOW()` (which the parser
/// spells `CURRENT_TIMESTAMP`) on a type that is neither TIMESTAMP nor
/// DATETIME.
fn is_invalid_default_value(column: &ColumnDef, code: FieldTypeCode) -> bool {
    let Some(default) = column.options.iter().rev().find_map(|option| match option {
        ColumnOption::Default(expr) => Some(expr),
        _ => None,
    }) else {
        return false;
    };
    !matches!(code, FieldTypeCode::Timestamp | FieldTypeCode::Datetime)
        && matches!(default, tidb_ast::Expr::Func { name, .. }
            if name.eq_ignore_ascii_case("current_timestamp"))
}

/// Whether `expr` is what Go's parser builds as a `*driver.ValueExpr`
/// (`Some(true)` for the NULL literal). A signed number is a unary
/// expression there, not a value.
fn value_expr_is_null(expr: &tidb_ast::Expr) -> Option<bool> {
    use tidb_ast::Expr;
    match expr {
        Expr::Null => Some(true),
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::CharsetBinary { .. }
        | Expr::Bool(_) => Some(false),
        _ => None,
    }
}

/// Go `checkAutoIncrementOp` over the option at `index`: whether it is
/// AUTO_INCREMENT, refusing a default the column cannot take.
fn check_auto_increment_op(column: &ColumnDef, index: usize) -> Result<bool, MysqlError> {
    let options = &column.options;
    let invalid_default = || {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                .generate_with_stack_by_args(&[column.name.clone().into()]),
        )
    };
    let mut has_auto_increment = false;
    if matches!(options[index], ColumnOption::AutoIncrement) {
        has_auto_increment = true;
        for later in &options[index + 1..] {
            let ColumnOption::Default(expr) = later else {
                continue;
            };
            if value_expr_is_null(expr) == Some(false) {
                return Err(invalid_default());
            }
            if matches!(expr, tidb_ast::Expr::Func { name, .. } if name.eq_ignore_ascii_case("current_date"))
            {
                return Err(invalid_default());
            }
        }
    }
    if let ColumnOption::Default(expr) = &options[index] {
        if index + 1 != options.len() {
            if value_expr_is_null(expr) == Some(true) {
                return Ok(has_auto_increment);
            }
            if options[index + 1..]
                .iter()
                .any(|later| matches!(later, ColumnOption::AutoIncrement))
            {
                // A plain `errors.Errorf`, so 1105 with the same text.
                return Err(MysqlError::unknown(format!(
                    "Invalid default value for '{}'",
                    column.name
                )));
            }
        }
    }
    Ok(has_auto_increment)
}

/// Go `checkAutoIncrement`: one AUTO_INCREMENT column at most, of an
/// integer or floating type.
fn check_auto_increment(create: &CreateTableStmt) -> Result<(), MysqlError> {
    let mut auto_columns = Vec::new();
    for column in &create.columns {
        let mut has_auto_increment = false;
        for index in 0..column.options.len() {
            if check_auto_increment_op(column, index)? {
                has_auto_increment = true;
            }
        }
        if has_auto_increment {
            auto_columns.push(column);
        }
    }
    if auto_columns.len() > 1 {
        return Err(MysqlError::from_terror(
            &tidb_error::catalog::autoid::ERR_WRONG_AUTO_KEY.generate_with_stack_by_args(&[]),
        ));
    }
    for column in auto_columns {
        let integer_or_float =
            column.ty.type_code().is_some_and(|code| {
                matches!(
                    code,
                    FieldTypeCode::Tiny
                        | FieldTypeCode::Short
                        | FieldTypeCode::Long
                        | FieldTypeCode::Float
                        | FieldTypeCode::Double
                        | FieldTypeCode::LongLong
                        | FieldTypeCode::Int24
                )
            });
        if !integer_or_float {
            return Err(MysqlError::unknown(format!(
                "Incorrect column specifier for column '{}'",
                column.name
            )));
        }
    }
    Ok(())
}

/// Go `checkContainDotColumn`: a column written `db.t.c` must name the
/// table being created.
fn check_contain_dot_column(create: &CreateTableStmt) -> Result<(), MysqlError> {
    let table = path_name(&create.name);
    let schema = match create.name.as_slice() {
        [schema, _] => schema.as_str(),
        _ => "",
    };
    for column in &create.columns {
        let (column_schema, column_table) = match column.qualifier.as_slice() {
            [table] => ("", table.as_str()),
            [schema, table] => (schema.as_str(), table.as_str()),
            _ => ("", ""),
        };
        if !column_schema.is_empty() && column_schema != schema {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_DB_NAME
                    .generate_with_stack_by_args(&[column_schema.into()]),
            ));
        }
        if !column_table.is_empty() && column_table != table {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_TABLE_NAME
                    .generate_with_stack_by_args(&[column_table.to_owned().into()]),
            ));
        }
    }
    Ok(())
}

// Go's error constructors this file raises, rendered as the driver rendered
// them (`tidb-dbterror`'s `to_mysql_error` arms, verbatim).

#[cfg(test)]
mod ddl_grammar_tests {
    use super::*;

    #[test]
    fn table_engine_validation_matches_the_go_allowlist_and_error() {
        for engine in [
            "ARCHIVE",
            "blackhole",
            "csv",
            "example",
            "federated",
            "InnoDB",
            "memory",
            "merge",
            "mgr_myisam",
            "myisam",
            "ndb",
            "heap",
            "aria",
            "myrocks",
            "tokudb",
        ] {
            check_unsupported_table_options(&[TableOption::Engine(engine.to_owned())])
                .unwrap_or_else(|error| panic!("Go admits ENGINE={engine}: {error}"));
        }

        let stmt = tidb_parser::parse("CREATE TABLE invalid_engine (id BIGINT) ENGINE=imaginary")
            .expect("the statement parses");
        let error = check_ddl_grammar(&stmt, true)
            .expect_err("Go rejects an engine outside its compatibility allowlist");
        assert_eq!(error.code, 1286);
        assert_eq!(error.state, *b"42000");
        assert_eq!(error.message, "Unknown storage engine 'imaginary'");
    }
}
