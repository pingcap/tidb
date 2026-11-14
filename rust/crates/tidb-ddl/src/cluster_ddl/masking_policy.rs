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

//! Masking policies as Go runs them (`pkg/ddl/masking_policy.go`): the
//! session's executor checks the statement and queues a job; the owner's
//! `onCreateMaskingPolicy`, `onAlterMaskingPolicy` and `onDropMaskingPolicy`
//! change `mysql.tidb_masking_policy` through the step's session, and the
//! table, column and schema jobs keep that table's rows in step with what
//! they change.

use tidb_ast::MaskingPolicyRestrictOps;
use tidb_datatype::Datum;
use tidb_model::{ColumnInfo, MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType};
use tidb_sqlescape::SqlArg;
use tidb_terror::MysqlError;

use super::add_index::{action_step, AddIndexSubmission};
use super::*;

/// Go `maskingPolicySelectSQL`... the columns `maskingPolicyFromSysTableRow`
/// reads, in its order.
pub const SELECT_MASKING_POLICY_SQL: &str = "SELECT policy_id, policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, masking_type, restrict_on, created_at, updated_at, created_by FROM mysql.tidb_masking_policy";

/// Go's executor halves up to `doDDLJob2`: the session already checked the
/// statement and built the policy. `None` for any other statement.
pub fn prepare_masking_policy_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let spec = match statement {
        DdlStatement::CreateMaskingPolicy {
            policy,
            replace_on_exist,
            ..
        } => create_masking_policy_with_info(policy, *replace_on_exist, job_context),
        DdlStatement::AlterMaskingPolicy {
            policy,
            policy_id,
            schema_name,
        } => alter_masking_policy_with_info(policy, *policy_id, schema_name, job_context),
        DdlStatement::DropMaskingPolicy { policy } => drop_masking_policy(policy, job_context),
        _ => return Ok(None),
    };
    submit_built_job(
        snapshot,
        Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())),
        start_ts,
        upgrading,
    )
}

/// Go `executor.createMaskingPolicyWithInfo`: the job before `doDDLJob2`.
pub fn create_masking_policy_with_info(
    policy: &MaskingPolicyInfo,
    replace_on_exist: bool,
    context: &dyn DdlSessionContext,
) -> crate::JobWrapper {
    masking_policy_job(
        ActionType::ACTION_CREATE_MASKING_POLICY,
        0,
        policy.db_name.lowercase(),
        policy,
        tidb_model::MaskingPolicyArgs {
            policy: Some(GoShared::new(policy.clone())),
            replace_on_exist,
            ..Default::default()
        },
        context,
    )
}

/// Go `executor.AlterTableMaskingPolicyState`, `ModifyMaskingPolicyExpression`,
/// and `ModifyMaskingPolicyRestrictOn`: the job before `doDDLJob2`.
pub fn alter_masking_policy_with_info(
    policy: &MaskingPolicyInfo,
    policy_id: i64,
    schema_name: &str,
    context: &dyn DdlSessionContext,
) -> crate::JobWrapper {
    masking_policy_job(
        ActionType::ACTION_ALTER_MASKING_POLICY,
        policy_id,
        schema_name,
        policy,
        tidb_model::MaskingPolicyArgs {
            policy: Some(GoShared::new(policy.clone())),
            policy_id,
            ..Default::default()
        },
        context,
    )
}

/// Go `executor.DropMaskingPolicy`: the job before `doDDLJob2`.
pub fn drop_masking_policy(
    policy: &MaskingPolicyInfo,
    context: &dyn DdlSessionContext,
) -> crate::JobWrapper {
    masking_policy_job(
        ActionType::ACTION_DROP_MASKING_POLICY,
        policy.id,
        policy.db_name.lowercase(),
        policy,
        tidb_model::MaskingPolicyArgs {
            policy_name: policy.name.clone(),
            policy_id: policy.id,
            ..Default::default()
        },
        context,
    )
}

/// Go `executor`'s shared masking-policy job fields.
fn masking_policy_job(
    action: ActionType,
    schema_id: i64,
    schema_name: &str,
    policy: &MaskingPolicyInfo,
    args: tidb_model::MaskingPolicyArgs,
    job_context: &dyn DdlSessionContext,
) -> crate::JobWrapper {
    let mut job = session_job(job_context, action);
    job.schema_id = schema_id;
    job.schema_name = schema_name.into();
    job.table_id = policy.table_id;
    job.table_name = policy.table_name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![
        tidb_model::InvolvingSchemaInfo {
            database: schema_name.into(),
            table: policy.table_name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
        tidb_model::InvolvingSchemaInfo {
            policy: policy.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        },
    ]);
    crate::JobWrapper {
        job,
        args: JobArgsValue::MaskingPolicy(Some(GoShared::new(args))),
        id_allocated: true,
    }
}

/// Go `w.sess.Execute` of a masking-policy statement. The job handlers run
/// only on a worker with a session.
fn execute(
    txn: &JobTxn<'_>,
    sql: &str,
    args: &[SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>, MysqlError> {
    txn.execute(sql, args).unwrap_or_else(|| {
        Err(MysqlError::unknown(
            "the masking policy step has no session",
        ))
    })
}

/// Go `queryMaskingPoliciesFromSysTable`.
fn query_masking_policies_from_sys_table(
    txn: &JobTxn<'_>,
    condition: &str,
    args: &[SqlArg<'_>],
) -> Result<Vec<MaskingPolicyInfo>, MysqlError> {
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMissingMaskingPolicySysTable").is_some() {
        return Err(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&["mysql".into(), "tidb_masking_policy".into()]),
        ));
    }
    let rows = execute(txn, &format!("{SELECT_MASKING_POLICY_SQL}{condition}"), args)?;
    rows.iter()
        .map(|row| masking_policy_from_sys_table_row(row).map_err(MysqlError::unknown))
        .collect()
}

/// Go `getMaskingPolicyByIDFromSysTable`.
fn get_masking_policy_by_id_from_sys_table(
    txn: &JobTxn<'_>,
    policy_id: i64,
) -> Result<Option<MaskingPolicyInfo>, MysqlError> {
    let policies = query_masking_policies_from_sys_table(
        txn,
        " WHERE policy_id = %? ORDER BY policy_id",
        &[SqlArg::Signed(policy_id)],
    )?;
    Ok(policies.into_iter().next())
}

/// Go `getMaskingPoliciesByTableIDFromSysTable`.
fn get_masking_policies_by_table_id_from_sys_table(
    txn: &JobTxn<'_>,
    table_id: i64,
) -> Result<Vec<MaskingPolicyInfo>, MysqlError> {
    query_masking_policies_from_sys_table(
        txn,
        " WHERE table_id = %? ORDER BY policy_id",
        &[SqlArg::Signed(table_id)],
    )
}

/// Go `getMaskingPoliciesByTableColumnFromSysTable`.
fn get_masking_policies_by_table_column_from_sys_table(
    txn: &JobTxn<'_>,
    table_id: i64,
    column_id: i64,
) -> Result<Vec<MaskingPolicyInfo>, MysqlError> {
    query_masking_policies_from_sys_table(
        txn,
        " WHERE table_id = %? AND column_id = %? ORDER BY policy_id",
        &[SqlArg::Signed(table_id), SqlArg::Signed(column_id)],
    )
}

/// Go `validateMaskingPolicyTarget`: the policy's table and column as the
/// latest schema has them, their names taken over.
fn validate_masking_policy_target(
    catalog: &SessionExtendedInfoSchema,
    policy: &mut MaskingPolicyInfo,
) -> Result<(), DdlPlanError> {
    let schemas = catalog.stored_schemas();
    let Some(stored) = schemas.iter().find_map(|database| {
        database
            .read()
            .deprecated_tables
            .iter_deref()
            .find(|stored| stored.read().id == policy.table_id)
    }) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                policy.db_name.original().into(),
                policy.table_name.original().into(),
            ]),
        ));
    };
    let table = stored.read();
    let Some(schema) = catalog.schema_by_id(table.db_id) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[policy.db_name.original().into()]),
        ));
    };
    let schema_name = schema.read().name.clone();
    let Some(column) = table.find_column_by_id(policy.column_id) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS.gen_with_stack_by_args(&[
                policy.column_name.original().into(),
                table.name.original().into(),
            ]),
        ));
    };
    let column = column.read();
    policy.db_name = schema_name.clone();
    policy.table_name = table.name.clone();
    policy.column_name = column.name.clone();
    check_masking_policy_table(&schema_name, &table)?;
    check_masking_policy_column(&column)?;
    Ok(())
}

/// Go `checkMaskingPolicyTable`.
///
/// # Errors
///
/// The table is a view, a sequence, temporary or a system table.
pub fn check_masking_policy_table(
    schema_name: &CiString,
    table: &TableInfo,
) -> Result<(), TerrorError> {
    if table.is_view() || table.is_sequence() {
        return Err(tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[
            schema_name.original().into(),
            table.name.original().into(),
            "BASE TABLE".into(),
        ]));
    }
    if table.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(tidb_dbterror::ERR_OPT_ON_TEMPORARY_TABLE
            .gen_with_stack_by_args(&["masking policy".into()]));
    }
    if tidb_filter::is_system_schema(schema_name.lowercase()) {
        return Err(tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL
            .gen_with_stack_by_args(&["masking policy on system table".into()]));
    }
    Ok(())
}

/// Go `checkMaskingPolicyColumn`.
///
/// # Errors
///
/// The column is generated or of a type a policy cannot mask.
pub fn check_masking_policy_column(column: &ColumnInfo) -> Result<(), TerrorError> {
    if column.is_generated() {
        return Err(tidb_dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN
            .gen_with_stack_by_args(&["masking policy on generated column".into()]));
    }
    if !is_masking_policy_supported_type(column.field_type.code()) {
        return Err(tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL
            .gen_with_stack_by_args(&["masking policy on unsupported column type".into()]));
    }
    Ok(())
}

/// Go `isMaskingPolicySupportedType`.
#[must_use]
pub fn is_masking_policy_supported_type(tp: FieldTypeCode) -> bool {
    tp.is_type_char()
        || tp.is_type_varchar()
        || tp.is_type_blob()
        || tp.is_type_numeric()
        || tp.is_type_time()
        || matches!(tp, FieldTypeCode::Duration | FieldTypeCode::Year)
}

/// Go `insertMaskingPolicyIntoSysTable`: the row, and the id it drew.
fn insert_masking_policy_into_sys_table(
    txn: &JobTxn<'_>,
    policy: &mut MaskingPolicyInfo,
) -> Result<(), MysqlError> {
    const INSERT_SQL: &str = "INSERT INTO mysql.tidb_masking_policy \
        (policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, masking_type, restrict_on, created_at, updated_at, created_by) \
        VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)";
    let status = policy.status.to_string();
    let restrict_on = masking_policy_restrict_ops_to_string(policy.restrict_ops);
    execute(
        txn,
        INSERT_SQL,
        &[
            SqlArg::from(policy.name.original()),
            SqlArg::from(policy.db_name.original()),
            SqlArg::from(policy.table_name.original()),
            SqlArg::Signed(policy.table_id),
            SqlArg::from(policy.column_name.original()),
            SqlArg::Signed(policy.column_id),
            SqlArg::from(policy.expression.as_str()),
            SqlArg::from(status.as_str()),
            SqlArg::from(policy.masking_type.as_str()),
            SqlArg::from(restrict_on.as_str()),
            SqlArg::Time(Some(policy.created_at.naive_local())),
            SqlArg::Time(Some(policy.updated_at.naive_local())),
            SqlArg::from(policy.created_by.as_str()),
        ],
    )?;
    let rows = execute(txn, "SELECT LAST_INSERT_ID()", &[])?;
    if rows.len() != 1 {
        return Err(MysqlError::unknown(format!(
            "unexpected last insert id row count: {}",
            rows.len()
        )));
    }
    policy.id = datum_int(&rows[0][0]);
    Ok(())
}

/// Go `updateMaskingPolicyInSysTable`.
fn update_masking_policy_in_sys_table(
    txn: &JobTxn<'_>,
    policy: &MaskingPolicyInfo,
) -> Result<(), MysqlError> {
    const UPDATE_SQL: &str = "UPDATE mysql.tidb_masking_policy \
        SET policy_name = %?, db_name = %?, table_name = %?, table_id = %?, column_name = %?, column_id = %?, \
        expression = %?, status = %?, masking_type = %?, restrict_on = %?, updated_at = %? \
        WHERE policy_id = %?";
    let status = policy.status.to_string();
    let restrict_on = masking_policy_restrict_ops_to_string(policy.restrict_ops);
    execute(
        txn,
        UPDATE_SQL,
        &[
            SqlArg::from(policy.name.original()),
            SqlArg::from(policy.db_name.original()),
            SqlArg::from(policy.table_name.original()),
            SqlArg::Signed(policy.table_id),
            SqlArg::from(policy.column_name.original()),
            SqlArg::Signed(policy.column_id),
            SqlArg::from(policy.expression.as_str()),
            SqlArg::from(status.as_str()),
            SqlArg::from(policy.masking_type.as_str()),
            SqlArg::from(restrict_on.as_str()),
            SqlArg::Time(Some(policy.updated_at.naive_local())),
            SqlArg::Signed(policy.id),
        ],
    )?;
    Ok(())
}

/// Go `maskingPolicyRestrictOpsToString`.
#[must_use]
pub fn masking_policy_restrict_ops_to_string(ops: MaskingPolicyRestrictOps) -> String {
    if ops.is_empty() {
        return "NONE".to_owned();
    }
    ops.names().collect::<Vec<_>>().join(",")
}

/// Go `deleteMaskingPolicyFromSysTable`.
fn delete_masking_policy_from_sys_table(txn: &JobTxn<'_>, policy_id: i64) -> Result<(), MysqlError> {
    execute(
        txn,
        "DELETE FROM mysql.tidb_masking_policy WHERE policy_id = %?",
        &[SqlArg::Signed(policy_id)],
    )?;
    Ok(())
}

/// Go `allowMissingMaskingPolicyTableDuringBootstrap`: a bootstrap or an
/// upgrade runs its jobs before `mysql.tidb_masking_policy` exists.
fn allow_missing_masking_policy_table_during_bootstrap(txn: &JobTxn<'_>, error: &MysqlError) -> bool {
    error.code == tidb_errno::errcode::ErrNoSuchTable
        && matches!(
            txn.start_mode(),
            crate::StartMode::Bootstrap | crate::StartMode::Upgrade
        )
}

/// The policies a table or column job keeps in step, or `None` where Go's
/// hook returns nil before it reads them: a bootstrap's step (it has no
/// session here, and no policy exists yet), or the table missing under
/// `allowMissingMaskingPolicyTableDuringBootstrap`.
fn policies_of_hook(
    txn: &JobTxn<'_>,
    policies: impl FnOnce(&JobTxn<'_>) -> Result<Vec<MaskingPolicyInfo>, MysqlError>,
) -> Result<Option<Vec<MaskingPolicyInfo>>, DdlPlanError> {
    if txn.session().is_none() {
        return Ok(None);
    }
    match policies(txn) {
        Ok(policies) => Ok(Some(policies)),
        Err(error) if allow_missing_masking_policy_table_during_bootstrap(txn, &error) => Ok(None),
        Err(error) => Err(DdlPlanError::from_mysql_error(error)),
    }
}

/// Go `dropMaskingPoliciesOnTable`.
pub(super) fn drop_masking_policies_on_table(
    txn: &JobTxn<'_>,
    table_id: i64,
) -> Result<(), DdlPlanError> {
    let Some(policies) = policies_of_hook(txn, |txn| {
        get_masking_policies_by_table_id_from_sys_table(txn, table_id)
    })?
    else {
        return Ok(());
    };
    for policy in policies {
        delete_masking_policy_from_sys_table(txn, policy.id)
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(())
}

/// Go `dropMaskingPoliciesByDBName`.
pub(super) fn drop_masking_policies_by_db_name(
    txn: &JobTxn<'_>,
    db_name: &str,
) -> Result<(), MysqlError> {
    match txn.execute(
        "DELETE FROM mysql.tidb_masking_policy WHERE db_name = %?",
        &[SqlArg::from(db_name)],
    ) {
        Some(Err(error)) if error.code != tidb_errno::errcode::ErrNoSuchTable => Err(error),
        _ => Ok(()),
    }
}

/// Go `updateMaskingPolicyTableIDAfterTruncate`.
pub(super) fn update_masking_policy_table_id_after_truncate(
    txn: &JobTxn<'_>,
    old_table_id: i64,
    new_table_id: i64,
) -> Result<(), DdlPlanError> {
    let Some(policies) = policies_of_hook(txn, |txn| {
        get_masking_policies_by_table_id_from_sys_table(txn, old_table_id)
    })?
    else {
        return Ok(());
    };
    const UPDATE_SQL: &str = "UPDATE mysql.tidb_masking_policy \
        SET table_id = %?, updated_at = %? \
        WHERE policy_id = %?";
    let now = chrono::Local::now().naive_local();
    for policy in policies {
        execute(
            txn,
            UPDATE_SQL,
            &[
                SqlArg::Signed(new_table_id),
                SqlArg::Time(Some(now)),
                SqlArg::Signed(policy.id),
            ],
        )
        .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(())
}

/// Go `dropMaskingPoliciesOnColumn`.
pub(super) fn drop_masking_policies_on_column(
    txn: &JobTxn<'_>,
    table_id: i64,
    column_id: i64,
) -> Result<(), DdlPlanError> {
    let Some(policies) = policies_of_hook(txn, |txn| {
        get_masking_policies_by_table_column_from_sys_table(txn, table_id, column_id)
    })?
    else {
        return Ok(());
    };
    for policy in policies {
        delete_masking_policy_from_sys_table(txn, policy.id)
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(())
}

/// Go `updateMaskingPolicyNamesAfterRename`.
pub(super) fn update_masking_policy_names_after_rename(
    txn: &JobTxn<'_>,
    table_id: i64,
    new_db_name: &CiString,
    new_table_name: &CiString,
) -> Result<(), DdlPlanError> {
    let Some(policies) = policies_of_hook(txn, |txn| {
        get_masking_policies_by_table_id_from_sys_table(txn, table_id)
    })?
    else {
        return Ok(());
    };
    // Go `updateMaskingPolicyNamesInSysTable`.
    const UPDATE_SQL: &str = "UPDATE mysql.tidb_masking_policy \
        SET db_name = %?, table_name = %?, updated_at = %? \
        WHERE policy_id = %?";
    for policy in policies {
        if policy.db_name.lowercase() == new_db_name.lowercase()
            && policy.table_name.lowercase() == new_table_name.lowercase()
        {
            continue;
        }
        execute(
            txn,
            UPDATE_SQL,
            &[
                SqlArg::from(new_db_name.original()),
                SqlArg::from(new_table_name.original()),
                SqlArg::Time(Some(chrono::Local::now().naive_local())),
                SqlArg::Signed(policy.id),
            ],
        )
        .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(())
}

/// Go `syncMaskingPolicyForModifiedColumn`: the policy on a modified column
/// follows its new id and name, and refuses a type it cannot mask.
pub(super) fn sync_masking_policy_for_modified_column(
    txn: &JobTxn<'_>,
    table: &TableInfo,
    old_col: &ColumnInfo,
    new_col: &ColumnInfo,
) -> Result<(), DdlPlanError> {
    let Some(policies) = policies_of_hook(txn, |txn| {
        get_masking_policies_by_table_id_from_sys_table(txn, table.id)
    })?
    else {
        return Ok(());
    };
    for policy in policies {
        if policy.table_id != table.id {
            continue;
        }
        if policy.column_id != old_col.id
            && policy.column_name.lowercase() != old_col.name.lowercase()
            && policy.column_name.lowercase() != new_col.name.lowercase()
        {
            continue;
        }

        // Reject type changes that are unsupported by masking policy.
        check_masking_policy_column(new_col)?;

        let mut new_policy = policy.clone();
        new_policy.table_name = table.name.clone();
        new_policy.column_id = new_col.id;
        new_policy.column_name = new_col.name.clone();
        if policy.column_name.lowercase() != new_col.name.lowercase() {
            new_policy.expression = rewrite_masking_policy_expr_column_name(
                &policy.expression,
                &policy.column_name,
                &new_col.name,
            )
            .map_err(DdlPlanError::plain_refusal)?;
        }
        new_policy.updated_at = chrono::Local::now().fixed_offset();
        update_masking_policy_in_sys_table(txn, &new_policy)
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(())
}

/// Go `renameMaskingExprVisitor`.
struct RenameMaskingExprVisitor<'a> {
    old_col: &'a CiString,
    new_col: &'a CiString,
}

impl tidb_ast::Visitor for RenameMaskingExprVisitor<'_> {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if let Some(tidb_ast::Expr::Column(path)) = node.downcast_mut::<tidb_ast::Expr>() {
            if let Some(name) = path.last_mut() {
                if name.to_lowercase() == self.old_col.lowercase() {
                    *name = self.new_col.original().to_owned();
                }
            }
        }
        false
    }

    fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
        true
    }
}

/// Go `rewriteMaskingPolicyExprColumnName`.
///
/// # Errors
///
/// The stored expression does not parse as one expression.
pub fn rewrite_masking_policy_expr_column_name(
    expr: &str,
    old_col: &CiString,
    new_col: &CiString,
) -> Result<String, String> {
    if old_col.lowercase() == new_col.lowercase() {
        return Ok(expr.to_owned());
    }
    let stmt = tidb_parser::parse(&format!("SELECT {expr}")).map_err(|error| error.message)?;
    let invalid = || "invalid masking policy expression".to_owned();
    let tidb_ast::Stmt::Query(query) = stmt else {
        return Err(invalid());
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        return Err(invalid());
    };
    let [tidb_ast::SelectField::Expr { expr, .. }] = select.fields.fields() else {
        return Err(invalid());
    };
    let mut expr = expr.clone();
    let mut visitor = RenameMaskingExprVisitor { old_col, new_col };
    if !tidb_ast::Visitable::accept(&mut expr, &mut visitor) {
        return Err("failed to rewrite masking policy expression".to_owned());
    }
    Ok(expr.restore())
}

fn datum_int(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        _ => 0,
    }
}

fn datum_string(datum: &Datum) -> String {
    match datum {
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        _ => String::new(),
    }
}

fn datum_go_time(datum: &Datum) -> Result<chrono::DateTime<chrono::FixedOffset>, String> {
    match datum {
        Datum::Time(time) => time
            .clone()
            .go_time(&chrono::Local)
            .map(|time| time.fixed_offset())
            .map_err(|error| error.to_string()),
        _ => Err("masking policy time is not a DATETIME".to_owned()),
    }
}

/// Go `maskingPolicyFromSysTableRow`: one row of
/// [`SELECT_MASKING_POLICY_SQL`].
///
/// # Errors
///
/// A status, restrict option or time the row cannot hold.
pub fn masking_policy_from_sys_table_row(row: &[Datum]) -> Result<MaskingPolicyInfo, String> {
    let status = masking_policy_status_from_string(&datum_string(&row[8]))?;
    let restrict_ops = masking_policy_restrict_ops_from_string(&datum_string(&row[10]))?;
    let created_at = datum_go_time(&row[11])?;
    let updated_at = datum_go_time(&row[12])?;
    Ok(MaskingPolicyInfo {
        id: datum_int(&row[0]),
        name: CiString::new(datum_string(&row[1])),
        db_name: CiString::new(datum_string(&row[2])),
        table_name: CiString::new(datum_string(&row[3])),
        table_id: datum_int(&row[4]),
        column_name: CiString::new(datum_string(&row[5])),
        column_id: datum_int(&row[6]),
        expression: datum_string(&row[7]),
        status,
        masking_type: masking_policy_type_from_string(&datum_string(&row[9])),
        restrict_ops,
        created_at,
        updated_at,
        created_by: datum_string(&row[13]),
        state: SchemaState::PUBLIC,
        ..MaskingPolicyInfo::default()
    })
}

/// Go `maskingPolicyStatusFromString`.
fn masking_policy_status_from_string(status: &str) -> Result<MaskingPolicyStatus, String> {
    match status.trim().to_uppercase().as_str() {
        "ENABLE" | "ENABLED" => Ok(MaskingPolicyStatus::ENABLE),
        "DISABLE" | "DISABLED" => Ok(MaskingPolicyStatus::DISABLE),
        _ => Err(format!("unknown masking policy status: {status}")),
    }
}

/// Go `maskingPolicyTypeFromString`.
fn masking_policy_type_from_string(tp: &str) -> MaskingPolicyType {
    let tp = MaskingPolicyType::new(&tp.trim().to_uppercase());
    if [
        MaskingPolicyType::FULL,
        MaskingPolicyType::PARTIAL,
        MaskingPolicyType::NULL,
        MaskingPolicyType::DATE,
        MaskingPolicyType::CUSTOM,
    ]
    .contains(&tp)
    {
        tp
    } else {
        MaskingPolicyType::CUSTOM
    }
}

/// Go `maskingPolicyRestrictOpsFromString`.
fn masking_policy_restrict_ops_from_string(
    restrict_on: &str,
) -> Result<MaskingPolicyRestrictOps, String> {
    let restrict_on = restrict_on.to_uppercase();
    let restrict_on = restrict_on.trim();
    let mut ops = MaskingPolicyRestrictOps::default();
    if restrict_on.is_empty() || restrict_on == "NONE" {
        return Ok(ops);
    }
    for token in restrict_on.split(',') {
        let token = token.trim();
        if matches!(token, "NONE" | "") {
            continue;
        }
        if !ops.insert_name(token) {
            return Err(format!("unknown masking policy restrict option: {token}"));
        }
    }
    Ok(ops)
}

/// The job's args, as each handler's `model.GetMaskingPolicyArgs` reads
/// them.
fn masking_policy_args(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<tidb_model::MaskingPolicyArgs, DdlPlanError> {
    let args = tidb_model::get_masking_policy_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("masking policy job has nil args".to_owned()),
            )
        })?;
    let args = args.read().clone();
    Ok(args)
}

/// The policy the create and alter jobs carry.
fn policy_of_args(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    args: &tidb_model::MaskingPolicyArgs,
) -> Result<MaskingPolicyInfo, DdlPlanError> {
    match &args.policy {
        Some(policy) => Ok(policy.read().clone()),
        None => Err(cancel_ddl_job(
            active,
            DdlPlanError::plain_refusal("masking policy args missing policy info"),
        )),
    }
}

/// The tail of each handler: `updateSchemaVersion` and `FinishDBJob`.
fn finish_masking_policy_job(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    finished_state: SchemaState,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let meta = txn.meta();
    let action_type = active.job.type_;
    let schema_id = active.job.schema_id;
    let table_id = active.job.table_id;
    let (schema_version, diff) = update_schema_version(
        &meta,
        active,
        SchemaDiff {
            action_type,
            schema_id,
            table_id,
            ..SchemaDiff::default()
        },
    )?;
    active
        .job
        .finish_db_job(JobState::DONE, finished_state, schema_version, None);
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(action_step(write))
}

/// Go `onCreateMaskingPolicy`.
pub(super) fn on_create_masking_policy(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = masking_policy_args(active)?;
    let mut policy_info = policy_of_args(active, &args)?;
    policy_info.state = SchemaState::NONE;

    validate_masking_policy_target(catalog, &mut policy_info)
        .map_err(|error| cancel_ddl_job(active, error))?;

    let existing = get_masking_policies_by_table_id_from_sys_table(txn, policy_info.table_id)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
    for policy in &existing {
        if policy.name.lowercase() != policy_info.name.lowercase() {
            continue;
        }
        // Go wraps `meta.ErrMaskingPolicyExists` with a message the job's
        // error (`toTError` of its cause) does not keep.
        if policy.column_id != policy_info.column_id || !args.replace_on_exist {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_meta::meta_errors::ERR_MASKING_POLICY_EXISTS.gen_with_stack_by_args(&[]),
                ),
            ));
        }
        let mut replace_policy = policy.clone();
        replace_policy.db_name = policy_info.db_name;
        replace_policy.table_name = policy_info.table_name;
        replace_policy.table_id = policy_info.table_id;
        replace_policy.column_name = policy_info.column_name;
        replace_policy.column_id = policy_info.column_id;
        replace_policy.expression = policy_info.expression;
        replace_policy.status = policy_info.status;
        replace_policy.masking_type = policy_info.masking_type;
        replace_policy.restrict_ops = policy_info.restrict_ops;
        replace_policy.updated_at = policy_info.updated_at;
        update_masking_policy_in_sys_table(txn, &replace_policy)
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
        active.job.schema_id = replace_policy.id;
        return finish_masking_policy_job(txn, active, SchemaState::PUBLIC);
    }

    match policy_info.state {
        SchemaState::NONE => {
            policy_info.state = SchemaState::PUBLIC;
            insert_masking_policy_into_sys_table(txn, &mut policy_info)
                .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
            active.job.schema_id = policy_info.id;
            finish_masking_policy_job(txn, active, SchemaState::PUBLIC)
        }
        state => Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack_by_args(&[
                FormatArg::from("masking policy"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        )),
    }
}

/// Go `onAlterMaskingPolicy`.
pub(super) fn on_alter_masking_policy(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = masking_policy_args(active)?;
    let altered = policy_of_args(active, &args)?;

    let old_policy = get_masking_policy_by_id_from_sys_table(txn, args.policy_id)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
    let Some(mut old_policy) = old_policy else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::plain_refusal(format!(
                "masking policy {} doesn't exist",
                altered.name.original()
            )),
        ));
    };
    validate_masking_policy_target(catalog, &mut old_policy)
        .map_err(|error| cancel_ddl_job(active, error))?;

    let mut new_policy = old_policy;
    new_policy.expression = altered.expression;
    new_policy.status = altered.status;
    new_policy.masking_type = altered.masking_type;
    new_policy.restrict_ops = altered.restrict_ops;
    new_policy.updated_at = altered.updated_at;
    update_masking_policy_in_sys_table(txn, &new_policy)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
    finish_masking_policy_job(txn, active, SchemaState::PUBLIC)
}

/// Go `onDropMaskingPolicy`.
pub(super) fn on_drop_masking_policy(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = masking_policy_args(active)?;

    let policy = get_masking_policy_by_id_from_sys_table(txn, args.policy_id)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
    let Some(policy) = policy else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::plain_refusal(format!(
                "masking policy {} doesn't exist",
                args.policy_name.original()
            )),
        ));
    };
    delete_masking_policy_from_sys_table(txn, policy.id)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
    finish_masking_policy_job(txn, active, SchemaState::NONE)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EmptySnapshot;

    impl tidb_infoschema::cluster_catalog::MetaSnapshot for EmptySnapshot {
        fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(
            &mut self,
            _prefix: &[u8],
        ) -> Result<tidb_infoschema::cluster_catalog::MetaPairs, tidb_infoschema::cluster_catalog::ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    /// A session pool the failpoint answers before any statement reaches.
    struct UnusedPool;

    impl tidb_ddl_build::session_pool::DdlSessionPool for UnusedPool {
        fn system_session(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
            unreachable!("the failpoint answers before a session is needed")
        }

        fn restricted_sql_executor(&self) -> std::sync::Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
            unreachable!("the failpoint answers before a statement runs")
        }
    }

    /// Go `TestMaskingPolicyMissingSysTableRequiresBootstrapOrUpgrade`: a
    /// table or column job's masking-policy hook tolerates a missing
    /// `mysql.tidb_masking_policy` only during a bootstrap or an upgrade.
    #[test]
    fn masking_policy_missing_sys_table_requires_bootstrap_or_upgrade() {
        let _missing = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockMissingMaskingPolicySysTable",
            "return(true)",
        );
        let helpers: [(&str, fn(&JobTxn<'_>) -> Result<(), DdlPlanError>); 5] = [
            ("modify_column", |txn| {
                sync_masking_policy_for_modified_column(
                    txn,
                    &TableInfo { id: 1, ..TableInfo::default() },
                    &ColumnInfo::default(),
                    &ColumnInfo::default(),
                )
                .map(drop)
            }),
            ("drop_table", |txn| drop_masking_policies_on_table(txn, 1).map(drop)),
            ("truncate_table", |txn| update_masking_policy_table_id_after_truncate(txn, 1, 2).map(drop)),
            ("rename_table", |txn| {
                update_masking_policy_names_after_rename(
                    txn,
                    1,
                    &CiString::new("new_db"),
                    &CiString::new("new_table"),
                )
                .map(drop)
            }),
            ("drop_column", |txn| drop_masking_policies_on_column(txn, 1, 1).map(drop)),
        ];
        for (name, helper) in helpers {
            for (mode, want_err) in [
                (crate::StartMode::Normal, true),
                (crate::StartMode::Bootstrap, false),
                (crate::StartMode::Upgrade, false),
            ] {
                let mut snapshot = EmptySnapshot;
                let txn = JobTxn::new(&mut snapshot, 1).with_session(Some(crate::job_txn::StepSession {
                    pool: std::sync::Arc::new(UnusedPool),
                    buffer: Default::default(),
                    sql: Default::default(),
                    start_mode: mode,
                }));
                let result = helper(&txn);
                if want_err {
                    let error = result.expect_err(name).to_string();
                    assert!(error.contains("Table 'mysql.tidb_masking_policy' doesn't exist"), "{name} {mode:?}: {error}");
                } else {
                    assert!(result.is_ok(), "{name} {mode:?}: {result:?}");
                }
            }
        }
    }
}
