//! Go `pkg/planner/core/planbuilder.go`: `PlanBuilder.buildImportInto`.

use super::{
    PlanBuilder,
    insert_values::InsertTarget,
    visit_info::{VisitAuthError, VisitInfo},
};
use crate::common_plans::{ImportInto, LoadDataOpt, Plan};
use std::collections::BTreeMap;
use tidb_ast::{ImportIntoStmt, ImportSource, TableRef};
use tidb_exprctx::Columns;
use tidb_infoschema::table_source::TableSource;
use tidb_planner_core_base::PlanError;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `PlanBuilder.buildImportInto`. The bridge optimizes `SelectPlan`
    /// after this build, with the same `optFlag`, as for other query plans.
    pub fn build_import_into(&mut self, stmt: &ImportIntoStmt) -> Result<Plan, PlanError> {
        let (path, format, local) = match &stmt.source {
            ImportSource::File { path, format } => {
                let (path, local) = check_import_source_path(path)?;
                (path, format.clone(), local)
            }
            ImportSource::Select { .. } => (String::new(), None, false),
        };
        let mock = self.build_table_dual();
        let mut options = Vec::with_capacity(stmt.options.len());
        for option in &stmt.options {
            options.push(LoadDataOpt {
                name: option.name.clone(),
                value: option
                    .value
                    .as_ref()
                    .map(|value| self.rewrite_scalar_with_plan(value, &mock, &BTreeMap::new()))
                    .transpose()?,
            });
        }
        crate::import_into::check_import_into_input_vars(stmt)?;
        let (database, name) = match stmt.table.as_slice() {
            [name] if !self.source.current_database().is_empty() => {
                (self.source.current_database(), name.as_str())
            }
            [database, name] => (database.as_str(), name.as_str()),
            _ => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                ));
            }
        };
        let table = self
            .source
            .try_find_table(database, name)?
            .ok_or_else(|| {
                PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .fast_gen_by_args(&[database.into(), name.into()]),
                )
            })?
            .clone();
        // Go master `buildImportInto`: before the temporary-table refusal.
        if let Some(info) = &table.table_info {
            let info = info.read();
            crate::util::check_mview_updatable(&crate::util::MViewSessionVars::of(self.ctx), &info, info.name.original(), "IMPORT")?;
        }
        if table.is_temporary() {
            return Err(PlanError::internal(
                "IMPORT INTO does not support temporary table",
            ));
        }
        if table.is_cached() {
            return Err(PlanError::internal(
                "IMPORT INTO does not support cached table",
            ));
        }
        for (privilege, command) in [
            (tidb_mysql::SelectPriv, "SELECT"),
            (tidb_mysql::UpdatePriv, "UPDATE"),
            (tidb_mysql::InsertPriv, "INSERT"),
            (tidb_mysql::DeletePriv, "DELETE"),
            (tidb_mysql::AlterPriv, "ALTER"),
        ] {
            self.visit_info.push(VisitInfo {
                privilege,
                database: tidb_hack::go_to_lower(database),
                table: tidb_hack::go_to_lower(name),
                column: String::new(),
                error: VisitAuthError::TableAccessDenied {
                    command,
                    table: name.to_owned(),
                },
                dynamic_privs: Vec::new(),
                dynamic_with_grant: false,
                alter_writable: false,
            });
        }
        if local {
            self.visit_info.push(VisitInfo {
                privilege: tidb_mysql::FilePriv,
                database: String::new(),
                table: String::new(),
                column: String::new(),
                error: VisitAuthError::SpecificAccessDenied { privilege: "FILE" },
                dynamic_privs: Vec::new(),
                dynamic_with_grant: false,
                alter_writable: false,
            });
        }
        if self
            .source
            .latest_index_schema()
            .is_some_and(|latest| latest.indexes_for_table(table.table_id()).is_none())
        {
            if self.session_ctx.is_some_and(|ctx| ctx.snapshot_ts() != 0) {
                return Err(PlanError::internal(
                    "can not execute IMPORT statement when 'tidb_snapshot' is set",
                ));
            }
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .fast_gen_by_args(&[self.source.current_database().into(), name.into()]),
            ));
        }
        let target = InsertTarget {
            database: table.db_name.clone(),
            table: table.table_name(),
            columns: Vec::new(),
            generated: Vec::new(),
            offsets: Vec::new(),
            extra_handle_offset: None,
        };
        let gen_cols = self
            .resolve_generated_columns(&target)?
            .into_iter()
            .flatten()
            .collect();
        let (schema, names, select_plan) = match &stmt.source {
            ImportSource::Select { query, .. } => {
                let select = self.build_query_stmt(query, false)?;
                let expected = if stmt.columns_and_user_vars.is_empty() {
                    table
                        .columns
                        .iter()
                        .filter(|column| {
                            let column = column.read();
                            !column.hidden && column.state == tidb_model::SchemaState::PUBLIC
                        })
                        .count()
                } else {
                    stmt.columns_and_user_vars.len()
                };
                if select.schema().map_or(0, |schema| schema.columns.len()) != expected {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                            .gen_with_stack_by_args(&[1.into()]),
                    ));
                }
                (
                    Default::default(),
                    Vec::new(),
                    Some(Box::new(Plan::Logical(select))),
                )
            }
            ImportSource::File { .. } => {
                let (schema, names) = crate::show_schema::build_import_into_schema();
                (schema, names, None)
            }
        };
        Ok(Plan::ImportInto(Box::new(ImportInto {
            table: tidb_resolve::TableNameW {
                table_name: TableRef {
                    identity: Default::default(),
                    name: vec![table.db_name.clone(), table.table_name()],
                    partitions: Vec::new(),
                    alias: None,
                    as_of: None,
                    hints: Vec::new(),
                    sample: None,
                },
                db_info: table.db_info,
                table_info: table.table_info,
            },
            column_assignments: stmt.column_assignments.clone(),
            columns_and_user_vars: stmt.columns_and_user_vars.clone(),
            path,
            format,
            options,
            gen_cols,
            select_plan,
            schema,
            names,
            stmt: tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::ImportInto(
                Box::new(stmt.clone()),
            )))
            .restore(),
        })))
    }
}

/// Go `buildImportInto`'s source checks for `IMPORT INTO ... FROM <path>`:
/// the path must parse as a URL, SEM v1 forbids server disk, and a
/// next-generation kernel under SEM stamps its keyspace as the S3 external
/// ID. Answers the (possibly rewritten) path and whether it is server disk.
pub(crate) fn check_import_source_path(path: &str) -> Result<(String, bool), PlanError> {
    let mut u = tidb_go_url::parse(path).map_err(|err| {
        // Go reports `err.Error()`, which quotes the path; its credentials
        // are redacted here.
        PlanError::from(tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.fast_gen_by_args(&[
            "data source".into(),
            err.redact_url(tidb_ast::redact_url).to_string().into(),
        ]))
    })?;
    let import_from_server = tidb_objstore::is_local(&u);
    if tidb_sem::is_enabled() && import_from_server {
        return Err(PlanError::from(
            tidb_plannererrors::ERR_NOT_SUPPORTED_WITH_SEM
                .gen_with_stack_by_args(&["IMPORT INTO from server disk".into()]),
        ));
    }
    // A next-generation cluster can be shared by tenants that use the same
    // AWS role; the external ID restricts access to the intended tenant.
    if tidb_config_kerneltype::is_next_gen()
        && tidb_sem_compat::is_enabled()
        && tidb_objstore::is_s3_like(&u)
    {
        let is_starter = tidb_config_deploymode::is_starter();
        if is_starter {
            check_starter_s3_path(&u)?;
        }
        check_next_gen_s3_path_with_sem(&u)?;
        // Non-Starter deployments derive the external ID from the keyspace.
        if !is_starter {
            let mut values = u.query();
            values.set(
                tidb_objstore_s3like::S3_EXTERNAL_ID,
                tidb_config::config_tree::config::get_global_keyspace_name(),
            );
            u.raw_query = values.encode();
            return Ok((u.string(), import_from_server));
        }
    }
    Ok((path.to_owned(), import_from_server))
}

/// Go `checkStarterS3Path`: a Starter deployment requires an explicit,
/// non-empty external ID. Go walks the query map and stops at the first
/// empty external ID, so any empty one fails whatever the map order.
pub(crate) fn check_starter_s3_path(u: &tidb_go_url::URL) -> Result<(), PlanError> {
    let values = u.query();
    let mut has_external_id = false;
    for (key, vs) in &values.0 {
        let key = tidb_objstore::normalize_query_parameter_key(&String::from_utf8_lossy(key));
        if key != tidb_objstore_s3like::S3_EXTERNAL_ID {
            continue;
        }
        has_external_id = vs.first().is_some_and(|v| !v.is_empty());
        if !has_external_id {
            break;
        }
    }
    if !has_external_id {
        return Err(PlanError::from(tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.fast_gen_by_args(&[
            "data source".into(),
            "external ID is required for Starter deployments".into(),
        ])));
    }
    Ok(())
}

/// Go `checkNextGenS3PathWithSem`: under SEM a tenant must bring its own key
/// pair or role ARN, and outside Starter may not choose the external ID.
pub(crate) fn check_next_gen_s3_path_with_sem(u: &tidb_go_url::URL) -> Result<(), PlanError> {
    use tidb_objstore_s3like::{S3_ACCESS_KEY, S3_EXTERNAL_ID, S3_ROLE_ARN, S3_SECRET_ACCESS_KEY};
    let not_supported = |feature: &str| {
        PlanError::from(
            tidb_plannererrors::ERR_NOT_SUPPORTED_WITH_SEM
                .gen_with_stack_by_args(&[feature.to_owned().into()]),
        )
    };
    let values = u.query();
    let expected_external_id = tidb_config::config_tree::config::get_global_keyspace_name();
    let is_starter = tidb_config_deploymode::is_starter();
    let (mut has_access_key, mut has_secret_access_key, mut has_role_arn) = (false, false, false);
    for (key, vs) in &values.0 {
        let normalized = tidb_objstore::normalize_query_parameter_key(&String::from_utf8_lossy(key));
        let has_value = !values.get(key).is_empty();
        match normalized.as_str() {
            S3_EXTERNAL_ID if !is_starter => {
                if vs.iter().any(|v| v.as_slice() != expected_external_id.as_bytes()) {
                    return Err(not_supported("IMPORT INTO with explicit external ID"));
                }
            }
            S3_ACCESS_KEY => has_access_key |= has_value,
            S3_SECRET_ACCESS_KEY => has_secret_access_key |= has_value,
            S3_ROLE_ARN => has_role_arn |= has_value,
            _ => {}
        }
    }
    if !has_role_arn && !(has_access_key && has_secret_access_key) {
        return Err(not_supported(
            "IMPORT INTO from S3-like storage without access key/secret access key or role ARN",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sem_error(u: &str) -> Option<String> {
        let u = tidb_go_url::parse(u).unwrap();
        check_next_gen_s3_path_with_sem(&u).err().map(|e| e.to_string())
    }

    /// Go `TestProcessNextGenS3Path`.
    #[test]
    fn test_process_next_gen_s3_path() {
        let backup = tidb_config::config_tree::config::get_global_keyspace_name();
        struct Restore(String);
        impl Drop for Restore {
            fn drop(&mut self) {
                let name = std::mem::take(&mut self.0);
                tidb_config::config_tree::config::update_global(|c| c.keyspace_name = name);
            }
        }
        let _restore = Restore(backup);
        tidb_config::config_tree::config::update_global(|c| c.keyspace_name = "aaa".into());
        for u in [
            "S3://bucket?External-id=abc&access-key=ak&secret-access-key=sk",
            "s3://bucket?external_id=abc&access-key=ak&secret-access-key=sk",
            "s3://bucket?external-id=aaa&external_id=abc&access-key=ak&secret-access-key=sk",
            "oss://bucket?External-id=abc&role-arn=arn",
            "oSS://bucket?External-id=abc&access-key=ak&secret-access-key=sk",
        ] {
            let err = sem_error(u).unwrap();
            assert!(err.contains("IMPORT INTO with explicit external ID"), "{u}: {err}");
            assert!(err.contains("is not supported when security enhanced mode is enabled"));
        }
        for u in [
            "s3://bucket?external-id=aaa&access-key=ak&secret-access-key=sk",
            "s3://bucket?external_id=aaa&access-key=ak&secret-access-key=sk",
            "s3://bucket?external-id=aaa&external_id=aaa&access-key=ak&secret-access-key=sk",
            "s3://bucket?access-key=ak&secret-access-key=sk",
            "s3://bucket?access_key=ak&secret_access_key=sk",
            "oss://bucket?role-arn=arn",
            "oss://bucket?role_arn=arn",
        ] {
            assert_eq!(sem_error(u), None, "{u}");
        }
        for u in [
            "s3://bucket",
            "s3://bucket?access-key=&secret-access-key=",
            "s3://bucket?access-key=ak",
            "s3://bucket?secret-access-key=sk",
            "s3://bucket?profile=dev",
            "oss://bucket",
            "oss://bucket?role-arn=",
        ] {
            let err = sem_error(u).unwrap();
            assert!(
                err.contains("IMPORT INTO from S3-like storage without access key/secret access key or role ARN"),
                "{u}: {err}"
            );
        }
        // Go runs its Starter cases only in a next-generation build, where
        // the deployment mode can be set. `checkStarterS3Path` itself reads
        // no mode, so its cases run in every build here.
        let starter = |u: &str| check_starter_s3_path(&tidb_go_url::parse(u).unwrap());
        for u in [
            "S3://bucket?External-id=abc&access-key=ak&secret-access-key=sk",
            "s3://bucket?external_id=abc&access-key=ak&secret-access-key=sk",
            "s3://bucket?external-id=aaa&external_id=abc&access-key=ak&secret-access-key=sk",
            "oss://bucket?External-id=abc&role-arn=arn",
            "s3://bucket?external-id=allowed",
        ] {
            assert!(starter(u).is_ok(), "{u}");
        }
        for u in [
            "s3://bucket?access-key=ak&secret-access-key=sk",
            "s3://bucket?external-id=&access-key=ak&secret-access-key=sk",
            "s3://bucket?external-id=abc&external_id=&access-key=ak&secret-access-key=sk",
            "oss://bucket?role-arn=arn",
        ] {
            let err = starter(u).unwrap_err().to_string();
            assert!(err.contains("external ID is required for Starter deployments"), "{u}: {err}");
        }
    }
}
