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

//! Go `executor.AlterSchema` (`ddl/executor.go`) with
//! `ModifySchemaCharsetAndCollate` and `ModifySchemaDefaultPlacement`, and
//! the schema placement `CreateSchemaWithInfo` resolves
//! (`handleDatabasePlacement`, `checkAndNormalizePlacementPolicy`).

use tidb_ast::DatabaseOption;
use tidb_datatype::{Charset, Collation};
use tidb_executor::{Catalog, DriverError, SchemaErrorKind, TableCharset};
use tidb_hack::GoCaseCompare;

use crate::{Session, StmtOutput};

/// Go `defaultPlacementPolicyName`.
const DEFAULT_PLACEMENT_POLICY_NAME: &str = "default";

/// Go `checkAndNormalizePlacementPolicy`: `default` removes the placement,
/// any other name must be a policy, whose ID the reference then carries.
pub(crate) fn check_and_normalize_placement_policy(
    catalog: &Catalog,
    name: &str,
) -> Result<Option<tidb_model::PolicyRefInfo>, DriverError> {
    if name.go_lower_eq(DEFAULT_PLACEMENT_POLICY_NAME) {
        return Ok(None);
    }
    let policy = catalog
        .policy(name)
        .ok_or_else(|| DriverError::PlacementPolicyNotExists(name.to_owned()))?;
    Ok(Some(tidb_model::PolicyRefInfo {
        id: policy.id,
        name: tidb_ast::CiString::new(name),
    }))
}

fn conflicting_charsets(previous: &str, next: &str) -> DriverError {
    DriverError::DdlCoded {
        proto: &tidb_util::dbterror::ERR_CONFLICTING_DECLARATIONS,
        message: format!(
            "Conflicting declarations: 'CHARACTER SET {previous}' and 'CHARACTER SET {next}'"
        ),
    }
}

impl Session {
    /// Go `executor.AlterSchema`.
    pub(crate) fn alter_schema_stmt(
        &mut self,
        name: Option<&str>,
        options: &[DatabaseOption],
    ) -> Result<StmtOutput, DriverError> {
        // Go's preprocessor names the current database for `ALTER DATABASE`
        // without one.
        let name = match name {
            Some(name) => name.to_owned(),
            None if !self.vars.current_db.is_empty() => self.vars.current_db.clone(),
            None => return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected)),
        };
        // Go `checkMultiSchemaSpecs`.
        if options.len() > 1
            && options
                .iter()
                .filter(|option| matches!(option, DatabaseOption::SetTiFlashReplica { .. }))
                .count()
                > 1
        {
            return Err(DriverError::DdlCoded {
                proto: &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
                message: "Unsupported multi schema change for set tiflash replica".to_owned(),
            });
        }
        let mut to_charset: Option<String> = None;
        let mut to_collate: Option<Collation> = None;
        let mut placement: Option<&str> = None;
        let mut tiflash_replica = false;
        for option in options {
            match option {
                DatabaseOption::CharacterSet(value) => match &to_charset {
                    None => to_charset = Some(value.to_ascii_lowercase()),
                    Some(previous) if !previous.eq_ignore_ascii_case(value) => {
                        return Err(conflicting_charsets(previous, value));
                    }
                    Some(_) => {}
                },
                DatabaseOption::Collate(value) => {
                    let collation =
                        Collation::from_name(value).ok_or_else(|| DriverError::DdlCoded {
                            proto: &tidb_error::catalog::collate::ERR_UNSUPPORTED_COLLATION,
                            message: format!("Unknown collation: '{value}'"),
                        })?;
                    let charset = collation.charset().name();
                    match &to_charset {
                        None => to_charset = Some(charset.to_owned()),
                        Some(previous) if previous != charset => {
                            return Err(conflicting_charsets(previous, charset));
                        }
                        Some(_) => {}
                    }
                    to_collate = Some(collation);
                }
                DatabaseOption::PlacementPolicy(value) => placement = Some(value),
                DatabaseOption::SetTiFlashReplica { .. } => tiflash_replica = true,
                // Go `AlterSchema` reads no other option.
                DatabaseOption::Encryption(_) => {}
            }
        }
        if let Some(charset) = to_charset {
            let charset = Charset::from_name(&charset).ok_or_else(|| DriverError::DdlCoded {
                proto: &tidb_util::dbterror::ERR_UNKNOWN_CHARACTER_SET,
                message: format!("Unknown character set: '{charset}'"),
            })?;
            let collation = to_collate.unwrap_or_else(|| charset.default_collation());
            self.modify_schema_charset_and_collate(&name, TableCharset { charset, collation })?;
        }
        if let Some(policy) = placement {
            self.modify_schema_default_placement(&name, policy)?;
        }
        if tiflash_replica {
            return Err(DriverError::unsupported(
                "ALTER DATABASE ... SET TIFLASH REPLICA (ModifySchemaSetTiFlashReplica) is not \
                 supported yet",
            ));
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `executor.ModifySchemaCharsetAndCollate` and its job
    /// (`onModifySchemaCharsetAndCollate`).
    fn modify_schema_charset_and_collate(
        &mut self,
        name: &str,
        charset: TableCharset,
    ) -> Result<(), DriverError> {
        let context = self.ddl_statement_context();
        self.with_persistent_catalog_mut(|catalog| {
            let Some(current) = catalog.database_charset(name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                    name.to_owned(),
                )));
            };
            if current == charset {
                return Ok(());
            }
            tidb_executor::ddl::submit_ddl_job(
                catalog,
                &context,
                name,
                "",
                tidb_model::ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            )?;
            catalog.set_database_charset(name, charset);
            Ok(())
        })
    }

    /// Go `executor.ModifySchemaDefaultPlacement` and its job
    /// (`onModifySchemaDefaultPlacement`).
    fn modify_schema_default_placement(
        &mut self,
        name: &str,
        policy: &str,
    ) -> Result<(), DriverError> {
        let context = self.ddl_statement_context();
        self.with_persistent_catalog_mut(|catalog| {
            if !catalog.has_database(name) {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                    name.to_owned(),
                )));
            }
            let reference = check_and_normalize_placement_policy(catalog, policy)?;
            tidb_executor::ddl::submit_ddl_job(
                catalog,
                &context,
                name,
                "",
                tidb_model::ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
            )?;
            // The job changes nothing when the reference is already this one.
            if reference.is_some() && catalog.database_placement_policy(name) == reference.as_ref()
            {
                return Ok(());
            }
            catalog.set_database_placement_policy(name, reference);
            Ok(())
        })
    }
}
