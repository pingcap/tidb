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

//! Go `pkg/executor/simple.go`: the executors of Go's `core.Simple` plans.

use crate::sessionctx::SessionContext;
use crate::{Catalog, DriverError, SchemaErrorKind};

/// Go `SimpleExec.executeUse`: an invisible schema is refused (1044) before
/// it is looked up, so it cannot be told apart from a missing one; an unknown
/// schema is `ErrDatabaseNotExists`; the switch sets `CurrentDB`,
/// `CurrentDBChanged` and `collation_database`, whose hook mirrors the
/// charset into `character_set_database`.
pub fn execute_use(
    ctx: &mut dyn SessionContext,
    is: &Catalog,
    name: &str,
) -> Result<(), DriverError> {
    // An empty schema name (`USE ``) is `ErrNoDB`, as Go's preprocessor
    // answers it, not an unknown database.
    if name.is_empty() {
        return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
    }
    if !ctx.privilege_manager().db_is_visible(name) {
        let vars = ctx.session_vars();
        let (user, host) = vars
            .current_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .unwrap_or_default();
        return Err(DriverError::DbAccessDenied {
            user: user.to_owned(),
            host: host.to_owned(),
            database: name.to_owned(),
        });
    }
    let Some(charset) = is.database_charset(name) else {
        return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
            name.to_owned(),
        )));
    };
    let vars = ctx.session_vars_mut();
    vars.current_db_changed = name != vars.current_db;
    vars.current_db = name.to_owned();
    // If new collations are enabled, the default collation stands in for one
    // they do not support.
    let collation =
        tidb_datatype::substitute_missing_collation_to_default(charset.collation.name());
    vars.set_system("collation_database", collation)?;
    Ok(())
}

/// A variable error as the driver error the wire layer renders.
impl From<tidb_variable::VarError> for DriverError {
    fn from(error: tidb_variable::VarError) -> Self {
        use tidb_variable::VarError;
        DriverError::Var(match error {
            VarError::UnknownSystemVariable(name) => {
                crate::VarErrorKind::UnknownSystemVariable(name)
            }
            VarError::RemovedSystemVariable { name, reason } => {
                crate::VarErrorKind::RemovedSystemVariable { name, reason }
            }
            VarError::ReadOnlyVariable(name) => crate::VarErrorKind::ReadOnlyVariable(name),
            VarError::WrongTypeForVar(name) => crate::VarErrorKind::WrongTypeForVar(name),
            VarError::WrongValueForVar(name, value) => {
                crate::VarErrorKind::WrongValueForVar(name, value)
            }
            VarError::SqlError(error) => crate::VarErrorKind::SqlError(error),
            VarError::SessionOnlyVariable(name) => crate::VarErrorKind::SessionOnlyVariable(name),
            VarError::GlobalOnlyVariable(name) => crate::VarErrorKind::GlobalOnlyVariable(name),
            VarError::NoGlobalCopy(name) => crate::VarErrorKind::NoGlobalCopy(name),
            VarError::IncorrectScope(name, allowed) => {
                crate::VarErrorKind::IncorrectScope(name, allowed.to_owned())
            }
            VarError::ValidationRefused(message) => crate::VarErrorKind::ValidationRefused(message),
        })
    }
}
