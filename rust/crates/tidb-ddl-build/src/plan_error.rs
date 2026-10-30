// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Why a planned catalog change cannot be built: Go `pkg/ddl`'s refusals,
//! carried from the planner to the job worker's checkpoint.

use std::fmt;

use tidb_metadef::MAX_USER_GLOBAL_ID;

use tidb_infoschema::cluster_catalog::ClusterCatalogError;
use tidb_terror::TerrorError;
use tidb_txnkv::transaction::MutationSetError;

use crate::table_info_build::DdlAdmissionError;

/// Why a planned catalog change cannot be built from the observed snapshot.
#[derive(Clone, Debug)]
pub enum DdlPlanError {
    /// A source-defined error whose class, code and message must survive the
    /// worker checkpoint. SQL projection belongs at the submitting connection.
    Source(TerrorError),
    /// The catalog could not be read or decoded.
    Catalog(ClusterCatalogError),
    /// A source-defined DDL refusal reported as TiDB's generic 1105.
    Unsupported(String),
    /// A refusal raised by the shared admission code while the change was
    /// being planned, carrying the MySQL error number Go reports for it.
    ///
    /// `ALTER COLUMN ... SET DEFAULT` is the first change whose validation
    /// runs against the STORED column type and therefore cannot happen at
    /// lowering time; without this the exact refusals -- 1067 for a bad
    /// default, 1171 for a NULL default on a primary key -- would all
    /// flatten to the generic 1105.
    Admission(crate::table_info_build::DdlAdmissionError),
    /// Go `GenGlobalIDs`' own limit: the user ID space is exhausted.
    GlobalIdExhausted {
        /// The ID the allocation would have reached.
        wanted: i64,
    },
    /// A catalog object could not be encoded.
    Encode(String),
    /// The mutation set was rejected before it could be published.
    Mutations(MutationSetError),
}

impl fmt::Display for DdlPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => fmt::Display::fmt(error, formatter),
            Self::Catalog(error) => write!(formatter, "{error}"),
            Self::Unsupported(reason) => formatter.write_str(reason),
            Self::Admission(error) => formatter.write_str(&error.reason),
            Self::GlobalIdExhausted { wanted } => write!(
                formatter,
                "global id:{wanted} exceeds the limit:{MAX_USER_GLOBAL_ID}"
            ),
            Self::Encode(detail) => write!(formatter, "{detail}"),
            Self::Mutations(error) => write!(formatter, "{error}"),
        }
    }
}

impl DdlPlanError {
    /// A refusal carrying an already classified driver-tier failure.
    pub fn from_mysql_error(error: tidb_terror::MysqlError) -> Self {
        Self::Admission(DdlAdmissionError::from_mysql_error(error))
    }

    /// A refusal Go raises as a plain `errors.Errorf` (1105, no class).
    pub fn plain_refusal(message: impl Into<String>) -> Self {
        Self::Admission(DdlAdmissionError::plain(message))
    }

    /// The existing SQL contract is owned with the error, so direct execution
    /// and persisted action checkpoints cannot select different error numbers.
    pub fn mysql_error_code(&self) -> Option<u16> {
        use tidb_errno::errcode;
        Some(match self {
            Self::Source(error) => error.to_sql_error().code,
            Self::Admission(error) => error.code,
            Self::Unsupported(_) => errcode::ErrUnknown,
            Self::Catalog(_)
            | Self::Encode(_)
            | Self::Mutations(_)
            | Self::GlobalIdExhausted { .. } => return None,
        })
    }

    /// The Go `*terror.Error` identity this refusal is raised as, `None` for
    /// the plain errors and the failures that are not Go errors at all.
    #[must_use]
    pub fn identity(&self) -> Option<tidb_terror::TerrorIdentity> {
        match self {
            Self::Source(error) => Some(error.identity()),
            Self::Admission(error) => error.identity,
            _ => None,
        }
    }

    /// Durable job error. Coded legacy errors retain their numeric envelope;
    /// uncoded errors follow Go `toTError`'s DDL/CodeUnknown fallback.
    /// Source RFC identities must be carried from their producers, not guessed
    /// from a MySQL number shared by several Go error classes.
    pub fn to_job_error(&self) -> tidb_terror::TerrorError {
        if let Self::Source(error) = self {
            return error.clone();
        }
        // The Go definition the refusal was raised as: the job's `err` JSON
        // carries its class, code and RFC identity (`{"class":8,"code":1062,
        // "rfccode":"kv:1062"}`), exactly as Go persists a `*terror.Error`.
        if let Some(identity) = self.identity() {
            return tidb_terror::TerrorError::synthesize(
                identity.class(),
                identity.code(),
                self.to_string(),
            );
        }
        match self.mysql_error_code() {
            Some(code) => tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(
                    isize::try_from(code).expect("u16 error code fits isize"),
                ),
                self.to_string(),
            ),
            None => unknown_ddl_job_error(self.to_string()),
        }
    }

    /// Converts a direct refusal through the same contract as durable history.
    pub fn to_sql_error(&self) -> tidb_mysql::errors::SqlError {
        ddl_job_error_to_sql_error(&self.to_job_error())
    }

    /// Go `proto.Equal(err)`: whether this is the Go error `proto` raises.
    #[must_use]
    pub fn equal(&self, proto: &TerrorError) -> bool {
        self.identity() == Some(proto.identity())
    }
}

/// A DDL-class error with the unknown code, carrying `message` as its text.
pub fn unknown_ddl_job_error(message: impl Into<String>) -> tidb_terror::TerrorError {
    tidb_terror::TerrorError::synthesize(
        tidb_terror::TerrorClass::Ddl,
        tidb_terror::CODE_UNKNOWN,
        message,
    )
}

/// Projects the durable DDL error envelope onto SQL. Legacy Rust job rows have
/// numeric codes without RFC identities; they must remain readable after restart.
/// SQLSTATE comes from the shared Go MySQL catalog, never a transport-local map.
pub fn ddl_job_error_to_sql_error(
    error: &tidb_terror::TerrorError,
) -> tidb_mysql::errors::SqlError {
    let code = u16::try_from(error.code().value()).unwrap_or(tidb_errno::errcode::ErrUnknown);
    tidb_mysql::errors::SqlError {
        code,
        state: tidb_mysql::errors::mysql_state(code),
        message: error.message().to_owned(),
        identity: Some(error.identity()),
    }
}

impl std::error::Error for DdlPlanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Admission(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Mutations(error) => Some(error),
            _ => None,
        }
    }
}

impl From<TerrorError> for DdlPlanError {
    fn from(error: TerrorError) -> Self {
        Self::Source(error)
    }
}

impl From<DdlAdmissionError> for DdlPlanError {
    fn from(error: DdlAdmissionError) -> Self {
        Self::Admission(error)
    }
}

impl From<ClusterCatalogError> for DdlPlanError {
    fn from(error: ClusterCatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<MutationSetError> for DdlPlanError {
    fn from(error: MutationSetError) -> Self {
        Self::Mutations(error)
    }
}

impl From<tidb_syssession::mysql_system_tables::SystemTableError> for DdlPlanError {
    fn from(error: tidb_syssession::mysql_system_tables::SystemTableError) -> Self {
        Self::Encode(error.to_string())
    }
}

/// What `jobsubmit` returned, as the DDL statement's error: its refusals as
/// Go raises them (a `*terror.Error`, or a plain `errors.Errorf`), the rest
/// as this crate's own variants.
impl From<tidb_ddl_jobsubmit::JobSubmitError> for DdlPlanError {
    fn from(error: tidb_ddl_jobsubmit::JobSubmitError) -> Self {
        use tidb_ddl_jobsubmit::JobSubmitError;
        match error {
            JobSubmitError::Catalog(error) => Self::Catalog(error),
            JobSubmitError::Encode(detail) => Self::Encode(detail),
            JobSubmitError::Mutations(error) => Self::Mutations(error),
            JobSubmitError::GlobalIdExhausted { wanted } => Self::GlobalIdExhausted { wanted },
            refusal => match refusal.to_terror() {
                Some(go) => Self::Admission(DdlAdmissionError::from_terror(&go)),
                None => Self::plain_refusal(refusal.to_string()),
            },
        }
    }
}
