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

//! Statement labels used by TiDB metrics and diagnostics.

use crate::{AdminStmt, DdlStmt, DmlStmt, QueryStmt, SessionStmt, Stmt};

impl Stmt {
    /// Returns the same coarse statement label as Go's `ast.GetStmtLabel`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Query(query) => query.label(),
            Self::Dml(dml) => dml.label(),
            Self::Ddl(ddl) => ddl.label(),
            Self::Admin(admin) => admin.label(),
            Self::Session(session) => session.label(),
        }
    }

    fn is_show_statement(&self) -> bool {
        matches!(self, Self::Admin(admin) if admin.is_show())
    }
}

impl QueryStmt {
    fn label(&self) -> &'static str {
        match self {
            Self::Select(_) => "Select",
            Self::SetOpr(_) => "other",
        }
    }
}

impl DmlStmt {
    fn label(&self) -> &'static str {
        match self {
            Self::With { statement, .. } => statement.label(),
            Self::Insert(insert) if insert.replace => "Replace",
            Self::Insert(_) => "Insert",
            Self::Update(_) => "Update",
            Self::Delete(_) => "Delete",
            Self::ImportInto(_) => "ImportInto",
            Self::LoadData(_) => "LoadData",
            Self::Call(_) => "Call",
            Self::Batch(_) | Self::DistributeTable(_) => "other",
        }
    }
}

impl DdlStmt {
    fn label(&self) -> &'static str {
        match self {
            Self::AlterTable(_) => "AlterTable",
            Self::CreateDatabase { .. } => "CreateDatabase",
            Self::CreateIndex(_) => "CreateIndex",
            Self::CreateTable(_) => "CreateTable",
            Self::CreateView(_) => "CreateView",
            Self::CreateUser { .. } => "CreateUser",
            Self::DropDatabase { .. } => "DropDatabase",
            Self::DropIndex(_) => "DropIndex",
            Self::DropTable(_) => "DropTable",
            Self::DropView { .. } => "DropView",
            Self::TruncateTable(_) => "TruncateTable",
            _ => "other",
        }
    }
}

impl SessionStmt {
    fn label(&self) -> &'static str {
        match self {
            Self::Use(_) => "Use",
            Self::Set(_) | Self::SetPassword(_) => "Set",
            Self::Prepare { .. } => "Prepare",
            Self::Execute { .. } => "Execute",
            Self::Deallocate(_) => "Deallocate",
            Self::Begin(_) => "Begin",
            Self::Commit(_) => "Commit",
            Self::Rollback { .. } => "Rollback",
            Self::Savepoint(_) => "Savepoint",
            _ => "other",
        }
    }
}

impl AdminStmt {
    fn label(&self) -> &'static str {
        match self {
            Self::Grant(_) => "Grant",
            Self::Revoke(_) => "Revoke",
            Self::CreateBinding(_) => "CreateBinding",
            Self::DropBinding(_) => "DropBinding",
            Self::AnalyzeTable(_) | Self::AnalyzeIncremental(_) => "AnalyzeTable",
            Self::DescribeTable(_) => "DescTable",
            Self::Explain(explain) if explain.statement().is_some_and(Stmt::is_show_statement) => {
                "DescTable"
            }
            Self::Explain(explain) if explain.analyze => "ExplainAnalyzeSQL",
            Self::Explain(_) => "ExplainSQL",
            statement if statement.is_show() => "Show",
            _ => "other",
        }
    }

    fn is_show(&self) -> bool {
        matches!(
            self,
            Self::ShowGrants(_)
                | Self::ShowMasterStatus
                | Self::ShowPrivileges
                | Self::ShowBuiltins
                | Self::ShowImportJobs(_)
                | Self::ShowImportGroups(_)
                | Self::ShowBdrRole
                | Self::ShowSlow(_)
                | Self::ShowDdl
                | Self::ShowDdlJobs(_)
                | Self::ShowDdlJobQueries(_)
                | Self::ShowNextRowId(_)
                | Self::ShowCreate { .. }
                | Self::ShowCreateUser(_)
                | Self::ShowVariables(_)
                | Self::ShowStatus(_)
                | Self::ShowWarnings(_)
                | Self::ShowErrors(_)
                | Self::ShowCollation(_)
                | Self::ShowEngines(_)
                | Self::ShowCharset(_)
                | Self::ShowStatsHistograms(_)
                | Self::ShowStatsBuckets(_)
                | Self::ShowStatsLocked(_)
                | Self::ShowStatsTopN(_)
                | Self::ShowDatabases(_)
                | Self::ShowTables(_)
                | Self::ShowOpenTables(_)
                | Self::ShowTableStatus(_)
                | Self::ShowTableNextRowId(_)
                | Self::ShowColumns(_)
                | Self::ShowIndex(_)
                | Self::ShowInspection(_)
                | Self::ShowDistributionJobs(_)
                | Self::ShowTablePlacement(_)
                | Self::ShowPlacement(_)
                | Self::ShowProfile(_)
                | Self::ShowMaskingPolicies(_)
                | Self::ShowBindings(_)
        )
    }
}
