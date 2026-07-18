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

//! Go's distinct `ShowOpenTables` AST payload.

use crate::util::push_name_path;

/// `SHOW OPEN TABLES [IN | FROM schema]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowOpenTablesStmt {
    /// Optional schema name, restored with Go's canonical `IN` introducer.
    pub database: Option<String>,
}

impl ShowOpenTablesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW OPEN TABLES");
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
    }
}
