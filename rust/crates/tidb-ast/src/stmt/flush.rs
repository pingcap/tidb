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

//! Standalone `FLUSH` payloads and restore translated from
//! `pkg/parser/ast/misc.go`.

use crate::util::push_name_path;

/// The selected, state-free standalone `FLUSH` statement forms.
#[derive(Debug, Clone, PartialEq)]
pub enum FlushStmt {
    /// `FLUSH STATUS`.
    Status,
    /// `FLUSH PRIVILEGES`.
    Privileges,
    /// `FLUSH TABLE[S] [table [, ...]] [WITH READ LOCK]`.
    Tables {
        /// Optional affected table paths; an empty list means all tables.
        tables: Vec<Vec<String>>,
        /// Whether Go's `WITH READ LOCK` suffix was present.
        read_lock: bool,
    },
}

impl FlushStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Status => out.push_str("FLUSH STATUS"),
            Self::Privileges => out.push_str("FLUSH PRIVILEGES"),
            Self::Tables { tables, read_lock } => {
                out.push_str("FLUSH TABLES");
                for (index, table) in tables.iter().enumerate() {
                    out.push_str(if index == 0 { " " } else { ", " });
                    push_name_path(out, table);
                }
                if *read_lock {
                    out.push_str(" WITH READ LOCK");
                }
            }
        }
    }
}
