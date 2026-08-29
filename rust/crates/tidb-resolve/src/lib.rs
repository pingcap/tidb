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

//! Semantic-resolution context from `pkg/planner/core/resolve`.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_ast::{CiString, TableRef};
use tidb_model::{ColumnInfo, DBInfo, GoShared, TableInfo};

/// Go `TableNameW`, including the AST table's pointer identity.
#[derive(Clone, Debug)]
pub struct TableNameW {
    /// Go embedded `*ast.TableName`.
    pub table_name: Arc<TableRef>,
    /// Resolved database metadata.
    pub db_info: Option<GoShared<DBInfo>>,
    /// Resolved table metadata.
    pub table_info: Option<GoShared<TableInfo>>,
}

/// Go `NodeW`: an AST node paired with a shared resolve context.
#[derive(Clone, Debug)]
pub struct NodeW<N> {
    /// Wrapped AST node.
    pub node: N,
    resolve_ctx: GoShared<Context>,
}

impl<N> NodeW<N> {
    /// Go `NewNodeW`.
    pub fn new(node: N) -> Self {
        Self {
            node,
            resolve_ctx: GoShared::new(Context::new()),
        }
    }

    /// Go `NewNodeWWithCtx`.
    pub fn with_context(node: N, resolve_ctx: GoShared<Context>) -> Self {
        Self { node, resolve_ctx }
    }

    /// Go `CloneWithNewNode`.
    pub fn clone_with_new_node<M>(&self, node: M) -> NodeW<M> {
        NodeW {
            node,
            resolve_ctx: self.resolve_ctx.clone(),
        }
    }

    /// Go `GetResolveContext`.
    pub fn resolve_context(&self) -> GoShared<Context> {
        self.resolve_ctx.clone()
    }
}

/// Go `Context`, keyed by `*ast.TableName` identity rather than value.
#[derive(Clone, Debug, Default)]
pub struct Context {
    table_names: HashMap<usize, TableNameW>,
}

impl Context {
    /// Go `NewContext`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `AddTableName`.
    pub fn add_table_name(&mut self, table_name: TableNameW) {
        self.table_names
            .insert(table_name_key(&table_name.table_name), table_name);
    }

    /// Go `GetTableName`.
    pub fn table_name(&self, table_name: &Arc<TableRef>) -> Option<&TableNameW> {
        self.table_names.get(&table_name_key(table_name))
    }

    /// Go `GetTableNames`.
    pub fn table_names(&self) -> &HashMap<usize, TableNameW> {
        &self.table_names
    }
}

fn table_name_key(table_name: &Arc<TableRef>) -> usize {
    Arc::as_ptr(table_name) as usize
}

/// Go `ResultField`.
#[derive(Clone, Debug, Default)]
pub struct ResultField {
    /// Source column metadata, if this field is column-backed.
    pub column: Option<GoShared<ColumnInfo>>,
    /// Visible column name or alias.
    pub column_as_name: CiString,
    /// Whether the original column name must be empty.
    pub empty_org_name: bool,
    /// Source table metadata, if this field is table-backed.
    pub table: Option<GoShared<TableInfo>>,
    /// Visible table name or alias.
    pub table_as_name: CiString,
    /// Database name.
    pub db_name: CiString,
}

/// Go `*ResultField`.
pub type ResultFieldRef = GoShared<ResultField>;
