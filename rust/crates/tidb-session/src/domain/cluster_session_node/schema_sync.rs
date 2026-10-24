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

//! The node's session-facing half of the schema-version ack loop. The loop
//! itself (Go `issyncer`'s `MDLCheckLoop` and the pin registry it consults)
//! lives in `tidb-ddl`; [`ConnectionMdlSink`] is the piece that needs the
//! session, so it stays beside the connection layer.

use std::sync::Arc;

pub use tidb_ddl::schema_sync::*;

/// The session-facing half of the registry: one connection's recorder,
/// handed to the driver session as its [`crate::MdlRelatedTableSink`].
///
/// Recording lands only while the connection holds a pin (a live statement
/// or an open transaction) -- `record_table_use` is a no-op for an idle
/// connection, exactly as Go's map lives on the `TransactionContext` and a
/// session without one blocks nothing.
#[derive(Debug)]
pub struct ConnectionMdlSink {
    registry: Arc<SchemaPinRegistry>,
    connection_id: u64,
}

impl ConnectionMdlSink {
    /// Binds one connection's recorder to the node's registry.
    #[must_use]
    pub fn new(registry: Arc<SchemaPinRegistry>, connection_id: u64) -> Self {
        Self {
            registry,
            connection_id,
        }
    }
}

impl crate::MdlRelatedTableSink for ConnectionMdlSink {
    fn record_table(&self, table_id: i64, version: i64) {
        self.registry
            .record_table_use(self.connection_id, table_id, version);
    }

    fn record_unresolved(&self) {
        // Go's related-table map contains only concrete table IDs. View
        // dependencies are expanded by the session before this callback;
        // an actually unresolved name does not add a synthetic all-table
        // dependency.
    }
}
