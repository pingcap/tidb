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

//! Final server-local adapter for the production read engine.

use std::time::Duration;

use tidb_distsql::DirectUnaryTransportEvidenceHandle;
use tidb_exec::distsql_recordset::DistSqlRecordSet;
use tidb_exec::real_tikv_read::{PdTimestampSource, ProductionReadTransport, RealTiKvReadEngine};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

use crate::node_config::{ConfiguredReadColumnKind, NodeConfig};
use crate::resultset_source::ResultSetSource;
use crate::sql_node::{
    SerialQueryEngine, SerialQueryResult, SerialSqlNode, SqlNodeError, SqlQueryError,
};

/// Server-local owner that adapts the execution crate without reversing its
/// dependency into MySQL protocol code.
pub struct RealTiKvSerialEngine {
    inner: RealTiKvReadEngine<ProductionReadTransport, PdTimestampSource>,
}

impl RealTiKvSerialEngine {
    /// Connects the real engine from validated node configuration.
    pub fn connect(config: &NodeConfig) -> Result<Self, SqlQueryError> {
        let columns: Vec<_> = config
            .read_table
            .columns
            .iter()
            .map(|column| match column.kind {
                ConfiguredReadColumnKind::ClusteredPrimaryKey => {
                    ConfiguredColumn::clustered_primary_key(&column.name, column.id)
                }
                ConfiguredReadColumnKind::StoredNotNull => {
                    ConfiguredColumn::stored_not_null(&column.name, column.id)
                }
            })
            .collect();
        let table = ConfiguredTable::new(
            &config.read_table.database,
            &config.read_table.table,
            config.read_table.table_id,
            columns,
        );
        let inner = RealTiKvReadEngine::connect(
            config.pd_endpoints.clone(),
            Duration::from_secs(60),
            table,
        )
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        Ok(Self { inner })
    }

    /// Returns the PD cluster identity validated during engine bootstrap.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.inner.cluster_id()
    }
}

impl SerialQueryEngine for RealTiKvSerialEngine {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<SerialQueryResult<'a>, SqlQueryError> {
        let query = self
            .inner
            .execute(sql)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let snapshot_ts = query.snapshot_ts();
        let table_id = query.table_id();
        let cluster_id = self.inner.cluster_id();
        let evidence = self.inner.transport_evidence_handle();
        eprintln!(
            "{{\"event\":\"query_snapshot\",\"cluster_id\":{cluster_id},\"snapshot_ts\":{snapshot_ts},\"table_id\":{table_id}}}"
        );
        Ok(SerialQueryResult::new(Box::new(ObservedResultSet {
            inner: query.into_record_set(),
            evidence,
            emitted: false,
        })))
    }
}

struct ObservedResultSet {
    inner: DistSqlRecordSet,
    evidence: DirectUnaryTransportEvidenceHandle,
    emitted: bool,
}

impl ObservedResultSet {
    fn emit_evidence(&mut self) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        let evidence = self.evidence.snapshot();
        let located_regions = evidence
            .located_region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let dispatched_regions = evidence
            .dispatched_region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "{{\"event\":\"query_transport\",\"located_region_ids\":[{located_regions}],\"dispatched_region_ids\":[{dispatched_regions}],\"batch_attempts\":{},\"unary_attempts\":{}}}",
            evidence.batch_attempts, evidence.unary_attempts
        );
    }
}

impl ResultSetSource for ObservedResultSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner
            .next_batch(max_rows)
            .map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        Ok(self.inner.columns().to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        let result = self.inner.finish().map_err(|error| error.to_string());
        self.emit_evidence();
        result
    }

    fn close(&mut self) -> Result<(), String> {
        let result = self.inner.close().map_err(|error| error.to_string());
        self.emit_evidence();
        result
    }
}

/// Starts the first bounded production Rust SQL node.
pub fn run_configured_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let engine = RealTiKvSerialEngine::connect(&config).map_err(RunConfiguredNodeError::Engine)?;
    let cluster_id = engine.cluster_id();
    let mut node = SerialSqlNode::bind(&config, engine).map_err(RunConfiguredNodeError::Node)?;
    let address = node.local_addr().map_err(RunConfiguredNodeError::Node)?;
    let column_descriptors = config
        .read_table
        .columns
        .iter()
        .map(|column| {
            format!(
                "{}:{}:{}",
                column.name,
                column.id,
                column.kind.descriptor_name()
            )
        })
        .collect::<Vec<_>>();
    eprintln!(
        "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"pd_endpoints\":{},\"cluster_id\":{cluster_id},\"database\":\"{}\",\"table\":\"{}\",\"table_id\":{},\"column_count\":{},\"columns\":{:?}}}",
        config.pd_endpoints.len(),
        config.read_table.database,
        config.read_table.table,
        config.read_table.table_id,
        column_descriptors.len(),
        column_descriptors,
    );
    node.run().map_err(RunConfiguredNodeError::Node)
}

/// Startup/runtime failure from the fully composed node.
#[derive(Debug)]
pub enum RunConfiguredNodeError {
    /// Production query-engine construction failed.
    Engine(SqlQueryError),
    /// Listener or connection runtime failed.
    Node(SqlNodeError),
}

impl std::fmt::Display for RunConfiguredNodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(error) => {
                write!(formatter, "cannot construct read engine: {}", error.message)
            }
            Self::Node(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RunConfiguredNodeError {}
