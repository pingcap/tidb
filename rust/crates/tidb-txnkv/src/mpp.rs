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

//! MPP metadata and client/coordinator contracts from `pkg/kv/mpp.go`.

use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

use crate::{KeyRange, PartitionIdAndRanges};
pub use tidb_proto::MppTaskMeta;

/// MPP plan protocol version.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct MppVersion(i64);

#[allow(non_upper_case_globals)]
impl MppVersion {
    /// Initial protocol.
    pub const V0: Self = Self(0);
    /// TiFlash 6.6+.
    pub const V1: Self = Self(1);
    /// TiFlash 7.3+ task-status reporting.
    pub const V2: Self = Self(2);
    /// TiFlash 9.0+ string serde.
    pub const V3: Self = Self(3);
    /// Illegal or unspecified version.
    pub const Unspecified: Self = Self(-1);
    /// Newest supported version.
    pub const NEWEST: Self = Self::V3;
    /// Source spelling of the unspecified version.
    pub const UNSPECIFIED_NAME: &'static str = "UNSPECIFIED";

    /// Returns the wire integer.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// Preserves every raw Go `int64` value carried by an MPP request.
    #[must_use]
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Parses the source case-insensitive name/integer domain.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case(Self::UNSPECIFIED_NAME) {
            return Some(Self::Unspecified);
        }
        match name.parse::<i64>().ok()? {
            -1 => Some(Self::Unspecified),
            0 => Some(Self::V0),
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            3 => Some(Self::V3),
            _ => None,
        }
    }
}

/// Physical location of an MPP task.
pub trait MppTaskLocation {
    /// TiFlash address.
    fn address(&self) -> &str;
}

/// Query-wide MPP identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MppQueryId {
    /// Query execution timestamp.
    pub query_ts: u64,
    /// TiDB-local query ID.
    pub local_query_id: u64,
    /// TiDB server ID.
    pub server_id: u64,
}

impl MppTaskLocation for MppTaskMeta {
    fn address(&self) -> &str {
        &self.address
    }
}

/// Minimum execution unit of an MPP computation.
pub struct MppTask<L> {
    /// TiFlash location.
    pub location: L,
    /// Task ID.
    pub id: i64,
    /// Start timestamp.
    pub start_ts: u64,
    /// Gather operator ID.
    pub gather_id: u64,
    /// Query identity.
    pub query_id: MppQueryId,
    /// Physical table ID.
    pub table_id: i64,
    /// MPP version.
    pub version: MppVersion,
    /// Session ID.
    pub session_id: u64,
    /// Session alias.
    pub session_alias: String,
    /// Physical partition table IDs.
    pub partition_table_ids: Vec<i64>,
    /// Static-pruning marker.
    pub tiflash_static_prune: bool,
}

impl<L: MppTaskLocation> MppTask<L> {
    /// Produces the exact task-meta field projection.
    #[must_use]
    pub fn to_meta(&self) -> MppTaskMeta {
        MppTaskMeta {
            start_ts: self.start_ts,
            gather_id: self.gather_id,
            query_ts: self.query_id.query_ts,
            local_query_id: self.query_id.local_query_id,
            server_id: self.query_id.server_id,
            task_id: self.id,
            mpp_version: self.version.as_i64(),
            connection_id: self.session_id,
            connection_alias: self.session_alias.clone(),
            address: if self.id == -1 {
                String::new()
            } else {
                self.location.address().to_owned()
            },
            ..MppTaskMeta::default()
        }
    }
}

/// MPP task lifecycle state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum MppTaskState {
    /// Ready for dispatch.
    #[default]
    Ready = 0,
    /// Running.
    Running,
    /// Cancelled.
    Cancelled,
    /// Finished.
    Done,
}

/// One MPP task dispatch request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MppDispatchRequest {
    /// Encoded DAG request.
    pub data: Vec<u8>,
    /// TiFlash task location.
    pub meta: MppTaskMeta,
    /// Root-task marker.
    pub root: bool,
    /// Connection timeout.
    pub timeout: u64,
    /// Schema version.
    pub schema_version: i64,
    /// Start timestamp.
    pub start_ts: u64,
    /// Query identity.
    pub query_id: MppQueryId,
    /// Gather operator ID.
    pub gather_id: u64,
    /// Task ID.
    pub id: i64,
    /// MPP version.
    pub version: MppVersion,
    /// Coordinator address.
    pub coordinator_address: String,
    /// Execution-summary marker.
    pub report_execution_summary: bool,
    /// Current lifecycle state.
    pub state: MppTaskState,
    /// Resource-group name.
    pub resource_group_name: String,
    /// Connection ID.
    pub connection_id: u64,
    /// Connection alias.
    pub connection_alias: String,
    /// SQL digest.
    pub sql_digest: String,
    /// Plan digest.
    pub plan_digest: String,
}

/// Parameters for cancelling MPP tasks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CancelMppTasksParam {
    /// TiFlash addresses selected for cancellation.
    pub store_addresses: HashMap<String, bool>,
    /// Requests whose tasks must be cancelled.
    pub requests: Vec<MppDispatchRequest>,
}

/// Parameters for establishing one MPP stream.
#[derive(Clone, Debug, PartialEq)]
pub struct EstablishMppConnsParam<C, B> {
    /// Request context/cancellation owner.
    pub context: C,
    /// Dispatch request.
    pub request: MppDispatchRequest,
    /// Target task metadata.
    pub task_meta: MppTaskMeta,
    /// Retry backoffer.
    pub backoffer: B,
}

/// Parameters for dispatching one MPP task.
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchMppTaskParam<C, B> {
    /// Request context/cancellation owner.
    pub context: C,
    /// Dispatch request.
    pub request: MppDispatchRequest,
    /// Whether execution information is collected.
    pub collect_execution_info: bool,
    /// Retry backoffer.
    pub backoffer: B,
}

/// MPP store client contract.
pub trait MppClient {
    /// Request context type.
    type Context;
    /// Error type.
    type Error;
    /// Backoffer type.
    type Backoffer;
    /// Dispatch policy.
    type DispatchPolicy;
    /// TiFlash replica-read policy.
    type ReplicaRead;
    /// Dispatch response.
    type DispatchResponse;
    /// Established stream response.
    type StreamResponse;

    /// Schedules task locations for one plan fragment.
    fn construct_mpp_tasks(
        &mut self,
        context: &Self::Context,
        request: &MppBuildTasksRequest,
        timeout: Duration,
        policy: Self::DispatchPolicy,
        replica_read: Self::ReplicaRead,
        on_error: &mut dyn FnMut(&Self::Error),
    ) -> Result<Vec<MppTaskMeta>, Self::Error>;

    /// Dispatches one task; `retry` is true only when the response is unusable.
    fn dispatch_mpp_task(
        &mut self,
        parameters: DispatchMppTaskParam<Self::Context, Self::Backoffer>,
    ) -> Result<(Self::DispatchResponse, bool), Self::Error>;

    /// Establishes one MPP connection.
    fn establish_mpp_connections(
        &mut self,
        parameters: EstablishMppConnsParam<Self::Context, Self::Backoffer>,
    ) -> Result<(Self::StreamResponse, bool), Self::Error>;

    /// Cancels the selected tasks.
    fn cancel_mpp_tasks(&mut self, parameters: CancelMppTasksParam);

    /// Checks whether `start_ts` is visible.
    fn check_visibility(&self, start_ts: u64) -> Result<(), Self::Error>;

    /// Returns the number of TiFlash stores.
    fn mpp_store_count(&self) -> Result<usize, Self::Error>;
}

/// MPP execution coordinator contract.
pub trait MppCoordinator {
    /// Request context type.
    type Context;
    /// Error type.
    type Error;
    /// Initial response type.
    type Response;
    /// Incremental result type.
    type ResultSubset;

    /// Builds and executes all tasks for a physical MPP plan.
    fn execute(
        &mut self,
        context: &Self::Context,
    ) -> Result<(Self::Response, Vec<KeyRange>), Self::Error>;

    /// Returns the next result subset.
    fn next(&mut self, context: &Self::Context) -> Result<Option<Self::ResultSubset>, Self::Error>;

    /// Reports TiFlash task execution status.
    fn report_status(
        &mut self,
        request: tidb_proto::MppReportTaskStatusRequest,
    ) -> Result<(), Self::Error>;

    /// Closes and releases resources.
    fn close(&mut self) -> Result<(), Self::Error>;

    /// Returns whether the coordinator is closed.
    fn is_closed(&self) -> bool;

    /// Returns the number of nodes participating in computation.
    fn node_count(&self) -> usize;
}

/// MPP task-allocation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MppBuildTasksRequest {
    /// Non-partitioned key ranges; `Some(empty)` remains non-partitioned.
    pub key_ranges: Option<Vec<KeyRange>>,
    /// Start timestamp.
    pub start_ts: u64,
    /// Partitioned key ranges.
    pub partition_id_and_ranges: Vec<PartitionIdAndRanges>,
}

impl MppBuildTasksRequest {
    /// Returns the exact cache-key string used by Go.
    #[must_use]
    pub fn cache_key(&self) -> String {
        let mut output = String::new();
        if let Some(ranges) = self.key_ranges.as_ref() {
            for (index, range) in ranges.iter().enumerate() {
                let _ = write!(
                    output,
                    "range_id{index}{}{}",
                    range.start_key, range.end_key
                );
            }
            return output;
        }
        for partition in &self.partition_id_and_ranges {
            let _ = write!(output, "partition_id{}", partition.id);
            for (index, range) in partition.key_ranges.iter().enumerate() {
                let _ = write!(
                    output,
                    "range_id{index}{}{}",
                    range.start_key, range.end_key
                );
            }
        }
        output
    }
}
