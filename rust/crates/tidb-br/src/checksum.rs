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

//! Go `br/pkg/checksum/executor.go`: a table's checksum through
//! coprocessor checksum requests, one for the record range and one per
//! public index of each physical table. A restore rewrites the old table's
//! key prefix to the new one in TiKV (`ChecksumRewriteRule`).

use std::sync::Arc;

use prost::Message;
use tidb_distsql::RequestBuilder;
use tidb_gocontext::Context;
use tidb_model::{IndexInfo, SchemaState, TableInfo};
use tidb_proto::tipb;
use tidb_txnkv::TransportRequest;

use crate::errors::{self, Result};
use crate::utils::backoff::new_checksum_backoff_strategy;
use crate::utils::retry::with_retry;

/// Go `vardef.DefDistSQLScanConcurrency`.
pub const DEF_DIST_SQL_SCAN_CONCURRENCY: u64 = 15;

/// Go `ExecutorBuilder`.
pub struct ExecutorBuilder<'a> {
    table: &'a TableInfo,
    ts: u64,
    old_table: Option<&'a TableInfo>,
    concurrency: u64,
    backoff_weight: i32,
    old_keyspace: Vec<u8>,
    new_keyspace: Vec<u8>,
    resource_group_name: String,
    request_source: tidb_txnkv::RequestSource,
}

impl<'a> ExecutorBuilder<'a> {
    /// Go `NewExecutorBuilder`.
    #[must_use]
    pub fn new(table: &'a TableInfo, ts: u64) -> Self {
        Self {
            table,
            ts,
            old_table: None,
            concurrency: DEF_DIST_SQL_SCAN_CONCURRENCY,
            backoff_weight: 0,
            old_keyspace: Vec::new(),
            new_keyspace: Vec::new(),
            resource_group_name: String::new(),
            request_source: tidb_txnkv::RequestSource::default(),
        }
    }

    /// Go `SetOldTable`: the backed-up table, whose keys TiKV rewrites.
    #[must_use]
    pub fn set_old_table(mut self, old: Option<&'a TableInfo>) -> Self {
        self.old_table = old;
        self
    }

    /// Go `SetConcurrency`.
    #[must_use]
    pub fn set_concurrency(mut self, c: u64) -> Self {
        self.concurrency = c;
        self
    }

    /// Go `SetBackoffWeight`.
    #[must_use]
    pub fn set_backoff_weight(mut self, w: i32) -> Self {
        self.backoff_weight = w;
        self
    }

    /// Go `SetOldKeyspace`.
    #[must_use]
    pub fn set_old_keyspace(mut self, k: Vec<u8>) -> Self {
        self.old_keyspace = k;
        self
    }

    /// Go `SetNewKeyspace`.
    #[must_use]
    pub fn set_new_keyspace(mut self, k: Vec<u8>) -> Self {
        self.new_keyspace = k;
        self
    }

    /// Go `SetResourceGroupName`.
    #[must_use]
    pub fn set_resource_group_name(mut self, name: &str) -> Self {
        self.resource_group_name = name.to_owned();
        self
    }

    /// Go `SetExplicitRequestSourceType`.
    #[must_use]
    pub fn set_explicit_request_source_type(mut self, name: &str) -> Self {
        self.request_source.explicit_source_type = name.to_owned();
        self
    }

    /// Go `Build`.
    ///
    /// # Errors
    /// An old table without the partition of a new one, or a request that
    /// cannot be built.
    pub fn build(self) -> Result<Executor> {
        Ok(Executor {
            reqs: self.build_checksum_request()?,
            backoff_weight: self.backoff_weight,
        })
    }

    /// Go `buildChecksumRequest`.
    fn build_checksum_request(&self) -> Result<Vec<TransportRequest>> {
        let mut reqs = Vec::new();
        let old_table_id = self.old_table.map_or(0, |t| t.id);
        reqs.extend(self.build_request(self.table.id, old_table_id)?);
        if let Some(part) = self.table.get_partition_info() {
            let defs: Vec<(i64, String)> = part
                .read()
                .definitions
                .map_visible(|d| (d.id, d.name.lowercase().to_owned()));
            for (id, name_l) in defs {
                let old_part_id = match self.old_table {
                    Some(old) => get_partition_by_name(old, &name_l)?,
                    None => 0,
                };
                reqs.extend(self.build_request(id, old_part_id)?);
            }
        }
        Ok(reqs)
    }

    /// Go `buildRequest`: the record range, then each public index. Go
    /// panics when the old table lacks an index of the new one.
    fn build_request(&self, table_id: i64, old_table_id: i64) -> Result<Vec<TransportRequest>> {
        let mut reqs = vec![self.build_table_request(table_id, old_table_id)?];
        for index in self.table.indices.iter_deref() {
            let index = index.read();
            if index.state != SchemaState::PUBLIC {
                continue;
            }
            let old_index = match self.old_table {
                Some(old) => {
                    let found = old
                        .indices
                        .iter_deref()
                        .find(|o| o.read().name == index.name)
                        .map(|o| o.read().clone());
                    if found.is_none() {
                        panic!(
                            "index not found in origin table, please check the restore table has the same index info with origin table: table id {table_id}, table name {}, origin table id {old_table_id}, origin table name {}, index name {}",
                            self.table.name.original(), old.name.original(), index.name.original()
                        );
                    }
                    found
                }
                None => None,
            };
            reqs.push(self.build_index_request(
                table_id,
                &index,
                old_table_id,
                old_index.as_ref(),
            )?);
        }
        Ok(reqs)
    }

    /// Go `buildTableRequest`.
    fn build_table_request(&self, table_id: i64, old_table_id: i64) -> Result<TransportRequest> {
        let rule = self.old_table.map(|_| tipb::ChecksumRewriteRule {
            old_prefix: Some(
                [
                    self.old_keyspace.as_slice(),
                    &tidb_tablecodec::table_key::gen_table_record_prefix(old_table_id),
                ]
                .concat(),
            ),
            new_prefix: Some(
                [
                    self.new_keyspace.as_slice(),
                    &tidb_tablecodec::table_key::gen_table_record_prefix(table_id),
                ]
                .concat(),
            ),
        });
        let checksum = tipb::ChecksumRequest {
            scan_on: Some(tipb::ChecksumScanOn::Table as i32),
            algorithm: Some(tipb::ChecksumAlgorithm::Crc64Xor as i32),
            rule,
            ..tipb::ChecksumRequest::default()
        };
        let ranges = if self.table.is_common_handle {
            tidb_ranger::points::full_not_null_range()
        } else {
            tidb_ranger::points::full_int_range(false)
        };
        let mut builder = RequestBuilder::new();
        builder.set_priority(tidb_txnkv::Priority::Low);
        builder.set_handle_ranges(None, table_id, self.table.is_common_handle, &ranges);
        self.finish(builder, &checksum)
    }

    /// Go `buildIndexRequest`.
    fn build_index_request(
        &self,
        table_id: i64,
        index: &IndexInfo,
        old_table_id: i64,
        old_index: Option<&IndexInfo>,
    ) -> Result<TransportRequest> {
        let rule = old_index.map(|old| tipb::ChecksumRewriteRule {
            old_prefix: Some(
                [
                    self.old_keyspace.as_slice(),
                    &tidb_tablecodec::table_key::encode_table_index_prefix(old_table_id, old.id),
                ]
                .concat(),
            ),
            new_prefix: Some(
                [
                    self.new_keyspace.as_slice(),
                    &tidb_tablecodec::table_key::encode_table_index_prefix(table_id, index.id),
                ]
                .concat(),
            ),
        });
        let checksum = tipb::ChecksumRequest {
            scan_on: Some(tipb::ChecksumScanOn::Index as i32),
            algorithm: Some(tipb::ChecksumAlgorithm::Crc64Xor as i32),
            rule,
            ..tipb::ChecksumRequest::default()
        };
        let mut builder = RequestBuilder::new();
        builder.set_priority(tidb_txnkv::Priority::Low);
        builder.set_index_ranges(None, table_id, index.id, &tidb_ranger::points::full_range());
        self.finish(builder, &checksum)
    }

    /// Go's `SetStartTS`, `SetChecksumRequest`, `SetConcurrency`,
    /// `SetResourceGroupName`, `SetRequestSource`, `Build`. Go's builder
    /// starts from a zero `kv.Request`, whose paging is off; the coprocessor
    /// refuses a paged checksum.
    fn finish(
        &self,
        mut builder: RequestBuilder,
        checksum: &tipb::ChecksumRequest,
    ) -> Result<TransportRequest> {
        builder
            .set_start_ts(self.ts)
            .set_checksum_request(checksum.encode_to_vec())
            .set_paging(false)
            .set_concurrency(self.concurrency)
            .set_resource_group_name(self.resource_group_name.clone())
            .set_request_source(self.request_source.clone());
        builder
            .build_transport_request(Arc::new(tidb_txnkv::CancelHandle::default()))
            .map_err(|e| errors::errorf(format!("checksum request: {e:?}")))
    }
}

/// Go `utils.GetPartitionByName`.
///
/// # Errors
/// The table has no such partition.
pub fn get_partition_by_name(table: &TableInfo, name_l: &str) -> Result<i64> {
    let Some(part) = table.get_partition_info() else {
        return Err(errors::errorf(format!(
            "the table {}[id={}] does not have parition",
            table.name.original(),
            table.id
        )));
    };
    let id = part.read().get_partition_id_by_name(name_l);
    if id > 0 {
        return Ok(id);
    }
    Err(errors::errorf(format!(
        "partition is not found in the table {}[id={}]",
        table.name.original(),
        table.id
    )))
}

/// Go `updateChecksumResponse`.
pub fn update_checksum_response(
    resp: &mut tipb::ChecksumResponse,
    update: &tipb::ChecksumResponse,
) {
    resp.checksum = Some(resp.checksum() ^ update.checksum());
    resp.total_kvs = Some(resp.total_kvs().wrapping_add(update.total_kvs()));
    resp.total_bytes = Some(resp.total_bytes().wrapping_add(update.total_bytes()));
}

/// Go `sendChecksumRequest`.
fn send_checksum_request(
    client: &dyn tidb_txnkv::Client,
    req: &TransportRequest,
) -> Result<tipb::ChecksumResponse> {
    let mut res = tidb_distsql::checksum(client, req).map_err(errors::errorf)?;
    let mut resp = tipb::ChecksumResponse::default();
    while let Some(data) = res.next_raw().map_err(|e| errors::errorf(e.to_string()))? {
        let checksum = tipb::ChecksumResponse::decode(data.as_slice())
            .map_err(|e| errors::errorf(e.to_string()))?;
        update_checksum_response(&mut resp, &checksum);
    }
    Ok(resp)
}

/// Go `Executor`.
pub struct Executor {
    reqs: Vec<TransportRequest>,
    backoff_weight: i32,
}

impl Executor {
    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.reqs.len()
    }

    /// Whether there are no requests.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reqs.is_empty()
    }

    /// Go `RawRequests`.
    ///
    /// # Errors
    /// A request does not decode.
    pub fn raw_requests(&self) -> Result<Vec<tipb::ChecksumRequest>> {
        self.reqs
            .iter()
            .map(|r| {
                tipb::ChecksumRequest::decode(r.metadata().data.as_deref().unwrap_or_default())
                    .map_err(|e| errors::errorf(e.to_string()))
            })
            .collect()
    }

    /// Go `Execute`: each request in turn, retried with the checksum
    /// strategy, calling `update` after each.
    ///
    /// # Errors
    /// A request's error after its retries, or `ctx` done.
    pub fn execute(
        &self,
        ctx: &Context,
        client: &dyn tidb_txnkv::Client,
        mut update: impl FnMut(),
    ) -> Result<tipb::ChecksumResponse> {
        let _ = self.backoff_weight;
        let mut total = tipb::ChecksumResponse::default();
        for req in &self.reqs {
            let mut resp = None;
            with_retry(
                ctx,
                || {
                    let r = send_checksum_request(client, req);
                    if tidb_failpoint::eval(
                        "github.com/pingcap/tidb/br/pkg/checksum/checksumRetryErr",
                    )
                    .is_some_and(|v| v.bool())
                    {
                        return Err(errors::errorf("inject checksum error"));
                    }
                    resp = Some(r?);
                    Ok(())
                },
                &mut new_checksum_backoff_strategy(),
            )?;
            update_checksum_response(&mut total, resp.as_ref().unwrap());
            update();
        }
        if let Some(err) = ctx.err() {
            return Err(errors::annotate(
                Arc::new(err),
                "context is cancelled by other error",
            ));
        }
        Ok(total)
    }
}
