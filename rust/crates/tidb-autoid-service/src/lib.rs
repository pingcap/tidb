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

//! Go `pkg/autoid_service`: the AutoID service an `AUTO_ID_CACHE 1` table
//! allocates from. One TiDB, the owner of `tidb/autoid/leader`, serves
//! `AllocAutoID` / `Rebase` over gRPC on its status port, caching a batch of
//! each table's IDs read from the table's `IncrementID` meta key; on a store
//! without a cluster ([`Service::mock_for_test`], Go `MockForTest`) the
//! process serves itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once, PoisonError};
use std::time::Duration;

use tidb_autoid::cluster_auto_id::{ClusterAutoIdStore, AUTO_ID_LEADER_PATH};
use tidb_autoid::cluster_auto_id::{
    AutoIdServiceAllocRequest, AutoIdServiceRebaseRequest, AutoIdServiceRpc, AutoIdServiceRpcError,
};
use tidb_autoid::AutoIdCall;
use tidb_txnkv::kv_storage::Storage;
use tikv_client::proto::autoid;

/// Go `errAutoincReadFailed`.
const ERR_AUTOINC_READ_FAILED: &str = "auto increment action failed";

/// Go `batch`: the IDs each meta transaction reserves.
const BATCH: i64 = 4000;

/// How long each meta read and the commit may take.
const META_TIMEOUT: Duration = Duration::from_secs(10);

/// Go `autoIDKey`.
type AutoIdKey = (i64, i64);

/// Go `autoIDValue` (its mutex is the map's `Arc<Mutex<_>>`).
#[derive(Default)]
struct AutoIdValue {
    base: i64,
    end: i64,
}

/// Go `seekToFirstAutoIDSigned`.
fn seek_to_first_auto_id_signed(base: i64, increment: i64, offset: i64) -> i64 {
    let nr = base.wrapping_add(increment).wrapping_sub(offset) / increment;
    nr.wrapping_mul(increment).wrapping_add(offset)
}

/// Go `seekToFirstAutoIDUnSigned`.
fn seek_to_first_auto_id_unsigned(base: u64, increment: u64, offset: u64) -> u64 {
    let nr = base.wrapping_add(increment).wrapping_sub(offset) / increment;
    nr.wrapping_mul(increment).wrapping_add(offset)
}

/// Go `calcNeededBatchSize`.
fn calc_needed_batch_size(base: i64, n: i64, increment: i64, offset: i64, is_unsigned: bool) -> i64 {
    if increment == 1 {
        return n;
    }
    if is_unsigned {
        // SeekToFirstAutoIDUnSigned seeks to the next unsigned valid position.
        let mut nr = seek_to_first_auto_id_unsigned(base as u64, increment as u64, offset as u64);
        // calculate the total batch size needed.
        nr = nr.wrapping_add((n as u64).wrapping_sub(1).wrapping_mul(increment as u64));
        return nr.wrapping_sub(base as u64) as i64;
    }
    let mut nr = seek_to_first_auto_id_signed(base, increment, offset);
    // calculate the total batch size needed.
    nr = nr.wrapping_add((n - 1).wrapping_mul(increment));
    nr.wrapping_sub(base)
}

fn log_info(message: &str, fields: Vec<tidb_log::Field>) {
    let mut fields = fields;
    fields.push(tidb_log::Field::new(
        "category",
        tidb_log::Value::Str("autoid service".to_owned()),
    ));
    tidb_logutil::bg_logger().info(message, &fields);
}

fn i64_field(name: &str, value: i64) -> tidb_log::Field {
    tidb_log::Field::new(name, tidb_log::Value::I64(value))
}

impl AutoIdValue {
    /// Go `alloc4Unsigned`.
    fn alloc4_unsigned(
        &mut self,
        counter: &ClusterAutoIdStore,
        db_id: i64,
        tbl_id: i64,
        n: u64,
        increment: i64,
        offset: i64,
    ) -> Result<(i64, i64), String> {
        // Check offset rebase if necessary.
        if offset.wrapping_sub(1) as u64 > self.base as u64 {
            self.rebase4_unsigned(counter, db_id, tbl_id, offset.wrapping_sub(1) as u64)?;
        }
        // calcNeededBatchSize calculates the total batch size needed.
        let mut n1 = calc_needed_batch_size(self.base, n as i64, increment, offset, true);
        if u64::MAX - self.base as u64 <= n1 as u64 {
            return Err(ERR_AUTOINC_READ_FAILED.to_owned());
        }
        // The local rest is not enough for alloc.
        if (self.base as u64).wrapping_add(n1 as u64) > self.end as u64 || self.base == 0 {
            let from_base = self.base;
            let (base, end) = (self.base, self.end);
            let (new_base, new_end, needed) = counter
                .run_in_new_txn(|new_base| {
                    let mut n1 = n1;
                    if base == 0 || new_base != end {
                        n1 = calc_needed_batch_size(new_base, n as i64, increment, offset, true);
                    }
                    // Although the step is customized by user, we still need
                    // to make sure nextStep is big enough for insert batch.
                    let next_step = BATCH.max(n1);
                    let tmp_step = (u64::MAX - new_base as u64).min(next_step as u64) as i64;
                    // The global rest is not enough for alloc.
                    if tmp_step < n1 {
                        return (None, Err(ERR_AUTOINC_READ_FAILED.to_owned()));
                    }
                    let new_end = new_base.wrapping_add(tmp_step);
                    (Some(new_end), Ok((new_base, new_end, n1)))
                })
                .map_err(|error| error.to_string())??;
            if base == 0 || new_base != end {
                self.base = new_base;
                self.end = new_base;
                n1 = needed;
            }
            if new_base as u64 == u64::MAX {
                return Err(ERR_AUTOINC_READ_FAILED.to_owned());
            }
            log_info(
                "alloc4Unsigned from",
                vec![
                    i64_field("dbID", db_id),
                    i64_field("tblID", tbl_id),
                    i64_field("from base", from_base),
                    i64_field("from end", self.end),
                    i64_field("to base", new_base),
                    i64_field("to end", new_end),
                ],
            );
            self.end = new_end;
        }
        let minv = self.base;
        // Use uint64 n directly.
        self.base = (self.base as u64).wrapping_add(n1 as u64) as i64;
        Ok((minv, self.base))
    }

    /// Go `alloc4Signed`.
    fn alloc4_signed(
        &mut self,
        counter: &ClusterAutoIdStore,
        db_id: i64,
        tbl_id: i64,
        n: u64,
        increment: i64,
        offset: i64,
    ) -> Result<(i64, i64), String> {
        // Check offset rebase if necessary.
        if offset.wrapping_sub(1) > self.base {
            self.rebase4_signed(counter, db_id, tbl_id, offset.wrapping_sub(1))?;
        }
        // calcNeededBatchSize calculates the total batch size needed.
        let mut n1 = calc_needed_batch_size(self.base, n as i64, increment, offset, false);
        // Condition alloc.base+N1 > alloc.end will overflow when alloc.base +
        // N1 > MaxInt64. So need this.
        if i64::MAX.wrapping_sub(self.base) <= n1 {
            return Err(ERR_AUTOINC_READ_FAILED.to_owned());
        }
        // The local rest is not enough for allocN. If alloc.base is 0, the
        // alloc may not be initialized, force fetch from remote.
        if self.base.wrapping_add(n1) > self.end || self.base == 0 {
            let from_base = self.base;
            let (base, end) = (self.base, self.end);
            let (new_base, new_end, needed) = counter
                .run_in_new_txn(|new_base| {
                    let mut n1 = n1;
                    // alloc.base == 0 means uninitialized; newBase !=
                    // alloc.end means something abnormal, maybe transaction
                    // conflict and retry?
                    if base == 0 || new_base != end {
                        n1 = calc_needed_batch_size(new_base, n as i64, increment, offset, false);
                    }
                    let next_step = BATCH.max(n1);
                    let tmp_step = i64::MAX.wrapping_sub(new_base).min(next_step);
                    // The global rest is not enough for alloc.
                    if tmp_step < n1 {
                        return (None, Err(ERR_AUTOINC_READ_FAILED.to_owned()));
                    }
                    let new_end = new_base.wrapping_add(tmp_step);
                    (Some(new_end), Ok((new_base, new_end, n1)))
                })
                .map_err(|error| error.to_string())??;
            if base == 0 || new_base != end {
                self.base = new_base;
                self.end = new_base;
                n1 = needed;
            }
            if new_base == i64::MAX {
                return Err(ERR_AUTOINC_READ_FAILED.to_owned());
            }
            log_info(
                "alloc4Signed from",
                vec![
                    i64_field("dbID", db_id),
                    i64_field("tblID", tbl_id),
                    i64_field("from base", from_base),
                    i64_field("from end", self.end),
                    i64_field("to base", new_base),
                    i64_field("to end", new_end),
                ],
            );
            self.end = new_end;
        }
        let minv = self.base;
        self.base = self.base.wrapping_add(n1);
        Ok((minv, self.base))
    }

    /// Go `rebase4Unsigned`.
    fn rebase4_unsigned(
        &mut self,
        counter: &ClusterAutoIdStore,
        db_id: i64,
        tbl_id: i64,
        required_base: u64,
    ) -> Result<(), String> {
        // Satisfied by alloc.base, nothing to do.
        if required_base <= self.base as u64 {
            return Ok(());
        }
        // Satisfied by alloc.end, need to update alloc.base.
        if required_base <= self.end as u64 {
            self.base = required_base as i64;
            return Ok(());
        }
        let (old_value, new_base, new_end) = counter
            .run_in_new_txn(|current_end| {
                let u_current_end = current_end as u64;
                let new_base = u_current_end.max(required_base);
                let new_end = (u64::MAX - BATCH as u64).min(new_base) + BATCH as u64;
                (Some(new_end as i64), (current_end, new_base, new_end))
            })
            .map_err(|error| error.to_string())?;
        log_info(
            "rebase4Unsigned from",
            vec![
                i64_field("dbID", db_id),
                i64_field("tblID", tbl_id),
                i64_field("from", old_value),
                tidb_log::Field::new("to", tidb_log::Value::U64(new_end)),
            ],
        );
        self.base = new_base as i64;
        self.end = new_end as i64;
        Ok(())
    }

    /// Go `rebase4Signed`.
    fn rebase4_signed(
        &mut self,
        counter: &ClusterAutoIdStore,
        db_id: i64,
        tbl_id: i64,
        required_base: i64,
    ) -> Result<(), String> {
        // Satisfied by alloc.base, nothing to do.
        if required_base <= self.base {
            return Ok(());
        }
        // Satisfied by alloc.end, need to update alloc.base.
        if required_base <= self.end {
            self.base = required_base;
            return Ok(());
        }
        let (old_value, new_base, new_end) = counter
            .run_in_new_txn(|current_end| {
                let new_base = current_end.max(required_base);
                let new_end = (i64::MAX - BATCH).min(new_base) + BATCH;
                (Some(new_end), (current_end, new_base, new_end))
            })
            .map_err(|error| error.to_string())?;
        log_info(
            "rebase4Signed from",
            vec![
                i64_field("dbID", db_id),
                i64_field("tblID", tbl_id),
                i64_field("from", old_value),
                i64_field("to", new_end),
            ],
        );
        self.base = new_base;
        self.end = new_end;
        Ok(())
    }

    /// Go `forceRebase`.
    fn force_rebase(
        &mut self,
        counter: &ClusterAutoIdStore,
        db_id: i64,
        tbl_id: i64,
        required_base: i64,
        is_unsigned: bool,
    ) -> Result<(), String> {
        let old_value = counter
            .run_in_new_txn(|current_end| {
                // Go `idAcc.Inc(step)` with the step computed in the
                // variable's domain: it wraps either way, so the stored value
                // becomes `requiredBase`.
                (Some(required_base), current_end)
            })
            .map_err(|error| error.to_string())?;
        log_info(
            "forceRebase from",
            vec![
                i64_field("dbID", db_id),
                i64_field("tblID", tbl_id),
                i64_field("from", old_value),
                i64_field("to", required_base),
                tidb_log::Field::new("isUnsigned", tidb_log::Value::Bool(is_unsigned)),
            ],
        );
        self.base = required_base;
        self.end = required_base;
        Ok(())
    }
}

type AutoIdMap = Arc<Mutex<HashMap<AutoIdKey, Arc<Mutex<AutoIdValue>>>>>;

/// Go `Service`: the gRPC `AutoIDAlloc` service.
pub struct Service {
    auto_id_map: AutoIdMap,
    leadership: Option<Arc<dyn tidb_owner::Manager>>,
    store: Arc<dyn Storage>,
    /// Go `s.store.GetCodec().GetKeyspaceID()`; `u32::MAX` is Nullspace.
    keyspace_id: u32,
    /// Go `cancel`: the owner manager's context, canceled on `Close` before
    /// the campaign is joined.
    owner_ctx: Option<tidb_owner::Context>,
    /// Go `closeOnce`.
    close_once: Once,
}

/// Go `ownerListener`.
struct OwnerListener {
    auto_id_map: AutoIdMap,
    self_addr: String,
}

impl tidb_owner::Listener for OwnerListener {
    fn on_become_owner(&self) {
        // Reset the map to avoid a case that a node lose leadership and
        // regain it, then improperly use the stale map to serve the autoid
        // requests (pingcap/tidb#52600).
        self.auto_id_map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        log_info(
            "leader change of autoid service, this node become owner",
            vec![tidb_log::Field::new("addr", tidb_log::Value::Str(self.self_addr.clone()))],
        );
    }

    fn on_retire_owner(&self) {}
}

impl Service {
    /// Go `New`: the service campaigning for `tidb/autoid/leader` through
    /// `owner_store` with `self_addr` as its ID, which is the address the
    /// AutoID clients dial.
    ///
    /// # Errors
    ///
    /// The campaign cannot start.
    pub fn new(
        self_addr: &str,
        owner_store: Arc<dyn tidb_owner::OwnerStore>,
        store: Arc<dyn Storage>,
        keyspace_id: u32,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_context(&tidb_gocontext::Context::background(), self_addr, owner_store, store, keyspace_id)
    }

    /// Go `NewWithContext` (e52b3131151): `ctx` may cancel the service's
    /// initialization. Once it succeeds, `ctx` no longer controls the
    /// service; the caller must close it.
    ///
    /// # Errors
    ///
    /// The campaign cannot start, or `ctx` ended during it.
    pub fn new_with_context(
        ctx: &tidb_gocontext::Context,
        self_addr: &str,
        owner_store: Arc<dyn tidb_owner::OwnerStore>,
        store: Arc<dyn Storage>,
        keyspace_id: u32,
    ) -> Result<Arc<Self>, String> {
        // Go `newWithCli`: the owner manager runs under its own context,
        // which startup cancellation reaches only until construction
        // completes.
        let owner_ctx = tidb_owner::Context::cancelable();
        let leadership: Arc<dyn tidb_owner::Manager> = Arc::new(tidb_owner::OwnerManager::new(
            owner_ctx.clone(),
            owner_store,
            "autoid",
            self_addr,
            AUTO_ID_LEADER_PATH,
        ));
        Self::new_with_manager_context(ctx, self_addr, leadership, Some(owner_ctx), store, keyspace_id)
    }

    /// Go `newWithCli`: the service over `leadership`, which it campaigns
    /// with (10-second lease) after installing its owner listener.
    ///
    /// # Errors
    ///
    /// The campaign cannot start.
    pub fn new_with_manager(
        self_addr: &str,
        leadership: Arc<dyn tidb_owner::Manager>,
        store: Arc<dyn Storage>,
        keyspace_id: u32,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_manager_context(
            &tidb_gocontext::Context::background(),
            self_addr,
            leadership,
            None,
            store,
            keyspace_id,
        )
    }

    /// Go `newWithCli`'s body: a failed or canceled campaign closes the
    /// service, revoking any ownership it gained, and answers the error.
    fn new_with_manager_context(
        ctx: &tidb_gocontext::Context,
        self_addr: &str,
        leadership: Arc<dyn tidb_owner::Manager>,
        owner_ctx: Option<tidb_owner::Context>,
        store: Arc<dyn Storage>,
        keyspace_id: u32,
    ) -> Result<Arc<Self>, String> {
        let auto_id_map = AutoIdMap::default();
        leadership.set_listener(Arc::new(OwnerListener {
            auto_id_map: Arc::clone(&auto_id_map),
            self_addr: self_addr.to_owned(),
        }));
        let mut service = Arc::new(Self {
            auto_id_map,
            leadership: Some(Arc::clone(&leadership)),
            store,
            keyspace_id,
            owner_ctx,
            close_once: Once::new(),
        });
        // 10 means that autoid service's etcd lease is 10s.
        let mut result = leadership.campaign_owner(&[10]);
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/autoid_service/afterAutoIDCampaign", &mut service);
        if let Some(error) = ctx.err() {
            result = Err(error.to_string());
        }
        if let Err(error) = result {
            service.close();
            return Err(error);
        }
        Ok(service)
    }

    /// Go `MockForTest`: the service a store without a cluster serves
    /// itself, with no leadership.
    #[must_use]
    pub fn mock_for_test(store: Arc<dyn Storage>, keyspace_id: u32) -> Arc<Self> {
        Arc::new(Self {
            auto_id_map: AutoIdMap::default(),
            leadership: None,
            store,
            keyspace_id,
            owner_ctx: None,
            close_once: Once::new(),
        })
    }

    /// Go `Close`: once, canceling the election's context before joining
    /// the campaign.
    pub fn close(&self) {
        self.close_once.call_once(|| {
            if let Some(owner_ctx) = &self.owner_ctx {
                owner_ctx.cancel();
            }
            if let Some(leadership) = &self.leadership {
                leadership.close();
            }
        });
    }

    /// Go `IsOwner`.
    #[must_use]
    pub fn is_owner(&self) -> bool {
        self.leadership.as_ref().is_some_and(|leadership| leadership.is_owner())
    }

    fn counter(&self, db_id: i64, tbl_id: i64) -> ClusterAutoIdStore {
        // Go `GetAutoIDAccessors(dbID, tblID).IncrementID(TableInfoVersion5)`.
        ClusterAutoIdStore::over_key(
            Arc::clone(&self.store),
            tidb_meta::key::auto_increment_id_kv_key(db_id, tbl_id),
            META_TIMEOUT,
        )
    }

    /// Go `getAlloc`.
    fn get_alloc(&self, db_id: i64, tbl_id: i64) -> Arc<Mutex<AutoIdValue>> {
        Arc::clone(
            self.auto_id_map
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry((db_id, tbl_id))
                .or_default(),
        )
    }

    fn not_leader(&self, what: &str) -> Option<String> {
        if self.leadership.as_ref().is_some_and(|leadership| !leadership.is_owner()) {
            log_info(&format!("{what} fail, not leader"), Vec::new());
            return Some("not leader".to_owned());
        }
        None
    }

    /// Go `AllocAutoID`.
    ///
    /// # Errors
    ///
    /// The request names another keyspace, or this node is not the leader.
    pub fn alloc_auto_id(&self, req: &autoid::AutoIdRequest) -> Result<autoid::AutoIdResponse, String> {
        let request_keyspace_id = match req.keyspace {
            Some(autoid::auto_id_request::Keyspace::KeyspaceId(id)) => id,
            _ => 0,
        };
        if request_keyspace_id != self.keyspace_id {
            log_info(
                "Current service is not request keyspace leader.",
                vec![
                    tidb_log::Field::new("req-keyspace-id", tidb_log::Value::U64(u64::from(request_keyspace_id))),
                    tidb_log::Field::new("service-keyspace-id", tidb_log::Value::U64(u64::from(self.keyspace_id))),
                ],
            );
            return Err("not leader".to_owned());
        }
        self.alloc(req)
    }

    /// Go `allocAutoID`.
    fn alloc(&self, req: &autoid::AutoIdRequest) -> Result<autoid::AutoIdResponse, String> {
        if let Some(error) = self.not_leader("Alloc AutoID") {
            return Err(error);
        }
        let val = self.get_alloc(req.db_id, req.tbl_id);
        let mut val = val.lock().unwrap_or_else(PoisonError::into_inner);
        let counter = self.counter(req.db_id, req.tbl_id);
        if req.n == 0 {
            if val.base != 0 {
                return Ok(autoid::AutoIdResponse {
                    min: val.base,
                    max: val.base,
                    ..Default::default()
                });
            }
            // This item is not initialized, get the data from remote.
            return Ok(match counter.run_in_new_txn(|current_end| (None, current_end)) {
                Ok(current_end) => {
                    val.base = current_end;
                    val.end = current_end;
                    autoid::AutoIdResponse {
                        min: current_end,
                        max: current_end,
                        ..Default::default()
                    }
                }
                Err(error) => autoid::AutoIdResponse {
                    errmsg: error.to_string().into_bytes().into(),
                    ..Default::default()
                },
            });
        }
        let allocated = if req.is_unsigned {
            val.alloc4_unsigned(&counter, req.db_id, req.tbl_id, req.n, req.increment, req.offset)
        } else {
            val.alloc4_signed(&counter, req.db_id, req.tbl_id, req.n, req.increment, req.offset)
        };
        Ok(match allocated {
            Ok((minv, maxv)) => autoid::AutoIdResponse {
                min: minv,
                max: maxv,
                ..Default::default()
            },
            Err(error) => autoid::AutoIdResponse {
                errmsg: error.into_bytes().into(),
                ..Default::default()
            },
        })
    }

    /// Go `Rebase`.
    ///
    /// # Errors
    ///
    /// This node is not the leader.
    pub fn rebase(&self, req: &autoid::RebaseRequest) -> Result<autoid::RebaseResponse, String> {
        if let Some(error) = self.not_leader("Rebase()") {
            return Err(error);
        }
        let val = self.get_alloc(req.db_id, req.tbl_id);
        let mut val = val.lock().unwrap_or_else(PoisonError::into_inner);
        let counter = self.counter(req.db_id, req.tbl_id);
        let errmsg = |error: String| autoid::RebaseResponse {
            errmsg: error.into_bytes().into(),
            ..Default::default()
        };
        if req.force {
            if let Err(error) =
                val.force_rebase(&counter, req.db_id, req.tbl_id, req.base, req.is_unsigned)
            {
                return Ok(errmsg(error));
            }
        }
        let rebased = if req.is_unsigned {
            val.rebase4_unsigned(&counter, req.db_id, req.tbl_id, req.base as u64)
        } else {
            val.rebase4_signed(&counter, req.db_id, req.tbl_id, req.base)
        };
        Ok(match rebased {
            Ok(()) => autoid::RebaseResponse::default(),
            Err(error) => errmsg(error),
        })
    }
}

/// Go's gRPC registration (`autoid.RegisterAutoIDAllocServer`): the service
/// as the status port's `AutoIDAlloc` server.
#[derive(Clone)]
pub struct GrpcService(pub Arc<Service>);

#[tonic::async_trait]
impl autoid::auto_id_alloc_server::AutoIdAlloc for GrpcService {
    async fn alloc_auto_id(
        &self,
        request: tonic::Request<autoid::AutoIdRequest>,
    ) -> Result<tonic::Response<autoid::AutoIdResponse>, tonic::Status> {
        let service = Arc::clone(&self.0);
        let request = request.into_inner();
        // The meta transaction blocks on the store's own runtime.
        let answer = run_blocking(move || service.alloc_auto_id(&request)).await?;
        answer.map(tonic::Response::new).map_err(tonic::Status::unknown)
    }

    async fn rebase(
        &self,
        request: tonic::Request<autoid::RebaseRequest>,
    ) -> Result<tonic::Response<autoid::RebaseResponse>, tonic::Status> {
        let service = Arc::clone(&self.0);
        let request = request.into_inner();
        let answer = run_blocking(move || service.rebase(&request)).await?;
        answer.map(tonic::Response::new).map_err(tonic::Status::unknown)
    }
}

/// Runs `work` on a thread of its own, outside the server's async runtime
/// (the store's sync client refuses to run inside one).
async fn run_blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Result<T, tonic::Status> {
    let (respond, response) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("autoid-service".into())
        .spawn(move || {
            let _ = respond.send(work());
        })
        .map_err(|error| tonic::Status::internal(error.to_string()))?;
    response
        .await
        .map_err(|_| tonic::Status::internal("the autoid service handler panicked"))
}

/// Go `mockClient`: the in-process service as an `AutoIDAllocClient`.
impl AutoIdServiceRpc for Service {
    fn alloc_auto_id(
        &self,
        _call: &AutoIdCall,
        request: AutoIdServiceAllocRequest,
    ) -> Result<(i64, i64), AutoIdServiceRpcError> {
        let response = Service::alloc_auto_id(
            self,
            &autoid::AutoIdRequest {
                db_id: request.db_id,
                tbl_id: request.table_id,
                is_unsigned: request.unsigned,
                n: request.n,
                increment: request.increment,
                offset: request.offset,
                keyspace: Some(autoid::auto_id_request::Keyspace::KeyspaceId(request.keyspace_id)),
            },
        )
        .map_err(AutoIdServiceRpcError::Rpc)?;
        if !response.errmsg.is_empty() {
            return Err(AutoIdServiceRpcError::Other(
                String::from_utf8_lossy(&response.errmsg).into_owned(),
            ));
        }
        Ok((response.min, response.max))
    }

    fn rebase(
        &self,
        _call: &AutoIdCall,
        request: AutoIdServiceRebaseRequest,
    ) -> Result<(), AutoIdServiceRpcError> {
        let response = Service::rebase(
            self,
            &autoid::RebaseRequest {
                db_id: request.db_id,
                tbl_id: request.table_id,
                is_unsigned: request.unsigned,
                base: request.new_base,
                force: request.force,
                keyspace: None,
            },
        )
        .map_err(AutoIdServiceRpcError::Rpc)?;
        if !response.errmsg.is_empty() {
            return Err(AutoIdServiceRpcError::Other(
                String::from_utf8_lossy(&response.errmsg).into_owned(),
            ));
        }
        Ok(())
    }
}

/// Go `autoid_test.go`.
#[cfg(test)]
mod autoid_test {
    use super::*;

    /// Go's `NullspaceID` keyspace of a classic store.
    const NULLSPACE: u32 = u32::MAX;

    type Dest = (i64, i64);

    fn store() -> Arc<dyn Storage> {
        let (_pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        Arc::new(opener)
    }

    /// An owner manager whose campaign wins or fails as told, recording
    /// its `Close` calls (Go's lease revocation).
    struct CampaignManager {
        fail: Option<&'static str>,
        owner: std::sync::atomic::AtomicBool,
        closes: std::sync::atomic::AtomicUsize,
    }

    impl CampaignManager {
        fn new(fail: Option<&'static str>) -> Arc<Self> {
            Arc::new(Self {
                fail,
                owner: std::sync::atomic::AtomicBool::new(false),
                closes: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        fn closes(&self) -> usize {
            self.closes.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl tidb_owner::Manager for CampaignManager {
        fn id(&self) -> &str {
            "candidate"
        }
        fn is_owner(&self) -> bool {
            self.owner.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn retire_owner(&self) {
            self.owner.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        fn get_owner_id(&self, _context: &tidb_owner::Context) -> Result<String, String> {
            Ok("candidate".to_owned())
        }
        fn set_owner_op_value(&self, _context: &tidb_owner::Context, _op: tidb_owner::OpType) -> Result<(), String> {
            Ok(())
        }
        fn campaign_owner(&self, _with_ttl: &[i64]) -> Result<(), String> {
            if let Some(error) = self.fail {
                return Err(error.to_owned());
            }
            self.owner.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn campaign_cancel(&self) {}
        fn break_campaign_loop(&self) {}
        fn resign_owner(&self, _context: &tidb_owner::Context) -> Result<(), String> {
            Ok(())
        }
        fn close(&self) {
            self.owner.store(false, std::sync::atomic::Ordering::SeqCst);
            self.closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn set_listener(&self, _listener: Arc<dyn tidb_owner::Listener>) {}
        fn force_to_be_owner(&self, _context: &tidb_owner::Context) -> Result<(), String> {
            Ok(())
        }
    }

    /// Go master e52b3131151's `TestGRPC` subtests "canceled startup
    /// revokes lease" and the campaign error path: a service whose startup
    /// was canceled after it won ownership, or whose campaign failed, is
    /// closed (its ownership revoked) and answers the error; once built,
    /// startup cancellation no longer controls it and `Close` runs once.
    #[test]
    fn test_canceled_or_failed_startup_closes_the_service() {
        // canceled startup revokes ownership
        let manager = CampaignManager::new(None);
        let (ctx, cancel) = tidb_gocontext::Context::with_cancel(&tidb_gocontext::Context::background());
        let cancel = std::sync::Mutex::new(Some(cancel));
        let _guard = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/autoid_service/afterAutoIDCampaign",
            move |service| {
                let service = service.downcast_mut::<Arc<Service>>().unwrap();
                assert!(service.is_owner());
                if let Some(cancel) = cancel.lock().unwrap().take() {
                    cancel.cancel();
                }
            },
        );
        let error = Service::new_with_manager_context(&ctx, "addr", manager.clone(), None, store(), NULLSPACE)
            .err()
            .expect("canceled startup fails");
        assert_eq!(error, "context canceled");
        assert_eq!(manager.closes(), 1);
        assert!(!tidb_owner::Manager::is_owner(manager.as_ref()));
        drop(_guard);

        // a failed campaign closes the service
        let manager = CampaignManager::new(Some("campaign failed"));
        let error = Service::new_with_manager(
            "addr",
            Arc::clone(&manager) as Arc<dyn tidb_owner::Manager>,
            store(),
            NULLSPACE,
        )
        .err()
        .expect("a failed campaign fails");
        assert_eq!(error, "campaign failed");
        assert_eq!(manager.closes(), 1);

        // once built, cancellation does not control the service; Close
        // runs once
        let manager = CampaignManager::new(None);
        let (ctx, cancel) = tidb_gocontext::Context::with_cancel(&tidb_gocontext::Context::background());
        let service = Service::new_with_manager_context(&ctx, "addr", manager.clone(), None, store(), NULLSPACE).unwrap();
        cancel.cancel();
        assert!(service.is_owner());
        assert_eq!(manager.closes(), 0);
        service.close();
        service.close();
        assert_eq!(manager.closes(), 1);
        assert!(!service.is_owner());
    }

    fn auto_id_request(
        cli: &Service,
        to: Dest,
        unsigned: bool,
        n: u64,
        keyspace_id: u32,
        more: &[i64],
    ) -> autoid::AutoIdResponse {
        let increment = more.first().copied().unwrap_or(1);
        let offset = more.get(1).copied().unwrap_or(1);
        cli.alloc_auto_id(&autoid::AutoIdRequest {
            db_id: to.0,
            tbl_id: to.1,
            is_unsigned: unsigned,
            n,
            increment,
            offset,
            keyspace: Some(autoid::auto_id_request::Keyspace::KeyspaceId(keyspace_id)),
        })
        .unwrap()
    }

    fn check(response: &autoid::AutoIdResponse, minv: i64, maxv: i64) {
        assert_eq!(
            (response.min, response.max, response.errmsg.len()),
            (minv, maxv, 0),
            "{:?}",
            String::from_utf8_lossy(&response.errmsg)
        );
    }

    fn check_errmsg(response: &autoid::AutoIdResponse) {
        assert!(!response.errmsg.is_empty(), "{response:?}");
    }

    fn rebase_request(cli: &Service, to: Dest, unsigned: bool, n: i64, force: bool) -> String {
        let response = cli
            .rebase(&autoid::RebaseRequest {
                db_id: to.0,
                tbl_id: to.1,
                base: n,
                is_unsigned: unsigned,
                force,
                keyspace: None,
            })
            .unwrap();
        String::from_utf8_lossy(&response.errmsg).into_owned()
    }

    fn check_curr_value(cli: &Service, to: Dest, minv: i64, maxv: i64, keyspace_id: u32) {
        check(&auto_id_request(cli, to, false, 0, keyspace_id, &[]), minv, maxv);
    }

    /// Go `TestConcurrent`.
    #[test]
    fn test_concurrent() {
        let cli = Service::mock_for_test(store(), NULLSPACE);
        let to = (2, 102);
        const CONCURRENCY: i64 = 30;
        // Rebase to some value
        assert_eq!(rebase_request(&cli, to, false, 666, true), "");
        check_curr_value(&cli, to, 666, 666, NULLSPACE);
        // And +1 concurrently for 30 times
        std::thread::scope(|scope| {
            for _ in 0..CONCURRENCY {
                let cli = Arc::clone(&cli);
                scope.spawn(move || auto_id_request(&cli, to, false, 1, NULLSPACE, &[]));
            }
        });
        // Check the result is increased by 30
        check_curr_value(&cli, to, 666 + CONCURRENCY, 666 + CONCURRENCY, NULLSPACE);
    }

    /// Go `TestAPI` (`testAPIWithKeyspace(t, nil)`, the classic kernel).
    #[test]
    fn test_api() {
        let cli = Service::mock_for_test(store(), NULLSPACE);
        let to = (2, 104);
        let ks = NULLSPACE;

        // basic auto id operation
        check(&auto_id_request(&cli, to, false, 1, ks, &[]), 0, 1);
        check(&auto_id_request(&cli, to, false, 10, ks, &[]), 1, 11);
        check_curr_value(&cli, to, 11, 11, ks);
        check(&auto_id_request(&cli, to, false, 128, ks, &[]), 11, 139);
        check(&auto_id_request(&cli, to, false, 1, ks, &[10, 5]), 139, 145);

        // basic rebase operation
        assert_eq!(rebase_request(&cli, to, false, 666, false), "");
        check(&auto_id_request(&cli, to, false, 1, ks, &[]), 666, 667);

        assert_eq!(rebase_request(&cli, to, false, 6666, false), "");
        check(&auto_id_request(&cli, to, false, 1, ks, &[]), 6666, 6667);

        // rebase will not decrease the value without 'force'
        assert_eq!(rebase_request(&cli, to, false, 44, false), "");
        check_curr_value(&cli, to, 6667, 6667, ks);
        assert_eq!(rebase_request(&cli, to, false, 44, true), "");
        check_curr_value(&cli, to, 44, 44, ks);

        // max increase 1
        assert_eq!(rebase_request(&cli, to, false, i64::MAX, true), "");
        check_curr_value(&cli, to, i64::MAX, i64::MAX, ks);
        check_errmsg(&auto_id_request(&cli, to, false, 1, ks, &[]));

        assert_eq!(rebase_request(&cli, to, true, 0, true), "");
        check_curr_value(&cli, to, 0, 0, ks);
        check(&auto_id_request(&cli, to, true, 1, ks, &[]), 0, 1);
        check(&auto_id_request(&cli, to, true, 10, ks, &[]), 1, 11);
        check(&auto_id_request(&cli, to, true, 128, ks, &[]), 11, 139);
        check(&auto_id_request(&cli, to, true, 1, ks, &[10, 5]), 139, 145);

        // max increase 1
        assert_eq!(rebase_request(&cli, to, true, i64::MAX, false), "");
        check_curr_value(&cli, to, i64::MAX, i64::MAX, ks);
        check(&auto_id_request(&cli, to, true, 1, ks, &[]), i64::MAX, i64::MIN);
        check(&auto_id_request(&cli, to, true, 1, ks, &[]), i64::MIN, i64::MIN + 1);

        assert_eq!(rebase_request(&cli, to, true, -1, false), "");
        check_curr_value(&cli, to, -1, -1, ks);
        // rebase to max value, the next request should fail
        check_errmsg(&auto_id_request(&cli, to, true, 1, ks, &[]));
    }

    /// Go `TestGRPC`: the elected service answers over gRPC. Go campaigns on
    /// an embedded etcd cluster; here the owner crate's mock manager elects.
    #[test]
    fn test_grpc() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let manager: Arc<dyn tidb_owner::Manager> = Arc::new(tidb_owner::MockManager::new(
            tidb_owner::Context::background(),
            addr.clone(),
            Some("autoid_test_grpc"),
            AUTO_ID_LEADER_PATH,
        ));
        let service = Service::new_with_manager(&addr, manager, store(), NULLSPACE).unwrap();
        let mut i = 0;
        while !service.is_owner() {
            std::thread::sleep(Duration::from_millis(100));
            i += 1;
            if i >= 20 {
                break;
            }
        }
        assert!(i < 20);

        let server = autoid::auto_id_alloc_server::AutoIdAllocServer::new(GrpcService(Arc::clone(&service)));
        runtime.spawn(
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let response = runtime.block_on(async {
            let mut cli = autoid::auto_id_alloc_client::AutoIdAllocClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
            cli.alloc_auto_id(autoid::AutoIdRequest {
                db_id: 0,
                tbl_id: 0,
                n: 1,
                increment: 1,
                offset: 1,
                is_unsigned: false,
                keyspace: Some(autoid::auto_id_request::Keyspace::KeyspaceId(NULLSPACE)),
            })
            .await
        });
        assert!(response.is_ok(), "{response:?}");
        service.close();
    }
}
