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

//! Go `pkg/domain/sqlsvrapi`: the SQL server's runtime view of a keyspace,
//! through its store and its internal sessions.
//!
//! Go's session pool hands out `pools.Resource`, which each user asserts to
//! `sessionctx.Context`; here the pool's session type `C` is the view its
//! user declares (DXF's `tidb_dxf_storage::SessionContext`). Not ported yet,
//! with the callers that need them: `Runtime.AlterTableMode` (`IMPORT
//! INTO`) and `Server.GetDDLOwnerMgr` (DXF's status port, row O15).

use std::sync::Arc;

/// Go `Runtime`: the view for reaching a keyspace through its store and
/// sessions.
pub trait Runtime<C: ?Sized>: Send + Sync {
    /// Go `Store().GetKeyspace()`: the keyspace of the runtime's store.
    /// The narrowed Rust `kv.Storage` carries no keyspace, so the runtime
    /// answers for its store.
    fn store_keyspace(&self) -> String;
    /// Go `SysSessionPool`: the keyspace's internal sessions.
    fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<C>>;
}

/// Go `KSRuntimeHandle`: an acquired runtime of a target keyspace.
pub trait KsRuntimeHandle<C: ?Sized>: Runtime<C> {
    /// Go `Release`: ends this holding of the runtime. The handle is a view
    /// and does not manage the runtime's lifecycle; it is unusable after.
    fn release(&self);
    /// The handle as its runtime view.
    fn as_runtime(self: Arc<Self>) -> Arc<dyn Runtime<C>>;
}

/// Go `Server`: the SQL server, which manages nearly everything related to
/// SQL execution.
pub trait Server<C: ?Sized>: Send + Sync {
    /// Go `GetRuntime`: the runtime of the current instance.
    fn get_runtime(&self) -> Arc<dyn Runtime<C>>;
    /// Go `AcquireKSRuntime`: a runtime handle of the target keyspace, to
    /// release after use. Only next-gen reaches keyspaces other than the
    /// current one.
    ///
    /// # Errors
    /// The keyspace cannot be reached from this instance.
    fn acquire_ks_runtime(&self, target_ks: &str, holder_id: &str) -> Result<Arc<dyn KsRuntimeHandle<C>>, String>;
}
