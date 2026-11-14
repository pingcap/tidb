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

//! Go `manifest.go`: bootstrap and pooled session contracts.
use std::sync::Arc;
use tidb_datatype::Datum;
use tidb_sqlexec::{ExecutionContext, SqlExecutor};

/// Go `pools.Resource` as used by extension bootstrap.
pub trait SessionResource: SqlExecutor {
    /// Go's resource type assertion.
    fn as_any(&self) -> &dyn std::any::Any;
    /// Go `pools.Resource.Close`.
    fn close(&self);
}
/// Go `extension.SessionPool`.
pub trait SessionPool: Send + Sync {
    /// Go `SessionPool.Get`.
    fn get(&self) -> tidb_sqlexec::Result<Box<dyn SessionResource>>;
    /// Go `SessionPool.Put`.
    fn put(&self, resource: Box<dyn SessionResource>);
}
/// Go `BootstrapContext`.
pub trait BootstrapContext {
    /// Go embedded `context.Context`.
    fn context(&self) -> &dyn ExecutionContext;
    /// Go `BootstrapContext.ExecuteSQL`.
    fn execute_sql(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Vec<Datum>>>;
    /// Go `BootstrapContext.EtcdClient`.
    fn etcd_client(&self) -> Option<Arc<tidb_pd_client::EtcdClient>>;
    /// Go `BootstrapContext.SessionPool`.
    fn session_pool(&self) -> Arc<dyn SessionPool>;
}
/// Go `Manifest.bootstrap`.
pub type Bootstrap = Arc<dyn Fn(&dyn BootstrapContext) -> tidb_sqlexec::Result<()> + Send + Sync>;
