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

//! Go `pkg/extension/extensionimpl/bootstrap.go`.
use std::sync::Arc;
use tidb_domain_core::{
    domain::Domain,
    sys_session::{DomainSysSession, DomainSysSessionPool},
};
use tidb_extension::{BootstrapContext, SessionPool, SessionResource};
use tidb_sqlexec::{ExecutionContext, SqlExecutor};

/// Go `bootstrapContext`.
struct Context {
    context: Arc<dyn ExecutionContext>,
    sql_executor: Option<Box<dyn SessionResource>>,
    etcd_client: Option<Arc<tidb_pd_client::EtcdClient>>,
    session_pool: Arc<dyn SessionPool>,
}
impl BootstrapContext for Context {
    fn context(&self) -> &dyn ExecutionContext {
        self.context.as_ref()
    }
    fn execute_sql(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Vec<tidb_datatype::Datum>>> {
        if let Some(error) = context
            .as_any()
            .downcast_ref::<tidb_gocontext::Context>()
            .and_then(tidb_gocontext::Context::err)
        {
            return Err(Box::new(error));
        }
        let trace = context
            .as_any()
            .downcast_ref::<tikv_client::trace::TraceContext>()
            .cloned()
            .unwrap_or_else(tikv_client::trace::TraceContext::new);
        let context = tikv_client::util::with_internal_source_type(
            &trace,
            tidb_txnkv::INTERNAL_TXN_BOOTSTRAP,
        );
        let Some(mut rs) = self
            .sql_executor
            .as_ref()
            .expect("bootstrap session checked out")
            .execute_internal(&context, sql, &[])?
        else {
            return Ok(Vec::new());
        };
        let rows = tidb_sqlexec::drain_record_set(&context, rs.as_mut(), 8);
        let close = rs.close();
        // Go's defer returns the close error only if execution/draining succeeded.
        match rows {
            Err(error) => Err(error),
            Ok(rows) => {
                close?;
                Ok(rows)
            }
        }
    }
    fn etcd_client(&self) -> Option<Arc<tidb_pd_client::EtcdClient>> {
        self.etcd_client.clone()
    }
    fn session_pool(&self) -> Arc<dyn SessionPool> {
        self.session_pool.clone()
    }
}
struct Pool(Arc<DomainSysSessionPool>);
struct Resource(tidb_syssession::Session<dyn DomainSysSession>);
impl SessionPool for Pool {
    fn get(&self) -> tidb_sqlexec::Result<Box<dyn SessionResource>> {
        Ok(Box::new(Resource(self.0.get()?)))
    }
    fn put(&self, resource: Box<dyn SessionResource>) {
        self.0.put(
            &SessionResource::as_any(resource.as_ref())
                .downcast_ref::<Resource>()
                .expect("resource from domain session pool")
                .0,
        );
    }
}
impl SessionResource for Resource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn close(&self) {
        self.0.close();
    }
}
impl SqlExecutor for Resource {
    fn execute(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.0.execute(context, sql)
    }
    fn execute_internal(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.0.execute_internal(context, sql, arguments)
    }
    fn execute_stmt(
        &self,
        context: &dyn ExecutionContext,
        statement: &tidb_ast::Stmt,
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.0.execute_stmt(context, statement)
    }
}
/// Go `extensionimpl.Bootstrap`.
pub fn bootstrap(context: Arc<dyn ExecutionContext>, domain: &Arc<Domain>) -> Result<(), String> {
    let Some(extensions) =
        tidb_extension::get_extensions().map_err(|error| format!("{error:?}"))?
    else {
        return Ok(());
    };
    let session_pool: Arc<dyn SessionPool> = Arc::new(Pool(domain.advanced_sys_session_pool()));
    let executor = session_pool.get().map_err(|error| error.to_string())?;
    let context = Context {
        context,
        sql_executor: Some(executor),
        etcd_client: domain.etcd_client(),
        session_pool,
    };
    let result = extensions
        .bootstrap(&context)
        .map_err(|error| error.to_string());
    drop(context);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `pkg/extension/bootstrap_test.go:TestBootstrap`.
    #[test]
    fn test_bootstrap() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                tidb_extension::reset();
            }
        }
        let _reset = Reset;
        tidb_extension::reset();
        tidb_extension::register(
            "test1",
            vec![tidb_extension::with_bootstrap_sql(vec![
                "create table test.t1 (a int)".into(),
            ])],
        )
        .unwrap();
        tidb_extension::register(
            "test2",
            vec![tidb_extension::with_bootstrap(Arc::new(|ctx| {
                ctx.execute_sql(ctx.context(), "insert into test.t1 values(1)")?;
                let rows = ctx.execute_sql(ctx.context(), "select * from test.t1 where a=1")?;
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].as_int(), Some(1));
                Ok(())
            }))],
        )
        .unwrap();
        tidb_extension::setup().unwrap();
        let store = crate::domain::mock_store::MockStoreDomain::create().unwrap();
        let mut session = store.new_session().unwrap();
        assert_eq!(
            crate::tests_support::query_text(&mut session, "select * from test.t1").1,
            vec![vec!["1"]]
        );
    }
}

impl Drop for Context {
    /// Go `Bootstrap`'s deferred `pool.Put(r)`.
    fn drop(&mut self) {
        if let Some(executor) = self.sql_executor.take() {
            self.session_pool.put(executor);
        }
    }
}
