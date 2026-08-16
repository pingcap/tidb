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

//! Go `kv_exec_count.go`: counting SQL executions in the kv dimension.
//!
//! boundary: Go builds an `interceptor.RPCInterceptor` over
//! `client-go/v2/tikvrpc.{Request,Response}`. Neither type exists below this
//! crate, and the counter never inspects them — it only needs the RPC target
//! string and the ability to call through. [`RpcInterceptor::wrap`] therefore
//! keeps client-go's exact wrap-a-handler shape while staying generic over the
//! request, response, and error types, so the kv-side counting logic ports in
//! full rather than being dropped.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use super::stmtstats::{new_sql_plan_digest, SqlPlanDigest, StatementStats};
use crate::topsql_state::top_sql_enabled;

/// Go's `interceptor.NewRPCInterceptor("kv-exec-counter", ...)` name.
pub const KV_EXEC_COUNTER_INTERCEPTOR_NAME: &str = "kv-exec-counter";

/// Go `KvExecCounter`: counts the number of SQL executions of the kv layer.
///
/// It calls `StatementStats::add_kv_exec_count` at the right time so that the
/// "SQL execution count of TiKV" semantic holds.
#[derive(Debug)]
pub struct KvExecCounter {
    stats: Arc<StatementStats>,
    /// Go's `marked map[string]struct{}` — a `HashSet<Target>`.
    marked: Mutex<HashSet<String>>,
    digest: SqlPlanDigest,
}

impl StatementStats {
    /// Go `StatementStats.CreateKvExecCounter`: creates an associated
    /// [`KvExecCounter`].
    ///
    /// The created counter can only be used during a single statement
    /// execution and cannot be reused.
    #[must_use]
    pub fn create_kv_exec_counter(
        self: &Arc<Self>,
        sql_digest: &[u8],
        plan_digest: &[u8],
    ) -> Arc<KvExecCounter> {
        Arc::new(KvExecCounter {
            stats: Arc::clone(self),
            digest: new_sql_plan_digest(sql_digest, plan_digest),
            marked: Mutex::new(HashSet::new()),
        })
    }
}

impl KvExecCounter {
    /// Go `KvExecCounter.RPCInterceptor`: returns an interceptor for
    /// client-go.
    ///
    /// The returned interceptor is generally expected to be bound to a
    /// transaction or snapshot. That way the logic preset by [`KvExecCounter`]
    /// runs before each RPC request is initiated, counting SQL executions in
    /// the TiKV dimension.
    #[must_use]
    pub fn rpc_interceptor(self: &Arc<Self>) -> RpcInterceptor {
        RpcInterceptor {
            name: KV_EXEC_COUNTER_INTERCEPTOR_NAME,
            counter: Arc::clone(self),
        }
    }

    /// Go's `mark`: marks this target during the current execution of the
    /// statement. If the target is marked for the first time, the number of
    /// executions is increased. Thread-safe.
    pub fn mark(&self, target: &str) {
        let first_mark = self
            .marked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(target.to_owned());
        if first_mark {
            self.stats.add_kv_exec_count(
                self.digest.sql_digest.as_bytes(),
                self.digest.plan_digest.as_bytes(),
                target,
                1,
            );
        }
    }

    /// The targets marked so far, Go's `c.marked` field read.
    #[must_use]
    pub fn marked(&self) -> HashSet<String> {
        self.marked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// boundary: Go's `interceptor.RPCInterceptor` for a [`KvExecCounter`].
///
/// client-go's interceptor is a named handler-decorator; only the decoration
/// is meaningful here, so it is kept and the client-go request/response types
/// are left to the caller as type parameters of [`RpcInterceptor::wrap`].
#[derive(Debug)]
pub struct RpcInterceptor {
    name: &'static str,
    counter: Arc<KvExecCounter>,
}

impl RpcInterceptor {
    /// Go's `interceptor.RPCInterceptor.Name`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Go's `interceptor.RPCInterceptor.Wrap`: decorates `next` so that each
    /// RPC target is marked before the call is forwarded.
    pub fn wrap<Req, Resp, Err, Next>(&self, next: Next) -> impl Fn(&str, Req) -> Result<Resp, Err>
    where
        Next: Fn(&str, Req) -> Result<Resp, Err>,
    {
        let counter = Arc::clone(&self.counter);
        move |target, req| {
            if top_sql_enabled() {
                counter.mark(target);
            }
            next(target, req)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::stmtstats::create_statement_stats;
    use super::super::test_support::{global_test_guard, reset_topsql_state, sql_plan_digest};
    use crate::topsql_state::enable_top_sql;

    /// Stand-ins for `tikvrpc.Request` / `tikvrpc.Response`, which the counter
    /// never inspects.
    type Request = ();
    type Response = ();

    // Go `TestKvExecCounter`.
    #[test]
    fn kv_exec_counter() {
        let _guard = global_test_guard();
        reset_topsql_state();
        enable_top_sql();
        let stats = create_statement_stats();
        let counter = stats.create_kv_exec_counter(b"SQL-1", b"");
        let interceptor = counter.rpc_interceptor();
        for _ in 0..10 {
            let _ = interceptor
                .wrap(|_target: &str, _req: Option<Request>| Ok::<Option<Response>, ()>(None))(
                "TIKV-1", None,
            );
        }
        for _ in 0..10 {
            let _ = interceptor
                .wrap(|_target: &str, _req: Option<Request>| Ok::<Option<Response>, ()>(None))(
                "TIKV-2", None,
            );
        }
        let marked = counter.marked();
        assert_eq!(marked.len(), 2);
        assert!(marked.contains("TIKV-1"));
        assert!(marked.contains("TIKV-2"));
        let inner = stats.lock();
        let data = &inner.data;
        assert!(data.contains_key(&sql_plan_digest("SQL-1", "")));
        assert_eq!(
            data[&sql_plan_digest("SQL-1", "")]
                .kv_stats_item
                .kv_exec_count["TIKV-1"],
            1
        );
        reset_topsql_state();
    }
}
