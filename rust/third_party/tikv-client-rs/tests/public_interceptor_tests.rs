// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::future::BoxFuture;
use tikv_client::tikv::Request;
use tikv_client::{
    MockInterceptorManager, RpcDispatchResult, RpcInterceptor, RpcInterceptorChain,
    RpcInterceptorHandle, RpcNext,
};

struct DownstreamInterceptor;

impl RpcInterceptor for DownstreamInterceptor {
    fn name(&self) -> &str {
        "downstream"
    }

    fn wrap<'a>(
        &'a self,
        _target: &'a str,
        _request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        let repeat = next.clone();
        Box::pin(async move {
            next().await?;
            repeat().await
        })
    }
}

#[test]
fn downstream_interceptor_needs_only_the_source_contract() {
    let interceptor: RpcInterceptorHandle = Arc::new(DownstreamInterceptor);
    let manager = MockInterceptorManager::new();
    let mut chain = RpcInterceptorChain::new();
    chain
        .link(interceptor)
        .link(manager.create_mock_interceptor("mock"));

    assert_eq!(chain.len(), 2);
    assert_eq!(manager.begin_count(), 0);
    assert_eq!(manager.end_count(), 0);
    assert!(manager.exec_log().is_empty());
    manager.reset();
}
