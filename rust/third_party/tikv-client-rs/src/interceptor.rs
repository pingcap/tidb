// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Decorators for TiKV RPC dispatch.
//!
//! An interceptor receives the target address and the request immediately before
//! the client sends it. It may run logic around, replace, or skip the following
//! dispatch. Interceptors attached to a transaction execute for every physical
//! TiKV RPC, including retries and lock-resolution requests.

use std::any::Any;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::store::Request;
use crate::Result;

/// Result produced by a transport dispatch before it is decoded for its typed request.
pub type RpcDispatchResult = Result<Box<dyn Any>>;

/// The remaining RPC dispatch in an interceptor chain.
pub type RpcNext<'a> = Box<dyn FnOnce() -> BoxFuture<'a, RpcDispatchResult> + Send + 'a>;

/// A decorator for an RPC sent to TiKV.
///
/// Implementations normally call `next` once and perform work before and after
/// awaiting it. They may instead return their own response or error.
pub trait RpcInterceptor: Send + Sync {
    /// Stable name used when linking chains. A later interceptor with the same
    /// name replaces an earlier one.
    fn name(&self) -> &str;

    /// Downcasting hook used to flatten linked interceptor chains.
    #[doc(hidden)]
    fn as_any(&self) -> &dyn Any;

    /// Run this interceptor around `next`.
    fn wrap<'a>(
        &'a self,
        target: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult>;
}

/// Shared interceptor handle accepted by transaction configuration methods.
pub type RpcInterceptorHandle = Arc<dyn RpcInterceptor>;

type RpcInterceptorFn = dyn for<'a> Fn(&'a str, &'a dyn Request, RpcNext<'a>) -> BoxFuture<'a, RpcDispatchResult>
    + Send
    + Sync;

struct FunctionRpcInterceptor {
    name: String,
    function: Arc<RpcInterceptorFn>,
}

impl RpcInterceptor for FunctionRpcInterceptor {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn wrap<'a>(
        &'a self,
        target: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        (self.function)(target, request, next)
    }
}

/// Construct an interceptor from an async wrapping function.
///
/// The closure must return a boxed future so it can borrow the request and
/// continuation for the duration of the dispatch:
///
/// ```rust
/// # use futures::future::BoxFuture;
/// # use tikv_client::{new_rpc_interceptor, RpcDispatchResult};
/// let interceptor = new_rpc_interceptor("log", |target, request, next| {
///     Box::pin(async move {
///         log::debug!("sending {} to {target}", request.label());
///         next().await
///     }) as BoxFuture<'_, RpcDispatchResult>
/// });
/// ```
pub fn new_rpc_interceptor<F>(name: impl Into<String>, function: F) -> RpcInterceptorHandle
where
    F: for<'a> Fn(&'a str, &'a dyn Request, RpcNext<'a>) -> BoxFuture<'a, RpcDispatchResult>
        + Send
        + Sync
        + 'static,
{
    Arc::new(FunctionRpcInterceptor {
        name: name.into(),
        function: Arc::new(function),
    })
}

/// A named, ordered collection of RPC interceptors.
///
/// Link order is execution order on entry: `first` executes before `second`,
/// and returns after it. Names are unique; linking a duplicate replaces its
/// earlier entry. Linking another chain flattens it.
#[derive(Clone, Default)]
pub struct RpcInterceptorChain {
    interceptors: Vec<RpcInterceptorHandle>,
}

impl RpcInterceptorChain {
    /// Create an empty chain.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of linked interceptors.
    pub fn len(&self) -> usize {
        self.interceptors.len()
    }

    /// Whether this chain has no interceptors.
    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }

    /// Link an interceptor, replacing a prior interceptor with the same name.
    pub fn link(&mut self, interceptor: RpcInterceptorHandle) -> &mut Self {
        if let Some(chain) = interceptor.as_any().downcast_ref::<RpcInterceptorChain>() {
            for interceptor in chain.interceptors.iter().cloned() {
                self.link(interceptor);
            }
            return self;
        }
        if let Some(index) = self
            .interceptors
            .iter()
            .position(|existing| existing.name() == interceptor.name())
        {
            self.interceptors.remove(index);
        }
        self.interceptors.push(interceptor);
        self
    }

    fn dispatch_from<'a>(
        interceptors: &'a [RpcInterceptorHandle],
        target: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        match interceptors.split_first() {
            None => next(),
            Some((interceptor, remaining)) => interceptor.wrap(
                target,
                request,
                Box::new(move || Self::dispatch_from(remaining, target, request, next)),
            ),
        }
    }

    pub(crate) fn dispatch<'a>(
        &'a self,
        target: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        Self::dispatch_from(&self.interceptors, target, request, next)
    }
}

impl RpcInterceptor for RpcInterceptorChain {
    fn name(&self) -> &str {
        "interceptor-chain"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn wrap<'a>(
        &'a self,
        target: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        self.dispatch(target, request, next)
    }
}

/// Chain interceptors in parameter order, using the same duplicate-name rule
/// as [`RpcInterceptorChain::link`].
pub fn chain_rpc_interceptors(
    first: RpcInterceptorHandle,
    rest: impl IntoIterator<Item = RpcInterceptorHandle>,
) -> RpcInterceptorChain {
    let mut chain = RpcInterceptorChain::new();
    chain.link(first);
    for interceptor in rest {
        chain.link(interceptor);
    }
    chain
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use async_trait::async_trait;

    struct TestRequest;

    #[async_trait]
    impl Request for TestRequest {
        async fn dispatch(
            &self,
            _: &crate::proto::tikvpb::tikv_client::TikvClient<tonic::transport::Channel>,
            _: std::time::Duration,
        ) -> Result<Box<dyn Any>> {
            unreachable!("interceptor unit test does not use transport dispatch")
        }

        fn label(&self) -> &'static str {
            "test"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn set_leader(&mut self, _: &crate::region::RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _: crate::proto::kvrpcpb::ApiVersion) {}
    }

    #[test]
    fn chain_is_onion_ordered_and_replaces_duplicate_names() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let count = Arc::new(AtomicUsize::new(0));
        let interceptor = |name: &'static str| {
            let events = events.clone();
            new_rpc_interceptor(name, move |_, _, next| {
                let events = events.clone();
                Box::pin(async move {
                    events.lock().unwrap().push(format!("begin-{name}"));
                    let result = next().await;
                    events.lock().unwrap().push(format!("end-{name}"));
                    result
                })
            })
        };
        let mut chain = RpcInterceptorChain::new();
        chain.link(interceptor("first"));
        chain.link(interceptor("second"));
        chain.link(interceptor("first"));
        assert_eq!(chain.len(), 2);

        let request = TestRequest;
        let count_for_next = count.clone();
        futures::executor::block_on(chain.dispatch(
            "127.0.0.1:20160",
            &request,
            Box::new(move || {
                Box::pin(async move {
                    count_for_next.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(()) as Box<dyn Any>)
                })
            }),
        ))
        .unwrap();

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            *events.lock().unwrap(),
            ["begin-second", "begin-first", "end-first", "end-second"]
        );
    }
}
