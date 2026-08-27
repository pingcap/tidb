// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Decorators for TiKV RPC dispatch.
//!
//! An interceptor receives the target address and the request immediately before
//! the client sends it. It may run logic around, replace, or skip the following
//! dispatch. Interceptors attached to a transaction execute for every physical
//! TiKV RPC, including retries and lock-resolution requests.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use crate::store::Request;
use crate::Result;

/// Result produced by a transport dispatch before it is decoded for its typed request.
pub type RpcDispatchResult = Result<Box<dyn Any>>;

/// The remaining RPC dispatch in an interceptor chain.
///
/// Like client-go's function value, the continuation may be skipped, called
/// once, or called multiple times by an interceptor.
pub type RpcNext<'a> = Arc<dyn Fn() -> BoxFuture<'a, RpcDispatchResult> + Send + Sync + 'a>;

/// A decorator for an RPC sent to TiKV.
///
/// Implementations normally call `next` once and perform work before and after
/// awaiting it. They may instead return their own response or error.
#[doc(hidden)]
pub trait RpcInterceptorAny: Any {
    #[doc(hidden)]
    fn rpc_interceptor_as_any(&self) -> &dyn Any;
}

impl<T: Any> RpcInterceptorAny for T {
    fn rpc_interceptor_as_any(&self) -> &dyn Any {
        self
    }
}

pub trait RpcInterceptor: Send + Sync + RpcInterceptorAny {
    /// Stable name used when linking chains. A later interceptor with the same
    /// name replaces an earlier one.
    fn name(&self) -> &str;

    /// Downcasting hook used to flatten linked interceptor chains.
    ///
    /// This has a default implementation so downstream interceptors only need
    /// to implement client-go's public `Name` and `Wrap` contract.
    #[doc(hidden)]
    fn as_any(&self) -> &dyn Any {
        self.rpc_interceptor_as_any()
    }

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
            Some((interceptor, remaining)) => {
                let downstream =
                    Arc::new(move || Self::dispatch_from(remaining, target, request, next.clone()));
                interceptor.wrap(target, request, downstream)
            }
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

#[derive(Default)]
struct MockInterceptorState {
    begin: AtomicUsize,
    end: AtomicUsize,
    exec_log: Mutex<Vec<String>>,
}

struct MockInterceptorEndGuard {
    state: Arc<MockInterceptorState>,
}

impl Drop for MockInterceptorEndGuard {
    fn drop(&mut self) {
        self.state.end.fetch_add(1, Ordering::SeqCst);
    }
}

/// Creates named interceptors and records their entry/return counts and order.
///
/// This is the public test-support manager shipped by client-go's
/// `tikvrpc/interceptor` package. Clones observe the same counters and log.
#[derive(Clone, Default)]
pub struct MockInterceptorManager {
    state: Arc<MockInterceptorState>,
}

impl MockInterceptorManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a named interceptor whose executions are recorded by this manager.
    pub fn create_mock_interceptor(&self, name: impl Into<String>) -> RpcInterceptorHandle {
        let name = name.into();
        let manager = self.clone();
        new_rpc_interceptor(name.clone(), move |_, _, next| {
            let manager = manager.clone();
            let name = name.clone();
            Box::pin(async move {
                manager
                    .state
                    .exec_log
                    .lock()
                    .expect("mock interceptor log mutex poisoned")
                    .push(name);
                manager.state.begin.fetch_add(1, Ordering::SeqCst);
                let _end = MockInterceptorEndGuard {
                    state: manager.state.clone(),
                };
                next().await
            })
        })
    }

    /// Clear all counters and recorded names.
    pub fn reset(&self) {
        self.state.begin.store(0, Ordering::SeqCst);
        self.state.end.store(0, Ordering::SeqCst);
        self.state
            .exec_log
            .lock()
            .expect("mock interceptor log mutex poisoned")
            .clear();
    }

    /// Number of interceptor entries since construction or reset.
    pub fn begin_count(&self) -> usize {
        self.state.begin.load(Ordering::SeqCst)
    }

    /// Number of interceptor returns since construction or reset.
    pub fn end_count(&self) -> usize {
        self.state.end.load(Ordering::SeqCst)
    }

    /// Interceptor names in entry order.
    pub fn exec_log(&self) -> Vec<String> {
        self.state
            .exec_log
            .lock()
            .expect("mock interceptor log mutex poisoned")
            .clone()
    }
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

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn set_leader(&mut self, _: &crate::region::RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _: crate::proto::kvrpcpb::ApiVersion) {}
    }

    #[test]
    fn source_test_interceptor() {
        let manager = MockInterceptorManager::default();
        let mut chain = RpcInterceptorChain::new();
        chain
            .link(manager.create_mock_interceptor("INTERCEPTOR-1"))
            .link(manager.create_mock_interceptor("INTERCEPTOR-2"));

        let request = TestRequest;
        futures::executor::block_on(chain.wrap(
            "",
            &request,
            Arc::new(|| Box::pin(async { Ok(Box::new(()) as Box<dyn Any>) })),
        ))
        .unwrap();

        assert_eq!(manager.begin_count(), 2);
        assert_eq!(manager.end_count(), 2);
        assert_eq!(manager.exec_log(), ["INTERCEPTOR-1", "INTERCEPTOR-2"]);
    }

    #[test]
    fn source_test_append_chained_interceptor() {
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
            Arc::new(move || {
                let count_for_next = count_for_next.clone();
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

    #[test]
    fn source_uncovered_mock_manager_reset_and_clone_share_state() {
        let manager = MockInterceptorManager::new();
        let clone = manager.clone();
        let interceptor = manager.create_mock_interceptor("shared");
        let request = TestRequest;
        futures::executor::block_on(interceptor.wrap(
            "",
            &request,
            Arc::new(|| Box::pin(async { Ok(Box::new(()) as Box<dyn Any>) })),
        ))
        .unwrap();
        assert_eq!(clone.begin_count(), 1);
        assert_eq!(clone.end_count(), 1);
        assert_eq!(clone.exec_log(), ["shared"]);

        clone.reset();
        assert_eq!(manager.begin_count(), 0);
        assert_eq!(manager.end_count(), 0);
        assert!(manager.exec_log().is_empty());
    }

    #[test]
    fn source_uncovered_continuation_can_dispatch_more_than_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let interceptor = new_rpc_interceptor("repeat", |_, _, next| {
            Box::pin(async move {
                next().await?;
                next().await
            })
        });
        let calls_for_next = calls.clone();
        let request = TestRequest;
        futures::executor::block_on(interceptor.wrap(
            "",
            &request,
            Arc::new(move || {
                let call = calls_for_next.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move { Ok(Box::new(call) as Box<dyn Any>) })
            }),
        ))
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn source_uncovered_link_flattens_chains_and_keeps_last_duplicate() {
        let interceptor = |name: &'static str| {
            new_rpc_interceptor(name, move |_, _, next| {
                Box::pin(async move { next().await })
            })
        };
        let mut nested = RpcInterceptorChain::new();
        nested
            .link(interceptor("first"))
            .link(interceptor("second"));

        let chain = chain_rpc_interceptors(
            Arc::new(nested),
            [interceptor("third"), interceptor("first")],
        );
        assert_eq!(chain.name(), "interceptor-chain");
        assert_eq!(chain.len(), 3);
        assert!(!chain.is_empty());

        let events = Arc::new(Mutex::new(Vec::new()));
        let event_interceptor = |name: &'static str| {
            let events = events.clone();
            new_rpc_interceptor(name, move |_, _, next| {
                let events = events.clone();
                Box::pin(async move {
                    events.lock().unwrap().push(name);
                    next().await
                })
            })
        };
        let mut first = RpcInterceptorChain::new();
        first
            .link(event_interceptor("first"))
            .link(event_interceptor("second"));
        let chain = chain_rpc_interceptors(
            Arc::new(first),
            [event_interceptor("third"), event_interceptor("first")],
        );
        let request = TestRequest;
        futures::executor::block_on(chain.wrap(
            "",
            &request,
            Arc::new(|| Box::pin(async { Ok(Box::new(()) as Box<dyn Any>) })),
        ))
        .unwrap();
        assert_eq!(*events.lock().unwrap(), ["second", "third", "first"]);
    }

    #[test]
    fn source_test_intercepted_client() {
        let executed = Arc::new(AtomicUsize::new(0));
        let executed_by_interceptor = executed.clone();
        let interceptor = new_rpc_interceptor("test", move |_, _, next| {
            let executed_by_interceptor = executed_by_interceptor.clone();
            Box::pin(async move {
                executed_by_interceptor.fetch_add(1, Ordering::SeqCst);
                next().await
            })
        });
        let request = TestRequest;
        futures::executor::block_on(interceptor.wrap(
            "",
            &request,
            Arc::new(|| Box::pin(async { Ok(Box::new(()) as Box<dyn Any>) })),
        ))
        .unwrap();

        assert_eq!(executed.load(Ordering::SeqCst), 1);
    }
}
