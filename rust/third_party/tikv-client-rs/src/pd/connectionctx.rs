// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD stream ownership, following the complete pinned `pkg/connectionctx`.
//! Cancellation and collection run under the manager's write lock, as in Go;
//! callbacks must not re-enter this manager.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rand::Rng;

use crate::async_util::Cancellation;

/// A stream and its cancellation scope, retained by readers across replacement.
pub struct ConnectionCtx<T> {
    /// The stream's context, distinct from any individual request context.
    pub ctx: Cancellation,
    /// The URL used to establish this stream.
    pub stream_url: String,
    /// The stream handle; retrieving the context never clones the stream itself.
    pub stream: T,
    cancel: Box<dyn Fn() + Send + Sync>,
}

impl<T> ConnectionCtx<T> {
    /// Wraps the caller's stream and cancellation callback without registering it.
    pub fn new(
        ctx: Cancellation,
        cancel: impl Fn() + Send + Sync + 'static,
        stream_url: String,
        stream: T,
    ) -> Self {
        Self {
            ctx,
            stream_url,
            stream,
            cancel: Box::new(cancel),
        }
    }

    /// Cancels the stream through the callback provided by its creator.
    pub fn cancel(&self) {
        (self.cancel)();
    }
}

/// Owns registered connections until explicitly released by their lifecycle owner.
/// It starts no worker and does not close unregistered candidates on rejection.
pub struct Manager<T> {
    connections: RwLock<HashMap<String, Arc<ConnectionCtx<T>>>>,
}

impl<T> Default for Manager<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Manager<T> {
    /// Creates an empty manager with Go's initial map capacity.
    pub fn new() -> Self {
        Self {
            connections: RwLock::new(HashMap::with_capacity(3)),
        }
    }

    /// Reports whether a context is registered, including a canceled context.
    pub fn exist(&self, url: &str) -> bool {
        self.connections
            .read()
            .expect("PD connection manager poisoned")
            .contains_key(url)
    }

    /// Registers a connection, canceling the previous entry only on overwrite.
    /// False leaves candidate cancellation with the caller, just like Go Store.
    pub fn store(&self, connection: &Arc<ConnectionCtx<T>>, overwrite: bool) -> bool {
        let mut connections = self
            .connections
            .write()
            .expect("PD connection manager poisoned");
        if !overwrite && connections.contains_key(&connection.stream_url) {
            return false;
        }
        Self::release_locked(&mut connections, &connection.stream_url);
        connections.insert(connection.stream_url.clone(), connection.clone());
        true
    }

    /// Releases other URLs before registering this one exclusively.
    /// A duplicate preserves its existing entry and returns false, even though
    /// other URLs have already been canceled and removed.
    pub fn clean_all_and_store(&self, connection: &Arc<ConnectionCtx<T>>) -> bool {
        let mut connections = self
            .connections
            .write()
            .expect("PD connection manager poisoned");
        Self::gc_locked(&mut connections, |url| url != connection.stream_url);
        if connections.contains_key(&connection.stream_url) {
            return false;
        }
        connections.insert(connection.stream_url.clone(), connection.clone());
        true
    }

    /// Releases every registered context whose URL matches the predicate.
    pub fn gc(&self, condition: impl FnMut(&str) -> bool) {
        let mut connections = self
            .connections
            .write()
            .expect("PD connection manager poisoned");
        Self::gc_locked(&mut connections, condition);
    }

    fn gc_locked(
        connections: &mut HashMap<String, Arc<ConnectionCtx<T>>>,
        mut condition: impl FnMut(&str) -> bool,
    ) {
        connections.retain(|url, connection| {
            if condition(url) {
                connection.cancel();
                false
            } else {
                true
            }
        });
    }

    /// Cancels and removes all registered entries; the manager can be reused.
    pub fn release_all(&self) {
        self.gc(|_| true);
    }

    /// Cancels and removes this URL; releasing a missing URL is a no-op.
    pub fn release(&self, url: &str) {
        let mut connections = self
            .connections
            .write()
            .expect("PD connection manager poisoned");
        Self::release_locked(&mut connections, url);
    }

    fn release_locked(connections: &mut HashMap<String, Arc<ConnectionCtx<T>>>, url: &str) {
        if let Some(connection) = connections.get(url) {
            connection.cancel();
            connections.remove(url);
        }
    }

    /// Selects an entry uniformly with reservoir sampling, or None when empty.
    pub fn randomly_pick(&self) -> Option<Arc<ConnectionCtx<T>>> {
        let connections = self
            .connections
            .read()
            .expect("PD connection manager poisoned");
        let mut rng = rand::thread_rng();
        let mut selected = None;
        for (index, connection) in connections.values().enumerate() {
            if rng.gen_range(0..=index) == 0 {
                selected = Some(connection);
            }
        }
        selected.cloned()
    }

    /// Retains the same context identity as Store; removal does not invalidate it.
    pub fn get_connection_ctx(&self, url: &str) -> Option<Arc<ConnectionCtx<T>>> {
        self.connections
            .read()
            .expect("PD connection manager poisoned")
            .get(url)
            .cloned()
    }

    /// Returns Go Manager.Size, including registered canceled contexts.
    pub fn len(&self) -> usize {
        self.connections
            .read()
            .expect("PD connection manager poisoned")
            .len()
    }

    /// Reports whether no context is registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn connection<T>(url: &str, stream: T) -> Arc<ConnectionCtx<T>> {
        let ctx = Cancellation::default();
        let cancel = ctx.clone();
        Arc::new(ConnectionCtx::new(
            ctx,
            move || cancel.cancel(),
            url.to_owned(),
            stream,
        ))
    }

    #[test]
    fn source_go_connectionctx_test_cancel_func() {
        let manager = Manager::new();
        let entry = connection("test-url", 1);
        assert!(manager.store(&entry, false));
        assert!(manager.exist("test-url"));
        manager.gc(|url| url == "test-url");
        assert!(entry.ctx.is_cancelled());
    }

    #[test]
    fn source_go_connectionctx_test_manager() {
        // The original Go case deliberately shares one context/cancel across
        // entries, including re-registering it after it has been canceled.
        let shared = Cancellation::default();
        let connection = |url: &str, stream| {
            let cancel = shared.clone();
            Arc::new(ConnectionCtx::new(
                shared.clone(),
                move || cancel.cancel(),
                url.to_owned(),
                stream,
            ))
        };
        let manager = Manager::new();
        assert!(!manager.exist("test-url"));
        let first = connection("test-url", 1);
        assert!(manager.store(&first, false));
        assert!(manager.exist("test-url"));
        let picked = manager.randomly_pick().unwrap();
        assert_eq!(picked.stream_url, "test-url");
        assert_eq!(picked.stream, 1);
        assert!(Arc::ptr_eq(
            &picked,
            &manager.get_connection_ctx("test-url").unwrap()
        ));

        let second = connection("test-url", 2);
        assert!(!manager.store(&second, false));
        let picked = manager.randomly_pick().unwrap();
        assert_eq!(picked.stream_url, "test-url");
        assert_eq!(picked.stream, 1);
        assert!(Arc::ptr_eq(
            &picked,
            &manager.get_connection_ctx("test-url").unwrap()
        ));
        assert!(!first.ctx.is_cancelled());
        assert!(!second.ctx.is_cancelled());

        assert!(manager.store(&second, true));
        let picked = manager.randomly_pick().unwrap();
        assert_eq!(picked.stream_url, "test-url");
        assert_eq!(picked.stream, 2);
        assert!(Arc::ptr_eq(
            &picked,
            &manager.get_connection_ctx("test-url").unwrap()
        ));
        assert!(first.ctx.is_cancelled());
        assert!(second.ctx.is_cancelled());
        let third = connection("test-another-url", 3);
        assert!(manager.store(&third, false));
        let mut counts = HashMap::new();
        for _ in 0..1000 {
            *counts
                .entry(manager.randomly_pick().unwrap().stream_url.clone())
                .or_insert(0) += 1;
        }
        assert!(counts["test-url"] > 0);
        assert!(counts["test-another-url"] > 0);
        assert_eq!(counts.values().sum::<usize>(), 1000);
        manager.gc(|url| url == "test-url");
        assert!(!manager.exist("test-url"));
        assert!(manager.get_connection_ctx("test-url").is_none());
        assert!(manager.exist("test-another-url"));
        assert_eq!(
            manager
                .get_connection_ctx("test-another-url")
                .unwrap()
                .stream,
            3
        );

        let first = connection("test-url", 1);
        assert!(manager.clean_all_and_store(&first));
        assert!(manager.exist("test-url"));
        let rejected = connection("test-url", 2);
        assert!(!manager.clean_all_and_store(&rejected));
        assert_eq!(manager.get_connection_ctx("test-url").unwrap().stream, 1);
        assert!(!manager.exist("test-another-url"));
        assert!(manager.get_connection_ctx("test-another-url").is_none());
        rejected.cancel(); // Caller owns a rejected candidate.

        assert!(manager.store(&third, false));
        let fourth = connection("test-unique-url", 4);
        assert!(manager.clean_all_and_store(&fourth));
        assert!(manager.exist("test-unique-url"));
        assert_eq!(
            manager
                .get_connection_ctx("test-unique-url")
                .unwrap()
                .stream,
            4
        );
        assert!(!manager.exist("test-url"));
        assert!(manager.get_connection_ctx("test-url").is_none());
        assert!(!manager.exist("test-another-url"));
        assert!(manager.get_connection_ctx("test-another-url").is_none());
        manager.release("test-unique-url");
        assert!(!manager.exist("test-unique-url"));
        assert!(manager.get_connection_ctx("test-unique-url").is_none());

        for i in 0..1000 {
            assert!(manager.store(&connection(&format!("test-url-{i}"), i), false));
        }
        assert_eq!(manager.len(), 1000);
        manager.release_all();
        assert!(manager.is_empty());
        assert!(manager.randomly_pick().is_none());
    }

    #[test]
    fn overwrite_cancels_only_the_previous_retained_context() {
        let manager = Manager::new();
        let original = connection("url", 1);
        let candidate = connection("url", 2);
        assert!(manager.store(&original, false));
        assert!(manager.store(&candidate, true));
        assert!(original.ctx.is_cancelled());
        assert!(!candidate.ctx.is_cancelled());
        assert_eq!(original.stream, 1);
        assert!(Arc::ptr_eq(
            &candidate,
            &manager.get_connection_ctx("url").unwrap()
        ));
        manager.release_all();
        assert!(candidate.ctx.is_cancelled());
    }

    #[test]
    fn duplicate_exclusive_store_collects_other_urls_but_preserves_both_candidates() {
        let manager = Manager::new();
        let original = connection("same", 1);
        let other = connection("other", 2);
        let candidate = connection("same", 3);
        assert!(manager.store(&original, false));
        assert!(manager.store(&other, false));
        assert!(!manager.clean_all_and_store(&candidate));
        assert_eq!(manager.len(), 1);
        assert!(Arc::ptr_eq(&original, &manager.randomly_pick().unwrap()));
        assert!(other.ctx.is_cancelled());
        assert!(!original.ctx.is_cancelled());
        assert!(!candidate.ctx.is_cancelled());
        manager.release_all();
        assert!(original.ctx.is_cancelled());
        assert!(!candidate.ctx.is_cancelled());
        candidate.cancel();
    }

    #[test]
    fn release_cancels_once_and_retained_nonclone_stream_outlives_registration() {
        let manager = Manager::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let resource = Arc::new(());
        let weak = Arc::downgrade(&resource);
        struct NonClone(Arc<()>);
        let ctx = Cancellation::default();
        let cancel = ctx.clone();
        let entry = Arc::new(ConnectionCtx::new(
            ctx,
            move || {
                count.fetch_add(1, Ordering::SeqCst);
                cancel.cancel();
            },
            "url".to_owned(),
            NonClone(resource),
        ));
        assert!(manager.store(&entry, false));
        drop(entry);
        let retained = manager.get_connection_ctx("url").unwrap();
        manager.release("url");
        manager.release("url");
        manager.release_all();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(retained.ctx.is_cancelled());
        assert!(Arc::ptr_eq(&retained.stream.0, &weak.upgrade().unwrap()));
        drop(retained);
        assert!(weak.upgrade().is_none());
        assert!(manager.store(&connection("url", NonClone(Arc::new(()))), false));
        manager.release_all();
    }

    #[test]
    fn concurrent_store_has_one_owner_and_no_cancellation_of_rejected_candidates() {
        let manager = Manager::new();
        let candidates = (0..32).map(|i| connection("same", i)).collect::<Vec<_>>();
        let accepted = std::thread::scope(|scope| {
            let handles = candidates
                .iter()
                .map(|candidate| {
                    let manager = &manager;
                    scope.spawn(move || manager.store(candidate, false))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>()
        });
        assert_eq!(accepted, 1);
        let winner = manager.randomly_pick().unwrap().stream;
        manager.release_all();
        for candidate in candidates {
            assert_eq!(candidate.ctx.is_cancelled(), candidate.stream == winner);
            candidate.cancel();
        }
    }

    #[test]
    fn parent_cancellation_does_not_remove_or_replace_a_registered_context() {
        let manager = Manager::new();
        let parent = Cancellation::default();
        let child = parent.child();
        let cancel = child.clone();
        let entry = Arc::new(ConnectionCtx::new(
            child,
            move || cancel.cancel(),
            "url".to_owned(),
            1,
        ));
        assert!(manager.store(&entry, false));
        parent.cancel();
        assert!(entry.ctx.is_cancelled());
        assert!(manager.exist("url"));
        let rejected = connection("url", 2);
        assert!(!manager.store(&rejected, false));
        assert!(!rejected.ctx.is_cancelled());
        manager.release_all();
        rejected.cancel();
    }
}
