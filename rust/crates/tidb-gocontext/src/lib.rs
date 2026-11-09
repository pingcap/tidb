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

//! Go's standard `context` package, as `pkg/ttl` and `pkg/dxf` use it: a cancellation
//! tree whose `Done()` is a channel a `select` can wait on (here a
//! `crossbeam_channel::Receiver` that disconnects when the context ends),
//! `Err()`, `WithCancel`, `WithCancelCause` (with `Cause`) and
//! `WithTimeout`, and `WithValue` / `Value` (a value chain every derived
//! context inherits, keyed by a key type as Go keys are by their type).

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};

/// Go's `context.Canceled` and `context.DeadlineExceeded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtxError {
    /// Go `context.Canceled`.
    Canceled,
    /// Go `context.DeadlineExceeded`.
    DeadlineExceeded,
}

impl std::fmt::Display for CtxError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Canceled => "context canceled",
            Self::DeadlineExceeded => "context deadline exceeded",
        })
    }
}

impl std::error::Error for CtxError {}

/// The cause of a context's cancellation (Go's `context.Cause`).
pub type Cause = Arc<dyn std::error::Error + Send + Sync>;

struct Inner {
    /// Dropped on cancel, which disconnects every `done` receiver.
    done_tx: Mutex<Option<Sender<()>>>,
    done_rx: Receiver<()>,
    /// The first cancellation's reason and cause; also guards `children`
    /// registration.
    err: Mutex<Option<(CtxError, Cause)>>,
    children: Mutex<Vec<Weak<Inner>>>,
}

impl Inner {
    fn new() -> Arc<Self> {
        let (done_tx, done_rx) = crossbeam_channel::bounded(0);
        Arc::new(Self {
            done_tx: Mutex::new(Some(done_tx)),
            done_rx,
            err: Mutex::new(None),
            children: Mutex::new(Vec::new()),
        })
    }

    /// Go `cancelCtx.cancel`: a `None` cause is the reason itself; the
    /// descendants get the same reason and cause.
    fn cancel(&self, reason: CtxError, cause: Option<Cause>) {
        let cause = cause.unwrap_or_else(|| Arc::new(reason));
        {
            let mut err = self.err.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if err.is_some() {
                return;
            }
            *err = Some((reason, Arc::clone(&cause)));
            self.done_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        }
        let children =
            std::mem::take(&mut *self.children.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for child in children.iter().filter_map(Weak::upgrade) {
            child.cancel(reason, Some(Arc::clone(&cause)));
        }
    }
}

/// Go `valueCtx`: one key's value over its parent's.
struct ValueNode {
    key: std::any::TypeId,
    value: Arc<dyn std::any::Any + Send + Sync>,
    parent: Option<Arc<ValueNode>>,
}

/// Go `context.Context`. The default value is `context.Background()`.
#[derive(Clone, Default)]
pub struct Context {
    inner: Option<Arc<Inner>>,
    values: Option<Arc<ValueNode>>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Context").field("err", &self.err()).finish()
    }
}

/// Go `context.CancelFunc`.
#[derive(Clone)]
pub struct CancelFunc {
    inner: Arc<Inner>,
}

impl CancelFunc {
    /// Cancels the context and its descendants (Go's `cancel()`).
    pub fn cancel(&self) {
        self.inner.cancel(CtxError::Canceled, None);
    }
}

/// Go `context.CancelCauseFunc`.
#[derive(Clone)]
pub struct CancelCauseFunc {
    inner: Arc<Inner>,
}

impl CancelCauseFunc {
    /// Cancels the context and its descendants with `cause` (Go's
    /// `cancel(cause)`; a `None` cause is `context.Canceled`).
    pub fn cancel(&self, cause: Option<Cause>) {
        self.inner.cancel(CtxError::Canceled, cause);
    }
}

impl Context {
    /// Go `context.Background()`: never done.
    pub fn background() -> Self {
        Self::default()
    }

    fn child(parent: &Context) -> (Self, Arc<Inner>) {
        let inner = Inner::new();
        if let Some(parent) = &parent.inner {
            let err = parent.err.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*err {
                Some((reason, cause)) => inner.cancel(*reason, Some(Arc::clone(cause))),
                None => parent
                    .children
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(Arc::downgrade(&inner)),
            }
        }
        (
            Self {
                inner: Some(Arc::clone(&inner)),
                values: parent.values.clone(),
            },
            inner,
        )
    }

    /// Go `context.WithValue(parent, key, val)`, keyed by the key type `K`:
    /// the parent's cancellation, with `val` answering `Value(K)`.
    pub fn with_value<K: 'static, V: std::any::Any + Send + Sync>(parent: &Context, value: V) -> Self {
        Self {
            inner: parent.inner.clone(),
            values: Some(Arc::new(ValueNode {
                key: std::any::TypeId::of::<K>(),
                value: Arc::new(value),
                parent: parent.values.clone(),
            })),
        }
    }

    /// Go `ctx.Value(key)` for the key type `K`: the nearest value set under
    /// it, if it is a `V`.
    pub fn value<K: 'static, V: std::any::Any + Send + Sync>(&self) -> Option<&V> {
        let key = std::any::TypeId::of::<K>();
        let mut node = self.values.as_deref();
        while let Some(current) = node {
            if current.key == key {
                return current.value.downcast_ref::<V>();
            }
            node = current.parent.as_deref();
        }
        None
    }

    /// Go `context.WithCancel(parent)`.
    pub fn with_cancel(parent: &Context) -> (Self, CancelFunc) {
        let (ctx, inner) = Self::child(parent);
        (ctx, CancelFunc { inner })
    }

    /// Go `context.WithCancelCause(parent)`.
    pub fn with_cancel_cause(parent: &Context) -> (Self, CancelCauseFunc) {
        let (ctx, inner) = Self::child(parent);
        (ctx, CancelCauseFunc { inner })
    }

    /// Go `context.WithTimeout(parent, timeout)`.
    pub fn with_timeout(parent: &Context, timeout: Duration) -> (Self, CancelFunc) {
        let (ctx, inner) = Self::child(parent);
        if timeout.is_zero() {
            // Go `WithDeadline`: the deadline has already passed.
            inner.cancel(CtxError::DeadlineExceeded, None);
            return (ctx, CancelFunc { inner });
        }
        let timer = Arc::downgrade(&inner);
        let done = inner.done_rx.clone();
        std::thread::Builder::new()
            .name("context-timeout".to_owned())
            .spawn(move || {
                if let Err(crossbeam_channel::RecvTimeoutError::Timeout) = done.recv_timeout(timeout) {
                    if let Some(inner) = timer.upgrade() {
                        inner.cancel(CtxError::DeadlineExceeded, None);
                    }
                }
            })
            .expect("context timer thread spawns");
        (ctx, CancelFunc { inner })
    }

    /// Go `ctx.Done()`: a channel that disconnects once the context ends.
    /// `context.Background()`'s never does.
    pub fn done(&self) -> Receiver<()> {
        match &self.inner {
            Some(inner) => inner.done_rx.clone(),
            None => crossbeam_channel::never(),
        }
    }

    /// Go `ctx.Err()`.
    pub fn err(&self) -> Option<CtxError> {
        self.inner.as_ref().and_then(|inner| {
            inner.err.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().map(|(reason, _)| *reason)
        })
    }

    /// Go `context.Cause(ctx)`: why the context ended, `None` while it has
    /// not.
    pub fn cause(&self) -> Option<Cause> {
        self.inner.as_ref().and_then(|inner| {
            inner.err.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().map(|(_, cause)| Arc::clone(cause))
        })
    }

    /// Go's `select { case <-ctx.Done(): ... case <-time.After(d): }`:
    /// whether the context ended before `d` elapsed.
    pub fn sleep(&self, d: Duration) -> bool {
        matches!(
            self.done().recv_timeout(d),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `context.WithValue`: a value reaches every context derived from
    /// it, the nearest one wins, and cancellation is the parent's.
    #[test]
    fn values_reach_derived_contexts_and_the_nearest_wins() {
        struct Key;
        struct Other;
        let (parent, cancel) = Context::with_cancel(&Context::background());
        let with = Context::with_value::<Key, &str>(&parent, "a");
        let (child, _cancel) = Context::with_cancel(&with);
        assert_eq!(child.value::<Key, &str>(), Some(&"a"));
        assert_eq!(child.value::<Other, &str>(), None);
        let nearer = Context::with_value::<Key, &str>(&child, "b");
        assert_eq!(nearer.value::<Key, &str>(), Some(&"b"));
        assert_eq!(with.value::<Key, &str>(), Some(&"a"));
        cancel.cancel();
        assert_eq!(with.err(), Some(CtxError::Canceled));
        assert_eq!(nearer.err(), Some(CtxError::Canceled));
    }

    #[test]
    fn cancel_reaches_descendants_and_not_ancestors() {
        let (parent, cancel_parent) = Context::with_cancel(&Context::background());
        let (child, cancel_child) = Context::with_cancel(&parent);
        let (grandchild, _cancel) = Context::with_cancel(&child);

        cancel_child.cancel();
        assert_eq!(child.err(), Some(CtxError::Canceled));
        assert_eq!(grandchild.err(), Some(CtxError::Canceled));
        assert!(grandchild.done().recv().is_err());
        assert_eq!(parent.err(), None);

        cancel_parent.cancel();
        let (late, _cancel) = Context::with_cancel(&parent);
        assert_eq!(late.err(), Some(CtxError::Canceled));
        assert!(Context::background().err().is_none());
    }

    #[test]
    fn timeout_ends_with_deadline_exceeded() {
        let (ctx, _cancel) = Context::with_timeout(&Context::background(), Duration::from_millis(20));
        assert!(ctx.sleep(Duration::from_secs(5)));
        assert_eq!(ctx.err(), Some(CtxError::DeadlineExceeded));

        let (ctx, cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(60));
        cancel.cancel();
        assert_eq!(ctx.err(), Some(CtxError::Canceled));
        assert!(!Context::background().sleep(Duration::from_millis(1)));
    }

    /// Go `TestCause`, the rows of the constructors this port has.
    #[test]
    fn test_cause() {
        #[derive(Debug)]
        struct Named(&'static str);
        impl std::fmt::Display for Named {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.0)
            }
        }
        impl std::error::Error for Named {}

        let forever = Duration::from_secs(1_000_000);
        let parent_cause: Cause = Arc::new(Named("parentCause"));
        let child_cause: Cause = Arc::new(Named("childCause"));
        let canceled = || -> Option<Cause> { Some(Arc::new(CtxError::Canceled)) };
        let cases: Vec<(&str, Box<dyn Fn() -> Context>, Option<CtxError>, Option<Cause>)> = vec![
            ("Background", Box::new(Context::background), None, None),
            (
                "WithCancel",
                Box::new(|| {
                    let (ctx, cancel) = Context::with_cancel(&Context::background());
                    cancel.cancel();
                    ctx
                }),
                Some(CtxError::Canceled),
                canceled(),
            ),
            (
                "WithCancelCause",
                Box::new({
                    let parent_cause = Arc::clone(&parent_cause);
                    move || {
                        let (ctx, cancel) = Context::with_cancel_cause(&Context::background());
                        cancel.cancel(Some(Arc::clone(&parent_cause)));
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                Some(Arc::clone(&parent_cause)),
            ),
            (
                "WithCancelCause nil",
                Box::new(|| {
                    let (ctx, cancel) = Context::with_cancel_cause(&Context::background());
                    cancel.cancel(None);
                    ctx
                }),
                Some(CtxError::Canceled),
                canceled(),
            ),
            (
                "WithCancelCause: parent cause before child",
                Box::new({
                    let (parent_cause, child_cause) = (Arc::clone(&parent_cause), Arc::clone(&child_cause));
                    move || {
                        let (ctx, cancel_parent) = Context::with_cancel_cause(&Context::background());
                        let (ctx, cancel_child) = Context::with_cancel_cause(&ctx);
                        cancel_parent.cancel(Some(Arc::clone(&parent_cause)));
                        cancel_child.cancel(Some(Arc::clone(&child_cause)));
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                Some(Arc::clone(&parent_cause)),
            ),
            (
                "WithCancelCause: parent cause after child",
                Box::new({
                    let (parent_cause, child_cause) = (Arc::clone(&parent_cause), Arc::clone(&child_cause));
                    move || {
                        let (ctx, cancel_parent) = Context::with_cancel_cause(&Context::background());
                        let (ctx, cancel_child) = Context::with_cancel_cause(&ctx);
                        cancel_child.cancel(Some(Arc::clone(&child_cause)));
                        cancel_parent.cancel(Some(Arc::clone(&parent_cause)));
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                Some(Arc::clone(&child_cause)),
            ),
            (
                "WithCancelCause: parent cause before nil",
                Box::new({
                    let parent_cause = Arc::clone(&parent_cause);
                    move || {
                        let (ctx, cancel_parent) = Context::with_cancel_cause(&Context::background());
                        let (ctx, cancel_child) = Context::with_cancel(&ctx);
                        cancel_parent.cancel(Some(Arc::clone(&parent_cause)));
                        cancel_child.cancel();
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                Some(Arc::clone(&parent_cause)),
            ),
            (
                "WithCancelCause: parent cause after nil",
                Box::new({
                    let parent_cause = Arc::clone(&parent_cause);
                    move || {
                        let (ctx, cancel_parent) = Context::with_cancel_cause(&Context::background());
                        let (ctx, cancel_child) = Context::with_cancel(&ctx);
                        cancel_child.cancel();
                        cancel_parent.cancel(Some(Arc::clone(&parent_cause)));
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                canceled(),
            ),
            (
                "WithCancelCause: child cause after nil",
                Box::new({
                    let child_cause = Arc::clone(&child_cause);
                    move || {
                        let (ctx, cancel_parent) = Context::with_cancel(&Context::background());
                        let (ctx, cancel_child) = Context::with_cancel_cause(&ctx);
                        cancel_parent.cancel();
                        cancel_child.cancel(Some(Arc::clone(&child_cause)));
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                canceled(),
            ),
            (
                "WithCancelCause: child cause before nil",
                Box::new({
                    let child_cause = Arc::clone(&child_cause);
                    move || {
                        let (ctx, cancel_parent) = Context::with_cancel(&Context::background());
                        let (ctx, cancel_child) = Context::with_cancel_cause(&ctx);
                        cancel_child.cancel(Some(Arc::clone(&child_cause)));
                        cancel_parent.cancel();
                        ctx
                    }
                }),
                Some(CtxError::Canceled),
                Some(Arc::clone(&child_cause)),
            ),
            (
                "WithTimeout",
                Box::new(|| {
                    let (ctx, cancel) = Context::with_timeout(&Context::background(), Duration::ZERO);
                    cancel.cancel();
                    ctx
                }),
                Some(CtxError::DeadlineExceeded),
                Some(Arc::new(CtxError::DeadlineExceeded)),
            ),
            (
                "WithTimeout canceled",
                Box::new(move || {
                    let (ctx, cancel) = Context::with_timeout(&Context::background(), forever);
                    cancel.cancel();
                    ctx
                }),
                Some(CtxError::Canceled),
                canceled(),
            ),
        ];
        // Go compares the causes by identity; the reasons are values here.
        let same = |got: &Option<Cause>, want: &Option<Cause>| match (got, want) {
            (None, None) => true,
            (Some(got), Some(want)) => match (got.downcast_ref::<CtxError>(), want.downcast_ref::<CtxError>()) {
                (Some(got), Some(want)) => got == want,
                (None, None) => Arc::ptr_eq(got, want),
                _ => false,
            },
            _ => false,
        };
        for (name, ctx, err, cause) in cases {
            let ctx = ctx();
            assert_eq!(ctx.err(), err, "{name}");
            assert!(same(&ctx.cause(), &cause), "{name}: Cause(ctx) = {:?}", ctx.cause().map(|cause| cause.to_string()));
        }
    }
}
