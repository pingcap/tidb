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

mod timer;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};

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

static CANCELED: LazyLock<Cause> = LazyLock::new(|| Arc::new(CtxError::Canceled));
static DEADLINE_EXCEEDED: LazyLock<Cause> = LazyLock::new(|| Arc::new(CtxError::DeadlineExceeded));
static CLOSED: LazyLock<Receiver<()>> = LazyLock::new(|| crossbeam_channel::bounded(0).1);

struct Done {
    tx: Option<Sender<()>>,
    rx: Receiver<()>,
}

#[derive(Default)]
struct Cancellation {
    // A missing custom cause has the same value as the reason.
    err: Option<(CtxError, Option<Cause>)>,
    done: Option<Done>,
    children: HashMap<usize, Weak<Inner>>,
    timer: Option<Instant>,
}

struct Inner {
    state: Mutex<Cancellation>,
    parent: Option<Arc<Inner>>,
    deadline: Option<Instant>,
}

impl Inner {
    fn id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    fn detach(&self) {
        if let Some(parent) = &self.parent {
            parent
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .children
                .remove(&self.id());
        }
    }

    /// Go `cancelCtx.cancel`: a `None` cause is the reason itself; the
    /// descendants get the same reason and cause.
    fn cancel(&self, reason: CtxError, cause: Option<Cause>) {
        let (children, deadline) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.err.is_some() {
                return;
            }
            state.err = Some((reason, cause.clone()));
            if let Some(done) = &mut state.done {
                done.tx.take();
            }
            (std::mem::take(&mut state.children), state.timer.take())
        };
        self.detach();
        if let Some(deadline) = deadline {
            timer::remove(deadline, self.id());
        }
        for child in children.values().filter_map(Weak::upgrade) {
            child.cancel(reason, cause.clone());
        }
    }

    fn done(&self) -> Receiver<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(done) = &state.done {
            return done.rx.clone();
        }
        if state.err.is_some() {
            return CLOSED.clone();
        }
        let (tx, rx) = crossbeam_channel::bounded(0);
        state.done = Some(Done {
            tx: Some(tx),
            rx: rx.clone(),
        });
        rx
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state.err.is_none();
        let deadline = state.timer.take();
        if pending {
            self.detach();
        }
        if let Some(deadline) = deadline {
            timer::remove(deadline, self.id());
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

    fn child(parent: &Context, requested_deadline: Option<Instant>) -> (Self, Arc<Inner>) {
        let inherited = parent.inner.as_ref().and_then(|parent| parent.deadline);
        let own_deadline =
            requested_deadline.filter(|&deadline| inherited.is_none_or(|p| deadline < p));
        let inner = Arc::new(Inner {
            state: Mutex::new(Cancellation::default()),
            parent: parent.inner.clone(),
            deadline: own_deadline.or(inherited),
        });
        let cancellation = parent.inner.as_ref().and_then(|parent| {
            let mut state = parent
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(cancellation) = &state.err {
                Some(cancellation.clone())
            } else {
                state.children.insert(inner.id(), Arc::downgrade(&inner));
                None
            }
        });
        if let Some((reason, cause)) = cancellation {
            inner.cancel(reason, cause);
        } else if let Some(deadline) = own_deadline {
            if deadline <= Instant::now() {
                inner.cancel(CtxError::DeadlineExceeded, None);
            } else {
                let mut state = inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.err.is_none() {
                    timer::insert(deadline, &inner);
                    state.timer = Some(deadline);
                }
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
        let (ctx, inner) = Self::child(parent, None);
        (ctx, CancelFunc { inner })
    }

    /// Go `context.WithCancelCause(parent)`.
    pub fn with_cancel_cause(parent: &Context) -> (Self, CancelCauseFunc) {
        let (ctx, inner) = Self::child(parent, None);
        (ctx, CancelCauseFunc { inner })
    }

    /// Go `context.WithTimeout(parent, timeout)`.
    pub fn with_timeout(parent: &Context, timeout: Duration) -> (Self, CancelFunc) {
        let (ctx, inner) = Self::child(parent, Instant::now().checked_add(timeout));
        (ctx, CancelFunc { inner })
    }

    /// Go `ctx.Done()`: a channel that disconnects once the context ends.
    /// `context.Background()`'s never does.
    pub fn done(&self) -> Receiver<()> {
        match &self.inner {
            Some(inner) => inner.done(),
            None => crossbeam_channel::never(),
        }
    }

    /// Go `ctx.Err()`.
    pub fn err(&self) -> Option<CtxError> {
        self.inner.as_ref().and_then(|inner| {
            inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .err
                .as_ref()
                .map(|(reason, _)| *reason)
        })
    }

    /// Go `context.Cause(ctx)`: why the context ended, `None` while it has
    /// not.
    pub fn cause(&self) -> Option<Cause> {
        self.inner.as_ref().and_then(|inner| {
            inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .err
                .as_ref()
                .map(|(reason, cause)| {
                    cause.as_ref().map_or_else(
                        || match reason {
                            CtxError::Canceled => Arc::clone(&CANCELED),
                            CtxError::DeadlineExceeded => Arc::clone(&DEADLINE_EXCEEDED),
                        },
                        Arc::clone,
                    )
                })
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

    #[test]
    fn earlier_deadlines_wake_the_scheduler_and_reach_descendants() {
        let background = Context::background();
        let (later, cancel_later) = Context::with_timeout(&background, Duration::from_secs(60));
        let (parent, cancel_parent) = Context::with_timeout(&background, Duration::from_millis(5));
        let (child, _) = Context::with_timeout(&parent, Duration::from_secs(60));
        let parent_deadline = parent.inner.as_ref().unwrap().deadline;
        let child_inner = child.inner.as_ref().unwrap();
        assert_eq!(child_inner.deadline, parent_deadline);
        assert!(child_inner.state.lock().unwrap().timer.is_none());
        drop((parent, cancel_parent));

        let done = child.done();
        assert_eq!(
            done.recv_timeout(Duration::from_secs(2)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        );
        assert!(done.same_channel(&child.done()));
        assert_eq!(child.err(), Some(CtxError::DeadlineExceeded));
        assert_eq!(later.err(), None);
        cancel_later.cancel();
    }

    #[test]
    fn canceled_and_dropped_timeouts_remove_their_registrations() {
        let (parent, _) = Context::with_cancel(&Context::background());
        let parent_inner = parent.inner.as_ref().unwrap();
        for cancel_now in [true, false] {
            let (child, cancel) = Context::with_timeout(&parent, Duration::from_secs(60));
            let inner = child.inner.as_ref().unwrap();
            let id = inner.id();
            let deadline = inner.deadline.unwrap();
            assert!(timer::contains(deadline, id));
            assert!(parent_inner
                .state
                .lock()
                .unwrap()
                .children
                .contains_key(&id));
            if cancel_now {
                cancel.cancel();
                assert!(!timer::contains(deadline, id));
                assert!(child.done().same_channel(&child.done()));
            }
            drop((child, cancel));
            assert!(!timer::contains(deadline, id));
            assert!(!parent_inner
                .state
                .lock()
                .unwrap()
                .children
                .contains_key(&id));
        }
    }

    #[test]
    fn deadline_registration_can_race_parent_cancellation() {
        for _ in 0..32 {
            let (parent, cancel_parent) = Context::with_cancel(&Context::background());
            std::thread::scope(|scope| {
                let canceling = scope.spawn(|| cancel_parent.cancel());
                let (child, _) = Context::with_timeout(&parent, Duration::from_secs(60));
                canceling.join().unwrap();
                assert_eq!(child.err(), Some(CtxError::Canceled));
                let inner = child.inner.as_ref().unwrap();
                assert!(!timer::contains(inner.deadline.unwrap(), inner.id()));
            });
        }
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
