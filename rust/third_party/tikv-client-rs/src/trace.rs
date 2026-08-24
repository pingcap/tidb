// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Client-side tracing hooks and TiKV trace-control flags.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::ops::{BitOr, BitOrAssign};
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;

/// Trace logging control bits sent to TiKV.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct TraceControlFlags(pub u64);

impl TraceControlFlags {
    pub const IMMEDIATE_LOG: Self = Self(1 << 0);
    pub const TIKV_CATEGORY_REQUEST: Self = Self(1 << 1);
    pub const TIKV_CATEGORY_WRITE_DETAILS: Self = Self(1 << 2);
    pub const TIKV_CATEGORY_READ_DETAILS: Self = Self(1 << 3);

    pub const fn has(self, flag: Self) -> bool {
        self.0 & flag.0 != 0
    }

    pub const fn with(self, flag: Self) -> Self {
        Self(self.0 | flag.0)
    }
}

impl BitOr for TraceControlFlags {
    type Output = Self;

    fn bitor(self, right: Self) -> Self::Output {
        Self(self.0 | right.0)
    }
}

impl BitOrAssign for TraceControlFlags {
    fn bitor_assign(&mut self, right: Self) {
        self.0 |= right.0;
    }
}

/// Client trace-event family. Discriminants match client-go.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum Category {
    TransactionTwoPhaseCommit = 0,
    TransactionLockResolve = 1,
    KvRequest = 2,
    RegionCache = 3,
}

/// Native structured field passed to a trace event handler.
#[derive(Clone)]
pub struct TraceField {
    pub name: String,
    value: Arc<dyn Any + Send + Sync>,
}

impl TraceField {
    pub fn new<V>(name: impl Into<String>, value: V) -> Self
    where
        V: Any + Send + Sync,
    {
        Self {
            name: name.into(),
            value: Arc::new(value),
        }
    }

    pub fn value<V: Any + Send + Sync>(&self) -> Option<&V> {
        self.value.downcast_ref()
    }
}

impl std::fmt::Debug for TraceField {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TraceField")
            .field("name", &self.name)
            .field("value_type", &self.value.type_id())
            .finish()
    }
}

/// Immutable context supporting type-keyed values and trace-ID propagation.
#[derive(Clone, Default)]
pub struct TraceContext {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    trace_id: Option<Arc<[u8]>>,
}

impl TraceContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a derived context with a value keyed by the marker type `K`.
    pub fn with_value<K, V>(&self, value: V) -> Self
    where
        K: 'static,
        V: Any + Send + Sync,
    {
        let mut derived = self.clone();
        derived.values.insert(TypeId::of::<K>(), Arc::new(value));
        derived
    }

    pub fn value<K, V>(&self) -> Option<&V>
    where
        K: 'static,
        V: Any + Send + Sync,
    {
        self.values
            .get(&TypeId::of::<K>())
            .and_then(|value| value.downcast_ref())
    }

    pub fn with_trace_id(&self, trace_id: impl Into<Vec<u8>>) -> Self {
        let mut derived = self.clone();
        derived.trace_id = Some(Arc::from(trace_id.into()));
        derived
    }

    pub fn trace_id(&self) -> Option<&[u8]> {
        self.trace_id.as_deref()
    }
}

pub type TraceEventHandler =
    Arc<dyn Fn(&TraceContext, Category, &str, &[TraceField]) + Send + Sync>;
pub type CategoryEnabledHandler = Arc<dyn Fn(Category) -> bool + Send + Sync>;
pub type TraceControlExtractor = Arc<dyn Fn(&TraceContext) -> TraceControlFlags + Send + Sync>;

fn no_op_event(_: &TraceContext, _: Category, _: &str, _: &[TraceField]) {}
fn no_categories(_: Category) -> bool {
    false
}
fn default_trace_control(_: &TraceContext) -> TraceControlFlags {
    TraceControlFlags::TIKV_CATEGORY_REQUEST
}

lazy_static! {
    static ref TRACE_EVENT_HANDLER: RwLock<TraceEventHandler> = RwLock::new(Arc::new(no_op_event));
    static ref CATEGORY_ENABLED_HANDLER: RwLock<CategoryEnabledHandler> =
        RwLock::new(Arc::new(no_categories));
    static ref TRACE_CONTROL_EXTRACTOR: RwLock<TraceControlExtractor> =
        RwLock::new(Arc::new(default_trace_control));
}

/// Replace the event handler; `None` restores the no-op implementation.
pub fn set_trace_event_handler(handler: Option<TraceEventHandler>) {
    *TRACE_EVENT_HANDLER.write().unwrap() = handler.unwrap_or_else(|| Arc::new(no_op_event));
}

/// Replace the category predicate; `None` restores the always-disabled implementation.
pub fn set_category_enabled_handler(handler: Option<CategoryEnabledHandler>) {
    *CATEGORY_ENABLED_HANDLER.write().unwrap() = handler.unwrap_or_else(|| Arc::new(no_categories));
}

/// Emit an event through the currently registered handler.
pub fn trace_event(context: &TraceContext, category: Category, name: &str, fields: &[TraceField]) {
    let handler = TRACE_EVENT_HANDLER.read().unwrap().clone();
    handler(context, category, name, fields);
}

pub fn is_category_enabled(category: Category) -> bool {
    let handler = CATEGORY_ENABLED_HANDLER.read().unwrap().clone();
    handler(category)
}

/// Replace the control extractor; `None` restores request-category tracing.
pub fn set_trace_control_extractor(extractor: Option<TraceControlExtractor>) {
    *TRACE_CONTROL_EXTRACTOR.write().unwrap() =
        extractor.unwrap_or_else(|| Arc::new(default_trace_control));
}

pub fn trace_control_flags(context: &TraceContext) -> TraceControlFlags {
    let extractor = TRACE_CONTROL_EXTRACTOR.read().unwrap().clone();
    extractor(context)
}

pub fn immediate_logging_enabled(context: &TraceContext) -> bool {
    trace_control_flags(context).has(TraceControlFlags::IMMEDIATE_LOG)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn reset_handlers() {
        set_trace_event_handler(None);
        set_category_enabled_handler(None);
        set_trace_control_extractor(None);
    }

    #[test]
    fn trace_control_flag_values_and_operations_match_client_go() {
        assert_eq!(TraceControlFlags::IMMEDIATE_LOG.0, 1 << 0);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_REQUEST.0, 1 << 1);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS.0, 1 << 2);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS.0, 1 << 3);

        let empty = TraceControlFlags::default();
        assert!(!empty.has(TraceControlFlags::IMMEDIATE_LOG));
        let flags = empty
            .with(TraceControlFlags::IMMEDIATE_LOG)
            .with(TraceControlFlags::TIKV_CATEGORY_REQUEST)
            .with(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS)
            .with(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS);
        assert_eq!(flags.0, 0b1111);
        assert_eq!(flags.with(TraceControlFlags::IMMEDIATE_LOG), flags);
    }

    #[test]
    #[serial]
    fn extractor_defaults_custom_context_values_and_reset_match_source() {
        reset_handlers();
        let context = TraceContext::new();
        assert_eq!(
            trace_control_flags(&context),
            TraceControlFlags::TIKV_CATEGORY_REQUEST
        );
        assert!(!immediate_logging_enabled(&context));

        set_trace_control_extractor(Some(Arc::new(|_| {
            TraceControlFlags::IMMEDIATE_LOG | TraceControlFlags::TIKV_CATEGORY_REQUEST
        })));
        assert!(immediate_logging_enabled(&context));

        struct FlagsKey;
        set_trace_control_extractor(Some(Arc::new(|context| {
            context
                .value::<FlagsKey, TraceControlFlags>()
                .copied()
                .unwrap_or_default()
        })));
        assert_eq!(trace_control_flags(&context), TraceControlFlags::default());
        let detailed = context.with_value::<FlagsKey, _>(
            TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS
                | TraceControlFlags::TIKV_CATEGORY_READ_DETAILS,
        );
        assert!(trace_control_flags(&detailed).has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
        assert!(!immediate_logging_enabled(&detailed));

        set_trace_control_extractor(None);
        assert_eq!(
            trace_control_flags(&detailed),
            TraceControlFlags::TIKV_CATEGORY_REQUEST
        );
        reset_handlers();
    }

    #[test]
    #[serial]
    fn event_and_category_handlers_are_independent_and_resettable() {
        reset_handlers();
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        set_trace_event_handler(Some(Arc::new(move |_, category, name, fields| {
            assert_eq!(category, Category::TransactionTwoPhaseCommit);
            assert_eq!(name, "test");
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "key");
            assert_eq!(fields[0].value::<&str>(), Some(&"value"));
            observed.store(true, Ordering::SeqCst);
        })));
        trace_event(
            &TraceContext::new(),
            Category::TransactionTwoPhaseCommit,
            "test",
            &[TraceField::new("key", "value")],
        );
        assert!(called.load(Ordering::SeqCst));

        assert!(!is_category_enabled(Category::TransactionTwoPhaseCommit));
        set_category_enabled_handler(Some(Arc::new(|category| {
            category == Category::TransactionTwoPhaseCommit
        })));
        assert!(is_category_enabled(Category::TransactionTwoPhaseCommit));
        assert!(!is_category_enabled(Category::TransactionLockResolve));

        called.store(false, Ordering::SeqCst);
        set_trace_event_handler(None);
        trace_event(
            &TraceContext::new(),
            Category::TransactionTwoPhaseCommit,
            "test",
            &[],
        );
        assert!(!called.load(Ordering::SeqCst));
        reset_handlers();
    }

    #[test]
    fn trace_ids_are_absent_in_root_contexts_and_override_in_derived_contexts() {
        let context = TraceContext::new();
        assert_eq!(context.trace_id(), None);
        let first = context.with_trace_id(vec![1, 2, 3, 4, 5]);
        assert_eq!(first.trace_id(), Some(&[1, 2, 3, 4, 5][..]));
        let second = first.with_trace_id(vec![6, 7, 8, 9, 10]);
        assert_eq!(second.trace_id(), Some(&[6, 7, 8, 9, 10][..]));
        assert_eq!(first.trace_id(), Some(&[1, 2, 3, 4, 5][..]));
    }
}
