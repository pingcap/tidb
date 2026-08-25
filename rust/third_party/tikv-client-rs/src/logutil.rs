// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Context-aware logging, tracing, and protobuf-safe formatting.
//!
//! This is the native Rust counterpart of client-go's `internal/logutil`
//! package. The package is public-but-hidden so every client module can share
//! one implementation without making it a supported top-level API.

use std::fmt;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;
use log::{Level, Log, Metadata, Record};
use prost::{Message, Name};
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, ReflectMessage, Value,
};

use crate::trace::TraceContext;

const DESCRIPTOR_BYTES: &[u8] = include_bytes!("generated/file_descriptor_set.bin");

lazy_static! {
    static ref PROTOBUF_DESCRIPTORS: DescriptorPool = DescriptorPool::decode(DESCRIPTOR_BYTES)
        .expect("checked-in protobuf descriptors are valid");
    static ref TRACE_EVENT_KEY: RwLock<String> = RwLock::new("event".to_owned());
}

struct ContextLoggerKey;
struct ContextSpanKey;

/// Return the process-wide logger, matching client-go's `BgLogger` fallback.
pub fn background_logger() -> &'static dyn Log {
    log::logger()
}

/// Derive a context carrying a logger. The last attached logger wins.
pub fn with_logger(context: &TraceContext, logger: Arc<dyn Log>) -> TraceContext {
    context.with_value::<ContextLoggerKey, _>(logger)
}

/// Return the contextual logger, or the process-wide logger when none exists.
pub fn logger<'a>(context: &'a TraceContext) -> &'a (dyn Log + 'static) {
    if let Some(logger) = context.value::<ContextLoggerKey, Arc<dyn Log>>() {
        logger.as_ref()
    } else {
        background_logger()
    }
}

/// Log a warning in production and panic in unit-test builds.
///
/// Structured zap fields map to the caller's formatted message in Rust. This
/// keeps the source assertion boundary while using the crate's `log` facade.
pub fn assert_warn(target: &dyn Log, message: &str) {
    if cfg!(test) {
        panic!("{message}");
    }

    let metadata = Metadata::builder()
        .level(Level::Warn)
        .target(module_path!())
        .build();
    if target.enabled(&metadata) {
        target.log(
            &Record::builder()
                .metadata(metadata)
                .args(format_args!("{message}"))
                .build(),
        );
    }
}

/// Context-owned span hooks used by [`event`], [`eventf`], and [`set_tag`].
///
/// A context without a span is intentionally a no-op. This directly matches
/// the source checks for both an OpenTracing span and its tracer.
pub trait ContextSpan: Send + Sync {
    /// Log one string field on the active span.
    fn log_field(&self, key: &str, value: &str);

    /// Set one typed tag on the active span.
    fn set_tag(&self, key: &str, value: &(dyn fmt::Debug + Send + Sync));
}

/// Derive a context carrying an active span. The last attached span wins.
pub fn with_span(context: &TraceContext, span: Arc<dyn ContextSpan>) -> TraceContext {
    context.with_value::<ContextSpanKey, _>(span)
}

/// Return the current replaceable event-field key.
pub fn trace_event_key() -> String {
    TRACE_EVENT_KEY.read().unwrap().clone()
}

/// Replace the event-field key used by subsequent span events.
pub fn set_trace_event_key(key: impl Into<String>) {
    *TRACE_EVENT_KEY.write().unwrap() = key.into();
}

/// Record an event on the current span, or do nothing when no span exists.
pub fn event(context: &TraceContext, value: &str) {
    let Some(span) = context.value::<ContextSpanKey, Arc<dyn ContextSpan>>() else {
        return;
    };
    span.log_field(&trace_event_key(), value);
}

/// Format and record an event on the current span.
pub fn eventf(context: &TraceContext, arguments: fmt::Arguments<'_>) {
    let Some(span) = context.value::<ContextSpanKey, Arc<dyn ContextSpan>>() else {
        return;
    };
    span.log_field(&trace_event_key(), &arguments.to_string());
}

/// Set a typed tag on the current span, or do nothing when no span exists.
pub fn set_tag<V>(context: &TraceContext, key: &str, value: V)
where
    V: fmt::Debug + Send + Sync,
{
    let Some(span) = context.value::<ContextSpanKey, Arc<dyn ContextSpan>>() else {
        return;
    };
    span.set_tag(key, &value);
}

/// Return the complete descriptor pool generated from the checked-in schemas.
pub fn protobuf_descriptors() -> &'static DescriptorPool {
    &PROTOBUF_DESCRIPTORS
}

/// A display wrapper that renders every protobuf field and hexadecimal byte
/// field without relying on message-specific formatting implementations.
pub struct Hex<'a, M> {
    message: &'a M,
}

/// Wrap any generated protobuf message for source-compatible safe formatting.
pub fn hex<M>(message: &M) -> Hex<'_, M>
where
    M: Message + Name,
{
    Hex { message }
}

impl<M> fmt::Display for Hex<'_, M>
where
    M: Message + Name,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let full_name = M::full_name();
        let descriptor = PROTOBUF_DESCRIPTORS
            .get_message_by_name(&full_name)
            .unwrap_or_else(|| panic!("protobuf descriptor missing for {full_name}"));
        let message = DynamicMessage::decode(descriptor, self.message.encode_to_vec().as_slice())
            .unwrap_or_else(|error| panic!("cannot reflect {full_name}: {error}"));
        format_message(formatter, &message)
    }
}

fn format_message(formatter: &mut fmt::Formatter<'_>, message: &DynamicMessage) -> fmt::Result {
    formatter.write_str("{")?;
    let mut fields: Vec<_> = message.descriptor().fields().collect();
    fields.sort_by_key(|field| field.path().last().copied().unwrap_or_default());

    let mut wrote_field = false;
    let mut emitted_oneofs = Vec::new();
    for field in fields {
        if let Some(oneof) = field.containing_oneof() {
            if !field
                .field_descriptor_proto()
                .proto3_optional
                .unwrap_or(false)
            {
                let name = oneof.full_name().to_owned();
                if emitted_oneofs.iter().any(|emitted| emitted == &name) {
                    continue;
                }
                emitted_oneofs.push(name);
                write_separator(formatter, &mut wrote_field)?;
                write!(formatter, "{}:", go_field_name(oneof.name()))?;
                if let Some(active) = oneof
                    .fields()
                    .find(|candidate| message.has_field(candidate))
                {
                    formatter.write_str("&{")?;
                    format_value(
                        formatter,
                        message.get_field(&active).as_ref(),
                        Some(&active.kind()),
                    )?;
                    formatter.write_str("}")?;
                } else {
                    formatter.write_str("<nil>")?;
                }
                continue;
            }
        }

        write_separator(formatter, &mut wrote_field)?;
        write!(formatter, "{}:", go_field_name(field.json_name()))?;
        format_field(formatter, message, &field)?;
    }
    formatter.write_str("}")
}

fn write_separator(formatter: &mut fmt::Formatter<'_>, wrote: &mut bool) -> fmt::Result {
    if *wrote {
        formatter.write_str(" ")?;
    }
    *wrote = true;
    Ok(())
}

fn format_field(
    formatter: &mut fmt::Formatter<'_>,
    message: &DynamicMessage,
    field: &FieldDescriptor,
) -> fmt::Result {
    if field.supports_presence() && !message.has_field(field) {
        return formatter.write_str("<nil>");
    }
    format_value(
        formatter,
        message.get_field(field).as_ref(),
        Some(&field.kind()),
    )
}

fn format_value(
    formatter: &mut fmt::Formatter<'_>,
    value: &Value,
    kind: Option<&Kind>,
) -> fmt::Result {
    match value {
        Value::Bool(value) => write!(formatter, "{value}"),
        Value::I32(value) => write!(formatter, "{value}"),
        Value::EnumNumber(value) => match kind {
            Some(Kind::Enum(descriptor)) => match descriptor.get_value(*value) {
                Some(value) => formatter.write_str(value.name()),
                None => write!(formatter, "{value}"),
            },
            _ => write!(formatter, "{value}"),
        },
        Value::I64(value) => write!(formatter, "{value}"),
        Value::U32(value) => write!(formatter, "{value}"),
        Value::U64(value) => write!(formatter, "{value}"),
        Value::F32(value) => write!(formatter, "{value}"),
        Value::F64(value) => write!(formatter, "{value}"),
        Value::String(value) => formatter.write_str(value),
        Value::Bytes(value) => formatter.write_str(&crate::redact::key(value)),
        Value::Message(value) => format_message(formatter, value),
        Value::List(values) => {
            formatter.write_str("[")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    formatter.write_str(" ")?;
                }
                format_value(formatter, value, kind)?;
            }
            formatter.write_str("]")
        }
        Value::Map(values) => {
            let mut values: Vec<_> = values.iter().collect();
            values.sort_by_key(|(key, _)| MapKey::clone(key));
            let value_kind = match kind {
                Some(Kind::Message(entry)) if entry.is_map_entry() => {
                    Some(entry.map_entry_value_field().kind())
                }
                _ => None,
            };
            formatter.write_str("map[")?;
            for (index, (key, value)) in values.into_iter().enumerate() {
                if index != 0 {
                    formatter.write_str(" ")?;
                }
                write!(formatter, "{}:", map_key_text(key))?;
                format_value(formatter, value, value_kind.as_ref())?;
            }
            formatter.write_str("]")
        }
    }
}

fn map_key_text(key: &MapKey) -> String {
    match key {
        MapKey::Bool(value) => value.to_string(),
        MapKey::I32(value) => value.to_string(),
        MapKey::I64(value) => value.to_string(),
        MapKey::U32(value) => value.to_string(),
        MapKey::U64(value) => value.to_string(),
        MapKey::String(value) => value.clone(),
    }
}

fn go_field_name(json_name: &str) -> String {
    let mut characters = json_name.chars();
    let Some(first) = characters.next() else {
        return String::new();
    };
    first.to_uppercase().chain(characters).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serial_test::serial;

    use super::*;
    use crate::proto::{kvrpcpb, metapb};

    #[derive(Default)]
    struct RecordingLogger {
        messages: Mutex<Vec<String>>,
    }

    impl Log for RecordingLogger {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn log(&self, record: &Record<'_>) {
            self.messages
                .lock()
                .unwrap()
                .push(record.args().to_string());
        }

        fn flush(&self) {}
    }

    #[derive(Default)]
    struct RecordingSpan {
        fields: Mutex<Vec<(String, String)>>,
        tags: Mutex<Vec<(String, String)>>,
    }

    impl ContextSpan for RecordingSpan {
        fn log_field(&self, key: &str, value: &str) {
            self.fields
                .lock()
                .unwrap()
                .push((key.to_owned(), value.to_owned()));
        }

        fn set_tag(&self, key: &str, value: &(dyn fmt::Debug + Send + Sync)) {
            self.tags
                .lock()
                .unwrap()
                .push((key.to_owned(), format!("{value:?}")));
        }
    }

    struct ResetRedaction;

    impl Drop for ResetRedaction {
        fn drop(&mut self) {
            crate::redact::set_redact_log_enabled(false);
        }
    }

    #[test]
    fn contextual_logger_overrides_the_background_fallback() {
        let recording = Arc::new(RecordingLogger::default());
        let context = with_logger(&TraceContext::new(), recording.clone());
        let metadata = Metadata::builder()
            .level(Level::Info)
            .target("source")
            .build();
        logger(&context).log(
            &Record::builder()
                .metadata(metadata)
                .args(format_args!("contextual"))
                .build(),
        );
        assert_eq!(
            recording.messages.lock().unwrap().as_slice(),
            ["contextual"]
        );
        assert!(std::ptr::eq(
            logger(&TraceContext::new()),
            background_logger()
        ));
    }

    #[test]
    #[should_panic(expected = "source invariant")]
    fn assert_warn_panics_in_test_builds() {
        assert_warn(background_logger(), "source invariant");
    }

    #[test]
    #[serial]
    fn events_and_tags_require_a_contextual_span() {
        set_trace_event_key("event");
        let empty = TraceContext::new();
        event(&empty, "ignored");
        set_tag(&empty, "ignored", 1_u64);

        let span = Arc::new(RecordingSpan::default());
        let context = with_span(&empty, span.clone());
        event(&context, "started");
        eventf(&context, format_args!("batch {}", 3));
        set_tag(&context, "commitTs", 42_u64);
        set_trace_event_key("kind");
        event(&context, "finished");

        assert_eq!(
            span.fields.lock().unwrap().as_slice(),
            [
                ("event".to_owned(), "started".to_owned()),
                ("event".to_owned(), "batch 3".to_owned()),
                ("kind".to_owned(), "finished".to_owned()),
            ]
        );
        assert_eq!(
            span.tags.lock().unwrap().as_slice(),
            [("commitTs".to_owned(), "42".to_owned())]
        );
        set_trace_event_key("event");
    }

    #[test]
    #[serial]
    fn protobuf_hex_walks_nested_repeated_and_byte_fields() {
        crate::redact::set_redact_log_enabled(false);
        let _reset = ResetRedaction;
        let region = metapb::Region {
            id: 7,
            start_key: vec![0xab, 0],
            end_key: vec![0xff],
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 2,
                version: 3,
            }),
            peers: vec![metapb::Peer {
                id: 11,
                store_id: 12,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            hex(&region).to_string(),
            "{Id:7 StartKey:AB00 EndKey:FF RegionEpoch:{ConfVer:2 Version:3} Peers:[{Id:11 StoreId:12 Role:Voter IsWitness:false}] EncryptionMeta:<nil> IsInFlashback:false FlashbackStartTs:0}"
        );

        crate::redact::set_redact_log_enabled(true);
        assert_eq!(
            hex(&region).to_string(),
            "{Id:7 StartKey:? EndKey:? RegionEpoch:{ConfVer:2 Version:3} Peers:[{Id:11 StoreId:12 Role:Voter IsWitness:false}] EncryptionMeta:<nil> IsInFlashback:false FlashbackStartTs:0}"
        );
    }

    #[test]
    #[serial]
    fn protobuf_hex_covers_empty_repeated_bytes_and_oneofs() {
        crate::redact::set_redact_log_enabled(false);
        let _reset = ResetRedaction;
        let buckets = metapb::BucketMeta {
            keys: vec![b"a".to_vec(), b"bc".to_vec()],
            ..Default::default()
        };
        assert_eq!(hex(&buckets).to_string(), "{Version:0 Keys:[61 6263]}");

        let empty_error = kvrpcpb::KeyError::default();
        let missing = hex(&empty_error).to_string();
        assert!(missing.starts_with("{Locked:<nil> Retryable:"));
        assert!(missing.ends_with("}") && missing.contains("PrimaryMismatch:<nil>"));
    }

    #[test]
    fn descriptor_pool_covers_every_generated_named_message() {
        assert_eq!(protobuf_descriptors().all_messages().count(), 1_029);
        assert!(protobuf_descriptors()
            .get_message_by_name(<metapb::Region as Name>::full_name().as_str())
            .is_some());
        assert!(protobuf_descriptors()
            .get_message_by_name(<kvrpcpb::KeyError as Name>::full_name().as_str())
            .is_some());
    }
}
