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

const DESCRIPTOR_BYTES: &[u8] = tikv_client_kvproto::FILE_DESCRIPTOR_SET;

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

/// zap's `zap.Duration` value as TiDB's logger encodes it (Go
/// `time.Duration.String()`), for structured log fields.
pub(crate) fn go_duration(duration: std::time::Duration) -> String {
    crate::trace::format_go_duration(duration)
}

/// zap's `zap.Time` value as TiDB's logger encodes it: pingcap/log's
/// `DefaultTimeEncoder` layout (`2006/01/02 15:04:05.000 -07:00`) in Go's
/// `time.Local`.
pub(crate) fn zap_time(time: std::time::SystemTime) -> String {
    let utc: chrono::DateTime<chrono::Utc> = time.into();
    utc.with_timezone(&crate::error::go_time_local())
        .format("%Y/%m/%d %H:%M:%S%.3f %:z")
        .to_string()
}

/// zap's `zap.Reflect` (and `zap.Any` of a non-Stringer) rendering of a
/// generated protobuf message: Go `encoding/json` over the gogo struct, whose
/// fields carry `json:"<proto name>,omitempty"` tags. `None` is a nil pointer.
pub(crate) fn go_json_protobuf<M>(message: Option<&M>) -> String
where
    M: Message + Name,
{
    let mut output = String::new();
    match message {
        Some(message) => append_go_json_message(&mut output, &reflect(message)),
        None => output.push_str("null"),
    }
    output
}

/// [`go_json_protobuf`] for a `[]*T` slice; `None` is a nil slice.
pub(crate) fn go_json_protobuf_slice<M>(messages: Option<&[M]>) -> String
where
    M: Message + Name,
{
    let Some(messages) = messages else {
        return "null".to_owned();
    };
    let mut output = String::from("[");
    for (index, message) in messages.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        append_go_json_message(&mut output, &reflect(message));
    }
    output.push(']');
    output
}

fn reflect<M>(message: &M) -> DynamicMessage
where
    M: Message + Name,
{
    let full_name = M::full_name();
    let descriptor = PROTOBUF_DESCRIPTORS
        .get_message_by_name(&full_name)
        .unwrap_or_else(|| panic!("protobuf descriptor missing for {full_name}"));
    DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice())
        .unwrap_or_else(|error| panic!("cannot reflect {full_name}: {error}"))
}

fn append_go_json_message(output: &mut String, message: &DynamicMessage) {
    output.push('{');
    let mut wrote = false;
    let mut emitted_oneofs = Vec::new();
    // Go struct fields follow the declaration order of the .proto file.
    for field in message.descriptor().fields() {
        if let Some(oneof) = field.containing_oneof() {
            if !field
                .field_descriptor_proto()
                .proto3_optional
                .unwrap_or(false)
            {
                // The oneof is one interface field, with no json tag (so no
                // omitempty), holding a pointer to a one-field wrapper struct.
                let name = oneof.full_name().to_owned();
                if emitted_oneofs.contains(&name) {
                    continue;
                }
                emitted_oneofs.push(name);
                if wrote {
                    output.push(',');
                }
                wrote = true;
                append_go_json_string(output, &go_field_name(&lower_camel(oneof.name())));
                output.push(':');
                match oneof
                    .fields()
                    .find(|candidate| message.has_field(candidate))
                {
                    Some(active) => {
                        // The wrapper's one field is itself `omitempty`.
                        output.push('{');
                        let value = message.get_field(&active);
                        if !go_json_is_empty(value.as_ref(), false) {
                            append_go_json_string(output, active.name());
                            output.push(':');
                            append_go_json_value(output, value.as_ref());
                        }
                        output.push('}');
                    }
                    None => output.push_str("null"),
                }
                continue;
            }
        }
        let value = message.get_field(&field);
        if go_json_is_empty(
            value.as_ref(),
            field.supports_presence() && !message.has_field(&field),
        ) {
            continue;
        }
        if wrote {
            output.push(',');
        }
        wrote = true;
        append_go_json_string(output, field.name());
        output.push(':');
        append_go_json_value(output, value.as_ref());
    }
    output.push('}');
}

fn lower_camel(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut upper = false;
    for character in name.chars() {
        if character == '_' {
            upper = true;
        } else if upper {
            result.extend(character.to_uppercase());
            upper = false;
        } else {
            result.push(character);
        }
    }
    result
}

/// `omitempty`: false, 0, "", nil pointers and empty slices/maps.
fn go_json_is_empty(value: &Value, absent: bool) -> bool {
    if absent {
        return true;
    }
    match value {
        Value::Bool(value) => !value,
        Value::I32(value) | Value::EnumNumber(value) => *value == 0,
        Value::I64(value) => *value == 0,
        Value::U32(value) => *value == 0,
        Value::U64(value) => *value == 0,
        Value::F32(value) => *value == 0.0,
        Value::F64(value) => *value == 0.0,
        Value::String(value) => value.is_empty(),
        Value::Bytes(value) => value.is_empty(),
        Value::Message(_) => false,
        Value::List(values) => values.is_empty(),
        Value::Map(values) => values.is_empty(),
    }
}

fn append_go_json_value(output: &mut String, value: &Value) {
    match value {
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        // gogo enums have no MarshalJSON, so encoding/json writes the number.
        Value::I32(value) | Value::EnumNumber(value) => output.push_str(&value.to_string()),
        Value::I64(value) => output.push_str(&value.to_string()),
        Value::U32(value) => output.push_str(&value.to_string()),
        Value::U64(value) => output.push_str(&value.to_string()),
        Value::F32(value) => output.push_str(&go_json_float(f64::from(*value), 32)),
        Value::F64(value) => output.push_str(&go_json_float(*value, 64)),
        Value::String(value) => append_go_json_string(output, value),
        Value::Bytes(value) => {
            output.push('"');
            output.push_str(&base64_std(value));
            output.push('"');
        }
        Value::Message(value) => append_go_json_message(output, value),
        Value::List(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                append_go_json_value(output, value);
            }
            output.push(']');
        }
        Value::Map(values) => {
            // encoding/json sorts map keys by their string form.
            let mut values: Vec<_> = values
                .iter()
                .map(|(key, value)| (map_key_text(key), value))
                .collect();
            values.sort_by(|left, right| left.0.cmp(&right.0));
            output.push('{');
            for (index, (key, value)) in values.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                append_go_json_string(output, &key);
                output.push(':');
                append_go_json_value(output, value);
            }
            output.push('}');
        }
    }
}

/// encoding/json's float encoding: shortest round-trip digits, plain
/// notation for 1e-6 <= |f| < 1e21, otherwise exponent form with a
/// two-digit-minimum exponent trimmed to Go's `e-07` -> `e-7` style.
fn go_json_float(value: f64, bits: u32) -> String {
    if value.is_nan() || value.is_infinite() {
        // encoding/json refuses these; zap then records the error text.
        return format!("\"json: unsupported value: {}\"", go_float_word(value));
    }
    let absolute = value.abs();
    let plain = absolute == 0.0 || (1e-6..1e21).contains(&absolute);
    if bits == 32 {
        let narrow = value as f32;
        if plain {
            return format!("{narrow}");
        }
        return go_exponent(&format!("{narrow:e}"));
    }
    if plain {
        return format!("{value}");
    }
    go_exponent(&format!("{value:e}"))
}

fn go_float_word(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value.is_sign_positive() {
        "+Inf"
    } else {
        "-Inf"
    }
}

fn go_exponent(rust: &str) -> String {
    let Some((mantissa, exponent)) = rust.split_once('e') else {
        return rust.to_owned();
    };
    let (sign, digits) = match exponent.strip_prefix('-') {
        Some(digits) => ('-', digits),
        None => ('+', exponent),
    };
    format!("{mantissa}e{sign}{digits}")
}

/// encoding/json string escaping, including its default HTML-safe escapes.
fn append_go_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                output.push_str(&format!("\\u{:04x}", character as u32))
            }
            character if (character as u32) < 0x20 => {
                output.push_str(&format!("\\u{:04x}", character as u32))
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

fn base64_std(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let triple = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for index in 0..4 {
            if index <= chunk.len() {
                output.push(ALPHABET[((triple >> (18 - 6 * index)) & 0x3f) as usize] as char);
            } else {
                output.push('=');
            }
        }
    }
    output
}

/// A process-wide capturing logger for tests that assert client-go's log
/// lines arrive with their structured fields, the way TiDB's `log` bridge
/// receives them: `"LEVEL message key=value ..."`. Only one logger can be
/// installed per process, so every test module shares this one.
/// Go `log.Fatal`'s effect after it logs its entry: zap syncs the logger and
/// the process exits with status 1 (`os.Exit(1)`). Go's client takes the whole
/// process down on these paths, so an embedding TiDB stops exactly as Go's
/// would; unit tests record the exit instead (see [`exits_for_test`]).
pub(crate) fn fatal_exit() {
    log::logger().flush();
    exit_process(1);
}

/// Go's runtime on a panic that no goroutine recovers: after printing the
/// panic (Rust's panic hook has printed it) the process exits with status 2.
pub(crate) fn unrecovered_panic_exit() {
    log::logger().flush();
    exit_process(2);
}

#[cfg(not(test))]
fn exit_process(code: i32) {
    std::process::exit(code);
}

#[cfg(test)]
static EXITS: std::sync::Mutex<Vec<i32>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn exit_process(code: i32) {
    EXITS.lock().unwrap().push(code);
}

/// The exit codes a unit test's process would have exited with so far.
#[cfg(test)]
pub(crate) fn exits_for_test() -> Vec<i32> {
    EXITS.lock().unwrap().clone()
}

/// The text of a caught panic's payload (Go's `recover()` value).
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "<non-string panic payload>".to_owned()
    }
}

#[cfg(test)]
pub(crate) mod test_capture {
    use std::sync::{Mutex, Once};

    static CAPTURED_LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static LOGGER: CaptureLogger = CaptureLogger;
    static LOGGER_INIT: Once = Once::new();

    struct CaptureLogger;

    struct CaptureFields<'a>(&'a mut String);

    impl<'kvs> log::kv::VisitSource<'kvs> for CaptureFields<'_> {
        fn visit_pair(
            &mut self,
            key: log::kv::Key<'kvs>,
            value: log::kv::Value<'kvs>,
        ) -> Result<(), log::kv::Error> {
            self.0.push_str(&format!(" {key}={value}"));
            Ok(())
        }
    }

    impl log::Log for CaptureLogger {
        fn enabled(&self, _metadata: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            let mut line = format!("{} {}", record.level(), record.args());
            let _ = record.key_values().visit(&mut CaptureFields(&mut line));
            if let Ok(mut logs) = CAPTURED_LOGS.lock() {
                logs.push(line);
            }
        }

        fn flush(&self) {}
    }

    /// Installs the capturing logger once; a foreign logger installed first
    /// is kept, and assertions then fail to find their markers.
    pub(crate) fn install() {
        LOGGER_INIT.call_once(|| {
            let _ = log::set_logger(&LOGGER);
            log::set_max_level(log::LevelFilter::Debug);
        });
    }

    /// The lines captured so far, across every test in the process.
    pub(crate) fn lines() -> Vec<String> {
        CAPTURED_LOGS.lock().unwrap().clone()
    }

    /// Whether a captured line satisfies `matches`.
    pub(crate) fn any(matches: impl Fn(&str) -> bool) -> bool {
        CAPTURED_LOGS
            .lock()
            .unwrap()
            .iter()
            .any(|line| matches(line))
    }

    /// Waits up to a second for a line satisfying `matches`, then panics with
    /// every captured line.
    pub(crate) async fn wait_for(description: &str, matches: impl Fn(&str) -> bool) {
        for _ in 0..200 {
            if any(&matches) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("expected {description}; captured: {:?}", lines());
    }
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
        assert_eq!(protobuf_descriptors().all_messages().count(), 1_032);
        assert!(protobuf_descriptors()
            .get_message_by_name(<metapb::Region as Name>::full_name().as_str())
            .is_some());
        assert!(protobuf_descriptors()
            .get_message_by_name(<kvrpcpb::KeyError as Name>::full_name().as_str())
            .is_some());
        assert!(protobuf_descriptors()
            .get_message_by_name(<kvrpcpb::LockUpgradeConflict as Name>::full_name().as_str())
            .is_some());
        assert!(protobuf_descriptors()
            .get_message_by_name(<kvrpcpb::SharedLockLost as Name>::full_name().as_str())
            .is_some());
        assert!(protobuf_descriptors()
            .get_message_by_name(
                <crate::proto::pdpb::GlobalGcBarriersInfo as Name>::full_name().as_str()
            )
            .is_some());
    }
}

#[cfg(test)]
mod go_encoding_tests {
    use super::*;
    use crate::proto::resource_manager as rmpb;

    /// zap.Reflect of `[]*rmpb.TokenBucketRequest`: encoding/json over the
    /// gogo structs -- proto field names, omitempty, the oneof as a named
    /// interface field holding its wrapper struct.
    #[test]
    fn protobuf_slices_render_as_go_encoding_json() {
        let request = rmpb::TokenBucketRequest {
            resource_group_name: "rg<1>".to_owned(),
            request: Some(rmpb::token_bucket_request::Request::RuItems(
                rmpb::token_bucket_request::RequestRu {
                    request_r_u: vec![rmpb::RequestUnitItem {
                        r#type: 0,
                        value: 1.5,
                    }],
                },
            )),
            keyspace_id: Some(rmpb::KeyspaceIdValue {
                keyspace: Some(rmpb::keyspace_id_value::Keyspace::Value(7)),
            }),
            ..Default::default()
        };
        assert_eq!(
            go_json_protobuf_slice(Some(&[request][..])),
            r#"[{"resource_group_name":"rg\u003c1\u003e","Request":{"ru_items":{"request_r_u":[{"value":1.5}]}},"keyspace_id":{"Keyspace":{"value":7}}}]"#
        );
        assert_eq!(
            go_json_protobuf_slice::<rmpb::TokenBucketResponse>(None),
            "null"
        );
        assert_eq!(
            go_json_protobuf(Some(&rmpb::TokenBucketRequest::default())),
            r#"{"Request":null}"#
        );
        assert_eq!(go_json_float(1e21, 64), "1e+21");
        assert_eq!(go_json_float(1e-7, 64), "1e-7");
        assert_eq!(go_json_float(0.25, 64), "0.25");
        assert_eq!(base64_std(b"ab"), "YWI=");
    }

    #[test]
    fn zap_time_uses_pingcap_logs_layout() {
        let time = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_234);
        let rendered = zap_time(time);
        // `1970/01/01 00:00:01.234 +00:00` in UTC; the zone follows time.Local.
        assert_eq!(rendered.len(), "1970/01/01 00:00:01.234 +00:00".len());
        assert!(rendered.contains(":01.234 "), "{rendered}");
    }
}
