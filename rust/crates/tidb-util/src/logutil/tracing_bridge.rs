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

//! Routes `tracing` events into the global logger.
//!
//! Ported call sites that Go writes as `logutil.BgLogger().Warn("msg",
//! zap.Error(err))` are spelled `tracing::warn!(error = %err, "msg")` in
//! several crates. Go has one process-global zap logger; this subscriber is
//! the Rust seam that sends those events to it, so they land in the log
//! file in the unified format: the event's message is the entry message, its
//! other fields are the entry fields in call-site order, and its source
//! location is the caller. Spans carry no log content and are not recorded.

use std::fmt;
use std::sync::Once;

use ::tracing::field::{Field as TracingField, Visit};
use ::tracing::span::{Attributes, Id, Record};
use ::tracing::subscriber::Interest;
use ::tracing::{Event, Metadata, Subscriber};
use tidb_log::{Field, Level, Value};

/// Installs the subscriber as the process-wide `tracing` dispatcher, once.
/// It reads the current global logger per event, so later
/// `init_logger`/`replace_logger` calls take effect without reinstalling.
pub(super) fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = ::tracing::subscriber::set_global_default(UnifiedLogSubscriber);
    });
    level_changed();
}

/// Re-reads the global level into `tracing`'s static filter, so events
/// below it (the transport crates' per-request debug/trace events) are
/// skipped at their call sites instead of reaching the subscriber.
pub(super) fn level_changed() {
    ::tracing::callsite::rebuild_interest_cache();
}

struct UnifiedLogSubscriber;

// zap has no trace level; TRACE events log at debug.
fn level_of(metadata: &Metadata<'_>) -> Level {
    match *metadata.level() {
        ::tracing::Level::ERROR => Level::Error,
        ::tracing::Level::WARN => Level::Warn,
        ::tracing::Level::INFO => Level::Info,
        _ => Level::Debug,
    }
}

impl Subscriber for UnifiedLogSubscriber {
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        // The level can change at runtime (`SetLevel`), so every event asks.
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && super::bg_logger().enabled(level_of(metadata))
    }

    fn max_level_hint(&self) -> Option<::tracing::level_filters::LevelFilter> {
        use ::tracing::level_filters::LevelFilter;
        Some(match super::get_level() {
            Level::Debug => LevelFilter::TRACE,
            Level::Info => LevelFilter::INFO,
            Level::Warn => LevelFilter::WARN,
            _ => LevelFilter::ERROR,
        })
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        let mut visitor = EntryVisitor::default();
        event.record(&mut visitor);
        super::bg_logger().emit_at(
            level_of(metadata),
            metadata.file().unwrap_or_default(),
            metadata.line().unwrap_or_default(),
            &visitor.message,
            &visitor.fields,
        );
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Default)]
struct EntryVisitor {
    message: String,
    fields: Vec<Field>,
}

impl EntryVisitor {
    fn push(&mut self, field: &TracingField, value: Value) {
        if field.name() == "message" {
            if let Value::Str(message) = value {
                self.message = message;
                return;
            }
        }
        self.fields.push(Field::new(field.name(), value));
    }
}

impl Visit for EntryVisitor {
    fn record_f64(&mut self, field: &TracingField, value: f64) {
        self.push(field, Value::F64(value));
    }

    fn record_i64(&mut self, field: &TracingField, value: i64) {
        self.push(field, Value::I64(value));
    }

    fn record_u64(&mut self, field: &TracingField, value: u64) {
        self.push(field, Value::U64(value));
    }

    fn record_bool(&mut self, field: &TracingField, value: bool) {
        self.push(field, Value::Bool(value));
    }

    fn record_str(&mut self, field: &TracingField, value: &str) {
        self.push(field, Value::Str(value.to_owned()));
    }

    fn record_error(&mut self, field: &TracingField, value: &(dyn std::error::Error + 'static)) {
        self.push(field, Value::Str(value.to_string()));
    }

    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        // `%value` arrives here wrapped so that `Debug` renders `Display`,
        // and the message arrives as `format_args!`, which also renders
        // without quotes.
        self.push(field, Value::Str(format!("{value:?}")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visitor_splits_message_from_fields_in_call_site_order() {
        struct Capture(std::sync::Mutex<Vec<(String, Vec<Field>)>>);
        impl Subscriber for Capture {
            fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _span: &Attributes<'_>) -> Id {
                Id::from_u64(1)
            }
            fn record(&self, _span: &Id, _values: &Record<'_>) {}
            fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
            fn event(&self, event: &Event<'_>) {
                let mut visitor = EntryVisitor::default();
                event.record(&mut visitor);
                self.0
                    .lock()
                    .unwrap()
                    .push((visitor.message, visitor.fields));
            }
            fn enter(&self, _span: &Id) {}
            fn exit(&self, _span: &Id) {}
        }
        let capture = std::sync::Arc::new(Capture(std::sync::Mutex::new(Vec::new())));
        let error = std::io::Error::other("disk full");
        ::tracing::subscriber::with_default(std::sync::Arc::clone(&capture), || {
            ::tracing::warn!(error = %error, table_id = 7_i64, "spill to disk failed");
        });
        let captured = capture.0.lock().unwrap();
        let (message, fields) = &captured[0];
        assert_eq!(message, "spill to disk failed");
        let rendered = tidb_log::TextEncoder::default().encode_entry(
            &tidb_log::Entry {
                time: chrono::Local::now().fixed_offset(),
                level: Level::Warn,
                logger_name: String::new(),
                caller: Some(("row_container.rs".to_owned(), 712)),
                message: message.clone(),
                stack: String::new(),
            },
            fields,
        );
        assert!(
            rendered.ends_with(
                "[WARN] [row_container.rs:712] [\"spill to disk failed\"] \
                 [error=\"disk full\"] [table_id=7]\n"
            ),
            "{rendered}"
        );
    }
}
