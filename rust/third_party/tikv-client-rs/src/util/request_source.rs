// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use crate::trace::TraceContext;

pub const INTERNAL_TXN_OTHERS: &str = "others";
pub const INTERNAL_TXN_GC: &str = "gc";
pub const INTERNAL_TXN_META: &str = INTERNAL_TXN_OTHERS;
pub const INTERNAL_TXN_STATS: &str = "stats";

pub const EXPLICIT_TYPE_EMPTY: &str = "";
pub const EXPLICIT_TYPE_LIGHTNING: &str = "lightning";
pub const EXPLICIT_TYPE_BR: &str = "br";
pub const EXPLICIT_TYPE_DUMPLING: &str = "dumpling";
pub const EXPLICIT_TYPE_BACKGROUND: &str = "background";
pub const EXPLICIT_TYPE_DDL: &str = "ddl";
pub const EXPLICIT_TYPE_STATS: &str = "stats";
pub const EXPLICIT_TYPE_IMPORT: &str = "import";
pub const EXPLICIT_TYPE_LIST: [&str; 8] = [
    EXPLICIT_TYPE_EMPTY,
    EXPLICIT_TYPE_LIGHTNING,
    EXPLICIT_TYPE_BR,
    EXPLICIT_TYPE_DUMPLING,
    EXPLICIT_TYPE_BACKGROUND,
    EXPLICIT_TYPE_DDL,
    EXPLICIT_TYPE_STATS,
    EXPLICIT_TYPE_IMPORT,
];

pub const INTERNAL_REQUEST: &str = "internal";
pub const INTERNAL_REQUEST_PREFIX: &str = "internal_";
pub const EXTERNAL_REQUEST: &str = "external";
pub const SOURCE_UNKNOWN: &str = "unknown";

struct RequestSourceKey;
struct ResourceGroupNameKey;

/// Source identity carried by TiKV requests and resource-control admission.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestSource {
    pub internal: bool,
    pub source_type: String,
    pub explicit_source_type: String,
}

impl RequestSource {
    pub fn set_internal(&mut self, internal: bool) {
        self.internal = internal;
    }

    pub fn set_source_type(&mut self, source_type: impl Into<String>) {
        self.source_type = source_type.into();
    }

    pub fn set_explicit_source_type(&mut self, source_type: impl Into<String>) {
        self.explicit_source_type = source_type.into();
    }

    pub fn get_request_source(&self) -> String {
        if self.source_type.is_empty() && self.explicit_source_type.is_empty() {
            return SOURCE_UNKNOWN.to_owned();
        }
        let origin = if self.internal {
            INTERNAL_REQUEST
        } else {
            EXTERNAL_REQUEST
        };
        let source = if self.source_type.is_empty() {
            SOURCE_UNKNOWN
        } else {
            self.source_type.as_str()
        };
        let mut value = format!("{origin}_{source}");
        if !self.explicit_source_type.is_empty() && self.explicit_source_type != self.source_type {
            value.push('_');
            value.push_str(&self.explicit_source_type);
        }
        value
    }

    pub fn context_value(&self) -> String {
        self.get_request_source()
    }

    pub fn is_internal(&self) -> bool {
        is_internal_request(&self.get_request_source())
    }
}

pub fn build_request_source(internal: bool, source: &str, explicit_source: &str) -> String {
    RequestSource {
        internal,
        source_type: source.to_owned(),
        explicit_source_type: explicit_source.to_owned(),
    }
    .get_request_source()
}

pub fn is_request_source_internal(source: Option<&RequestSource>) -> bool {
    source.is_some_and(RequestSource::is_internal)
}

pub fn is_internal_request(source: &str) -> bool {
    source.starts_with(INTERNAL_REQUEST)
}

pub fn with_internal_source_type(
    context: &TraceContext,
    source: impl Into<String>,
) -> TraceContext {
    context.with_value::<RequestSourceKey, _>(RequestSource {
        internal: true,
        source_type: source.into(),
        explicit_source_type: String::new(),
    })
}

pub fn with_internal_source_and_task_type(
    context: &TraceContext,
    source: impl Into<String>,
    task_name: impl Into<String>,
) -> TraceContext {
    context.with_value::<RequestSourceKey, _>(RequestSource {
        internal: true,
        source_type: source.into(),
        explicit_source_type: task_name.into(),
    })
}

pub fn with_request_source(context: &TraceContext, source: RequestSource) -> TraceContext {
    context.with_value::<RequestSourceKey, _>(source)
}

pub fn request_source_from_context(context: &TraceContext) -> String {
    context
        .value::<RequestSourceKey, RequestSource>()
        .map(RequestSource::get_request_source)
        .unwrap_or_else(|| SOURCE_UNKNOWN.to_owned())
}

pub fn with_resource_group_name(
    context: &TraceContext,
    group_name: impl Into<String>,
) -> TraceContext {
    context.with_value::<ResourceGroupNameKey, _>(group_name.into())
}

pub fn resource_group_name_from_context(context: &TraceContext) -> &str {
    context
        .value::<ResourceGroupNameKey, String>()
        .map(String::as_str)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_get_and_build_matrix_matches_go() {
        let mut source = RequestSource {
            internal: true,
            source_type: "test".to_owned(),
            explicit_source_type: "lightning".to_owned(),
        };
        assert_eq!(source.get_request_source(), "internal_test_lightning");
        source.internal = false;
        assert_eq!(source.get_request_source(), "external_test_lightning");
        assert_eq!(RequestSource::default().get_request_source(), "unknown");
        source.explicit_source_type.clear();
        assert_eq!(source.get_request_source(), "external_test");
        source.source_type.clear();
        source.explicit_source_type = "lightning".to_owned();
        assert_eq!(source.get_request_source(), "external_unknown_lightning");
        source.source_type = "lightning".to_owned();
        assert_eq!(source.get_request_source(), "external_lightning");

        assert_eq!(
            build_request_source(true, "test", "lightning"),
            "internal_test_lightning"
        );
        assert_eq!(
            build_request_source(false, "test", "lightning"),
            "external_test_lightning"
        );
        assert_eq!(build_request_source(false, "test", ""), "external_test");
        assert_eq!(
            build_request_source(false, "", "lightning"),
            "external_unknown_lightning"
        );
        assert_eq!(build_request_source(true, "", ""), "unknown");
    }

    #[test]
    fn typed_context_values_preserve_source_and_resource_group() {
        let base = TraceContext::new();
        assert_eq!(request_source_from_context(&base), SOURCE_UNKNOWN);
        assert_eq!(resource_group_name_from_context(&base), "");
        let context = with_internal_source_and_task_type(&base, "gc", "background");
        let context = with_resource_group_name(&context, "rg1");
        assert_eq!(
            request_source_from_context(&context),
            "internal_gc_background"
        );
        assert_eq!(resource_group_name_from_context(&context), "rg1");
        assert_eq!(request_source_from_context(&base), SOURCE_UNKNOWN);
    }
}
