// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `util.EvalFailpoint` over the `fail` registry: the
//! `tikvclient/<name>` failpoints client-go's tests enable.

/// A failpoint's `return(...)` value.
pub(crate) struct FailpointValue(Option<String>);

impl FailpointValue {
    /// Go `val.(string)`: a quoted `return("...")` value, unquoted.
    pub(crate) fn as_string(&self) -> Option<&str> {
        self.0.as_deref()?.strip_prefix('"')?.strip_suffix('"')
    }
}

/// client-go `util.EvalFailpoint(name)`: `Some` when the failpoint is
/// enabled.
pub(crate) fn eval(name: &str) -> Option<FailpointValue> {
    fail::eval(name, FailpointValue)
}
