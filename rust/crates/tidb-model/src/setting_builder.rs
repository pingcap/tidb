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

//! The `writeSetting*ToBuilder` helpers from `pkg/meta/model/placement.go`,
//! shared by the placement- and resource-group-settings renderers.
//!
//! Go's `writeSettingItemToBuilder` takes variadic separator closures. The
//! native helper retains that complete surface as a mutable slice of closures
//! receiving the builder. Convenience wrappers keep the common fixed-string
//! separator used by the owning call sites.

/// A Go separator callback, expressed with the builder as an explicit
/// argument so Rust can safely represent closures that mutate it and arbitrary
/// captured state.
pub(crate) type SeparatorFn<'a> = &'a mut dyn FnMut(&mut String);

/// Exact Go `writeSettingItemToBuilder`, including zero, one, or several
/// side-effecting separator callbacks in their source order.
pub(crate) fn write_setting_item_with_separators(
    sb: &mut String,
    item: &str,
    separator_fns: &mut [SeparatorFn<'_>],
) {
    if !sb.is_empty() {
        for separator in separator_fns.iter_mut() {
            separator(sb);
        }
        if separator_fns.is_empty() {
            sb.push(' ');
        }
    }
    sb.push_str(item);
}

fn with_fixed_separator(sb: &mut String, item: &str, separator: Option<&str>) {
    match separator {
        None => write_setting_item_with_separators(sb, item, &mut []),
        Some(separator) => {
            let mut write_separator = |builder: &mut String| builder.push_str(separator);
            write_setting_item_with_separators(sb, item, &mut [&mut write_separator]);
        }
    }
}

/// Go `writeSettingItemToBuilder`: append `item`, preceded by the separator
/// when the builder is non-empty (`sep` defaults to a single space).
pub(crate) fn write_setting_item(sb: &mut String, item: &str, sep: Option<&str>) {
    with_fixed_separator(sb, item, sep);
}

/// Go `writeSettingStringToBuilder`: `ITEM="value"` with `"` escaped to `\"`.
pub(crate) fn write_setting_string(sb: &mut String, item: &str, value: &str, sep: Option<&str>) {
    let rendered = format!("{item}=\"{}\"", value.replace('"', "\\\""));
    with_fixed_separator(sb, &rendered, sep);
}

/// Exact variadic-separator form of Go `writeSettingStringToBuilder`.
pub(crate) fn write_setting_string_with_separators(
    sb: &mut String,
    item: &str,
    value: &str,
    separator_fns: &mut [SeparatorFn<'_>],
) {
    let rendered = format!("{item}=\"{}\"", value.replace('"', "\\\""));
    write_setting_item_with_separators(sb, &rendered, separator_fns);
}

/// Go `writeSettingIntegerToBuilder`: `ITEM=value`.
pub(crate) fn write_setting_integer(sb: &mut String, item: &str, value: u64, sep: Option<&str>) {
    with_fixed_separator(sb, &format!("{item}={value}"), sep);
}

/// Exact variadic-separator form of Go `writeSettingIntegerToBuilder`.
pub(crate) fn write_setting_integer_with_separators(
    sb: &mut String,
    item: &str,
    value: u64,
    separator_fns: &mut [SeparatorFn<'_>],
) {
    write_setting_item_with_separators(sb, &format!("{item}={value}"), separator_fns);
}

/// Go `writeSettingDurationToBuilder`: render an `i64` nanosecond duration
/// through `time.Duration.String` and retain arbitrary separator callbacks.
pub(crate) fn write_setting_duration_with_separators(
    sb: &mut String,
    item: &str,
    duration_ns: i64,
    separator_fns: &mut [SeparatorFn<'_>],
) {
    let value = crate::go_duration::format_go_duration(duration_ns);
    let rendered = format!("{item}=\"{}\"", value.replace('"', "\\\""));
    write_setting_item_with_separators(sb, &rendered, separator_fns);
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;

    #[test]
    fn variadic_separator_callbacks_preserve_order_and_side_effects() {
        let mut builder = "FIRST=1".to_owned();
        let calls = RefCell::new(Vec::new());
        let mut first = |output: &mut String| {
            calls.borrow_mut().push(1);
            output.push('|');
        };
        let mut second = |output: &mut String| {
            calls.borrow_mut().push(2);
            output.push_str("->");
        };
        write_setting_item_with_separators(
            &mut builder,
            "SECOND=2",
            &mut [&mut first, &mut second],
        );
        assert_eq!(builder, "FIRST=1|->SECOND=2");
        assert_eq!(*calls.borrow(), vec![1, 2]);

        let mut empty = String::new();
        let called = Cell::new(false);
        let mut must_not_run = |_: &mut String| called.set(true);
        write_setting_item_with_separators(&mut empty, "ONLY=1", &mut [&mut must_not_run]);
        assert_eq!(empty, "ONLY=1");
        assert!(!called.get());
    }

    #[test]
    fn default_fixed_and_duration_separators_match_go() {
        let mut builder = String::new();
        write_setting_item(&mut builder, "A=1", None);
        write_setting_item(&mut builder, "B=2", None);
        write_setting_item(&mut builder, "C=3", Some(", "));
        assert_eq!(builder, "A=1 B=2, C=3");

        let mut duration = "A=1".to_owned();
        let mut separator = |output: &mut String| output.push_str(" / ");
        write_setting_duration_with_separators(
            &mut duration,
            "TIMEOUT",
            90_000_000_000,
            &mut [&mut separator],
        );
        assert_eq!(duration, "A=1 / TIMEOUT=\"1m30s\"");

        let mut delegated = "A=1".to_owned();
        let mut separator = |output: &mut String| output.push('|');
        write_setting_string_with_separators(&mut delegated, "S", "a\"b", &mut [&mut separator]);
        write_setting_integer_with_separators(&mut delegated, "N", 7, &mut []);
        assert_eq!(delegated, "A=1|S=\"a\\\"b\" N=7");
    }
}
