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
//! Go's `writeSettingItemToBuilder` takes variadic separator closures; in
//! practice a caller either passes none (default single-space separator) or a
//! single `", "` writer, so here the separator is an `Option<&str>` (`None`
//! meaning the default `" "`).

use crate::go_duration::format_go_duration_ms;

/// Go `writeSettingItemToBuilder`: append `item`, preceded by the separator
/// when the builder is non-empty (`sep` defaults to a single space).
pub(crate) fn write_setting_item(sb: &mut String, item: &str, sep: Option<&str>) {
    if !sb.is_empty() {
        sb.push_str(sep.unwrap_or(" "));
    }
    sb.push_str(item);
}

/// Go `writeSettingStringToBuilder`: `ITEM="value"` with `"` escaped to `\"`.
pub(crate) fn write_setting_string(sb: &mut String, item: &str, value: &str, sep: Option<&str>) {
    write_setting_item(
        sb,
        &format!("{item}=\"{}\"", value.replace('"', "\\\"")),
        sep,
    );
}

/// Go `writeSettingIntegerToBuilder`: `ITEM=value`.
pub(crate) fn write_setting_integer(sb: &mut String, item: &str, value: u64, sep: Option<&str>) {
    write_setting_item(sb, &format!("{item}={value}"), sep);
}

/// Go `writeSettingDurationToBuilder`: `ITEM="<go-duration>"` for a duration
/// given in milliseconds (Go: `writeSettingStringToBuilder(item, dur.String())`).
pub(crate) fn write_setting_duration_ms(sb: &mut String, item: &str, ms: i64, sep: Option<&str>) {
    write_setting_string(sb, item, &format_go_duration_ms(ms), sep);
}
