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

//! Configured SQL-error suffixes from `pkg/util/errmsg`.

use std::sync::{Arc, OnceLock, RwLock};

use regex::Regex;

use super::helpers::ErrorMessageExtension;

struct PreparedExtension {
    source: ErrorMessageExtension,
    matcher: Regex,
}

fn prepared_extensions() -> &'static RwLock<Arc<[PreparedExtension]>> {
    static PREPARED: OnceLock<RwLock<Arc<[PreparedExtension]>>> = OnceLock::new();
    PREPARED.get_or_init(|| RwLock::new(Arc::from([])))
}

pub(crate) fn replace_prepared_extensions(extensions: &[ErrorMessageExtension]) {
    let prepared: Arc<[PreparedExtension]> = extensions
        .iter()
        .map(|source| PreparedExtension {
            matcher: Regex::new(&source.pattern)
                .expect("validated error-message extension must compile"),
            source: source.clone(),
        })
        .collect::<Vec<_>>()
        .into();
    *prepared_extensions()
        .write()
        .expect("prepared error message extensions lock poisoned") = prepared;
}

pub(crate) fn configured_extensions() -> Vec<ErrorMessageExtension> {
    prepared_extensions()
        .read()
        .expect("prepared error message extensions lock poisoned")
        .iter()
        .map(|extension| extension.source.clone())
        .collect()
}

/// Returns the first configured extension of `message`, if any.
///
/// The returned string has the source `", suffix."` form and trailing periods
/// are normalized only when a matcher applies.
#[must_use]
pub fn extended_error_message(message: &str) -> Option<String> {
    let extensions = Arc::clone(
        &prepared_extensions()
            .read()
            .expect("prepared error message extensions lock poisoned"),
    );
    for extension in extensions.iter() {
        if extension.source.suffix.is_empty() || !extension.matcher.is_match(message) {
            continue;
        }
        return Some(format!(
            "{}, {}.",
            message.trim_end_matches('.'),
            extension.source.suffix.trim_end_matches('.')
        ));
    }
    None
}

/// Mutates one SQL-error message with its first configured suffix.
pub fn extend_error_message(message: &mut String) {
    if let Some(extended) = extended_error_message(message) {
        *message = extended;
    }
}
