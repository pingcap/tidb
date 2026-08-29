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

//! Prepared error-message-extension snapshots owned by `pkg/config`.

use std::sync::{Arc, OnceLock, RwLock};

use super::helpers::ErrorMessageExtension;

fn prepared_extensions() -> &'static RwLock<Arc<[ErrorMessageExtension]>> {
    static PREPARED: OnceLock<RwLock<Arc<[ErrorMessageExtension]>>> = OnceLock::new();
    PREPARED.get_or_init(|| RwLock::new(Arc::from([])))
}

pub(crate) fn replace_prepared_extensions(extensions: &[ErrorMessageExtension]) {
    let prepared: Arc<[ErrorMessageExtension]> = extensions.to_vec().into();
    *prepared_extensions()
        .write()
        .expect("prepared error message extensions lock poisoned") = prepared;
}

pub(crate) fn configured_extensions() -> Vec<ErrorMessageExtension> {
    prepared_extensions()
        .read()
        .expect("prepared error message extensions lock poisoned")
        .iter()
        .cloned()
        .collect()
}
