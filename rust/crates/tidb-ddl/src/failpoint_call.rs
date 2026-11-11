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

//! Go `failpoint.EnableCall` / `failpoint.InjectCall` for this package's
//! `github.com/pingcap/tidb/pkg/ddl/<name>` failpoints: a callback enabled
//! under a name runs with the injection site's arguments. As Go's marker, the
//! injection site is a no-op unless the build enables failpoints.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, PoisonError, RwLock};

/// A callback `enable_call` registers.
pub type Callback = Arc<dyn Fn(&mut dyn Any) + Send + Sync>;

static CALLS: LazyLock<RwLock<HashMap<String, Callback>>> = LazyLock::new(RwLock::default);

/// Go `failpoint.EnableCall(failpath, fn)`.
pub fn enable_call(name: &str, callback: Callback) {
    CALLS.write().unwrap_or_else(PoisonError::into_inner).insert(name.to_owned(), callback);
}

/// Go `failpoint.Disable(failpath)` of a callback.
pub fn disable(name: &str) {
    CALLS.write().unwrap_or_else(PoisonError::into_inner).remove(name);
}

/// Whether a callback is enabled under `name`.
pub fn is_enabled(name: &str) -> bool {
    CALLS.read().unwrap_or_else(PoisonError::into_inner).contains_key(name)
}

/// Go `failpoint.InjectCall(name, args)`: `args` is Go's pointer argument,
/// which a callback may write through.
pub fn inject_call(name: &str, args: &mut dyn Any) {
    if cfg!(feature = "failpoints") {
        let callback = CALLS.read().unwrap_or_else(PoisonError::into_inner).get(name).cloned();
        if let Some(callback) = callback {
            callback(args);
        }
    }
}
