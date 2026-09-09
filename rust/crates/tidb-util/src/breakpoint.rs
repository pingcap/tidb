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

//! Session breakpoints from Go `pkg/util/breakpoint`.

use std::any::Any;
#[cfg(feature = "failpoints")]
use std::sync::Arc;

use crate::context::ValueStoreContext;

/// Go `NotifyBreakPointFuncKey`.
pub const NOTIFY_BREAK_POINT_FUNC_KEY: &str = "breakPointNotifyFunc";

#[cfg(feature = "failpoints")]
type NotifyBreakPointFunc = Arc<dyn Fn(String) + Send + Sync + 'static>;

fn inject_value(value: Option<&(dyn Any + Send + Sync)>, name: &str) {
    #[cfg(feature = "failpoints")]
    {
        let _ = fail::eval(name, |_| {
            if let Some(callback) =
                value.and_then(|value| value.downcast_ref::<NotifyBreakPointFunc>())
            {
                callback(name.to_owned());
            }
        });
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = (value, name);
}

/// Executor-side half of [`inject`] for Rust's fused statement/executor
/// context. It is public only because that context lives in another crate.
#[doc(hidden)]
pub fn inject_stored_value(value: Option<&(dyn Any + Send + Sync)>, name: &str) {
    inject_value(value, name);
}

/// Go `Inject`: synchronously invokes the session callback only when the
/// named process failpoint is enabled and the stored value has the exact
/// callback type.
pub fn inject<C>(session: &C, name: &str)
where
    C: ValueStoreContext<Key = str> + ?Sized,
{
    inject_value(session.value(NOTIFY_BREAK_POINT_FUNC_KEY), name);
}
