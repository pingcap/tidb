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

//! Go `pkg/statistics/handle/util/test`.

use std::any::Any;
use std::fmt;

use tidb_txnkv::INTERNAL_TXN_STATS_FOREGROUND_PRIORITY;
use tikv_client::trace::TraceContext;
use tikv_client::util::{request_source_from_context, INTERNAL_REQUEST};

const DESCRIPTION: &str = "all txns should be internal stats foreground priority source";

/// Go `CtxMatcher` for an actual request context.
pub struct CtxMatcher;

impl CtxMatcher {
    /// Go `(*CtxMatcher).Matches`.
    ///
    /// A non-context value panics, matching Go's direct type assertion.
    pub fn matches(&self, value: &dyn Any) -> bool {
        let context = value
            .downcast_ref::<TraceContext>()
            .expect("CtxMatcher value is not a request context");
        request_source_from_context(context)
            == format!("{INTERNAL_REQUEST}_{INTERNAL_TXN_STATS_FOREGROUND_PRIORITY}")
    }
}

impl fmt::Display for CtxMatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(DESCRIPTION)
    }
}
