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

//! Go `pkg/expression/aggregation/explain.go`: how an aggregate renders in
//! `EXPLAIN` output.
//!
//! Only the `normalized == true` half is ported; the `ExplainInfo(ctx)` half
//! needs `Expression.StringWithCtx`, the boundary [`tidb_expr::expr_util`]
//! already names. See [`super`].

use super::names;
use super::AggFuncDesc;
use tidb_expr::expr_util::explain_normalized_info;
use std::fmt::Write as _;

/// What Go's `show-agg-mode` failpoint replaces.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use std::sync::atomic::{AtomicBool, Ordering};

    static SHOW_AGG_MODE: AtomicBool = AtomicBool::new(false);

    /// Go `failpoint.Enable("show-agg-mode", "return(true)")`: an aggregate
    /// explains as `name(mode,args...)`.
    pub fn set_show_agg_mode(show_mode: bool) {
        SHOW_AGG_MODE.store(show_mode, Ordering::Relaxed);
    }

    pub(super) fn show_agg_mode() -> bool {
        SHOW_AGG_MODE.load(Ordering::Relaxed)
    }
}

/// Go `ExplainAggFunc`'s `showMode`: whether the `show-agg-mode` failpoint is
/// enabled.
#[must_use]
pub fn show_agg_mode() -> bool {
    #[cfg(any(test, feature = "test-util"))]
    {
        mock::show_agg_mode()
    }
    #[cfg(not(any(test, feature = "test-util")))]
    {
        false
    }
}

/// Go `ExplainAggFunc(ctx, agg, normalized = true)` (`explain.go:26`).
#[must_use]
pub fn explain_agg_func_normalized(agg: &AggFuncDesc) -> String {
    let mut buffer = String::new();
    if show_agg_mode() {
        let _ = write!(buffer, "{}({},", agg.name(), agg.mode.as_str());
    } else {
        let _ = write!(buffer, "{}(", agg.name());
    }

    if agg.has_distinct {
        buffer.push_str("distinct ");
    }
    let arg_count = agg.args().len();
    for (i, arg) in agg.args().iter().enumerate() {
        if agg.name() == names::GROUP_CONCAT && i == arg_count - 1 {
            // The last GROUP_CONCAT argument is the separator, and the
            // aggregate's own ORDER BY prints just before it.
            if !agg.order_by_items.is_empty() {
                buffer.push_str(" order by ");
                let last = agg.order_by_items.len() - 1;
                for (j, item) in agg.order_by_items.iter().enumerate() {
                    buffer.push_str(&explain_normalized_info(&item.expr));
                    if item.desc {
                        buffer.push_str(" desc");
                    }
                    if j < last {
                        buffer.push_str(", ");
                    }
                }
            }
            buffer.push_str(" separator ");
        } else if i != 0 {
            buffer.push_str(", ");
        }
        buffer.push_str(&explain_normalized_info(arg));
    }
    buffer.push(')');
    buffer
}
