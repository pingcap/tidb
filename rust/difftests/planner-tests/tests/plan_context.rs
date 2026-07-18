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

//! Dependency-closed vectors for `pkg/planner/planctx/context.go`.
//!
//! The direct Go anchor is `TestContextDetach` at
//! `pkg/planner/planctx/context_test.go:26`.

use tidb_planner::plan_context::{BuildPbContext, OpaqueContextHandle};

#[test]
fn detach_replaces_only_expression_context_and_preserves_handles() {
    let expr_ctx = OpaqueContextHandle::new(1);
    let client = OpaqueContextHandle::new(2);
    let warn_handler = OpaqueContextHandle::new(3);
    let extra_warn_handler = OpaqueContextHandle::new(4);
    let context = BuildPbContext {
        expr_ctx,
        client,
        tiflash_fast_scan: true,
        tiflash_fine_grained_shuffle_batch_size: 64,
        group_concat_max_len: 1024,
        in_explain_stmt: true,
        warn_handler,
        extra_warn_handler,
    };

    let detached_expr_ctx = OpaqueContextHandle::new(9);
    let detached = context.detach(detached_expr_ctx);
    assert_eq!(detached.get_expr_ctx(), detached_expr_ctx);
    assert_eq!(detached.get_client(), client);
    assert_eq!(detached.tiflash_fast_scan, context.tiflash_fast_scan);
    assert_eq!(
        detached.tiflash_fine_grained_shuffle_batch_size,
        context.tiflash_fine_grained_shuffle_batch_size
    );
    assert_eq!(detached.group_concat_max_len, context.group_concat_max_len);
    assert_eq!(detached.in_explain_stmt, context.in_explain_stmt);
    assert_eq!(detached.warn_handler, warn_handler);
    assert_eq!(detached.extra_warn_handler, extra_warn_handler);
}

#[test]
fn accessors_return_original_handles() {
    let context = BuildPbContext {
        expr_ctx: OpaqueContextHandle::new(7),
        client: OpaqueContextHandle::new(8),
        ..BuildPbContext::default()
    };
    assert_eq!(context.get_expr_ctx().raw(), 7);
    assert_eq!(context.get_client().raw(), 8);
}
