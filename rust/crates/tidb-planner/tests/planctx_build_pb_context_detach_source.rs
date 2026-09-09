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

//! Port of `pkg/planner/planctx/context_test.go::TestContextDetach`
//! (`pkg/planner.part22` item 1266 on `origin/master`).
//!
//! Go's `BuildPBContext.Detach` is a value copy of the whole build state that
//! swaps ONLY the expression context (pkg/planner/planctx/context.go:146-150:
//! `newCtx := *b; newCtx.ExprCtx = staticExprCtx`). The test builds one with
//! non-zero switches, detaches it onto its own expression context, and pins:
//! 1. every switch field differs from the zero context (Go via
//!    `deeptest.AssertRecursivelyNotEqual` with `$.ExprCtx`, `$.Client`,
//!    `$.WarnHandler`, `$.ExtraWarnghandler` ignored — the interface handles
//!    are exactly what cannot be deep-compared there);
//! 2. the detached copy equals the original across everything else
//!    (`deeptest.AssertDeepClonedEqual` with the same ignore list);
//! 3. all four handle-typed fields carry over IDENTICALLY: ExprCtx (the
//!    detached-to handle), Client, WarnHandler, ExtraWarnghandler
//!    (context_test.go:82-86).
//!
//! The crate represents those four handles as opaque copyable ids
//! ([`tidb_planner::plan_context::OpaqueContextHandle`]), compared by raw id —
//! the same identity comparison Go's `require.Equal` performs on interfaces.

use tidb_planner::plan_context::{BuildPbContext, OpaqueContextHandle};

/// GO PORT of `pkg/planner/planctx/context_test.go:26 TestContextDetach`.
#[test]
fn detach_copies_every_field_but_swaps_only_the_expr_ctx() {
    // static expr context the build state starts from; Go constructs it as
    // exprstatic.NewExprContext() (context_test.go:31).
    let expr_ctx = OpaqueContextHandle::new(7);
    let warn_handler = OpaqueContextHandle::new(42);
    let client = OpaqueContextHandle::new(99);

    let obj = BuildPbContext {
        expr_ctx,
        client,
        tiflash_fast_scan: true,
        tiflash_fine_grained_shuffle_batch_size: 1,
        group_concat_max_len: 1,
        in_explain_stmt: true,
        warn_handler,
        extra_warn_handler: warn_handler,
    };

    // deeptest.AssertRecursivelyNotEqual against the zero context, ignoring
    // the four handles: every carried value field must differ from default.
    let zero = BuildPbContext::default();
    assert_ne!(obj.tiflash_fast_scan, zero.tiflash_fast_scan);
    assert_ne!(
        obj.tiflash_fine_grained_shuffle_batch_size,
        zero.tiflash_fine_grained_shuffle_batch_size
    );
    assert_ne!(obj.group_concat_max_len, zero.group_concat_max_len);
    assert_ne!(obj.in_explain_stmt, zero.in_explain_stmt);

    let detached = obj.detach(expr_ctx);

    // deeptest.AssertDeepClonedEqual ignoring the same paths — every value
    // field survives the copy unchanged.
    assert_eq!(
        detached.tiflash_fast_scan,
        obj.tiflash_fast_scan,
        "TiFlashFastScan must survive Detach"
    );
    assert_eq!(
        detached.tiflash_fine_grained_shuffle_batch_size,
        obj.tiflash_fine_grained_shuffle_batch_size,
        "TiFlashFineGrainedShuffleBatchSize must survive Detach"
    );
    assert_eq!(
        detached.group_concat_max_len,
        obj.group_concat_max_len,
        "GroupConcatMaxLen must survive Detach"
    );
    assert_eq!(
        detached.in_explain_stmt,
        obj.in_explain_stmt,
        "InExplainStmt must survive Detach"
    );

    // The four identity comparisons at context_test.go:82-86.
    assert_eq!(detached.expr_ctx.raw(), obj.expr_ctx.raw(), "ExprCtx");
    assert_eq!(detached.client.raw(), obj.client.raw(), "Client");
    assert_eq!(
        detached.warn_handler.raw(),
        obj.warn_handler.raw(),
        "WarnHandler"
    );
    assert_eq!(
        detached.extra_warn_handler.raw(),
        obj.extra_warn_handler.raw(),
        "ExtraWarnghandler"
    );
}
