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

//! Port of Go `pkg/executor/analyze_utils_test.go::TestGetAnalyzePanicErr`
//! for the panic-to-error recovery boundary owned by this crate. Broadcast
//! compatibility-error classification is owned and tested by `tidb-exec`.
//!
//! The helper under test is `getAnalyzePanicErr`
//! (`pkg/executor/analyze_utils.go:94`), ported as
//! [`crate::analyze::panic_recovery::get_analyze_panic_error`]: a panic value
//! that is (or renders as) the global analyze-memory sentinel becomes the
//! OOM error, any other error value propagates unchanged, and everything
//! else becomes the worker-panic sentinel.

use crate::analyze::panic_recovery::{
    get_analyze_panic_error, AnalyzePanicError, AnalyzePanicValue,
};

/// Go `pkg/executor/analyze_utils_test.go:41::TestGetAnalyzePanicErr`
/// (regression for pingcap/tidb#45690): recovering a panic that carries
/// `exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(123)` must render a
/// well-formed message. Go formats the returned error with `%s` and requires
/// the text NOT to contain `%!(EXTRA` -- the malformed-Sprintf artifact the
/// old implementation produced when the error's argument count outran its
/// format verbs.
///
/// Error-code context: `ErrMemoryExceedForQuery` is error 8175 registered on
/// the executor class (`pkg/util/dbterror/exeerrors/errors.go:64`); its
/// message template ends in the `[conn=%d]` verb
/// (`pkg/errno/errname.go`, ErrMemoryExceedForQuery), so `GenWithStackByArgs`
/// with one argument renders `...[conn=123]`. This tier's recovery boundary
/// receives the already-rendered message
/// ([`AnalyzePanicValue::Error`]) and must propagate it VERBATIM -- the same
/// rendering contract, with the format-string hazard living in whatever
/// produced the rendered text rather than in the recovery helper.
#[test]
fn get_analyze_panic_err_renders_a_propagated_error_without_sprintf_extras() {
    // The exact rendering of `ErrMemoryExceedForQuery.GenWithStackByArgs(123)`.
    let rendered = "Your query has been cancelled due to exceeding the allowed \
memory limit for a single SQL query. Please try narrowing your query scope or \
increase the tidb_mem_quota_query limit and try again.[conn=123]";
    let err = get_analyze_panic_error(AnalyzePanicValue::Error(rendered.to_owned()));

    // A non-sentinel error value propagates unchanged
    // (`pkg/executor/analyze_utils.go:103-105`: `return err`).
    assert_eq!(err, AnalyzePanicError::Propagated(rendered.to_owned()));
    // The `%s`-formatted message is well-formed: the malformed-Sprintf
    // artifact `%!(EXTRA ...)` must never appear.
    assert!(
        !err.rendered_message().contains("%!(EXTRA"),
        "malformed Sprintf artifact in {err:?}"
    );
    assert_eq!(err.rendered_message(), rendered);
}
