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

//! GO PORTS of the two part9 tests whose production symbols are deferred in
//! this crate: `TestCtxWithHandleTruncateErrLevel`
//! (`pkg/expression/exprctx/context_override_test.go:28`),
//! `TestNewValuesFunc` (`pkg/expression/expression_test.go:30`) and
//! `TestExpressionMemeoryUsage`
//! (`pkg/expression/expression_test.go:328`, also stubbed from
//! `vectorizable_and_chunk_eval_source.rs`). Each stays beside its family's
//! receipt row with a go-parity-gap reason and an anchor to the boundary note
//! that documents the deferral.

/// go-parity-gap: TestCtxWithHandleTruncateErrLevel
/// (`exprctx/context_override_test.go:28`) exercises
/// `exprctx.CtxWithHandleTruncateErrLevel` (`exprctx/context.go:203`), which
/// overrides ONLY the truncate error group of an eval context while aliasing
/// every other field, and returns the SAME context when no override is needed.
/// The helper is explicitly listed among the unported umbrella pieces of
/// `exprctx.rs`'s module header, and simple_expr.rs's virtual-column builder
/// documents its absence with a `boundary:` note (the Go call site wraps the
/// context so generated-expression truncation does not warn twice).
///
/// Once the helper lands, the port must assert, per LevelWarn/LevelIgnore/
/// LevelError: flags swap TruncateAsWarning/IgnoreTruncateErr exactly as
/// `errctx.LevelMap{ErrGroupTruncate: level}` says, DividedByZero keeps its own
/// error level, location/connection id pass through unchanged, the ORIGINAL
/// eval context is untouched, and re-wrapping with the same level returns the
/// same context object.
#[test]
#[ignore = "go-parity-gap: exprctx::CtxWithHandleTruncateErrLevel is not transcreated (deferred umbrella piece of exprctx.rs), so the truncate-level override contract has no carrier"]
fn test_ctx_with_handle_truncate_err_level() {}

/// go-parity-gap: TestNewValuesFunc (`expression_test.go:30`) builds
/// `NewValuesFunc(ctx, 0, TypeLonglong)` and asserts the "values" name, the
/// LongLong ret type and the `*builtinValuesIntSig` signature choice. No
/// `NewValuesFunc` symbol exists anywhere in the Rust workspace (the name only
/// appears inside unfoldable-function tables), so the constructor contract has
/// nothing to drive.
#[test]
#[ignore = "go-parity-gap: NewValuesFunc (expression.go:1168) is not transcreated; no values-signature construction exists in tidb-expr"]
fn test_new_values_func() {}
