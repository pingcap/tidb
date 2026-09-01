# `pkg/expression/exprctx` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains six tracked artifacts and 740 lines. Every production
source, test source, and Bazel target was read in full before editing. There is
no `doc.go`, fixture directory, generated output, platform-specific variant,
or additional generator input in this package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 40 | `29691d44bfd7c8aa8fcf46c47f9146b8ddf6b52d` | `65cbef4078775034f2f21fe96212e46f07f4f1108d5ebd665bb45a0ece2c08c8` | public library and four-shard flaky test target |
| `context.go` | 266 | `f97349caec41d0964f6dbd38cc783407675ec4f4` | `727a51352d79318685f7fc479037c6a9855217cc5bde4293d211a8075922066e` | allocator, Eval/Build/Expr context contracts, wrappers, truncate override, and static-conversion contracts |
| `context_override_test.go` | 85 | `ab50918f203220ee2bbba347af7609fdc684a91b` | `e4cbf790bd3f0e436a9f51d39a378298c100903d85fc9557bafb6a6a38a7b79f` | `TestCtxWithHandleTruncateErrLevel` |
| `optional.go` | 203 | `5b7526b468e899abadc850202deeec30a10776e4` | `bda032aa07a9e1962a4653c0288dde429491a7887b233753491fe080000d6487` | ten-key optional-property descriptors and bitmap operations |
| `optional_test.go` | 107 | `e7290325e654ad0d6a442a957da5d0d6543fc4c1` | `468796120f5ebba2a9452c4c291c6f2da2f3bbf997b90ce7f7ce2720449cec3f` | three optional-key/set tests |
| `param.go` | 39 | `24e119e71e88532b9731a5c2b1b7d7db731d4f51` | `f36be6e5329a73a687d98f8fb17ac28bdda192c58aacae1660a65d7ecb4c811f` | `ParamValues`, sentinel error, and empty implementation |

The production sources declare the allocator constructor and methods, all
Eval/Build/Expr and static-conversion interface contracts, two check wrappers,
the truncate-level override, the location assertion, ten optional-key methods,
and the empty-parameter lookup. The two test files define four top-level tests
in total. The master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is an added
`NewCollationEnabled` BuildContext method, plus the corresponding static
option/field and test coverage in its owning `exprstatic` package; no other
exprctx artifact changed.

## Rust ownership and parity fixes

The Rust owner is `rust/crates/tidb-expr/src/exprctx.rs`; static and live
implementations are in `src/exprstatic/{exprctx,evalctx}.rs` and
`src/sessionexpr.rs`.

The package previously exposed only the sentinel string for `param.go`. It now
has a typed `ParamValues` contract, a zero-sized
`ParamIndexExceedParamCounts` error preserving the exact Go message, and the
reusable `EMPTY_PARAM_VALUES` implementation. Both static and live
`EvalContext` types implement the contract. A focused regression failed before
the implementation because `EmptyParamValues` was absent, then passed after it.

The master-added per-context collation mode is also implemented: static
`ExprContext` captures the process setting by default, supports
`with_new_collation_enabled`, preserves it across `apply` and
`make_expr_context_static`, and live `sessionexpr::ExprContext` exposes the
same `NewCollationEnabled` read. The previous part9 receipt's divergence is
closed by this batch.

`EvalContext`/`BuildContext` umbrella traits and
`CtxWithHandleTruncateErrLevel` remain explicit boundaries: they require the
cross-package `types.Context`, `errctx.Context`, and session variable object
graph, while this crate's static/live contexts intentionally expose narrower,
dependency-closed traits. No Rust-only substitute or guessed wrapper was added;
the existing ignored source test remains the evidence for that boundary.

## Validation and risk

Profile: **Ready** for this batch. Only Rust sources/tests and receipts changed;
Go sources, imports, Bazel targets, and module files were not changed, so
`make bazel_prepare` was not required.

```text
# Focused regressions before implementation (expected failures):
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  exprctx::tests::empty_param_values_report_the_source_error --offline --locked
# failed to compile: cannot find value `EmptyParamValues`
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  exprstatic::exprctx::tests::new_collation_mode_is_captured_per_context \
  --offline --locked
# failed to compile: missing option/accessor

# Rust focused tests after implementation:
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib exprctx::tests --offline --locked
# 11 passed (including exprstatic exprctx carriers)
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib \
  exprstatic::exprctx::tests::new_collation_mode_is_captured_per_context \
  --offline --locked
# 1 passed
PATH=... OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test \
  --manifest-path rust/Cargo.toml -p tidb-expr --lib sessionexpr::tests::session_build_context \
  --offline --locked
# passed

# Exact Go-master package (detached worktree at 5e8a1a229a):
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/expression/exprctx -count=1                         # passed, 0.495s

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
```

Not verified here: Bazel execution, Windows behavior, full workspace test
suites, and the intentionally deferred umbrella-interface wrapper. The static
collation field snapshots the process setting at construction like Go; live
contexts read the shared runtime setting, so callers must preserve Go's
bootstrap ordering.

This receipt certifies the bounded `pkg/expression/exprctx` inventory and the
implemented parameter/collation behavior; it is not a repository-wide parity
claim.
