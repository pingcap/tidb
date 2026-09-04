# UNION `inUnion` cast parity receipt

Status: bounded Rust parity fix implemented and validated on an isolated
worktree. This receipt does not claim complete transcreation of the Go
`pkg/expression` or `pkg/planner` packages; it closes one dependency-ordered
behavior cluster across their AST, expression, and planner consumers.

Comparison source: Go `origin/master` at commit
`6331b8787b4203a91aafe49ee1dc801ee497bf98` (the user's `master` request).
Rust base: `origin/hparser-integration` at `0c5ef089dc23c38f226938dc7cd65519fea336e5`
when this worktree was created.

## Inventory completed before editing

The requested Go-side inventory was performed recursively before the Rust
changes. It covers all production, unit/benchmark/generated/platform files,
fixtures and Bazel build artifacts under the two owning trees:

| Tree | Package directories | Files (`*.go`, `BUILD*`, `*.bzl`) | Go lines |
| --- | ---: | ---: | ---: |
| `pkg/expression` (including nested packages) | 11 | 200 | 143,862 |
| `pkg/planner` (including nested packages) | 91 | 586 | 187,939 |

The Rust inventory covers 97 files in `tidb-ast`, 176 in `tidb-expr`, and 344
in `tidb-planner`, including Cargo manifests, aggregate-test build scripts,
unit/integration tests, fixtures and generated test inputs. Relevant Go
production/test/build artifacts read function by function were:

- `pkg/expression/builtin_cast.go`, `builtin_cast_vec.go`,
  `builtin_cast_test.go`, `builtin_cast_vec_test.go`,
  `builtin_cast_bench_test.go`;
- `pkg/expression/expr_to_pb.go`, `expr_to_pb_test.go`,
  `scalar_function.go`, `scalar_function_test.go`,
  `simple_rewriter.go`, `simple_rewriter_test.go`;
- `pkg/planner/core/logical_plan_builder.go`,
  `core/casetest/logicalplan/logical_plan_builder_test.go`,
  `core/rule_inject_extra_projection.go`, and
  `core/casetest/rule/rule_inject_extra_projection_test.go`.

The exact inventory commands and all paths remain reproducible with:

```text
find pkg/expression -type f \( -name '*.go' -o -name 'BUILD*' -o -name '*.bzl' \) | sort
find pkg/planner -type f \( -name '*.go' -o -name 'BUILD*' -o -name '*.bzl' \) | sort
find rust/crates/tidb-ast rust/crates/tidb-expr rust/crates/tidb-planner -type f | sort
```

## Go behavior restored

Go's `BuildCastFunction4Union` passes `inUnion=true` into every cast signature.
For unsigned integer targets, signed integer, string-as-int, real-as-int and
decimal-as-int signatures clamp a negative value to zero; ordinary
`BuildCastFunction` keeps the normal unsigned low-bit conversion and warning
behavior. Go recursive-CTE projection uses the same helper.

Rust now carries that build-time bit as the internal-only
`tidb_ast::CastType::UnsignedInUnion` variant and the
`cast_unsigned_in_union` scalar-function name. `build_cast_to` remains the
ordinary (`false`) wrapper; `build_cast_to_in_union` is used by UNION and
recursive-CTE projections. The evaluator applies the negative clamp by source
eval family and leaves temporal sources on their Go-specific ordinary path.
No SQL parser spelling or user-visible restore text changed.

## Focused regressions

- `tidb-expr::builtin_cast_semantics::tests::union_unsigned_integer_cast_clamps_negative_values`
  asserts AST result metadata, ordinary-vs-UNION internal names, the
  fail-before ordinary result `u64::MAX`, the post-fix UNION result `0`, and
  Go's negative string-as-int zero path without a spurious 8031 warning.
- `tidb-planner::plan_builder::set_opr_tests::union_unsigned_widening_uses_the_in_union_cast_signature`
  builds two unsigned UNION branches with different widths and asserts that
  the widened branch projection carries `cast_unsigned_in_union`.

The previously ignored Go-derived string-to-DECIMAL `inUnion` rows remain
ignored with a narrower reason: this batch implements the unsigned-integer
target carrier, while that separate decimal signature still needs its own
carrier. The vectorized differential harness remains unported because Rust
has one row-based evaluator tier.

## Validation

Focused tests:

```text
cargo test --locked -p tidb-expr --lib builtin_cast_semantics::tests::union_unsigned_integer_cast_clamps_negative_values
# 1 passed
cargo test --locked -p tidb-planner --lib plan_builder::set_opr_tests::union_unsigned_widening_uses_the_in_union_cast_signature
# 1 passed
```

Ready profile (run from `rust/`) is the package-owner check plus all-target
compile, formatting, lint and repository diff checks:

```text
cargo nextest run --locked -p tidb-expr -E 'not test(/bench/)' --no-fail-fast
cargo test --locked -p tidb-planner --lib plan_builder::set_opr_tests -- --test-threads=1
cargo check --locked -p tidb-ast -p tidb-expr -p tidb-planner -p tidb-exec --all-targets
cargo fmt --all -- --check
cargo clippy --locked -p tidb-ast -p tidb-expr -p tidb-planner -p tidb-exec --all-targets -- -D warnings
git diff --check
```

Observed results for this batch:

- Focused expression test: **passed** (1/1).
- Focused planner test: **passed** (1/1).
- `cargo test --offline --locked -j12 -p tidb-ast -p tidb-expr -p tidb-planner -p tidb-exec --all-targets`:
  all changed-crate tests compiled and ran; one unrelated
  `tidb-exec::label_delivery::tests::get_and_patch_use_pds_exact_region_label_api`
  failed because its local PD endpoint returned transport/502 errors.
- `cargo nextest run --offline --locked -p tidb-expr -E 'not test(/bench/)' --no-fail-fast`:
  1,128 passed, 1 unrelated HTTP fixture failure
  (`builtin_ext::json::tests::json_schema_valid_resolves_file_and_http_references`;
  its loopback HTTP fixture returned a response-decoding error), 134 skipped.
- Planner set-operation owner suite: **passed** (35/35).
- `cargo check --offline --locked -p tidb-ast -p tidb-expr -p tidb-planner -p tidb-exec --all-targets`:
  **passed**.
- `cargo fmt --all -- --check`: **passed**.
- `git diff --check`: **passed**.
- Strict clippy was attempted with `-D warnings` and is blocked by existing
  unrelated diagnostics in `tidb-mysql`, generated `tidb-proto`, and other
  workspace code; no changed-file diagnostic was reported.

The two network-backed failures are retained as environment-dependent baseline
failures rather than masked or changed by this Rust-only batch.

## Risks and remaining boundaries

- The joined UNION type is unsigned only when all integer branches are
  unsigned; the planner regression therefore uses two unsigned columns with a
  width mismatch to force the cast node.
- Go's decimal-target and vectorized `inUnion` signatures remain explicit
  follow-ups; no fake function name was added for them.
- The Go physical MPP extra-projection caller is not present in this Rust
  planner owner; when that owner is implemented it must use the same
  `build_cast_to_in_union` helper.
- The full Go package inventories remain larger than this bounded fix, so no
  package-complete parity claim is made.
