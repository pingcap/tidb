# Rust `tidb-planner` child-accessor boundary receipt

Status: bounded Rust-only alignment batch. This receipt covers the shared
logical/physical child-accessor contract; it does not claim completion of the
entire planner transcreation.

Comparison source: Go `origin/master` at `a85e0fd5df` (2026-09-02), with the
unchanged accessor definitions in `pkg/planner/core/operator/logicalop/
base_logical_plan.go`, `pkg/planner/core/operator/logicalop/logical_join.go`,
and `pkg/planner/core/operator/physicalop/base_physical_plan.go`. The complete
Go package inventories remain package-atomic: the nested physical-operator
inventory is recorded in `planner_physicalop_engine_usage.md`, while the
logical-operator source is compared only for these shared base/override
contracts and is not claimed as a completed transcreation here.

## Rust owner inventory

The owning crate is `rust/crates/tidb-planner`. Its tracked inventory contains
344 artifacts and 140,179 lines: `Cargo.toml`, 201 `src/` production and
in-module test files, and 142 standalone `tests/` sources. The crate has no
benchmark directory, fixture tree, or generated/platform source variant. Its
Cargo build input is the shared `rust/scripts/aggregate-tests.rs` (79 lines),
which generates the untracked `OUT_DIR/all_tests.rs` aggregate test module.
The only platform-named source is the ordinary cross-platform
`tests/casetest_windows_pushdown_source.rs` fixture; it has no `cfg`-specific
implementation of these accessors.

The edited production surfaces are `logical/mod.rs`, `logical/sequence.rs`,
and `physical/mod.rs`. Their adjacent unit tests in `logical/tests.rs`,
`logical/operator_tests.rs`, and `physical/tests.rs`, plus the logical
dismantling helper in `logical/derive_stats_tests.rs`, were read before
validation. No Go source, Bazel metadata, fixture result, generated output, or
platform variant was changed.

## Alignment

Go's `SetChild` directly indexes `children[i]`, so an invalid index panics;
the prior Rust implementation returned `None`, a Rust-only refusal path. Both
Rust base plans now index the child vector and retain the replaced child for a
valid index. Go's `GetChildStatsAndSchema` directly indexes child zero, and
the base `GetJoinChildStatsAndSchema` always panics; Rust now preserves those
boundaries instead of returning `None` for leaves/non-join operators.

Focused regressions cover logical and physical out-of-range `SetChild`, plus
leaf/non-join stats access through `catch_unwind`. The dismantling helper now
checks `cursor.children().is_empty()` before replacing child zero, so the new
panic contract does not turn its normal walk into an accidental out-of-range
call. The follow-up in the same owning crate also removes Rust-only optional
refusals from the sequence operator's last-child schema, predicate/pruning,
and stats helpers; `PhysicalSequence` reads its last attached child while
retaining a stamped schema during construction. Logical output-name dispatch
now distinguishes schema-producer overrides from base forwarding, and
physical child-request property getters/setters direct-index like Go. Focused
regressions cover last-child schema selection, empty-sequence panic boundaries,
schema-producer name ownership, and out-of-range child-request properties.

## Validation

Profile: Ready for this bounded Rust package batch.

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib set_child_panics_on_an_out_of_range_index_like_go -- --nocapture` — two panic-contract tests passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib join_child_stats_and_schema_only_answers_for_a_join -- --nocapture` — stats-accessor regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib schema_producer -- --nocapture` — three schema/output-name regressions passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib child_req_props -- --nocapture` — child-request property panic/round-trip regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib 'sequence_' -- --nocapture` — 11 sequence/planner tests passed.
- The initial focused compile caught and fixed an incomplete `child_len` call in
  the dismantling regression; the corrected helper checks the public child
  slice directly.
- The full `tidb-planner --lib` run reached 857 tests: 852 passed and five
  unrelated failures remained. The same `data_source_unique_index_keys...`
  failure reproduces at clean HEAD in a detached worktree, so these are not
  regressions from the accessor batch. The focused tests above, pinned Rust
  formatting, `make lint`, and `git diff --check` are the Ready evidence for
  this bounded change; the five baseline failures remain unverified here.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --test all -- --test-threads=1` — 260 passed and 1,078 documented gap tests ignored.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-codec -p tidb-planner --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` — passed on the committed tree.
- In a clean `a9c87b8068` worktree with the follow-up tests applied, the
  pre-fix `logical::tests::sequence_schema_uses_the_last_main_query_child_like_go`
  regression failed because the old dispatcher returned the first child's
  schema, and `physical::tests::child_req_props_round_trip` failed because an
  out-of-range getter returned `None`. Both regressions pass after the
  follow-up changes.
- The current `logical::tests` subset passes 20/20 and the current
  `physical::tests` subset passes 39/39; the crate-wide test inventory remains
  the 260 passing integration tests plus documented ignored gaps recorded
  above.

## Risks

- Correctness: invalid child access now panics like Go, which is the intended
  contract; callers must not probe absent children for a sentinel `None`.
- Compatibility: valid accessors retain their existing return shape and
  replacement semantics. No SQL execution path or Go package changed.
- Performance: direct indexing removes the prior bounds-check-to-`Option`
  branch only on invalid access; valid access remains one vector replacement.
