# `pkg/meta/model/index.go` Rust parity audit ExecPlan

This living plan follows `PLANS.md` and supplements the package inventory and
behavior evidence in
`rust/testport/receipts/meta_model_materialized_view.md`.

## Purpose / Big Picture

Keep the Rust `tidb-model` index owner aligned with Go's discardable return
contracts. This corrective Rust-only metadata batch changes no persisted
index fields, JSON merge behavior, equality identity, or lookup semantics.

## Progress

- [x] Re-inventory the current Rust crate (42 tracked artifacts, 32,466 lines)
  and read `index.rs`, all of its public/private functions and inline tests,
  its manifest/library registration, integration coverage, and affected caller
  sites. Per user direction, do not re-read or edit Go code.
- [x] Classify all 31 index annotations: remove the 25 direct Go-shaped
  annotations restored by `8d42bcc7035` and retain six Rust
  ownership/equality adapters (`clone_like_go`, `clone_pointer`, `equals_id`,
  and `equals`).
- [x] Restore `go_index_returns_may_be_ignored_like_go`. The restored source
  failed its compile probe with exactly 25 diagnostics; the edited source
  passes.
- [x] Run the focused regression, all 324 owner tests, all-target compilation,
  standalone formatting, Ready lint, and diff hygiene.
- [x] Prepare one `pkg/meta/model` corrective commit for normal rebase and
  publication to `origin/hparser-integration`.
- [ ] Continue the rolling Rust-only audit with the next package boundary.

## Scope and decision

Remove only the 25 `#[must_use]` annotations attached to direct Go-shaped
index APIs: vector/full-text metric maps, global-index state, changing-index
names, parser names, columnar/inverted-index helpers, index predicates and
lookups, and foreign-key index searches. Retain the six explicit Rust
ownership/equality adapters. No index metadata, serialization, clone depth,
identity, lookup, or predicate behavior changes. No Go, Bazel, Cargo, or
dependency file is in scope.

## Validation gate

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  --offline --locked -p tidb-model --lib \
  go_index_returns_may_be_ignored_like_go -- --test-threads=1
# PASS; 1 test

cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml \
  --offline --locked -p tidb-model --no-fail-fast --test-threads=1
# PASS; 324 tests

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  --offline --locked -p tidb-model --all-targets --quiet
# PASS

rustfmt +nightly-2026-08-22 --check --edition 2021 \
  rust/crates/tidb-model/src/index.rs
# PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
# PASS

git diff --check
# PASS
```

No Go tests or live SQL integration are run for this Rust-only diagnostic
alignment. No failpoint or `make bazel_prepare` gate applies because no Go
source, import section, Bazel, module, or generated metadata changed.

## Outcomes and retrospective

The index owner retains all runtime values and effects; only the reverted
Rust discard diagnostics are corrected. Publication is one package-scoped
commit, after which this rolling audit advances to the next restored package.
