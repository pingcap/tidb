# `pkg/meta/model/index.go` parity audit ExecPlan

This living plan follows `PLANS.md` and supplements the atomic inventory and behavior evidence in `rust/testport/receipts/meta_model_materialized_view.md`.

## Purpose / Big Picture

Keep the Rust `tidb-model` index owner aligned with Go's discardable return contracts. This Rust-only metadata follow-up changes no persisted index fields, JSON merge behavior, equality identity, or lookup semantics.

## Progress

- [x] Re-read the complete 23-artifact `pkg/meta/model` package at current Go master `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`: 10,721 lines in 14 production files, eight tests, and BUILD metadata; no fixtures, generated inputs, benchmarks, or platform variants.
- [x] Read the complete Rust index owner, all public/private functions, inline tests, direct callers, and workspace registration before editing.
- [x] Classify 31 index annotations: remove 25 direct Go-shaped annotations and retain six Rust ownership/equality adapters (`clone_like_go`, `clone_pointer`, `equals_id`, and `equals`).
- [x] Add `go_index_returns_may_be_ignored_like_go`. The pre-fix source failed with exactly 25 diagnostics; the edited source passes.
- [x] Run the affected owner tests, all-target check, formatting, Ready lint, and diff hygiene.
- [x] Commit once for `pkg/meta/model` index alignment, rebase/push to `origin/hparser-integration`, and verify the remote SHA in the task handoff.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Remove only the 25 `#[must_use]` annotations attached to direct Go-shaped index APIs: vector/full-text metric maps, global-index state, changing-index names, parser names, columnar/inverted-index helpers, index predicates and lookups, and foreign-key index searches. Retain the six explicit Rust ownership/equality adapters. No index metadata, serialization, clone depth, identity, lookup, or predicate behavior changes. No Go, Bazel, Cargo, or dependency file changes are in scope.

## Validation gate

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-model --offline --locked --lib go_index_returns_may_be_ignored_like_go -- --nocapture
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml -p tidb-model --offline --locked --no-fail-fast
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-model --all-targets --offline --locked
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
git diff --check
```

No Go tests or live SQL integration are run for this Rust-only alignment batch. No failpoint or `make bazel_prepare` gate applies because no Go source, import section, Bazel, module, or generated metadata changed.

## Outcomes and retrospective

The index owner retains all Go runtime behavior; only redundant Rust discard diagnostics are removed. Publication is tracked by the package-scoped commit and remote SHA in the task handoff while the rolling audit continues.
