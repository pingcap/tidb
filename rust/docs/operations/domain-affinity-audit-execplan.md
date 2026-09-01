# `pkg/domain/affinity` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master affinity manager artifact and establish its Rust
ownership boundary without fabricating a PD client or DDL integration.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all four artifacts and
  706 lines, including the 11-shard Bazel target and complete manager tests.
- [x] (2026-09-02) Verified all current files are byte-identical to Go master
  and ran `go test ./pkg/domain/affinity -count=1` successfully.
- [ ] Publish this receipt batch and continue the rolling package audit.

## Scope and decision

`pkg/domain/affinity` owns PD affinity-group create/delete/get operations,
compatibility fallbacks, bounded URL selection, retry logging, and a mock
manager. Rust has no dependency-closed PD HTTP or DDL owner, so this package
remains Go-native and unchanged.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/domain/affinity -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

No Go/Bazel/module or Rust source changed, so `make bazel_prepare` is not
required for this receipt-only batch.

## Outcome

The complete package inventory and explicit Go-only boundary are recorded in
`rust/testport/receipts/domain_affinity.md`; the repository-wide audit remains
in progress.
