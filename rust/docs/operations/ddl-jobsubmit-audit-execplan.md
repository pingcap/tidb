# `pkg/ddl/jobsubmit` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master DDL job-submission artifact and verify its
transaction, validation, and test boundary against Rust without inventing a
partial DDL runtime.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all six artifacts and
  1,119 lines, including the 6-shard Bazel target and both test files.
- [x] (2026-09-02) Verified every current artifact is byte-identical to Go
  master and ran the complete failpoint-aware package suite.
- [ ] Publish this receipt batch and continue the rolling package audit.

## Scope and decision

`pkg/ddl/jobsubmit` owns transactional DDL job submission, global-ID allocation
and retry, BDR/upgrading-state validation, table-mode job construction, and
owner notification. Its session, metadata, etcd, and system-table dependencies
are not represented by Rust's current crates; keep the package Go-native.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/jobsubmit -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

No Go/Bazel/module or Rust source changed, so `make bazel_prepare` is not
required for this docs-only batch.

## Outcome

The full package inventory and explicit Go-only boundary are recorded in
`rust/testport/receipts/ddl_jobsubmit.md`; the repository-wide audit remains
in progress.
