# `pkg/errctx` parity audit ExecPlan

## Objective

Keep the complete Go-master `pkg/errctx` inventory and the Rust
`tidb-error::errctx` owner aligned as one atomic package boundary. Read every
Go production, test, fixture, generated/platform variant, and build artifact
before editing; record the integration decision and validation in
`rust/testport/receipts/errctx.md`.

## Completed this batch

1. Inventoried all three tracked Go artifacts (389 lines): error context
   implementation, source test, and the public Bazel target. No fixture,
   generated, platform-specific, benchmark, fuzz, or extra support artifact
   exists.
2. Compared every production branch with `tidb-error::errctx`: seven error
   groups and their code map, level-map copy semantics, warning/note appending,
   root-cause alias handling, multi-error first-error behavior, strict shared
   context, and ignore/warn resolution all match Go.
3. Ran the Go package and the two Rust source-derived tests. No production
   fix was necessary and no compatibility-only API was added.

## Validation gate

- [x] Complete Go artifact inventory and Rust ownership decision.
- [x] Go package test.
- [x] Rust `errctx_source` tests (2 passed).
- [x] Workspace Rust check (the owner crate is included).
- [x] Diff quality and documentation review.
- [ ] Fetch remote, create one documentation batch commit, push to
      `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

There is no remaining `pkg/errctx` production boundary. Warning storage and
error construction belong to their respective Go utility/error packages and
are intentionally represented as Rust traits/types at the owner seam. The
repository package loop continues with the next unreceipted package.
