# `pkg/dumpformat` parity audit ExecPlan

## Objective

Inventory the Go dump-format root package, compare its public kind-definition
surface with Rust, and keep nested writer packages as separate claims.

## Completed

- Read all three Go-master root artifacts (46 lines): BUILD, OWNERS, and
  `kind.go` with its `FieldKind` type and three enum values.
- Confirmed the root has no tests, fixtures, generated/platform variants,
  benchmarks, fuzz corpora, or generator inputs.
- Ran the exact Go-master root compile check in a detached worktree because
  the hparser branch predates this root package. Rust has no dump-format owner
  or call site, so no Rust-only behavior was removed and no speculative enum
  API was added.

## Validation gate

- [x] Complete root inventory and nested-package boundary.
- [x] Go root compile, Rust formatting, lint, and diff checks pass (Ready).
- [ ] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote SHAs, and fast-forward pull the explicit branch ref.

## Remaining boundaries

Audit `pkg/dumpformat/csvfile`, `parquetfile`, `parsedef`, `sqlfile`, and
`testutils` separately. The Parquet unit includes generated Spark-rebase code
and two binary fixture inputs; those must be inventoried before any writer or
reader parity change.
