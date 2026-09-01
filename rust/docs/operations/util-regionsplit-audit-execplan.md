# `pkg/util/regionsplit` parity audit ExecPlan

## Objective

Inventory the complete Go-master region-split key helper package and keep its
ownership boundary current while the dependent DDL, executor, table metadata,
and storage-key owners remain separate.

## Completed

- Read both pinned artifacts (256 lines), all 12 production declarations, and
  the public Bazel target; confirmed no tests, fixtures, generated/platform
  variants, or nested packages exist.
- Refreshed the receipt to Go master `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
  and verified the package remains source-identical.
- Confirmed Rust owns only lower-level key encoders and transport; no
  dependency-closed high-level split-key implementation exists, so no
  speculative adapter or Rust-only behavior removal is appropriate.

## Validation gate

- [x] Current and exact-Go-master package compile probes pass (`[no test files]`).
- [x] Ready formatting, repository lint, and diff checks pass.
- [ ] Push this receipt/ExecPlan refresh, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Porting this package requires the complete DDL/executor split-region path,
table metadata, common-handle codecs, and PD/TiKV scheduling consumers as one
dependency-closed change with focused arithmetic and boundary regressions.

