# `pkg/parser/mysql` OPERATE VIEW parity ExecPlan

## Objective

Inventory all Go parser/mysql production, test, and BUILD artifacts, restore
the Go-master privilege registry, and keep the coordinated Rust privilege
owners and receipts accurate.

## Progress

- [x] (2026-09-02) Read all 15 package artifacts (4,839 pre-edit lines),
      including all constants/tables, tests, and BUILD metadata; confirmed no
      fixtures, generated/platform variants, or hidden package inputs.
- [x] (2026-09-02) Restored `OperateViewPriv` at bit 33, its SQL/set/catalog
      spellings, reverse maps, and global/database/table scope lists.
- [x] (2026-09-02) Added `TestOperateViewPrivilegeRegistry`; it failed before
      the fix with an undefined symbol and passes after the fix.
- [x] (2026-09-02) Ran focused and full failpoint-wrapped package tests,
      `make lint`, and `git diff --check`; attempted `make bazel_prepare`.
- [x] (2026-09-02) Committed only this package plus receipt/ExecPlan, pushed to
      `hparser-integration`, verified the remote SHA, and fast-forward pulled.

## Boundary and risks

The Rust privilege/parser/bootstrap owners are already synchronized by the
prior OPERATE VIEW batch; this change closes the remaining Go registry gap.
Privilege bit ordering and persisted `mysql.user`/`mysql.db` columns are
compatibility-sensitive. Bazel regeneration is blocked by the missing local
binary and remains unverified.
