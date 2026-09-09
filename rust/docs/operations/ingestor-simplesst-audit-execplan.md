# `pkg/ingestor/simplesst` parity audit ExecPlan

## Objective

Inventory the complete simple-SST Go package, restore the Go-master
multi-directory file enumeration contract, and keep the Rust ownership
boundary explicit because no dependency-closed Rust simple-SST owner exists.

## Progress

- [x] (2026-09-02) Read all 19 Go artifacts (6,545 pre-edit Go-master lines),
      including production/test files and BUILD metadata; confirmed no
      fixtures, generated/platform variants, fuzz, benchmark, or extra build
      inputs.
- [x] (2026-09-02) Restored variadic `GetAllFileNames` matching and named the
      existing writer connection-limit constant.
- [x] (2026-09-02) Added `TestGetAllFileNamesMatchesMultipleNonPartitionedDirs`;
      the pre-fix compile failed on the multi-directory call and the focused
      and full failpoint-wrapped suites pass after the fix.
- [x] (2026-09-02) Ran Ready lint/diff checks and attempted
      `make bazel_prepare` (blocked by missing `bazel`).
- [x] (2026-09-02) Committed this package plus receipt/ExecPlan, pushed to
      `hparser-integration`, verified the remote SHA, and fast-forward pulled.

## Ownership and risks

No Rust simple-SST implementation exists; adding a disconnected object-store
SST stack would invent an API. The Go variadic change is source-compatible for
existing one-directory callers and enables callers scanning multiple task
directories. Bazel regeneration remains unverified because the local Bazel
binary is unavailable.
