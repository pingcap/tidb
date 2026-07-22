# Whole-package execution

The filename remains for old links. The rewrite currently has one worker and
no coordination subsystem.

The minimum unit is one complete upstream Go package or module. It includes all
production files, build/platform/generated variants, tests, subtests,
benchmarks, fuzz targets, examples, fixtures, `testdata`, failpoints, helpers,
runner scripts, and build metadata. One Go package may map to several Rust
crates, but only one whole-package proof is accepted.

The workflow is:

1. Read the entire Go package and its tests/support.
2. Transcreate its complete behavior directly from Go.
3. Translate every original test obligation.
4. Run `scripts/package-port.py finish ...`.
5. Commit the implementation and generated `ports/<go-package>.toml` together.

Git owns isolation, review, rollback, and history. `package-port.py` derives the
inventory, checks dependencies, verifies touched crates/tests, and writes the
only bookkeeping artifact. There are no claims, queues, campaigns, worktree
leases, transfer ledgers, or receipts.
