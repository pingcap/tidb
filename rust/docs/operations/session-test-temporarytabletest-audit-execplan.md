# `pkg/session/test/temporarytabletest` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session temporary-table test
package and map local/global table, transaction, DML, and schema-lease
semantics to existing Rust owners without introducing duplicate catalogs or
mock-cluster behavior.

## Completed this batch

1. Inventoried all three artifacts (512 lines): the TestMain/goleak harness,
   three tests, and three-shard flaky BUILD target. No production, fixture,
   generated, benchmark, fuzz, or platform artifact was omitted.
2. Ran the exact Go-master failpoint-managed suite; all three tests passed in
   5.470s and failpoints were disabled during teardown.
3. Compared every test with Rust. Session overlays, row lifetime, temporary
   DDL guards, and core local/global behavior have executable Rust owners;
   exact mock TiKV point/batch/index-scan coverage and cross-session schema
   lease/MDL do not form one dependency-closed owner.
4. Attempted the focused Rust carrier tests; compilation was blocked by the
   environment's missing `pkg-config`/OpenSSL dependency. Found no safe
   missing behavior to implement and no Rust-only behavior to remove.
   Recorded inventory, hashes, validation evidence, and the explicit SEED
   boundary in `rust/testport/receipts/session_test_temporarytabletest.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [x] Rust focused test attempt recorded with the OpenSSL/pkg-config blocker.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated mock TiKV execution, duplicate-key and
warning semantics, and cross-session schema lease/MDL lifecycle owners. The
package loop continues with the next unrecorded session test package; this
plan does not claim whole-repo completion.
