# `pkg/session/test/clusteredindextest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 253 lines. Every Go test,
helper, harness, Bazel declaration, and referenced `testdata` path was checked
before comparing Rust. The BUILD file's `glob(["testdata/**"])` currently
matches no tracked fixture; there is no `doc.go`, generated output,
benchmark/fuzz target, or platform-specific variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 25 | `b305a017de5c7715fae19ef508a76bf3df521a15` | `1378f9e68892046a3bd49b073180e5099df3c81c3dffc788f38c5d2f3d8de835` | three-shard flaky/race-enabled target, empty testdata glob, and dependency closure |
| `clustered_index_test.go` | 162 | `960c3f6b60ac56d8730b8cd3eaf1d19233462a1f` | `a9c1ae499aeabf1e246b5e745f0602565dc4974681e2307a2eb0142894c01bb5` | clustered-index snapshot, old-row-format DML, and partition-pruning tests |
| `main_test.go` | 66 | `f8b2a5f3655bd7141115daf9e3ca6eb2f1c4daad` | `4ee08a274bba50aefe4e38ac5ee00ebb4e3405cadaf0db9008391a62e3b2c94f` | schema-lease, failpoint, async-commit, and goleak harness |

The inventory contains eight top-level functions: four runnable `Test*`
functions, one helper, one interface declaration, and `TestMain`. The tests
are `TestClusteredInsertIgnoreBatchGetKeyCount`,
`TestClusteredWithOldRowFormat`, and `TestPartitionTable`; `TestMain` is the
fourth `Test*` harness function. The helper is `createTestKit`, and the
test-local `SnapCacheSizeGetter` interface is the snapshot observation seam.
The old-row-format test covers issues 21568, 21502, 22193, and 23646 plus
collation and UnionScan paths; the partition test compares hash, range, and
ordinary clustered-primary-key scans over 400 randomized rows.

## Rust ownership and explicit boundary

Rust has ignored source carriers for all three runnable tests and `TestMain`
in `rust/crates/tidb-session/src/tests_session_bootstrap_common_source.rs`.
The Rust session/storage crates provide partial clustered-key encoding,
temporary overlays, row decoding, and partition metadata, but do not expose
the Go mock TiKV snapshot cache (`SnapCacheSize`), TestKit DML executor,
old-row-format session toggle, or randomized partition scan lifecycle as one
dependency-closed owner.

No Rust-only behavior was found to remove, and no safe standalone Go behavior
can be implemented in this test-only package without duplicating storage,
transaction, session, and executor integration. The package is therefore an
explicit SEED/boundary; its ignored carriers remain evidence rather than a
claim of executable parity.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit. No Go source,
imports, test declarations, Bazel metadata, or module files changed in this
batch, so `make bazel_prepare`, the Ready lint gate, and a new regression test
were not required.

The exact Go-master failpoint-managed package suite passed from a detached
`origin/master` worktree in 5.087s; failpoints were enabled and disabled by
the wrapper.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/clusteredindextest -count=1)
# passed: ok .../pkg/session/test/clusteredindextest (5.087s)
```

No Rust code changed, so a Rust behavior regression test was not applicable.
Not verified here: Bazel execution, `make lint`, full repository tests, live
TiKV snapshot-cache behavior, or Rust's ignored carrier target. Correctness,
compatibility, and performance behavior remain unchanged because this batch
modifies documentation only.

This receipt certifies the bounded clustered-index test inventory and explicit
ownership boundary; it is not a repository-wide transcreation claim.
