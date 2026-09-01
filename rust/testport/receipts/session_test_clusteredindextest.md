# `pkg/session/test/clusteredindextest` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 253 lines. Every clustered
index/partition test, helper, TestMain/goleak harness, failpoint setup, and
three-shard flaky Bazel target was read before this receipt was written. There
is no `doc.go`, fixture or `testdata` directory, generated output,
platform-specific variant, benchmark, fuzz target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 25 | `b305a017de5c7715fae19ef508a76bf3df521a15` | `1378f9e68892046a3bd49b073180e5099df3c81c3dffc788f38c5d2f3d8de835` | three-shard flaky clustered-index test target and dependency closure |
| `clustered_index_test.go` | 162 | `960c3f6b60ac56d8730b8cd3eaf1d19233462a1f` | `a9c1ae499aeabf1e246b5e745f0602565dc4974681e2307a2eb0142894c01bb5` | clustered primary-key snapshot, old-row-format DML, and partition-pruning tests |
| `main_test.go` | 66 | `f8b2a5f3655bd7141115daf9e3ca6eb2f1c4daad` | `4ee08a274bba50aefe4e38ac5ee00ebb4e3405cadaf0db9008391a62e3b2c94f` | common setup, TiKV failpoints, async-commit settings, and goleak harness |

`clustered_index_test.go` declares `TestClusteredInsertIgnoreBatchGetKeyCount`,
`TestClusteredWithOldRowFormat`, and `TestPartitionTable`, plus the
`createTestKit` helper and `SnapCacheSizeGetter` seam. The first test inspects
the storage snapshot cache after an optimistic insert-ignore; the second
exercises disabled row encoding, clustered/unique indexes, updates, and
collation; the third compares hash/range clustered partitions with a normal
table over randomized keys. `main_test.go`'s `TestMain` configures schema
lease and async-commit safety, enables client failpoints, and applies the
goleak allowlist.

The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all three artifacts.

## Rust ownership and explicit boundary

Rust has the source-backed ignored carriers
`tidb-session::tests_session_bootstrap_common_source::test_clustered_insert_ignore_batch_get_key_count`,
`test_clustered_with_old_row_format`, and `test_partition_table`. They
correctly remain ignored: the Go tests require TestKit + Domain + mock TiKV,
the storage snapshot `SnapCacheSize` inspection seam, row-encoder session
state, tablecodec-backed clustered DML, and partition executor behavior. The
current Rust session crates do not expose a dependency-closed owner for that
stack. No Rust-only behavior was found to remove, and no safe package-local
implementation can be added without duplicating storage/session ownership.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no new regression test or package-complete
Ready claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/session/test/clusteredindextest \
  -run '^TestClusteredInsertIgnoreBatchGetKeyCount$' -count=1       # passed
```

The exact detached Go-master worktree was used. The package source/build
metadata has no direct `failpoint.` calls (the harness only enables TiKV
client failpoints), so no failpoint wrapper was required for this targeted
run. Rust source, Bazel, and module files were unchanged;
`make bazel_prepare` and Ready lint were not required. Not verified: the two
remaining clustered-index tests, all three Bazel shards, full storage-backed
partition coverage, or live TiKV behavior. Correctness and performance risk
are unchanged because this batch modifies documentation only.

This receipt certifies the bounded clustered-index test-package inventory and
explicit ownership boundary; it is not a repository-wide parity claim.
