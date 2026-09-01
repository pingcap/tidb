# `pkg/ddl/jobsubmit` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly six tracked artifacts and 1,119 lines: three
production files, two tests, and one Bazel build file. Every file was read in
full in the pinned Go-master worktree before this receipt. There are no
fixtures, `testdata`, generated sources or inputs, platform variants,
benchmarks, fuzz targets, or `OWNERS` files.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 69 | `39b31bbb66c4fe59fe9eed7635e44177762e43ec` | `3a4d1a6afb2a0f8ad776e44a5d9e19504cb75e74f63ff0f5d8895f32e2f18cce` | public library plus six-shard flaky submitter test target |
| `submit.go` | 507 | `0a3571b4209ef2050332c40e9f6962b6030e4287` | `50ed789acd8def63ba6a41c9385869c445c59d745492ef3701e50f823257bca0` | transactional DDL job ID allocation, validation, insertion retry, and owner notification |
| `submit_test.go` | 343 | `9d4891b86a7ce109a8b5ce0a137cf3f24dd6723a` | `f68fcbf9884fdbdc84894230df4e58e26f4e822477caff8b5d5127a2df5b882c` | submit batching, ID assignment, upgrade pause, BDR checks, and retry cleanup tests |
| `table_mode.go` | 61 | `6e3c43bdfe692f57b76417c2b7cda2d825d01b51` | `2f0e5b27ce78a17be3078199233446ec542fed10be7f0676debcc035d2f8240e` | validated table-mode job construction |
| `table_mode_test.go` | 95 | `ec730e9437ec0c9c4075f050ba5d71431e5f39e1` | `a2f4bdc550a484942d948012826aaf5b2fa6d5535f2b7342280f9ed125bf97e1` | table-mode transition, no-op, invalid-mode, and session metadata tests |
| `types.go` | 44 | `23760900eb6fe614561cf82207cb0206b37c0459` | `8e65e157d5c43f5d59aeb97e0c4ecb5ad22191ebd2db29c21d4a7cf847ba9674` | `JobSpec` and submit dependency contracts |

The production inventory contains 25 declarations (22 in `submit.go`, one in
`table_mode.go`, and two in `types.go`). The test inventory contains six
top-level tests, with nested cases covering ID ordering, trace initialization,
BDR restrictions, upgrade pause, table-mode metadata, and retry cleanup.
Current files are byte-identical to all six pinned Go-master artifacts.

## Native integration decision

This package is a Go-native DDL submission boundary. It is coupled to TiDB
session pools, metadata transactions, BDR policy, system-table managers,
server-state synchronization, failpoints, and etcd owner notification. Rust's
workspace has no dependency-closed DDL job submitter or SQL-backed job-table
owner. No Rust-only implementation or behavior was found to remove, and no
speculative Rust facade was introduced.

## Validation and risk

Profile: **Ready** for this documentation-only package audit. The exact
failpoint-aware Go suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/jobsubmit -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/jobsubmit 7.292s
```

The shared Ready gates (`make lint`, Rust formatting, and `git diff --check`)
are run for this receipt batch. No Go, Bazel, module, or Rust source changed,
so `make bazel_prepare` is not required. Rust tests and a workspace build are
not run because no Rust owner changed.

## Outcome

The complete DDL submitter package remains explicitly Go-native; its exact
inventory, test evidence, and Rust ownership boundary are recorded here while
the rolling audit continues.
