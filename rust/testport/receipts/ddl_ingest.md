# `pkg/ddl/ingest` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly 20 tracked artifacts and 4,777 lines: 13
production Go files, six test Go files, and one Bazel build file. Every
production source, test, fixture/build input, and build artifact in the package
was read before editing. There is no package `doc.go`, fixture directory,
`testdata`, generated source/input, platform variant, benchmark, fuzz target,
or `OWNERS` file beyond the listed Bazel target.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 104 | `75c5e4cdb6348e14b564a9943d88b1ab644af5f3` | `cbb93b4d5ab95dc3d82bad4cc14f1532bf1c305f44e12b0a04960f1c43698ce5` | ingest library and 33-shard flaky test target |
| `backend.go` | 419 | `26f0d3ad2199fe6497ab446a41f088c894c19a82` | `b8c998f1d93853330175e836dbb997e22ca8fb9386dde8b43b168ca250f0e947` | Lightning backend lifecycle and ingestion |
| `backend_mgr.go` | 292 | `43a4eeddd487fc0ed00150de3b8cf4b8d0cd4faa` | `000d835ac3f8ae90b2d76da0c2086b39248efe4a83f6df9c90f6deecccfee5e0` | backend manager and task registration |
| `checkpoint.go` | 643 | `95a83c7dec61b187e0f303dde0ca35bd32deaccb` | `d9725ffa52819dc19439fed31618b15ba197b85190059a745a45b758d19606ce` | checkpoint persistence and recovery |
| `checkpoint_test.go` | 218 | `aa7fa2d855f42b1571c8ced8976279b8ab95b56d` | `282d03895bff68af10eba611f47340b1e01af9339f310c5733028bc9a104f67b` | checkpoint lifecycle and resume tests |
| `collector.go` | 206 | `86b629c126ae7fed80b4584fcd55328803f6fa54` | `cea70bc4e83ce0bb305e5781e64ef85a6aeb3266bea93325fd8449107f0f0e41` | ingest metrics and runtime collection |
| `config.go` | 193 | `a47b9072ef4b31194fff5f18dad4de0d3b3982e9` | `52461017e2bfce45ff3fee9c4274b64ea04170387519a0bc9f2a209837ed7db` | ingest configuration and defaults |
| `disk_root.go` | 287 | `a35d4d97154b18bc43e46e3104ecbad34b663083` | `ac6f699f78831302edbdb73a78d74aadfffdcefc12234a674d13a58f2ee146ec` | disk usage tracking and local-sort admission |
| `disk_root_test.go` | 163 | `9e5536d66dddaae20892131a107086e00dd63672` | `5c6a4e08e4b9e02e84aedf2132c191f1a89cecf3e3f095b54944ebf88e8e592c` | disk threshold and failpoint classification regressions |
| `engine.go` | 230 | `daf19e4e219d16d2f97ec26fa8cc0442431fb33e` | `21395470ecc2d00c8d5ddd2e332036db7d5755dcef3566476eef8e5df0293b0c` | engine creation, writers, and import |
| `engine_mgr.go` | 142 | `88cf5a49dad9132a6ec25b4babd63054019669f8` | `f6315bc3c9627664534eb7c60a62182516ce7b3c293426468d18a4b22a5f6574` | engine manager coordination |
| `env.go` | 208 | `3b1e92299af4b8dad94c246d8785ea3f146e3322` | `2861ff1604ba60fef20578c7fc30e86200a56a088fc8e407c306d9e7a1210661` | temporary directory and environment setup |
| `env_test.go` | 74 | `06493a9ea0539a6ae6e5a435eae2e02b06c58509` | `3605ff016f26ae06d5144fd21a90e3b47a92ca0c61bf49dc75fc6e29e8d5e849` | environment and backend-context tests |
| `integration_test.go` | 979 | `98a03ac5f185d3a03310264b987c1c6bdaba4097` | `7ce60928827fae5e83a0ec1d2ef2b934aa2c9b8c292edfec490c0aa3e2421f98` | add-index, checkpoint, partition, and cancellation integration tests |
| `main_test.go` | 37 | `bd895e58df2b39326359d6d3a66ec25f50ccd792` | `707748e2599f4c1402cae84d4350558f57d2f096755675feca6ea52acd198e0b` | package test setup |
| `mem_root.go` | 142 | `b42e6f262544cac606569f781b6eb56fc848bb51` | `6f543630ff43b093a33c5543abad1db89e027fd58e14391ff7639444c678e74b` | memory quota tracking |
| `mem_root_test.go` | 60 | `fad3a64f4c4c085bec1e919d7f7ae2ec956287c1` | `cc1ca6a09649c3c9c4ca1ff17e77603c4751035eb316dc754967cbfffa0c1480` | memory quota regression |
| `message.go` | 78 | `6db3aef665ccf6c841aa3fa0c0d8fa351cb2f857` | `9d275f88b2adc8b9b4c805ec12b374b6a07ad7f3a607841b4f882fdb5f8dcfa8` | ingest task messages |
| `mock.go` | 258 | `73f1eb09b94c0ed16b04c4bdba28a7b90d64ef7d` | `222f0cd471570fcccd65d244bf88380b723aa65ffb2ee4c89e631a48db1f5e0c` | test backend and engine mocks |
| `util.go` | 44 | `878a20c56d625ecbf913ea2a2ef7b1956aa44227` | `480bf3bf1ff396126730bc347abc7b678cd8db665590305d45027e5ba08039ee` | ingest utility helpers |

The production inventory contains 199 declaration lines; the test inventory
contains 34 top-level test functions (including `TestMain`). All current
package files now match the pinned Go master byte-for-byte.

## Go/Rust boundary and fixes

`pkg/ddl/ingest` owns TiDB Lightning local storage, checkpointing, engine
lifecycles, disk/memory admission, and SQL integration. Rust's `tidb-dxf`
crate has no dependency-closed owner for this TiDB-specific storage and DDL
integration, so no speculative Rust implementation was introduced.

The branch had a Rust-only exported `RiskOfDiskFull` helper and omitted Go
master's local-sort admission path. The batch restores the canonical unexported
`minFreeDiskBytes`/`riskOfDiskFull` helpers and
`CheckLocalSortDiskSpace`: it reserves two GiB per runtime slot, caps that
headroom at `tidb_ddl_disk_quota`, preserves retryable probe errors, and
classifies confirmed insufficient space as `ErrIngestCheckEnvFailed`. The
Rust-only memory-root test was removed. The Bazel target now includes the disk
regression test, embeds the library, restores 33 shards, and lists the exact
Go-master test dependencies.

## Validation and risk

Profile: **Ready** for this behavior restoration. The focused regression was
verified to fail before the fix (missing canonical helpers) and pass afterward
with failpoints enabled and disabled by the repository wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/ingest \
  -run '^(TestRiskOfDiskFull|TestCheckLocalSortDiskSpace|TestCheckLocalSortDiskSpaceErrorClassification)$' -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/ingest 1.393s
```

The complete failpoint-aware package suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/ingest -count=1
# PASS; ok github.com/pingcap/tidb/pkg/ddl/ingest 105.914s
```

`make lint`, Rust formatting, and `git diff --check` are required Ready gates
and pass for this batch. `make bazel_prepare` is required because a Go test
file was added, the test import/dependency metadata changed, and the Bazel
target changed; the local environment has no `bazel` executable, so that gate
is recorded as blocked rather than hidden. No real-TiKV test was required.

The principal compatibility risk is the boundary calculation (`>` rather than
`>=`) and the two-GiB-per-slot heuristic, both covered by table-driven tests.
Filesystem capacity varies by host; macOS keeps Go master's development-only
bypass for confirmed low-space errors.

## Outcome

The complete ingest inventory and Go/Rust ownership boundary are recorded here.
Go master's local-sort disk admission behavior and focused regression coverage
are restored; the rolling package audit continues after this published batch.
