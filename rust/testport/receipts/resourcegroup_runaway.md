# `pkg/resourcegroup/runaway` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The runaway package contains 10 tracked artifacts and 3,290 lines. Every
production source, source test, and Bazel target was read line by line. There
are no generated sources, fixtures, benchmarks, fuzz targets, or platform
variants in this package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 78 | `1fc06d0f310e35bd07bb51d8d66c0dbb197e9356` | `65c3b7ea738cb2b81e5aac0e1cdbfcb3b96a4d496ca64eb4f5e9b3fecab3dec3` | library/test target, failpoint and kvproto dependencies |
| `checker.go` | 431 | `c1c2393676f3d4a9d14bcd8db121d483067b2575` | `293d44f0197ee32ac39e4ce9fad2f93ea09b686f8bb0adba4520a36bdda0675c` | threshold, action, quarantine, switch-group, and request hooks |
| `flusher.go` | 140 | `c417df0f00c9861cba2f5320f6689605c73410d2` | `b047a2b0f7a486f9b501a2db813781ce52772fa598c2058abc98690055ba2c47` | generic batch flusher and ticker lifecycle |
| `manager.go` | 515 | `7329beeb331696ef75d97141f6409bbd2d4dd9e3` | `583b683db050d1689d379919c7b8c3e4a65eb3e8c7be2930a4287d8235553839` | watch cache, active counters, flush/sync loops, and lifecycle |
| `record.go` | 499 | `d3776ba01fd2ae49c49e26d862385db3a1606845` | `8ca03b6cf977e8303471bcdf5902a7ec61334470b9c976cbe89140f165d9c5be` | runaway/quarantine rows, SQL generation, add/remove, expiry, and restricted SQL |
| `syncer.go` | 384 | `31289c591d3a88caf6bcbc49aa8a55ff34ce15d6` | `7a17fd0193ca0fe5a7ba20a0ea035152f9ce1c0e88d5c0767a34eaee5142ab8e` | system-table readers, pagination, decoding, and cursor checkpoints |
| `checker_test.go` | 409 | `36e68a4b574fd0f350a2e7d43a9edbaf61cdc5f5` | `c04c380c9a01c4122722bce4e2f98b64ff758184d66072148e928c34a3b6575a` | counter ordering, threshold/action, nil safety, and concurrent CAS tests |
| `flusher_test.go` | 145 | `cc573bd4936f8215b58b25069ac3c9e284c7be1e` | `5f734f0ab2bf98b3a1ed77e35652888f1bb65c5da6dda2c716fe8386e7c0daf2` | add/merge/flush/empty flusher tests |
| `record_test.go` | 124 | `e5900a1401638f381f36c6e1d7c07286bc9101f6` | `eaeb4bc24683c96a865ae96a30432c609c532c5d2c17aac749340f143878459d` | record key and unavailable-table expiry behavior |
| `syncer_test.go` | 565 | `a2f37b8eb52b5e03b86f8c0c9a117baaaba9f1c1` | `de0c5820189f314d0382d9203975f7ae376132758f2005e5f2c29f3894fdb5ef` | SQL generation, watch/watch_done decoding, pagination, invalid tails, and cursor invariants |

Production behavior is centered on `Checker`, `Manager`, `Record`,
`QuarantineRecord`, `batchFlusher`, `syncer`, and `systemTableReader`. The
package preserves elapsed/RU/processed-key thresholds, dry-run/cooldown/kill/
switch-group actions, exact/similar/plan identifiers, CAS marking, quarantine
and watch records, TTL cleanup, asynchronous batch flushing, restricted SQL,
watch/watch_done cursor overlap, full-batch same-key livelock protection, and
manual point-query paths that do not perturb scan state.

The source tests cover `TestActiveGroupCounterOrdering`,
`TestConcurrentResetAndCheckThresholds`, `TestNewChecker`,
`TestExceedsThresholds`, `TestCheckerCheckAction`,
`TestGetSettingConvictIdentifier`, `TestNilCheckerSafety`,
`TestCheckThresholds`, `TestCheckRuleKillAction`,
`TestMarkRunawayBySettingsCAS`, all four `batchFlusher` tests, both record
tests, and the 10 SQL/cursor/pagination tests in `syncer_test.go`.

## Rust ownership and decision

No Rust crate implements the runaway manager, checker, SQL record writer,
system-table syncer, cache/TTL lifecycle, or failpoint-driven action paths.
`tidb-resourcemanager` owns process-global adaptive RU pools; it is not a
runaway-query controller. `tidb-ddl-resourcegroup` only converts DDL settings
to the resource-manager protobuf, while `tidb-txnkv` exposes an unimplemented
request-carrier trait. Treating those fragments as a complete owner would
create Rust-only behavior and would omit the Go manager's storage/lifecycle
contract.

This is an explicit boundary receipt. No Rust implementation or supplemental
test is added until the full manager, storage schema, executor hooks, and
session lifecycle can be integrated together.

## Validation and risk

Profile: **WIP** package audit. The package uses failpoints, so validation used
the repository failpoint wrapper; no Go or Bazel source changed.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/resourcegroup/runaway -count=1
# PASS; ok .../pkg/resourcegroup/runaway 0.982s
```

Correctness, compatibility, and performance are unchanged. Not verified:
Rust workspace-wide Ready validation, live TiKV/etcd integration, or a future
Rust runaway owner.
