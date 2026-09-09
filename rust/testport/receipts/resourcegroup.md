# `pkg/resourcegroup` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The root package contains two tracked artifacts and 63 lines. Both the public
interface source and its Bazel target were read line by line. There is no
package doc, source test, fixture, generated input/output, benchmark, or
platform-specific variant in this directory; the implementation package and
integration tests are separate units recorded in `resourcegroup_runaway.md`
and `resourcegroup_tests.md`.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `4a922cbe5b6f21e49e49178b11c2ae13205ff0df` | `2b871b0d35b438f3373bb5e1e042f798d8618eebf0efed1dca20a1cd0d2405d5` | public Go library target and kvproto/client dependencies |
| `checker.go` | 50 | `1c5e65ecc625505ab5d5fedf32f6be41ef012acd` | `94b2928f30af80cc8de31eb0d624b6e4cf64b4d11f6ff4954caf5ebd02e68685` | `DefaultResourceGroupName`, `RunawayChecker`, and `ConsumptionReporter` contracts |

`checker.go` has no executable implementation. It defines the default group
name and the cross-package interfaces used by the separate runaway manager:
executor/coprocessor hooks, threshold checks, action/kill decisions, processed
key reset, and RU/RU-v2 consumption reporting.

## Rust ownership and decision

Rust has matching request-carrier traits in `tidb-txnkv` and resource-group
model/configuration types in `tidb-model`, `tidb-ast`, and
`tidb-ddl-resourcegroup`. Those are consumers and data carriers, not an owner
of this Go package's interface declarations or the runaway manager. The
process-global RU manager in `tidb-resourcemanager` is a distinct Go package
and does not implement `RunawayChecker` or `ConsumptionReporter`.

This receipt records the root package boundary without adding a duplicate Rust
trait, adapter, or Rust-only behavior. A future owner must integrate the
interfaces with the complete `pkg/resourcegroup/runaway` package and its
session/executor call sites as one behaviorally closed unit.

## Validation and risk

Profile: **WIP** for this docs-only audit; no Go or Bazel source changed, so
`make bazel_prepare` is not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/resourcegroup -count=1
# passed (no test files)
```

Correctness, compatibility, and performance are unchanged. Not verified:
Rust workspace-wide Ready validation, Bazel analysis, and a future
dependency-closed Rust implementation of these interfaces.
