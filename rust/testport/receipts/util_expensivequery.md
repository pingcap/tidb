# `pkg/util/expensivequery` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). This package is a
server/session monitor with process-list, metrics, logging, and kill-action
consumers; it has no dependency-closed Rust owner.

## Complete inventory

All three Go-master artifacts were read in full before the ownership decision:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 31 | `6651e84bf8688557bec93083480ab20306d04d4c` | `06aa725ddd67e3603fe7476e1fb6afc61ce642f65bb7a18bc7b57d6775d03748` | library/test deps and flaky target inventoried |
| `expensivequery.go` | 155 | `998a07906c62748481b1a2a91f38483cb20aea2e` | `d13c6e2ff8be9df9c7aea1f16acf3667a19a53978151464d28fdbf24fdf03fe4` | handler lifecycle and kill policy inventoried |
| `expensivequery_test.go` | 34 | `f2065ee1d4a96400bc2abbfa65dfbad7788cb5ec` | `3dce36e712ed1a236d886b619d78c1c38877af2319956d34208964707621210a` | common `TestMain`/goleak harness inventoried; no source test functions |

The package contains no `doc.go`, README, fixture, generated/platform
variant, benchmark, fuzz target, or nested package. The test artifact is only
the repository's common setup and leak exclusions; `go test` reports no tests
to run.

## Go behavior

`Handle` stores the session manager atomically. `Run` starts a 100 ms polling
loop, reloads expensive-query and transaction thresholds, and every 15
seconds updates internal/general ongoing-transaction histograms. It logs
queries exceeding the configured threshold no more than once per minute,
transactions no more than once per ten minutes, kills statements exceeding
`MaxExecutionTime`, kills auto-analyze work past its configured maximum, and
applies resource-group runaway kill actions. `LogOnQueryExceedMemQuota` handles
bootstrap (manager absent) and missing-session cases before assembling the
shared SQL log fields. The goroutine exits on the supplied channel and all
logging is warning-level gated.

## Rust ownership and integration decision

Rust preserves the threshold variables in `tidb-vardef`/`tidb-config`, and
`tidb-session` has process/session data and kill signals. It does not yet own
the Go `expensivequery.Handle` polling worker, session-manager enumeration,
ongoing-transaction metrics, log throttling, auto-analyze/runaway kill policy,
or domain bootstrap registration. A detached timer or threshold helper would
be Rust-only behavior without the server consumer, so no source change is
justified.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix and no package-completion claim; `make bazel_prepare` and the Ready
lint gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/expensivequery -count=1
# ok; [no tests to run]
```

## Risks and unverified behavior

- Correctness: the common harness builds and no source tests exist; no Rust
  monitor is claimed.
- Compatibility: process-list fields, threshold reload cadence, histogram
  labels, logging intervals, and kill flags remain Go-only integration
  contracts.
- Performance: no runtime code changed. A future owner must preserve the
  100 ms polling cadence and throttling windows.
- Not verified locally: domain bootstrap wiring, live session/auto-analyze and
  runaway kills, metric contents, race/flaky Bazel execution, and server
  end-to-end behavior.
