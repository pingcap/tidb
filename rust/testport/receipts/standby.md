# `pkg/standby` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains exactly five tracked artifacts and 1,423 lines. Every
production file, classic test, `nextgen` build-tag test, and Bazel target was
read line by line before comparing Rust owners. There is no `doc.go`, fixture,
generated source, benchmark, or other platform-specific Go file. The
`standby_nextgen_test.go` build-tag source is a platform/build variant and is
included in this receipt even though the current Bazel target lists only the
classic test file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 39 | `8fbb9be48c7b82efaa20ef856a751e9e2b4c6a27` | `52833a9f3ba2d5fd66f98369c7ec89fa84803b09f83ad7f99a1c52f460ae24c1` | standby library and sharded classic test target |
| `idle_watcher.go` | 95 | `f9a80b1d64b0920a5676d647aa0997fadbeb4b9e` | `744dcae4357fe4d7d31fc418245f484672b7be6c9d0bab8e3590608ebde0e9eb` | idle-connection detection and signal dispatch |
| `standby.go` | 670 | `fc7dd5ce91f16c2e5c7545a9dc6fbd98653d3d25` | `16aee9764c78dd00026fd86d850b061ae0388d22c8d0ccd101582387e286e9cb` | activation HTTP API, lifecycle controller, shutdown and manager notification |
| `standby_nextgen_test.go` | 418 | `22ea08d8ab6d7877c18628edc3c0a316b01b5c7a` | `b1f732debb708b063dc9e48f9ccd8d93994277798965feffcd731932f97726f0` | `nextgen` shutdown, status, query parsing, and retry tests |
| `standby_test.go` | 201 | `8a610500167a22bfe8ed1166929f5bec792f39a6` | `ed6d3a1f60b54b1ae7a1ba6755f81e80bdf6f47b0589eadf113ee349eb23b51a` | classic activation, metadata, and deploy-mode tests |

### Production symbols

`idle_watcher.go`: `LoadKeyspaceController.OnConnActive` monotonically updates
the Unix-second activity timestamp; `OnServerCreated` starts the 10-second
idle watcher, counts connections/processes/transactions/interactive clients,
and selects Starter zero-backend `SIGTERM` versus ordinary `SIGINT` shutdown.

`standby.go`: constants and state (`standbyState`, `activatedState`,
`terminatingState`, HTTP paths, wait/retry limits); `ActivateRequest`,
`LoadKeyspaceController`, `KeyspaceMismatch`, `statusResponse`, `exitOptions`,
and `invalidExitOptionError`; `NewLoadKeyspaceController`; middleware
`keyspaceValidateMiddleware`; restart-log helpers
`loadTiDBNormalRestartInfoAndRemove`, `loadTiDBNormalRestartLog`,
`SaveTidbNormalRestartInfo`, and `IsPreTidbNormalRestart`; controller metadata
copy `ActivationMetadata`; HTTP registration and activation/exit/checkconn
behavior in `Handler`; `parseExitOptions`, `parseExitBool`, and
`parseExitWait`; `statusHandler`; close-wait accessors
`setCloseConnWait`/`getCloseConnWait`; standby listener/activation flow
`WaitForActivate`, readiness handoff `PrepareForActivation`, one-shot
completion `EndStandby`, Starter shutdown sequencing `OnServerShutdown`,
connection drain `waitZeroConn`, and manager retry reporting
`reportManagerFree`. The `invalidExitOptionError.Error` method and all package
state/constant declarations were included in the read.

### Tests, test by test

`standby_test.go` defines the manager/ready-server mocks and
`resetStandbyTestState`, then covers:

* `TestActivateRequestMetadata`: JSON metadata decoding and defensive copy;
* `TestActivateRequiresKeyspaceName`: empty activation request is HTTP 400;
* `TestActivateWaitsUntilServerReady`: activation blocks until listener
  readiness and returns activated status;
* `TestOnServerShutdownNoopOutsideStarter`: non-Starter shutdown does not call
  the manager;
* `TestStatusDoesNotReturnExportIDOutsideStarter`: ExportID is omitted outside
  Starter mode.

`standby_nextgen_test.go` is compiled only with `-tags=nextgen`. Its
`blockingShutdownServer` helper implements the shutdown interface, and the
seven tests cover Starter free reporting after connection drain, timeout
suppression, manager retry success/exhaustion, Starter-only status ExportID,
invalid exit query values and per-controller wait state, graceful TERM exit
defaults, manager-free notifier requirements, and all accepted/rejected wait
formats in `TestParseExitWait`.

## Rust ownership and decision

Rust currently owns only configuration and signal fragments related to this
package: `tidb-config::config_tree::sections::Standby` models the four standby
settings, `config_tree::load` handles one default, and
`tidb-server::main_flags` maps standby CLI values. `tidb-server` also records
the generic SIGINT/SIGTERM exit-code contract in `shutdown_signal.rs` and
`signal_exit.rs`. No Rust crate provides the Go dependency-closed
`LoadKeyspaceController`, `/tidb-pool/{status,activate,exit,checkconn}`
handlers, keyspace middleware, standby listener/activation handoff, idle
watcher, restart-log protocol, zero-connection drain, AutoID shutdown, or
manager-free retry path. The existing Rust fragments therefore cannot claim
package parity; combining them into an uncalled controller would introduce a
Rust-only behavior path.

This package is recorded as an explicit boundary with no speculative source
change and no new regression test. A future implementation must port the
entire lifecycle and both classic/`nextgen` test surfaces together.

## Validation and risk

Profile: **WIP** for this docs-only audit; the rolling repository loop remains
in progress. No Go or Bazel source changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/standby -count=1
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=nextgen ./pkg/standby -count=1
# passed
```

- Correctness: no SQL, activation, shutdown, or routing behavior changed;
  the complete Go contract remains the authoritative implementation.
- Compatibility: any future Rust owner must preserve Starter/non-Starter
  branching, signal choice, wait bounds, response status/body shapes,
  restart-log format, and manager retry timing.
- Performance: unchanged.
- Not verified locally: live HTTP/TLS listener lifecycle, real TiDB manager,
  AutoID ownership, connection migration, Bazel analysis, and workspace-wide
  Ready validation.
