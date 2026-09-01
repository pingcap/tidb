# `pkg/util/signal` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
The package is a small cross-platform process-signal adapter; its complete
inventory is recorded here before any Rust source change.

## Complete inventory

All five Go-master artifacts were read in full. There are no `doc.go`, Go test
files, fixtures, generated outputs, benchmarks, fuzz targets, or nested
packages.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 91 | `7c5d1e16b1503f901bc3eb6ead59803a9604bf37` | `c11ccef5f384348326091df831de120aca3e519f420823918a3bc5655c703dfc` | cross-platform library target and per-OS dependency matrix inventoried |
| `exit.go` | 32 | `534eec32c87dcc81cacabc2d74ad1435fae650ce` | `eaf2d9010655a56d13c8c1d0b91b302dc476a1ca2fec84e842797062901577ad` | non-Windows `TiDBExit` sends the requested signal to the current PID and logs failures |
| `signal_posix.go` | 87 | `721644d75d66b15047a3365859d0de417adab39d` | `408cea052aafe37e03eeda9b384d8994dd45ffa569fd63ebbd76cd9928829e5d` | POSIX goroutine-stack dump plus one-shot SIGHUP/SIGINT/SIGTERM/SIGQUIT shutdown handler |
| `signal_wasm.go` | 24 | `f1f4d455b104886e86df1883f72f3b8ca955fd86` | `2f1c8d556fbf732636e916866e8aa8919e8b27189d44f30c37164715dcd08279` | WASM no-op handlers; shutdown callback is never invoked |
| `signal_windows.go` | 55 | `0b0b9b82587c6a3b8c63c51cabd69e177ba14f51` | `d3189e859dc7b3e61c848d0cf57c8f427c47bc95c6b3edf896fbcd0ab4b85ca3` | Windows signal notification and best-effort current-process signaling |

The production declaration inventory is five functions: `TiDBExit`,
`getGoroutineStacks`, `SetupUSR1Handler`, and `SetupSignalHandler` (with the
platform-specific `TiDBExit` and handler definitions counted once per source
variant). The POSIX stack collector grows from 1 MiB to 64 MiB and then emits
the runtime trace; `SetupSignalHandler` consumes exactly the first notified
termination signal and delegates it to the caller. The Windows and WASM
variants intentionally differ for platform capability.

## Rust ownership and integration decision

`tidb-server::shutdown_signal` registers the four termination signals and
records the first signal for the Rust node's shutdown/exit-code path;
`tidb-server::signal_exit` covers the separate SIGINT exit-code contract.
Other Rust server nodes still have `ctrlc`-based handlers. These are server
startup owners, not a dependency-closed port of this utility package: Rust has
no equivalent of the POSIX `SIGUSR1` goroutine dump, Windows best-effort
`TiDBExit`, or WASM no-op build matrix, and no shared cross-platform signal
adapter consumed by all Go callers. Introducing another Rust signal thread or
stack-dump endpoint here would create Rust-only behavior and duplicate server
ownership. No source edit is justified in this package; the missing behavior
remains an explicit boundary for a future atomic server/platform migration.

## Validation

Profile: **WIP**. This is a complete inventory and boundary audit with no code
change, so `make bazel_prepare` and the Ready lint gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/signal -count=1
# ? github.com/pingcap/tidb/pkg/util/signal [no test files]
```

## Risks and unverified behavior

- Correctness: default-host POSIX compilation passed; no Rust replacement is
  claimed for the stack-dump or platform-specific handlers.
- Compatibility: signal ordering, one-shot shutdown, process signaling, and
  build tags are externally observable; any future native owner must preserve
  all four variants together.
- Performance: the Go POSIX handler allocates up to 64 MiB only while dumping
  stacks; no runtime code changed here.
- Not verified locally: Bazel execution, Windows/WASM runtime signal delivery,
  and Rust server end-to-end shutdown under each operating-system variant.
