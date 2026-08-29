# `pkg/statistics/handle/ddl/testutil` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 16 | `cc82e4b4088bd98a4e9f786a5719414233b26629` | build metadata inventoried |
| `util.go` | 70 | `de5ee871e48ff017af80264fa3b12484663aeb6a` | unclaimed: handle/notifier boundary absent |

The package has no generated, platform-specific, test, benchmark, fixture, or
other support artifacts.

## Package behavior and blockers

The package exports four test-support helpers. `HandleDDLEventWithTxn` borrows
a statistics system session, installs the internal DDL-notifier request
source, wraps a pessimistic transaction, and calls the real handle.
`HandleNextDDLEventWithTxn` blocks for the next handle-channel event before
delegating. `FindEvent` consumes and discards events until the requested action
type arrives. `FindEventWithTimeout` races the same channel consumption with a
seconds-based ticker and returns nil on timeout.

Rust does not have the ordinary statistics-handle owner or its notifier event
channel/context integration. The package remains explicitly unclaimed until
those shared runtime boundaries exist.

## Removed non-parity carrier

`find_event_with_timeout` accepted a pre-collected generic slice and returned
the first equal value. That API bypassed channel receive ordering, blocking,
timer behavior, notifier event decoding, and nil-on-timeout, and represented
only one of four package functions. The pinned package has no tests; Rust had
two source-absent tests for the carrier. The module and both tests were
removed.

## Validation

WIP profile: removal of an unused carrier is checked through the affected
statistics owner gate.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs`
- `git diff --check`
