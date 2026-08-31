# `pkg/statistics/handle/ddl/testutil` — complete package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 16 | `cc82e4b4088bd98a4e9f786a5719414233b26629` | `32a39a7766a6720bedf7c4f54bd66f0f6ac92fc1a8756c1b5fef66c32a120619` |
| `util.go` | 70 | `de5ee871e48ff017af80264fa3b12484663aeb6a` | `dde4c6984ce9b61d838b1090191f27deea1e9707342e35a59aa18b7b5da32194` |

All 86 lines were read. This public test-support library has no `doc.go`,
package-local test, fixture, generated input/output, platform/build-tag
variant, benchmark, fuzz target, example, or other support artifact.

## Package behavior

- `HandleDDLEventWithTxn` checks out a statistics system session, wraps one
  transaction, marks the context as internal DDL-notifier work, and calls the
  real statistics handle on one `SchemaChangeEvent`.
- `HandleNextDDLEventWithTxn` blocks on the statistics handle's DDL event
  channel and delegates the received event.
- `FindEvent` consumes and discards channel events until one exact
  `model.ActionType` arrives.
- `FindEventWithTimeout` races the same destructive channel consumption
  against a seconds-based ticker and returns nil on the first timer tick.

## Current Rust dependency boundary

Rust now updates statistics and the auto-analyze priority queue directly from
committed DDL changes. `PriorityQueueDdlEvent` covers the queue handler's
production action subset, but it is not Go's general notifier
`SchemaChangeEvent`, is not published through a statistics-handle event
channel, and cannot preserve these helpers' receive/discard/block/timeout
behavior. No ordinary Rust statistics handle exposes the source `SPool`,
`DDLEventCh`, and transactional `HandleDDLEvent` composition.

The package therefore remains **unclaimed**. Restoring a generic slice search
or creating a test-only private channel would be a workaround. Completion
requires the real notifier/event-channel and statistics-handle owners first;
then all four helpers can be supplied as one test-support package.

## Removed non-parity carrier

The prior `find_event_with_timeout` accepted a pre-collected generic slice and
returned the first equal value. It bypassed receive ordering, destructive
consumption, blocking, ticker expiry, event decoding, and three of the four Go
APIs. Its two source-absent Rust tests and the unused module remain removed.

## Validation

This 2026-08-30 re-audit changed only the receipt after reading both pinned
artifacts and the current Rust DDL/statistics event paths. No executable code,
Go, Bazel, or module file changed; `make bazel_prepare` is not required. The
next integrated batch owns formatting, diff hygiene, and its applicable
verification profile.
