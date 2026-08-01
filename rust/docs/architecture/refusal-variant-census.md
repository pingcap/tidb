# Refusal-variant census

"Refuse loudly rather than approximate" is a standing rule of this port, and every
`Unsupported*` variant is a promise that we decline a shape instead of guessing at
it. A refusal nothing pins can silently turn into an acceptance. This document is
the measurement of which of those promises are actually held to.

Scope: the ten dedicated refusal enums, 78 variants total, as of the census run.
`DriverError`'s six `Unsupported*` variants are a different shape (they share an
error enum with non-refusal variants) and are out of scope here.

## Method

A variant is only counted as *reachable* if some non-test code path can construct
it from real input. The census walks each `Enum::Variant` occurrence, splits
`crates/*/tests/**` and `#[cfg(test)]` regions from production regions, and then
reads each production site to decide whether it is a construction driven by input
or a `match` arm / `Display` arm that merely consumes a value someone else built.

That second step is what the raw grep cannot do, and it is where the census found
its results: several variants have production *consumers* and no production
*producer* at all.

A variant is only counted as *pinned* if a test would fail were the refusal to
flip to an acceptance. A test that constructs the refusal itself and asserts the
constructor returned it is not a pin — it is a tautology that survives deleting
the entire refusal path.

## Census

| Enum | Variants | Pinned | Live, unpinned | Unreachable |
| --- | --- | --- | --- | --- |
| `UnsupportedReadOnlyFeature` | 21 | 21 (set guard) | 0 | 0 |
| `UnsupportedPreparedWrite` | 20 | 17 | 3 | 0 |
| `UnsupportedJoinCondition` | 9 | 3 | 6 | 0 |
| `UnsupportedScanFeature` | 8 | 1 | 0 | 7 |
| `UnsupportedReadOnlyPredicate` | 5 | 5 | 0 | 0 |
| `UnsupportedCapability` | 4 | 0 | 0 | 4 |
| `ResponseChannelUnsupported` | 3 | 0 | 1 | 2 |
| `JoinOutputUnsupported` | 3 | 3 | 0 | 0 |
| `ChannelIterUnsupported` | 3 | 0 | 0 | 3 |
| `ResidualUnsupported` | 2 | 1 | 1 | 0 |
| **total** | **78** | **51** | **11** | **16** |

No variant fell into a fourth category the census looked for and did not find: a
*stale* refusal, one declining a shape we have since learned to serve. The two
plausible candidates are both deliberate rather than stale.
`UnsupportedReadOnlyFeature::Partition` refuses partitioned tables on the bounded
read-only path even though RANGE partitioning landed, but that path is closed to
new read features by design and its own guard says so. `UnsupportedScanFeature::
Partition` is unreachable rather than stale.

`UnsupportedReadOnlyFeature`'s 21 are pinned as a *set* by
`bounded_path_refusal_set_is_pinned_against_new_features`; 11 of the 21 also have
a per-variant behavioural pin. The set property is the one that matters here,
since it is what forces an author adding or removing a refusal to justify it.

## The unreachable sixteen

These variants cannot be produced by any production path today. They are not
refusals in force; they are declarations of a boundary that no caller stands at.

`UnsupportedScanFeature` — `TiKvTableScanSpec::unsupported` and
`TiKvIndexScanSpec::unsupported` are set by exactly one function,
`with_unsupported`, and no production caller invokes it: every production
constructor writes `unsupported: None`. The two consumers
(`dag_request::table_scan_to_pb`/`index_scan_to_pb` and
`logical_data_source_task::build_supported_table_task`) faithfully reject a
`Some(..)` they can never see. `MultiValuedIndex` is the one live member of the
enum, and it is live for an unrelated reason: `table_scan_to_pb` raises it
directly when a scan column carries `array: true`. The `Partition` case in
`build_supported_table_task` is doubly dead — `path.is_partitioned()` already
rejects the same shape one condition earlier.

`UnsupportedCapability` (all 4) — `unsupported_next_raw`, `unsupported_chunk`,
`unsupported_tikv_transport`, `unsupported_sorted_heap` are `pub const fn`
factories re-exported from `tidb-distsql`'s root with zero callers outside
`select_iter_source.rs`, which asserts each factory returns its own variant.

`ChannelIterUnsupported` (all 3) and two of three `ResponseChannelUnsupported`
have the same shape: `const fn` factories, no production caller. Only
`ResponseChannelUnsupported::TransportOwnedResponseMutation` is raised from a real
`match` arm on a live value.

The honest reading is that these sixteen are seed evidence for boundaries that
were mapped ahead of the code that would stand at them. Pinning them would pin
nothing — the guard would assert that a function nobody calls still returns what
it always returned.

## What this changes about pinning

Set guards belong on the enums that are live, where a new variant is a real
narrowing of what we serve and a removed variant is a real widening. For the
unreachable enums the set guard would be theatre. Their real defect is not a
missing test but a missing caller, and the fix is to wire the producer or drop
the enum, not to freeze the current shape in a test.
