# Refusal-variant census

"Refuse loudly rather than approximate" is a standing rule of this port, and every
`Unsupported*` variant is a promise that we decline a shape instead of guessing at
it. A refusal nothing pins can silently turn into an acceptance. This document is
the measurement of which of those promises are actually held to.

Scope: the ten dedicated refusal enums, 78 variants total, as of the census run.

**What this census does NOT cover, stated plainly.** Every number below — the 78,
the 51 pinned, and above all the *16 unreachable* — is a count inside those ten
enums only. It is not the port's unreachable-refusal total, and reading it as one
is the mistake this paragraph exists to prevent. `DriverError`, the driver's own
156-variant error enum, was never surveyed by the original run: not its six
`Unsupported*` variants and not the 150 others, several of which are refusals in
everything but the name (`CannotDropColumnWithCompositeIndex`,
`UnsupportedDropIntegerPrimaryKey`, `UnsafeFunctionInExpressionIndex`, ...). Two
`DriverError` variants with no producer at all were found later, by a reader
passing through `to_mysql_error` rather than by this census, and neither was in
the 16.

`DriverError` has since had a producer sweep of its own — see below — but the
method there is the cheap one (does a production path construct it?), not this
document's full reachable/pinned analysis. Treat the two as separate
measurements.

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

### Nine of the sixteen name a capability the crate has since grown

`UnsupportedCapability`, `ChannelIterUnsupported` and the two unreachable
`ResponseChannelUnsupported` variants decline raw tipb responses, chunk decoding
and TiKV transport. `tidb-distsql` now contains `chunk_decode` (raw tipb
`SelectResponse` and `Chunk`) and `transport`, and `response_channel` decodes
through both before handing rows to `ChannelIter`. The client itself lives in
`tidb-txnkv` and is exercised against a real cluster.

This is the closest thing the census found to a stale refusal, with one
important difference: because the variants are unreachable, no query is being
wrongly refused. What was stale was the prose. Three module docs still described
a workspace with no chunk decoder, no protobuf transport and no TiKV client, and
told a reader that feeding a raw tipb response to these leaves would produce a
loud refusal. It would not — there is no such path. Those claims are corrected
in `select_iter.rs` and `channel_iter.rs`; `transport.rs` carries the same stale
sentence about the rewrite having "no TiKV client, protobuf transport, or region
router" and is left to whoever owns that seam.

## What this changes about pinning

Set guards belong on the enums that are live, where a new variant is a real
narrowing of what we serve and a removed variant is a real widening. For the
unreachable enums the set guard would be theatre. Their real defect is not a
missing test but a missing caller, and the fix is to wire the producer or drop
the enum, not to freeze the current shape in a test.

Three guards followed from that, joining the existing
`bounded_path_refusal_set_is_pinned_against_new_features`:

| Enum | Guard | Home |
| --- | --- | --- |
| `UnsupportedPreparedWrite` | `prepared_write_refusal_set_is_pinned_in_both_directions` | `tests/prepared_dml_source.rs` |
| `UnsupportedJoinCondition` | `join_condition_refusal_set_is_pinned_in_both_directions` | `tests/configured_fullschema_join_source.rs` |
| `UnsupportedReadOnlyPredicate` | `bounded_path_predicate_refusal_set_is_pinned_against_new_shapes` | `tests/read_only_scan_source.rs` |

Each was mutation-probed in both directions: a probe variant added to the enum
fails the guard naming the addition, and deleting an existing variant fails it
naming the removal. A guard that only catches additions would let a refusal be
quietly downgraded to an acceptance, which is the failure this whole exercise
exists to prevent.

`JoinOutputUnsupported` (3 variants) and `ResidualUnsupported` (2) are live and
did not get a set guard. Both are small, both already have per-variant
behavioural pins covering four of their five variants, and neither sits on a
tier boundary where a widening changes what reaches storage. They are the
cheapest remaining pins if that judgement turns out to be wrong.

## `DriverError`: the producer sweep the census above never ran

`DriverError` is the driver's single failure type, and `to_mysql_error` renders
all 156 of its variants with NO wildcard arm — which is the only reason the two
findings below were visible at all. A variant nothing constructs still has a
rendering arm, so it reads exactly like a live one.

Two variants had a rendering arm and no producer anywhere in the workspace, and
they turned out to be opposite cases. Telling them apart is the whole point of
doing this by reading rather than by counting:

- `SequenceHasRunOut` was **dead**: a duplicate. Sequence exhaustion is
  implemented and does reach the client as 4135, through
  `EvalError::Sequence(SequenceEvalError::RunOut)` raised in `StmtContext`. The
  driver variant was a second spelling of an error the eval path already owned.
  Deleted.
- `DependentByFunctionalIndex` was a **missing feature**, and deleting it would
  have hidden a data-integrity bug. An expression index is stored as a hidden
  generated column plus an index over it; `ALTER TABLE ... DROP COLUMN` and
  `RENAME COLUMN` had no check for that dependence, so both SUCCEEDED and left
  the hidden column's expression reading a column that was gone. TiDB refuses
  both with 3837. The check was wired in (`KvTable::expression_index_depends_on`)
  and pinned by two tests in `tests_expression_indexes.rs`.

After those two, a mechanical producer sweep over all 156 variants — stripping
`#[cfg(test)]` regions and test files, and discounting `to_mysql_error`'s own
consuming arms — finds **no variant without a production producer**. The three
that look producerless to a grep (`MemoryExceedForQuery`, `JsonDocumentNullKey`,
`InvalidJsonCharset`) are built by `From<ExecError>` in the same module as their
rendering arms.

Not measured here: whether each `DriverError` refusal is *pinned*, in this
document's sense of a test that would fail if the refusal flipped to an
acceptance. That is the analysis the ten enums above got and `DriverError` has
not.
