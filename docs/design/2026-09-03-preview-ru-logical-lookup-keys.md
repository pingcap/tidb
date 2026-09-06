# Experimental logical lookup inputs and split preview weights

This extends the coefficient-free telemetry in #69977 with raw lookup inputs
and two independently priced stages in the experimental preview. Production
weights remain uncalibrated; this does not change customer RU billing or adopt
the provisional weights from an offline fit.

## Unit contract

`logical_lookup_keys` counts inputs to a lookup stage, not returned rows,
RPCs, regions, retries, or cache misses. Existing scan/network units remain
unchanged. Each plan node owns only its own stage.

| Plan node | Count | Example |
| --- | --- | --- |
| PointGet on a clustered primary key | One valid table key, hit or miss | `WHERE id=999`: 1 even if absent |
| PointGet on a unique secondary index | One index key; one additional table handle if the index lookup succeeds | Hit: 2; index miss: 1 |
| BatchPointGet | Valid keys after SQL duplicate elimination, at each index/table stage | PK `IN (1,1,2,999)`: 3; secondary unique keys with two hits and one miss: 3 index keys + 2 handles |
| IndexLookup | Handles submitted to the table-fetch stage | Three index matches requiring table fetch: 3, not 6 |
| IndexMerge | Final table-fetch handles after union/intersection deduplication | Branch handles `{1,2,3}` and `{2,4}`: union 4, intersection 1 |
| IndexJoin / IndexHashJoin / IndexMergeJoin | Eligible inner lookup key tuples generated from outer rows, before executor batch deduplication | Outer keys `101,101,205,999,NULL` with ordinary equality: 4; a composite tuple counts as one |

Invalid conversions and NULL keys rejected by the executor do not count.
NULL-safe equality may generate a valid NULL lookup tuple. A miss counts because
the lookup input was consumed. The index-join unit describes the outer binding
used to construct inner access, not the number of matching inner rows. It does
not count local hash-table probes in ordinary HashJoin or HashAgg.

An index join's inner non-covering IndexLookup can also perform a separate
table-fetch stage. Its handle count belongs to that IndexLookup node, not to
the join node. Do not infer that their two counts must be equal.

## Split preview formula

```text
preview_RU = existing_preview_RU
           + logical_index_probe_keys * IndexProbeWeight
           + logical_table_fetch_keys * TableFetchWeight
```

| New unit | Owner | Exact meaning | Example |
| --- | --- | --- | --- |
| `logical_index_probe_keys` | IndexJoin / IndexHashJoin / retained IndexMergeJoin | Eligible inner lookup tuples generated from outer rows, before execution-batch deduplication | Outer customer IDs `101,101,205,999,NULL`: 4 for ordinary equality, even if 999 is absent |
| `logical_table_fetch_keys` | IndexLookup / IndexMerge | Handles submitted to the table-fetch stage; IndexMerge uses handles after union/intersection | A non-covering index lookup that finds three handles: 3 |

These are typed aliases of the corresponding node's `logical_lookup_keys`;
they are not additional lookup attempts. The raw compatibility unit is **never
priced**. Price the typed units, or replay the old shared raw unit, never both.

Explicit PointGet/BatchPointGet retain only the raw compatibility unit and their
existing formula. Ordinary HashJoin and HashAgg emit neither new unit: their
in-memory hash work is not an index lookup. A non-covering index join can emit
both new units under different plan nodes. For example, three outer customer
IDs each matching ten orders produce 3 join probes and 30 table fetches.

`IndexProbeWeight` and `TableFetchWeight` are independent fields in the existing
private `readBillingDemoWeights` provider. They accept finite nonnegative
values. As with the other preview weights, there is no new SQL setting for
configuring them. Local formula tests inject calibrated values; the default
provider remains uncalibrated and does not report a numeric total. The staging
experiment fits and replays these coefficients offline from raw JSON.

## Collection

Enable `tidb_enable_read_billing_demo`. For per-statement JSON also enable the
existing general-log switch; leave general logging off during high-throughput
CPU validation.

The unit is available in:

- `tidb_read_billing_demo_base_units_total`, with `unit="logical_lookup_keys"`,
  `unit="logical_index_probe_keys"`, or `unit="logical_table_fetch_keys"`,
  and `input_source="executor_lookup_inputs"`. Sum/difference the counters using
  the same 30-second collection process as other raw units.
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_READ_BILLING_DEMO_BASE_UNITS`, including
  `VALUE` and `SAMPLE_COUNT`.
- The `units` array in `GENERAL_LOG_RU_UNITS`; its additive `statuses` array
  preserves the existing statement/operator coverage statuses.
- `EXPLAIN ANALYZE FORMAT='RU'`: the compatibility count stays diagnostic-only;
  the typed counts use their respective weights when the private provider is
  calibrated and the parent collector can render the query.

```sql
SET tidb_enable_read_billing_demo = ON;
SELECT pad FROM lookup_keys WHERE id IN (1, 1, 2, 999);

SELECT operator_kind, unit, value, sample_count
FROM information_schema.statements_summary_read_billing_demo_base_units
WHERE unit IN ('logical_lookup_keys', 'logical_index_probe_keys', 'logical_table_fetch_keys');
```

```json
{
  "site": "tikv",
  "op_class": "kv_point_lookup",
  "operator_kind": "batch_point_get",
  "dml_kind": "",
  "unit": "logical_lookup_keys",
  "input_source": "executor_lookup_inputs",
  "input_side": "all",
  "value": 3
}
```

The internal model version and default uncalibrated weight version remain
unchanged; archive the binary Git SHA with evidence to identify this extension.
Observed typed aliases do not add successful operator-status/event counts.
An active stage without its counter reports `missing_logical_lookup_keys` and
prevents a complete preview total. Stages proven unexecuted by the existing
execution mask are exempt. Lookup observations are independent of other primitive-coverage gates:
an incomplete physical scan detail must not erase an observed logical count.
Conversely, the presence of this unit does not certify the whole statement.
Retain and inspect coverage statuses, including execution errors.

## Experimental boundaries

- Zero means a supported stage executed with no lookup inputs. An absent unit
  means no observation, not zero. Preserve explicit zeros in exported JSON.
- Pushed-down/LocalIndexLookup is not instrumented: TiDB sees only residual
  handles and completed rows, not every lookup performed inside TiKV. Use the
  normal TiDB-side IndexLookup path for these experiments (`hint-only` policy,
  without a lookup-pushdown hint). An active LocalIndexLookup reports missing
  lookup coverage, never a complete count inferred from residual handles.
- Counts describe attempted execution. Fully consume results when comparing
  runs. Cancellation or LIMIT-driven prefetch can change how much work actually
  starts. Non-covering inner lookups can also deduplicate table handles per
  batch; do not treat that stage as batch-invariant without a controlled check.
- No values are reconstructed from `total_keys`, result cardinality, or RPC
  count. No TiKV, client-go, or protocol change is required.
- The parent ignores the deprecated `INL_MERGE_JOIN` hint. The retained
  IndexMergeJoin executor is instrumented and unit-tested directly; this change
  does not re-enable its optimizer selection.

## Local verification

Focused tests cover hit/miss and duplicate semantics, composite primary keys,
IndexLookup, IndexMerge union/intersection, IndexJoin and IndexHashJoin with
different batch/concurrency settings, the legacy IndexMergeJoin key builder,
prepared reuse, zero/missing evidence, and normal SQL metrics/General Log/summary
export. Collector tests verify independent prices, typed contributions counted
exactly once, unpriced compatibility aliases, uncalibrated defaults, and unchanged
operator-status/event counts. Nested index joins check that both stages are
reported separately. Ordinary HashJoin/HashAgg are negative controls.

The next [staging experiment](2026-09-07-preview-ru-split-lookup-test-plan.md)
compares the original shared term and the split term on the same evidence.
