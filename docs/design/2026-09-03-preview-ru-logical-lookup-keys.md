# Experimental logical lookup inputs

This extends the coefficient-free telemetry in #69977. It does not add a
coefficient, change the active RU formula, or charge additional RU. The purpose
is to collect data before deciding whether a per-lookup term is useful.

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

## Collection

Enable `tidb_enable_read_billing_demo`. For per-statement JSON also enable the
existing general-log switch; leave general logging off during high-throughput
CPU validation.

The unit is available in:

- `tidb_read_billing_demo_base_units_total`, with `unit="logical_lookup_keys"`
  and `input_source="executor_lookup_inputs"`. Sum/difference the counters using
  the same 30-second collection process as other raw units.
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_READ_BILLING_DEMO_BASE_UNITS`, including
  `VALUE` and `SAMPLE_COUNT`.
- The `units` array in `GENERAL_LOG_RU_UNITS`; its additive `statuses` array
  preserves the existing statement/operator coverage statuses.
- `EXPLAIN ANALYZE FORMAT='RU'` as a diagnostic-only count, with no weight or
  preview-RU contribution, when the parent collector can render the query.

```sql
SET tidb_enable_read_billing_demo = ON;
SELECT pad FROM lookup_keys WHERE id IN (1, 1, 2, 999);

SELECT operator_kind, unit, value, sample_count
FROM information_schema.statements_summary_read_billing_demo_base_units
WHERE unit = 'logical_lookup_keys';
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

Existing model/weight versions and existing operator-status counts do not
change. Lookup observations are independent of other primitive-coverage gates:
an incomplete physical scan detail must not erase an observed logical count.
Conversely, the presence of this unit does not certify the whole statement.
Retain and inspect coverage statuses, including execution errors.

## Experimental boundaries

- Zero means a supported stage executed with no lookup inputs. An absent unit
  means no observation, not zero. Preserve explicit zeros in exported JSON.
- Pushed-down/LocalIndexLookup is not instrumented: TiDB sees only residual
  handles and completed rows, not every lookup performed inside TiKV. Use the
  normal TiDB-side IndexLookup path for these experiments (`hint-only` policy,
  without a lookup-pushdown hint).
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
export. Collector tests ensure diagnostic units do not change existing scores or
successful operator-event counts.
