# Split lookup terms: staging experiment plan

## Decision to make

Does separately pricing index-join probes and table-fetch handles improve
customer-workload cost alignment enough to justify the extra coefficient?
The previous offline fit is a hypothesis, not an accepted price. Existing
frontend, CPU, scan, network, hash, join, write, and fixed-event terms remain.
Use the published offline model, retaining `write_statement` and the shared
operator-event term (excluding `sql_frontend`). Predictable write encoding
remains `rows * (indexes + 1)`, not encoded-mutation counters. The parent's
internal preview score is not a substitute for that published replay model.

The [unit contract](2026-09-03-preview-ru-logical-lookup-keys.md) defines both
terms and their limitations. Build the updated PR #70858 head and record the
running Git SHA and image digest before collecting evidence. Do not merge old
and new image windows into a single run.

## Execution

| Step | Run/check | Evidence and decision |
| --- | --- | --- |
| Verify once | Run ordinary SQL with General Log briefly enabled: IndexHashJoin, non-covering IndexLookup, their nested combination, empty lookup, explicit BatchPointGet, ordinary HashJoin | Confirm exact typed counts in log, metrics, and summary; preserve zero versus missing; raw alias equals typed count for each applicable operator. Stop affected tests if coverage fails. Turn General Log off afterward. |
| Prepare small fixtures | 100K customers and orders, deterministic keys; separate 32 B and 1 KiB payloads; analyze statistics. Add a ten-orders-per-customer fixture for fanout. | Inspect plans and actual data size. Keep total fixture storage below 1 GiB; do not import large TPC-E data. Disable IndexLookup pushdown for this experiment. |
| Probe-only contrasts | IndexHashJoin into clustered customer PK: 32 and 1,024 eligible outer keys at 32 B payload; repeat 1,024 with duplicate keys (32 distinct); run one matching IndexJoin control | Isolate join bindings from table-fetch handles; verify duplicate/batch semantics and existing scan/network contributions. |
| Fetch-only contrasts | Non-covering IndexLookup: 32 and 1,024 handles, each at 32 B and 1 KiB payload | Separate the new fetch count from existing bytes and fixed-event cost. No join-probe unit should appear. |
| Mixed stages | Non-covering IndexHashJoin, 1,024 outer bindings: one matching inner row versus ten per key | Both counts are present; probe count stays fixed while table-fetch count and returned bytes increase. Do not assume both stages have equal counts. |
| Freeze candidate, then holdouts | 8,192-key IndexHashJoin; 8,192-handle IndexLookup; 1,024-handle IndexMerge; ordinary HashJoin over the same tables | Test scale/operator transfer without refitting. HashJoin must receive no direct contribution from the new terms. |
| Customer validation | Inspect runnable TiWorkload candidates first. Select at least one real workload with material IndexLookup/IndexMerge keys and one with material IndexJoin-family probes, if available. | Record actual workload name, SQL mix, plans, latency and feature shares. Prior SaaS/Yum runs have too few relevant keys to demonstrate benefit. Pocket is usable only for covered shapes; its tuple-range queries must not be relabeled as BatchPointGet. If no join-rich customer workload is available, report that transfer as unverified. |
| Review and cleanup | Review each run before proceeding; stop runners, restore temporary settings and topology | Keep valid partial progress. Do not repeat an entire campaign because one shape fails. Publish one JSON per accepted run with a concise result table. |

Example fixture schema and targeted SQL (payload widths are separate fixture
variants; all results are fully consumed):

```sql
CREATE TABLE customers (id BIGINT PRIMARY KEY CLUSTERED, pad VARBINARY(1024));
CREATE TABLE orders (
    id BIGINT PRIMARY KEY CLUSTERED,
    customer_id BIGINT,
    pad VARBINARY(1024),
    KEY idx_customer(customer_id)
);

-- Probe-only: orders is outer; customers is the clustered-PK inner table.
SELECT /*+ INL_HASH_JOIN(c) */ o.id, c.pad
FROM orders o JOIN customers c ON o.customer_id = c.id
WHERE o.id BETWEEN 1 AND 1024;

-- Fetch-only: scan idx_customer, then fetch pad using its table handles.
SELECT pad FROM orders FORCE INDEX(idx_customer)
WHERE customer_id BETWEEN 1 AND 1024;

-- Mixed: customers is outer; orders uses non-covering idx_customer.
SELECT /*+ INL_HASH_JOIN(o) */ c.id, o.pad
FROM customers c JOIN orders o FORCE INDEX(idx_customer)
ON c.id = o.customer_id WHERE c.id BETWEEN 1 AND 1024;
```

The one-to-one fixture uses `customer_id=id`. The duplicate probe variant cycles
the first 1,024 outer rows across 32 customer IDs. The ten-to-one fixture has
100K orders for 10K customers. Confirm the requested physical operators using
EXPLAIN; do not silently accept a substituted plan.

## Run and collection rules

- Use an in-cluster runner and fixed replicas. Keep TiKV workers enabled and
  count their CPU. Keep TiFlash out of this campaign. Record actual component
  specs, HPA settings and restarts; do not expand the cluster automatically.
- Use short calibration to select a concurrency with useful foreground TiDB
  load (roughly 25-60%) and acceptable SQL latency. Check regular/worker TiKV and
  runner saturation too; do not force every case into an identical CPU band.
- Check idle CPU before and after workload activity. Allow warmup to settle,
  then collect **600 usable seconds** per accepted comparison, in 30-second
  intervals. If idle CPU or latency is abnormal, diagnose before accumulating
  more runs. Workload CPUs use the same adjacent-baseline method for every model.
- Collect timestamped CPU core-seconds by component; raw RUv1/RUv2; all existing
  primitives; both typed lookup units and the compatibility counter by operator;
  statuses; executed SQL count; query latency with units and aggregation. Keep
  statement-summary collection enabled at the supported interval and export
  closed buckets immediately when needed for predictable DML encoding.
- Store window boundaries, plans, settings, SHA/digest, fixture size, workload
  mix and concurrency in each self-contained JSON. Use the existing 30-second
  exporter, retaining nonzero units plus explicit observed zero for target
  stages. Verify it does not use an outdated unit whitelist. Missing is never
  converted to zero. General Log stays off during CPU measurement.

## Fair model comparison

Replay every candidate on the **same JSON and CPU denominator**. This is not
four live runs with different weights.

| Candidate | Additional RU | Why compare it |
| --- | --- | --- |
| Published baseline | None | Current accepted model |
| Original shared proposal | `logical_lookup_keys * shared_weight` across the original supported operators | Preserve the actual proposal we tested, including PointGet/BatchPointGet |
| Scope-matched shared control | `(logical_index_probe_keys + logical_table_fetch_keys) * shared_weight` | Distinguish the benefit of two prices from merely excluding explicit point gets |
| Split candidate | `logical_index_probe_keys * probe_weight + logical_table_fetch_keys * fetch_weight` | Test whether different stage prices transfer |

First fit only the added coefficient(s), keeping published weights frozen.
Then allow the existing shared operator-event coefficient to move at most 10%
to test overlap with fixed cost; keep the other published weights frozen. Use
the existing customer-prioritized constrained random search and multiple seeds,
not a different fitting objective for each candidate. Preserve the baseline as
a feasible candidate. Do not force a positive fetch coefficient if evidence
supports no incremental term; report the boundary instead of inventing a price.

For every run show RU/core-s, APE to the provisional 2,000 RU/core-s reference,
new-term contribution, and change versus baseline. RUv1/RUv2 remain raw and use
their own established reference for any error comparison, never the RUv3 scale.
Show customer-family error separately from supplemental error, coefficient
variation across searches, and frozen-candidate holdouts. A customer regression
must be visible, not offset by a better synthetic average.

Recommend the extra coefficient only for a meaningful, consistent gain on
relevant customer evidence and holdouts without material customer regression.
If its value or benefit remains unstable, retain it as an experiment and name
the specific missing contrast. Do not claim a universal per-key CPU price from
these measurements.
