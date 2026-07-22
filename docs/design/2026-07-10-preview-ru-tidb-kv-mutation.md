# Preview RU TiDB KV mutation operator

> **Current-model notice (2026-07-22):** the mutation lifecycle evidence in
> this document remains valid, but its v3 two-weight and commit-detail formulas
> are historical. The implemented v4 formula and statement-local DML/COMMIT
> request ownership are normative in
> `docs/design/2026-07-22-preview-ru-resource-formula-plan.md`.
> In that v4 contract, normalized mutation work is emitted as the shared
> `cpu_work` unit and identified by `site=tidb`, `op_class=kv_mutation`,
> `operator_kind=memdb_mutation`, `input_source=stmt_memdb_mutation_calls`, and
> `input_side=all`; it is not a separate weight-bearing unit.

## Status and scope

This document defines the write-side model implemented by the preview RU demo.
It does not change production RUv2 charging, reporting, or configuration.

The model intentionally covers only the narrow TiDB work that constructs and
encodes record/index KVs and prepares foreground transaction MemDB mutations.
Scan, reader, filter, projection, join, sort, row preparation, comparisons,
constraints, foreign keys, and auto-ID work remain separate plan or future
synthetic operators. Missing coverage is reported as partial; it is not folded
into a rows, cells, or DML-specific mutation formula.

## Operators and formulas

TiDB statement-local mutation work is represented by one shared operator:

```text
site=tidb
op_class=kv_mutation
operator_kind=memdb_mutation
scope=statement_attempted
source=stmt_memdb_mutation_calls

RU_tidb_mutation = encoded_mutation_count * tidb_mutation_count_weight
                 + encoded_mutation_bytes * tidb_mutation_byte_weight
```

All INSERT, UPDATE, DELETE, INSERT IGNORE, and UPSERT paths use this same weight
key. `dml_kind` is a diagnostic statement-summary dimension and does not
participate in `(site, op_class, weight_version)` lookup. The corresponding
`DML_KIND` information-schema column is appended after the existing base-unit
columns so older column ordinals remain stable.

The mutation weights are independent calibration slots. They are currently
zero and output is explicitly marked `uncalibrated`; they do not alias RUv2 L5,
RUv2 write-key, scan-byte, or TiKV coefficients. Shadow units `set_count`,
`delete_count`, `key_bytes`, and `value_bytes` have zero weight.

TiKV transaction payload remains separate:

```text
site=tikv
op_class=kv_write
operator_kind=txn_prewrite
scope=txn_prewrite_payload
source=commit_detail

RU_tikv_write = write_keys * tikv_write_key_weight
              + write_byte * tikv_write_byte_weight
```

`write_byte` is retained for preview-output compatibility and is annotated with
the semantic name `write_bytes`. Region and write-RPC counts remain zero-weight
diagnostics. TiKV coefficients are not used by the TiDB mutation operator.

## Exact mutation semantics

- Each attempted `Set`, `SetWithFlags`, `Delete`, or `DeleteWithFlags` counts
  once after key/value encoding and before the underlying MemDB call.
- Set bytes are `len(key)+len(value)`; delete bytes are `len(key)`.
- A failed MemDB call still counts because encoding and call preparation have
  already occurred.
- Same-key overwrites, pessimistic statement retries, and mutations later
  removed by staging cleanup or rollback remain counted.
- `UpdateFlags`, staging release/cleanup, lock-only operations, commit-time net
  mutations, and local-temporary-table apply copies are not counted as new
  foreground encoded mutations.
- The recorder lives on `StatementContext`. It is allocated only when the demo
  flag is on or `EXPLAIN ANALYZE FORMAT='RU'` explicitly requests preview data.
- Both restricted-SQL entry points execute through the internal-statement path,
  including their current-session form, so internal/meta DML cannot become a
  foreground mutation sample merely because the preview flag is enabled.

The transaction wrapper dynamically resolves the current statement context.
This preserves per-statement attribution during optimistic history replay.
Because a statement summary already emitted before a later optimistic COMMIT
cannot be rewritten, retryable explicit transactions are marked partial for
optimistic replay attribution.

## Availability and lifecycle

- Autocommit DML normally exposes TiDB mutation and TiKV prewrite operators.
- Explicit-transaction DML always retains statement-local TiDB units and its
  read tree. Missing commit detail creates a partial TiKV operator only. The
  unavailable operator remains visible in `FORMAT='RU'` as an unweighted
  status row with its site, class, scope, and reason; it is not reduced to a
  statement-level summary note.
  The final COMMIT emits the transaction-scoped TiKV prewrite payload with an
  empty `dml_kind`; it is never guessed or backfilled onto earlier DML.
- No-op/zero-match DML emits both TiDB primary units with value zero. A missing
  TiKV commit detail is partial rather than a successful zero payload.
- Pipelined mutation units remain valid, while missing TiKV logical flush
  payload is reported as `pipelined_tikv_payload_unsupported`. Pipelined
  transactions allocate a `CommitDetails` object without populating its
  `WriteKeys/WriteSize`; those empty fields are not published as zero units.
- Deprecated batch DML keeps one statement recorder across its internal
  transaction switches. The TiDB mutation count therefore includes every
  batch exactly once; available commit details continue to accumulate on the
  statement context.
- Ancillary DML CPU is currently partial for every supported DML kind. The gate
  cannot yet distinguish a simple DELETE from one that performs unmodeled FK
  checks or cascade orchestration, so DELETE is conservatively partial too.
  REPLACE remains rejected by the existing gate. IMPORT INTO and other
  non-foreground mutation paths are not represented by this operator.
- Local temporary table encoding is counted once on the foreground transaction
  MemDB. Its later copy to session temporary data does not pass through the
  recorder, and TiKV commit detail is unavailable/partial.

## Plan-tree composition

DML is flattened before result construction. The DML root is a non-billable
wrapper, while its SELECT/CTE/scalar-subquery and scan/filter/reader descendants
continue through the existing preview operator classifier. The mutation and
available TiKV write operators are appended once. Summary RU is the sum of all
weighted unit rows. An unsupported node or a node with missing runtime evidence
is emitted as an unweighted partial status row, and traversal continues so it
cannot hide independently usable descendants. These diagnostic rows do not
enter the summary total.

## Primary code locations

- Recorder and snapshot: `pkg/sessionctx/stmtctx/stmtctx.go`
- MemDB/transaction interception and statement lifecycle:
  `pkg/session/txn.go`, `pkg/session/session.go`
- Preview composition, weights, status, and output:
  `pkg/planner/core/explain_ru.go`
- Structured `dml_kind` diagnostic:
  `pkg/util/stmtsummary/read_billing.go`, `pkg/infoschema/tables.go`
- Regression coverage:
  `pkg/session/tidb_test.go`, `pkg/planner/core/common_plans_test.go`,
  `pkg/executor/explain_test.go`
