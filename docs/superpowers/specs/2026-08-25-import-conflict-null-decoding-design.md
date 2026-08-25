# IMPORT INTO conflict NULL decoding design

## Problem

`pkg/dxf/importinto/conflictedkv` decodes conflicted record KVs with
`tables.DecodeRawRowData` and the table's visible-column projection. When a
hidden functional-index column physically precedes a later visible nullable
column, the projection is shorter than the physical column domain. A missing
stored value for that nullable column enters default-value lookup.

`DecodeRawRowData` currently allocates its default-value cache with
`len(cols)`, but `tables.GetColDefaultValue` indexes the cache by the physical
`Column.Offset`. For a visible projection with physical offsets `0, 1, 3`, the
three-element cache cannot serve offset `3`, so conflict cleanup panics after
ingested KVs may already exist.

## Design

Keep decoded row values in the caller-provided projection order, but allocate
the default-value cache in the physical table-column domain using
`len(tbl.Meta().Columns)`. TiDB requires each `Column.Offset` to match its
position in `TableInfo.Columns`, so this covers public, hidden, and in-flight
DDL columns without changing the cache API or adding map overhead to row
decoding.

The production change is limited to `tables.DecodeRawRowData`. No import task,
cleanup, encoding, or SQL semantics change.

## Tests

Adapt the existing conflict-handler functional-index regression in
`pkg/dxf/importinto/conflictedkv/handler_test.go` so the later visible column is
`NULL`. Before the fix this reaches the reported out-of-range panic. After the
fix the handler must receive the three visible datums, preserve the `NULL`, and
re-encode exactly the expected record and index KVs.

Add a dedicated real-TiKV suite case in
`tests/realtikvtest/importintotest4/conflict_resolution_test.go`. It imports
`1,10,\N` and `2,10,\N` with duplicate capture into a table whose hidden
functional-index column precedes `tail`. Successful behavior is: conflict
cleanup retains no rows, `ADMIN CHECK TABLE` succeeds, a later `a=10` insert is
represented through the functional index, a second `a=10` insert is rejected,
and the final table remains consistent.

## Alternatives Rejected

Sizing the cache from the maximum projected offset would work but duplicates
the table metadata offset invariant. Replacing the slice with a map keyed by
column ID would remove offset coupling but broadens a shared helper contract
and adds unnecessary hot-path overhead.
