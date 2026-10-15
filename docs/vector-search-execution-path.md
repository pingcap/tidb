# TiDB Vector Search Execution Path

This note describes the current TiDB and TiFlash execution path for a vector nearest-neighbor query. It is intended for contributors investigating planner behavior or changing the vector-index implementation. The SQL syntax and design proposal are documented separately in the user guide and [vector-index proposal](./design/2024-07-12-support-vector-index.md).

## Query shape

The current planner path is considered for a single ascending vector-distance expression in `ORDER BY`, with a `LIMIT`, directly above a table data source and with no pushed-down filters. For example:

```sql
SELECT id
FROM items
ORDER BY VEC_L2_DISTANCE(embedding, '[0.1,0.2,0.3]')
LIMIT 10;
```

The vector TopN property is not produced when the query has multiple sort items, descending order, a non-`DataSource` child, or pushed-down conditions. Those queries may still run through another valid plan, but this ANN-specific path is not selected by that check. See [`PhysicalTopN.TryToGetChildProp`](../pkg/planner/core/operator/physicalop/physical_topn.go).

## TiDB planning and pushdown

1. The planner recognizes the vector-distance expression and derives a vector property containing the column, distance function, query vector, and `LIMIT + OFFSET` top-k value.
2. During task selection, only a matching vector index candidate can satisfy the vector property. TiDB builds a `ColumnarIndexExtra` with the index ID, column metadata, distance metric, query vector, and top-k, then attaches it to the TiFlash MPP scan.
3. If no matching TiFlash index path satisfies the property, the optimizer can choose a different eligible plan. An `EXPLAIN` plan showing `annIndex:...` under `mpp[tiflash]` is a useful check that the vector-index path was selected.

Implementation references: [`physical_topn.go`](../pkg/planner/core/operator/physicalop/physical_topn.go), [`columnar_index_utils.go`](../pkg/planner/core/columnar_index_utils.go), and [`find_best_task.go`](../pkg/planner/core/find_best_task.go).

## TiFlash local-index search

The TiFlash scan passes the ANN query information into its vector-index input stream. For each input stream that has a local vector index, TiFlash opens a reader and searches with the current row-validity bitmap. Streams without an applicable local index use the ordinary column-file input path (and compute distances there when requested).

Each indexed stream returns candidate row offsets and distances. TiFlash applies the validity bitmap, merges candidates across streams, keeps the global top-k by distance, then reads the selected rows from the underlying streams. This is where per-file ANN results become the query's global Top-K input; the surrounding MPP plan returns rows to TiDB for the final result.

Implementation references in TiFlash: [`ColumnFileInputStream.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Stream/ColumnFileInputStream.cpp), [`InputStream.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Stream/InputStream.cpp), and [`Reader.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Reader.cpp).

## Current index-file boundary

The current TiFlash writer constructs a USearch HNSW index and stores dimension and distance metric in the vector-index file properties. The current reader reconstructs the same index type from those properties and opens the saved index. The writer currently emits format version `0`; index encoding is therefore coupled to the writer/reader implementation and stored artifact.

When adding another index method or quantization, keep the planner's query and TiFlash stream/filter/Top-K path independent from the backend. The index kind or encoding must be represented in the persisted metadata and reader dispatch, while existing HNSW files remain readable. Relevant implementation references are [`Writer.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Writer.cpp), [`Reader.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Reader.cpp), and [`VectorIndex.h`](https://github.com/pingcap/tiflash/blob/master/dbms/src/TiDB/Schema/VectorIndex.h).

## Validation checklist

- Use `EXPLAIN` to confirm whether the planner selected `mpp[tiflash]` and reports `annIndex`.
- Compare indexed and fallback plans with the same table, query vector, and top-k.
- Check that TiFlash replicas are available and that the table's local index files have been built.
- Exercise filtered/deleted rows and multiple input streams when changing candidate filtering or top-k merging.
- For changes to the index backend or encoding, test build, persisted-file reload, restart, compaction, and compatibility with existing HNSW files; benchmark recall and resource use separately from functional correctness.

## Troubleshooting a query that does not use the vector index

Start with `EXPLAIN`. If the plan has no `annIndex` under an MPP TiFlash scan, check that the query has one ascending vector-distance expression in `ORDER BY`, a `LIMIT`, a `TopN` directly above the table data source, and no pushed-down filter conditions. These are current planner restrictions, not proof that the SQL query is invalid.

Then confirm that the table has a matching vector index on the searched column and that a TiFlash replica is available. If the plan is eligible but the index is not available for a particular local stream or column file, TiFlash can fall back to reading the column data and calculating distances when requested.
