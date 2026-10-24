# TiDB Vector Search Execution Path

This contributor guide follows a vector nearest-neighbor query from SQL planning to TiFlash's local index reader. It describes current behavior; it is separate from the SQL-oriented [vector-index proposal](./design/2024-07-12-support-vector-index.md) and the [vector search tracking issue](https://github.com/pingcap/tidb/issues/54245).

## Parse and bind the query

The SQL parser turns the example `SELECT` into an AST; the parser entry point and SELECT statement node are in [`parser.go`](../pkg/parser/parser.go) and [`dml.go`](../pkg/parser/ast/dml.go). During logical-plan construction, TiDB resolves table and column references and builds the order-by distance expression in [`logical_plan_builder.go`](../pkg/planner/core/logical_plan_builder.go). Vector-index DDL syntax coverage is in [`TestVectorIndexSyntax`](../pkg/expression/integration_test/integration_test.go). The planner's vector-index eligibility and ANN request construction happen after query binding.

## Index DDL and metadata

TiDB accepts vector-index syntax such as `CREATE VECTOR INDEX` and `ALTER TABLE ... ADD VECTOR INDEX`. The preprocessor checks the supported index kind and basic structure, then rewrites vector-index DDL into the columnar-index representation used by the rest of the DDL pipeline. The DDL path validates the distance function and metric, target column, and fixed vector dimension before building index metadata. The resulting metadata records the vector index kind, dimension, and distance metric. The current metadata model supports HNSW.

References: [`preprocess.go`](../pkg/planner/core/preprocess.go), [`index.go`](../pkg/meta/model/index.go), syntax coverage in [`integration_test.go`](../pkg/expression/integration_test/integration_test.go), and metadata coverage in [`index_modify_test.go`](../pkg/ddl/index_modify_test.go).

## Query shape and planner eligibility

The ANN-specific TopN property is considered for a vector-distance expression in a single ascending `ORDER BY` item, with `LIMIT`, directly above a table data source. The planner currently rejects this path when the data source has pushed-down conditions. These are planner eligibility restrictions; a query outside them can still use another valid plan, but it will not select this ANN TopN path.

For example:

```sql
SELECT id
FROM items
ORDER BY VEC_L2_DISTANCE(embedding, '[0.1,0.2,0.3]')
LIMIT 10;
```

The planner derives the searched column, distance function, and query vector from the expression. It computes the `LIMIT + OFFSET` top-k property separately from the logical TopN count and offset. During task selection, TiDB requires a matching vector index on that column. It serializes the index ID, metric, query vector, and top-k into `ColumnarIndexExtra` / `ANNQueryInfo` and attaches the request to the TiFlash MPP scan. An `EXPLAIN` plan showing `annIndex:...` under `mpp[tiflash]` is a useful check that the ANN path was selected.

References: [`physical_topn.go`](../pkg/planner/core/operator/physicalop/physical_topn.go), [`find_best_task.go`](../pkg/planner/core/find_best_task.go), [`columnar_index_utils.go`](../pkg/planner/core/columnar_index_utils.go), and [`physical_table_scan.go`](../pkg/planner/core/operator/physicalop/physical_table_scan.go). The `EXPLAIN` and syntax cases are covered in [`integration_test.go`](../pkg/expression/integration_test/integration_test.go).

## TiFlash local-index search and result handling

The TiFlash scan passes the ANN request into its vector-index input stream. For each input stream with an applicable local index, TiFlash opens a reader and searches with a row-validity bitmap. That bitmap filters invalid or deleted row offsets returned by the index. SQL predicates pushed into the data source currently prevent the ANN TopN property from being generated, so the bitmap should not be confused with support for arbitrary pushed-down SQL filters.

Streams without an applicable local vector index follow the ordinary column-file input path. Indexed streams return candidate row offsets and distances; TiFlash merges candidates across streams and keeps the requested global top-k before reading selected rows. TiDB's MPP plan then returns the rows, and can reuse the distance produced by the index when the vector column is only used for ordering.

References in TiFlash: [`ColumnFileInputStream.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Stream/ColumnFileInputStream.cpp), [`InputStream.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Stream/InputStream.cpp), and [`Reader.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Reader.cpp). TiDB's distance-projection optimization is in [`task.go`](../pkg/planner/core/task.go).

## Index-file compatibility boundary

The current TiFlash writer constructs a USearch HNSW index and stores the dimension and distance metric in the vector-index file properties. The reader reconstructs that index type from the properties and opens the saved index. The writer currently emits format version `0`; the persisted artifact and its reader therefore define the encoding contract.

Future index methods or quantization should keep the planner request and TiFlash stream/filter/top-k path independent from the backend. Backend or encoding identifiers and format versions belong in persisted metadata and reader dispatch. Existing HNSW files must continue through their current reader path when a new encoding is introduced.

References in TiFlash: [`Writer.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Writer.cpp), [`Reader.cpp`](https://github.com/pingcap/tiflash/blob/master/dbms/src/Storages/DeltaMerge/Index/VectorIndex/Reader.cpp), and [`VectorIndex.h`](https://github.com/pingcap/tiflash/blob/master/dbms/src/TiDB/Schema/VectorIndex.h).

## Validation and troubleshooting

- Use `EXPLAIN` to check for `annIndex` under an MPP TiFlash scan.
- Compare indexed and fallback plans with the same table, query vector, and top-k.
- Confirm that a matching vector index exists and that the TiFlash replica is available.
- Exercise invalid/deleted rows and multiple input streams when changing candidate filtering or top-k merging.
- For backend or encoding changes, test index build, persisted-file reload, restart, compaction, and compatibility with existing HNSW files. Measure recall and resource use separately from functional correctness.

If `EXPLAIN` has no `annIndex`, first check the current planner restrictions: one ascending vector-distance sort item, a `LIMIT`, a `TopN` directly above the table data source, and no pushed-down conditions. Then confirm that the index is on the searched vector column and the TiFlash replica is available. An eligible plan can still fall back to column-file reads when a particular local stream has no applicable index.
