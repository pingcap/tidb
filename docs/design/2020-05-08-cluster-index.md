# Proposal: Clustered Index

- Author(s):     [coocood](https://github.com/coocood) 
- Last updated:  2020-05-08

## Abstract

This proposal proposes to support the clustered index feature.

## Background

In the current TiDB implementation, we only support clustered index for single integer column primary key.
Every row in a table has one row-key entry that stores the full row data and index entries that point to the row-key.
 
#### row key format 

```
t | {table_id} | _r | {handle} // component
1 | 8          | 2  | 8        // byte size
```
The type of `table_id` and `handle` are int64, so the row key is always of the same length 19.

#### non-unique index key format
```
t | {table_id} | _i | {index_id} | {index_column_values} | encoded_handle // component
1 | 8          | 2  | 8          | size of the values    | 9              // byte size
```
The handle is the suffix of the index key.

#### unique index key format
```
t | {table_id} | _i | {index_id} | {index_column_values} // component
1 | 8          | 2  | 8          | size of the values    // byte size
```
The handle is in the value of the index entry.

#### On Write

If the primary key is a single integer column, 
we use the value of the column as the handle directly.

Otherwise, we allocate a handle internally for the row, then create an extra index entry that points to the row key.

#### On Point Select

If the primary key is a single integer column,
We construct the row key by the primary key column value directly, do a lookup on TiKV.

Otherwise, we need to construct the index key with the primary key value, read the handle for the index entry,
Then construct the row key and do another lookup on TiKV.

#### On Range Scan
If the primary key is a single integer column,
We construct the range of the row keys directly, do a single scan on TiKV.

Otherwise, we need to construct the index range, scan index entries on TiKV to collect the handle,
Then do many point lookups On TiKV to read the rows.

## Proposal

Support clustered index for not only single integer column primary key but all kinds of primary key. 

### the Handle interface
```go
package kv
// Handle is the ID of a row.
type Handle interface {
   // IsInt returns if the handle type is int64.
   IsInt() bool
   // IntValue returns the int64 value if IsInt is true, it panics if IsInt returns false.
   IntValue() int64
   // Next returns the minimum handle that is greater than this handle.
   Next() Handle
   // Equal returns if the handle equals to another handle, it panics if the types are different.
   Equal(h Handle) bool
   // Compare returns the comparison result of the two handles, it panics if the types are different.
   Compare(h Handle) int
   // Encoded returns the encoded bytes.
   Encoded() []byte
   // Len returns the length of the encoded bytes.
   Len() int
   // NumCols returns the number of columns of the handle,
   NumCols() int
   // EncodedCol returns the encoded column value at the given column index.
   EncodedCol(idx int) []byte
   // String implements the fmt.Stringer interface.
   String() string
}

// IntHandle implements the Handle interface for the int64 type handle.
type IntHandle int64

// CommonHandle implements the Handle interface for non-int64 type handle.
type CommonHandle struct {
   encoded       []byte
   colEndOffsets []uint16
}

// HandleMap is the map for Handle.
type HandleMap struct {
   ints map[int64]interface{}
   strs map[string]strHandleVal
}

type strHandleVal struct {
   h   Handle
   val interface{}
}
```
To extend the handle type, we need to abstract the handle to an interface that has the
needed methods. 

There are two concrete types: IntHandle and CommonHandle.

If the primary key is a single column integer, the handle type would be IntHandle.
Otherwise, the handle type would be CommonHandle.

handles are used as map key in many places, one way to adapt to it is to use the encoded bytes as the key, but that would introduce memory allocation for IntHandle, so we use a HandleMap type that has two maps for the two handle types to avoid the memory allocation for IntHandle.

Integer handle row key and index key formats are the same as the current implementation.
We only need to care about the common handle type.

### Codec

#### common handle row key format
 ```
 t | {table_id} | _r | {common_handle}     // component
 1 | 8          | 2  | len(common_handle)  // byte size
 ```
The `common_handle` is the same as `index_column_values` in the index key.
The row key length can be more than 19.

#### non-unique index key format
```
 t | {table_id} | _i | {index_id} | {idx_col_vals}     | {common_handle}    // component
 1 | 8          | 2  | 8          | len(idx_col_vals)  | len(common_handle) // byte size
```
Non-unique index value is unchanged.

#### unique index value format
```
 {tailLen} | {common_handle_flag} | {common_handle_len} | {common_handle}    // component
 1         | 1                    | 2                   | len(common_handle) // byte size
```
The unique index key is unchanged.

### Planner

#### Handle Primary Key Index differently
For common handle tables, treat the primary index access path is table path instead of index path, builds TableScan plan instead of IndexLookUp plan.

There may be some places implicit assume the TableScan plan use int handle, we need to carefully review the logic.

The cost model may need to be adjusted for clustered index tables.

#### IndexScan Schema

If any of the primary key columns are used, or it is in an IndexLookUp executor, the index scan schema should be:

`{index columns},{primary key columns}`

There is no extra handle column for common handle tables.

If only index columns are used, the schema should be:

`{index columns}`

For example:
```sql
create table t (a int, b int, c int, d int, e int, primary key (a, b), index c_d (c, d));
```
The schema of index scan on `c_d` should be `c, d, a, b` or `c, d` depends on whether any primary key column is used.

So if a query doesn't reference column `e`, the index `c_d` is a covering index, we can build an IndexScan plan
instead of an IndexLookUp plan.

### Coprocessor
Besides codec code, several coprocessor executors need to be changed to support common handle.

#### IndexScan
If the IndexScan schema doesn't contain primary key columns, the coprocessor logic is unchanged.

If the IndexScan schema contains primary key columns, the coprocessor should decode the common handle
to primary key column values.

The encoded column value can be obtained by the `EncodedCol` method of the handle.

#### Analyze Column
If the primary key is multi-column, CMSketch needs to insert prefix column values of the primary key. 

#### Fast Analyze
Since the handle is not integer anymore, we cannot randomly generate the sample key at a given position.
So we need to add a coprocessor request type that samples the key/value pair during the scan and return the statistics result.

### Executors
Several executors need to be changed to support common handle

#### Admin Executors
The `admin check/recover table/index` executors heavily depend on the assumption that handle is int64 handle, 
it needs to do major changes to support common handle.

#### PointGet/BatchPointGet
We need to decode the common handle to chunk. 

#### Insert
We don't need to encode primary columns into the row value.

#### Update
If any of the primary key columns changed, we need to delete the row and insert the row.

#### SplitTableRegion
We need to redesign this executor since we cannot find a middle key based on the region start/end key. 

## Rationale

The clustered index feature would remove the extra read/write cost for tables with a non-single-integer-column primary key.
An experiment on the tpcC workload by compressing multi-column PK into one integer PK shows a 33% improvement. 

## Compatibility

This feature doesn't break the backward compatibility.

We add a field `IsCommonHandle` in the `model.TableInfo` and a global variable `tidb_enable_clustered_index`.

When creating a new table, set this field to true if the global variable `tidb_enable_clustered_index` is set to `1`.

All the common handle related logic should check `IsCommonHandle` field before entering the common handle code block.

For a new cluster, we insert the global variable `tidb_enable_clustered_index` value `1` in the bootstrap phase, the clustered index feature is supported by default.

For old clusters upgrade to the latest release version, the user needs to explicitly set the global variable to `1` to use the clustered index feature.

## Implementation

```
implement the Handle interface for IntHandle and CommonHandle.
  |
  + refactor int64 handle to the Handle interface in all tidb packages.
  |
  +-+ implement the codec.
    |
    |-+ support codec in tikv.
    | |
    | +- support coprocessor indexScan 
    | |
    | +- support coprocessor fastAnalyze 
    | |
    | +- support coprocessor analyzeColumn 
    |
    +-+ support common handle in planner.
    |
    |-+ support common handle in executors.
    |
    |-+ support common handle in ddl.
    |
    +-+ support common handle in other packages thats need minor change.
```

## Open Issues

TiFlash and CDC also need to update to support the clustered index feature.
