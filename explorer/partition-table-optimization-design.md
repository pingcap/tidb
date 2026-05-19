# Infoschema V2 Partition Table Optimization Design Document

## 1. Background

### 1.1 Current State
Infoschema V2 was designed to reduce memory usage by:
- Keeping only lightweight name→id mappings in permanent memory
- Loading full table info on-demand via Sieve cache (evictable)

However, partition tables were not fully optimized during implementation:
- Partition tables are classified as "special attributes"
- Their full `PartitionInfo` remains resident in `tableInfoResident` (bypasses Sieve cache)
- This violates the V2 design principle and causes memory bloat for large-scale partitioned tables

### 1.2 Why Partition Tables Were Made Resident

The original design assumed:
1. Background tasks need to iterate over tables with special attributes
2. Different background tasks iterate over different attribute types
3. Without resident storage, repeated iterations over large table sets would be expensive
4. Since iteration only selects a small subset of tables, making them resident simplifies iteration

**Analysis shows this assumption is incorrect for partition tables**:
- Partition tables have the lowest access frequency among all special attributes
- No periodic background tasks iterate over partition tables
- Access patterns are primarily: startup, user-initiated queries, and DDL events

---

## 2. Problem Analysis

### 2.1 Current Memory Footprint

For a cluster with:
- 1,000 partitioned tables
- Average 100 partitions per table
- Each `PartitionInfo` containing full partition definitions

**Estimated memory waste**: ~1000 × `PartitionInfo` (including `Definitions[]`)

### 2.2 Partition Table Access Patterns

| Scenario | Caller | Frequency | Needs Full PartitionInfo? | Can Use Table ID Only? |
|----------|--------|-----------|---------------------------|------------------------|
| Startup LIST partition rebuild | `bootstrap.go:rebuildAllPartitionValueMapAndSorted` | Once per server restart | YES (LIST only) | NO |
| Stats initialization | `stats/handle/util/buildPartitionID2TableID` | Schema version change | NO | YES |
| SHOW STATS_META | `executor/show_stats.go:fetchShowStatsMeta` | User-initiated | NO | YES |
| SHOW STATS_HEALTHY | `executor/show_stats.go:fetchShowStatsHealthy` | User-initiated | NO | YES |
| DDL operations (exchange/reorganize) | `ddl/*.go` | Event-driven | Specific partitions only | YES (by ID) |

**Key findings**:
- Only LIST partition value map rebuilding needs full PartitionInfo
- All other use cases can work with just table IDs or specific partition IDs
- Stats initialization already optimized for V2 (early return, doesn't use list API)

### 2.3 Comparison with Other Special Attributes

| Special Attribute | Access Frequency | Background Tasks | Iteration Frequency |
|-------------------|------------------|------------------|---------------------|
| TTL | ⚡⚡⚡⚡⚡ Very High | `ttlworker` (every 5s or schema change) | Every 5s |
| TiFlash | ⚡⚡⚡⚡ High | Async replica progress monitoring | Continuous |
| Placement | ⚡⚡ Medium | Policy validation, placement tracking | On demand |
| **Partition** | **⚡⚡ Low** | **No periodic tasks** | **Startup / Query / DDL only** |
| TableLock | ⚡ Low | Lock timeout cleanup | On demand |

**Conclusion**: Partition tables are the best candidate for on-demand loading.

---

## 3. LIST Partition Value Map Analysis

### 3.1 What is the Value Map?

The LIST partition value map is an in-memory data structure for fast partition pruning:

```go
type ForListColumnPruning struct {
    valueMap map[string]ListPartitionLocation      // Value → partition (O(1) lookup)
    sorted   *btree.BTreeG[*btreeListColumnItem]    // Range queries (O(log n))
    // ...
}
```

**Purpose**: Enables O(1) partition lookup for queries like:
```sql
SELECT * FROM orders WHERE region = 'CA'  -- valueMap['CA'] → partition_id
```

### 3.2 Why Rebuild at Startup?

**Reason 1: Collation Dependency (#32416)**
```go
key := value.Encode()  // Depends on NewCollationEnabled setting
```
If collation settings change, string encoding changes, and lookups would fail.

**Reason 2: Expression Evaluation**
```sql
PARTITION BY LIST (YEAR(created_at)) (
    PARTITION p2023 VALUES IN (YEAR('2023-01-01'))  -- Needs evaluation
)
```

**Reason 3: Collation Settings Loaded Before Schema**
```go
BootstrapSession()
  → loadPluginAndSysVar()              // Load collation first
  → bootstrapSessionImpl()
  → rebuildAllPartitionValueMapAndSorted()  // Rebuild with correct collation
```

### 3.3 Optimization Opportunities

| Option | Feasibility | Risk | Trade-off |
|--------|-------------|------|-----------|
| Lazy build on first query | 🤔 Medium | Cold query slow | Moves cost from startup to runtime |
| Incremental update on DDL | ✅ Feasible | DDL complexity | Needs to handle ADD/DROP/ALTER PARTITION |
| Store encoded values | ⚠️ Partial | Schema bloat | Still needs encoding, depends on collation |
| **Keep current (LIST-only scan)** | ✅ Safest | Startup scan cost | LIST partitions are rare (<5% of tables) |

**Recommendation**: Short-term: scan only LIST partitions; Long-term: incremental updates.

---

## 4. MVCC Versioning Considerations

### 4.1 Current MVCC Support

Existing structures already support versioning:

```go
type Data struct {
    byID      atomic.Pointer[btree.BTreeG[*tableItem]]     // {tableID, schemaVersion}
    byName    atomic.Pointer[btree.BTreeG[*tableItem]]     // {dbName, tableName, schemaVersion}
    pid2tid   atomic.Pointer[btree.BTreeG[partitionItem]]  // {partitionID, schemaVersion} ✅
    // ...
}

type partitionItem struct {
    partitionID   int64
    schemaVersion int64  // ✅ Versioned
    tableID       int64
    tomb          bool   // ✅ MVCC tomb marker
}
```

### 4.2 Challenge: New Index Must Support Versioning

If we add a `partitionedTableIDs` index, it must be version-aware:

**Option A: Versioned Bitmap Storage**
```go
type partitionedTableIDsItem struct {
    schemaVersion int64
    tableIDs      *roaring.Bitmap
    tomb          bool
}

// Problem: High memory overhead (bitmap copied per version)
```

**Option B: Extract from pid2tid (Recommended)**
```go
func (is *infoschemaV2) GetPartitionedTableIDs(schemaVersion int64) []int64 {
    var tidSet map[int64]bool
    is.Data().pid2tid.Load().Ascend(func(item partitionItem) bool {
        if item.schemaVersion == schemaVersion && !item.tomb {
            tidSet[item.tableID] = true
        }
        return true
    })
    // Convert to slice
}
```

**Advantages**:
- No extra storage
- Automatically versioned (filter by schemaVersion)
- Reuses existing pid2tid btree

**Disadvantages**:
- Requires scanning pid2tid (O(M) where M = number of partitions)
- But M is typically smaller than number of tables (10× partitions per table = 10N tables)

### 4.3 Hybrid Solution (Best for Performance)

```go
type Data struct {
    // ... existing fields

    // Cache for latest version only (optional optimization)
    latestPartitionedTableIDs atomic.Pointer[roaring.Bitmap]
    latestSchemaVersion       atomic.Uint64
}

func (is *infoschemaV2) GetPartitionedTableIDs(schemaVersion int64) []int64 {
    // Fast path: latest version
    if schemaVersion == is.Data().latestSchemaVersion.Load() {
        if bm := is.Data().latestPartitionedTableIDs.Load(); bm != nil {
            return bm.ToArray()
        }
    }

    // Slow path: historical version, extract from pid2tid
    return extractFromPID2TIDByVersion(is, schemaVersion)
}
```

---

## 5. Optimization Design

### 5.1 Requirements Mapping

| Requirement | Source | Needs | Can Change to ID-based? | MVCC Impact |
|-------------|--------|-------|------------------------|-------------|
| partition ID → table ID | Query execution, stats, DDL | pid2tid | ✅ Already implemented | ✅ Already versioned |
| Check if table is partitioned | SHOW STATS_META, stats init | table ID → bool | ✅ From pid2tid | ✅ Version-aware |
| Get partition definitions | DDL operations | table ID → []PartitionDefinition | ✅ TableByID() + GetPartitionInfo() | ✅ table.Table isolated |
| LIST value map rebuild | Startup | Full PartitionInfo + table instance | ⚠️ Startup must scan | ✅ table.Table isolated |
| List all partition tables | Stats init, SHOW commands | All partition table IDs | ✅ From pid2tid | ✅ Version-aware |
| Partition pruning | Query planning | value map (cached) | ❌ Internal to table.Table | ✅ table.Table isolated |

### 5.2 Phase-wise Implementation Plan

#### Phase 1: Remove Partition from HasSpecialAttributes

**Change**: `pkg/infoschema/context/infoschema.go`

```go
func HasSpecialAttributes(ti *model.TableInfo) bool {
    return ti.GetTTLInfo() != nil ||
           ti.GetTiFlashReplicaInfo() != nil ||
           // ti.GetPartitionInfo() != nil ||  // ← REMOVE THIS LINE
           ti.GetPlacementPolicyRef() != nil ||
           ti.GetTableLockInfo() != nil ||
           ti.GetAffinityInfo() != nil
}
```

**Impact**:
- ✅ Partition tables no longer go into `tableInfoResident`
- ✅ `PartitionInfo` loaded from Sieve cache (evictable)
- ⚠️ `ListTablesWithSpecialAttribute(PartitionAttribute)` will return empty

**Risk**: LOW
- Existing code paths that rely on `ListTablesWithSpecialAttribute(PartitionAttribute)` will break
- Need to update all callers

#### Phase 2: Add Version-Aware Partition Table ID Query API

**Change**: `pkg/infoschema/infoschema_v2.go`

```go
// GetPartitionedTableIDs returns all partitioned table IDs for the given schema version
func (is *infoschemaV2) GetPartitionedTableIDs(schemaVersion int64) []int64 {
    var tidSet map[int64]bool
    is.Data().pid2tid.Load().Ascend(func(item partitionItem) bool {
        if item.schemaVersion == schemaVersion && !item.tomb {
            tidSet[item.tableID] = true
        }
        return true
    })

    result := make([]int64, 0, len(tidSet))
    for tid := range tidSet {
        result = append(result, tid)
    }
    return result
}
```

**Impact**:
- ✅ Provides version-aware API for listing partition tables
- ✅ Uses existing pid2tid, no extra storage

**Risk**: LOW
- New API, doesn't affect existing code

#### Phase 3: Update All Callers

##### 3.1: SHOW STATS Commands

**Change**: `pkg/executor/show_stats.go`

**Before**:
```go
dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
partitionedMap := make(map[int64]bool)
for _, db := range dbs {
    for _, tbl := range db.TableInfos {
        partitionedMap[tbl.ID] = true
    }
}
```

**After**:
```go
isV2, isv2 := infoschema.IsV2(is)
if isV2 {
    partitionedTableIDs := isv2.GetPartitionedTableIDs(is.MetaVersion())
    partitionedMap := make(map[int64]bool, len(partitionedTableIDs))
    for _, tid := range partitionedTableIDs {
        partitionedMap[tid] = true
    }
} else {
    // V1 fallback
    dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
    partitionedMap := make(map[int64]bool)
    for _, db := range dbs {
        for _, tbl := range db.TableInfos {
            partitionedMap[tbl.ID] = true
        }
    }
}
```

##### 3.2: Stats Initialization

**Change**: No action needed (V2 already has early return)

**File**: `pkg/statistics/handle/util/table_info.go:80-82`

```go
isV2, _ := infoschema.IsV2(is)
if isV2 {
    return c.TableInfoByID(is, physicalID)  // Already optimized
}
```

##### 3.3: LIST Partition Value Map Rebuild

**Change**: `pkg/session/bootstrap.go`

**Before**:
```go
dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
for _, db := range dbs {
    for _, t := range db.TableInfos {
        pi := t.GetPartitionInfo()
        if pi == nil || pi.Type != ast.PartitionTypeList {
            continue
        }
        // Rebuild value map
    }
}
```

**After**:
```go
isV2, isv2 := infoschema.IsV2(is)
var tableIDs []int64
if isV2 {
    // Extract partitioned table IDs
    partitionedTableIDs := isv2.GetPartitionedTableIDs(is.MetaVersion())

    // Filter to only LIST partitions (need to load each table's PartitionInfo)
    for _, tid := range partitionedTableIDs {
        tbl, ok := is.TableByID(ctx, tid)
        if !ok {
            continue
        }
        pi := tbl.Meta().GetPartitionInfo()
        if pi != nil && pi.Type == ast.PartitionTypeList {
            tableIDs = append(tableIDs, tid)
        }
    }
} else {
    // V1 fallback
    dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
    for _, db := range dbs {
        for _, t := range db.TableInfos {
            pi := t.GetPartitionInfo()
            if pi != nil && pi.Type == ast.PartitionTypeList {
                tableIDs = append(tableIDs, t.ID)
            }
        }
    }
}

// Rebuild for LIST partitions only
for _, tid := range tableIDs {
    tbl, ok := is.TableByID(ctx, tid)
    if !ok {
        continue
    }
    // ... rebuild value map
}
```

**Note**: Still requires loading PartitionInfo for each table to check if it's LIST type.
This is acceptable because LIST partitions are rare (<5% of partitioned tables).

#### Phase 4: Add LIST Partition Index (Optional Optimization)

**Change**: `pkg/infoschema/infoschema_v2.go`

```go
type Data struct {
    // ... existing fields

    // LIST partition table IDs (lightweight index for startup optimization)
    listPartitionTableIDs atomic.Pointer[btree.BTreeG[listPartitionTableItem]]
}

type listPartitionTableItem struct {
    tableID       int64
    schemaVersion int64
    tomb          bool
}

func (isd *Data) add(item tableItem, tbl table.Table) {
    // ... existing logic

    if pi := ti.GetPartitionInfo(); pi != nil {
        for _, def := range pi.Definitions {
            btreeSet(&isd.pid2tid, partitionItem{def.ID, item.schemaVersion, tbl.Meta().ID, false})
        }

        // Add to LIST partition index if applicable
        if pi.Type == ast.PartitionTypeList {
            btreeSet(&isd.listPartitionTableIDs, listPartitionTableItem{
                tableID:       tbl.Meta().ID,
                schemaVersion: item.schemaVersion,
                tomb:          false,
            })
        }
    }
}

func (is *infoschemaV2) GetListPartitionTableIDs(schemaVersion int64) []int64 {
    var tableIDs []int64
    is.Data().listPartitionTableIDs.Load().Ascend(func(item listPartitionTableItem) bool {
        if item.schemaVersion == schemaVersion && !item.tomb {
            tableIDs = append(tableIDs, item.tableID)
        }
        return true
    })
    return tableIDs
}
```

**Impact**:
- ✅ Eliminates need to load PartitionInfo for LIST check during startup
- ✅ Startup scan: all partitions → LIST partitions only (~95% reduction)
- ⚠️ Small additional memory overhead

**Risk**: LOW-MEDIUM
- New index requires updates during DDL
- Need to handle MVCC versioning correctly

---

## 6. Risk Assessment

| Change | Risk Level | Impact | Mitigation |
|--------|-----------|--------|------------|
| Remove Partition from HasSpecialAttributes | LOW | High | Update all callers before change; add comprehensive tests |
| Add GetPartitionedTableIDs API | LOW | Low | New API, backward compatible |
| Update SHOW commands | LOW | Moderate | V2-specific paths, V1 fallback preserved |
| Update LIST partition rebuild | LOW-MEDIUM | Moderate | List partitions are rare; acceptable latency |
| Add LIST partition index | LOW-MEDIUM | High | Thorough testing of MVCC and DDL paths |

### Test Coverage Requirements

1. **Unit Tests**:
   - `GetPartitionedTableIDs()` with different schema versions
   - MVCC version filtering
   - Tomb marker handling

2. **Integration Tests**:
   - Partition table DDL (CREATE/ALTER/DROP)
   - SHOW STATS_META with partitioned tables
   - LIST partition value map rebuilding
   - Snapshot read with different schema versions

3. **Performance Tests**:
   - Memory usage before/after optimization
   - Startup time with/without LIST partition index
   - Query performance with partition pruning

4. **Regression Tests**:
   - Ensure all existing functionality works
   - V1/V2 compatibility

---

## 7. Expected Benefits

### 7.1 Memory Savings

**Scenario**: 1,000 partitioned tables, average 100 partitions per table

| Component | Before | After | Savings |
|----------|--------|-------|---------|
| Partition table TableInfo (resident) | ~1,000 × TableInfo | 0 (Sieve cache) | ~100% |
| PartitionInfo definitions | 100,000 definitions | On-demand | ~100% |
| Overhead | No change | pid2tid (already exists) | 0 |

**Estimated savings**: Tens to hundreds of MB (depends on partition metadata size)

### 7.2 Startup Time

| Phase | Before | After | Improvement |
|-------|--------|-------|-------------|
| Scan all partitioned tables | O(N) | O(N_L) where N_L ≪ N | ~95% reduction (if LIST ≈ 5%) |
| Load PartitionInfo for LIST check | O(N_L) | O(N_L) | No change |
| Value map rebuild | O(N_L) | O(N_L) | No change |

**Total startup improvement**: ~95% reduction in partition table scan time

### 7.3 Query Performance

| Operation | Before | After | Impact |
|-----------|--------|-------|--------|
| SHOW STATS_META | List all + map build | Extract IDs + map build | No regression |
| Partition pruning | O(1) | O(1) | No change |
| DDL operations | Already by ID | Already by ID | No change |

---

## 8. Implementation Checklist

### Phase 1: Remove Partition from HasSpecialAttributes
- [ ] Modify `pkg/infoschema/context/infoschema.go`
- [ ] Update code comments to reflect change
- [ ] Add TODO for updating callers

### Phase 2: Add Version-Aware API
- [ ] Add `GetPartitionedTableIDs(schemaVersion int64)` to `infoschemaV2`
- [ ] Add unit tests for new API
- [ ] Test MVCC version filtering
- [ ] Test tomb marker handling

### Phase 3: Update Callers
- [ ] Update `pkg/executor/show_stats.go:fetchShowStatsMeta`
- [ ] Update `pkg/executor/show_stats.go:fetchShowStatsHealthy`
- [ ] Update `pkg/session/bootstrap.go:rebuildAllPartitionValueMapAndSorted`
- [ ] Add V1 fallback paths
- [ ] Add integration tests

### Phase 4: Optional LIST Index
- [ ] Add `listPartitionTableIDs` btree to `Data`
- [ ] Update `Data.add()` to populate index
- [ ] Add `GetListPartitionTableIDs()` API
- [ ] Update bootstrap to use new index
- [ ] Add DDL update logic
- [ ] Add MVCC tests

### Testing
- [ ] Unit tests for all new/modified code
- [ ] Integration tests for DDL operations
- [ ] Regression tests for existing functionality
- [ ] Performance benchmarks (memory, startup, queries)
- [ ] MVCC versioning tests

---

## 9. Rollback Plan

If issues arise after deployment:

1. **Disable V2 partition optimization**:
   ```go
   // Revert HasSpecialAttributes to include Partition
   func HasSpecialAttributes(ti *model.TableInfo) bool {
       return /* ... */ ||
           ti.GetPartitionInfo() != nil  // Re-add
   }
   ```

2. **Fallback to V1 behavior**:
   - Ensure V1 paths are preserved and tested
   - V2 optimization is behind feature flag or version check

3. **Monitor metrics**:
   - Cache hit/miss rates
   - Memory usage
   - Query latencies
   - Error rates

---

## 10. Related Issues and PRs

- Issue #32416: Collation dependency for LIST partition value maps
- Issue #54796: V2 MVCC tomb marker bugs (reference for MVCC handling)
- Any PRs related to infoschema V2 implementation

---

## 11. References

- `pkg/infoschema/infoschema_v2.go` - Core V2 implementation
- `pkg/infoschema/context/infoschema.go` - Special attribute filters
- `pkg/session/bootstrap.go` - Startup initialization
- `pkg/executor/show_stats.go` - SHOW command implementations
- `pkg/table/tables/partition.go` - Partition pruning and value maps
- `pkg/planner/core/rule/rule_partition_processor.go` - Partition pruning logic

---

## Appendix A: Migration Guide for Existing Code

If you have custom code that uses `ListTablesWithSpecialAttribute(PartitionAttribute)`:

**Before**:
```go
dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
partitionedMap := make(map[int64]bool)
for _, db := range dbs {
    for _, tbl := range db.TableInfos {
        partitionedMap[tbl.ID] = true
    }
}
```

**After**:
```go
isV2, isv2 := infoschema.IsV2(is)
if isV2 {
    partitionedTableIDs := isv2.GetPartitionedTableIDs(is.MetaVersion())
    partitionedMap := make(map[int64]bool, len(partitionedTableIDs))
    for _, tid := range partitionedTableIDs {
        partitionedMap[tid] = true
    }
} else {
    // V1 fallback (unchanged)
    dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
    // ...
}
```

---

## Appendix B: Performance Monitoring

Add metrics to track optimization effectiveness:

```go
// pkg/infoschema/metrics/partition.go

var (
    partitionTableCount = metrics.NewGauge(...)
    getPartitionedTableIDsDuration = metrics.NewHistogram(...)
    partitionCacheHitRate = metrics.NewGauge(...)
)
```

---

**Document Version**: 1.0
**Last Updated**: 2025-01-19
**Owner**: [To be assigned]
**Status**: Design Complete, Ready for Implementation