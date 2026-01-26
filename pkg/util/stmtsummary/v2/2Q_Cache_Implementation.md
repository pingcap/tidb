# StmtSummary 2Q Cache Implementation

This document details the refactoring of `pkg/util/stmtsummary/v2` to implement a Two-Queue (2Q) cache eviction strategy, replacing the simple LRU strategy. This change aims to mitigate cache pollution from scan-like workloads (many one-time queries).

## Overview

The statement summary cache is now split into two internal queues:
1.  **Probationary Queue (FIFO)**: All new queries enter here.
2.  **Main Queue (LRU)**: Queries accessed a second time are promoted here.

This ensures that "one-hit wonder" queries (rarely executed) stay in the Probationary Queue and are quickly evicted, while frequently executed queries move to the Main Queue and are protected.

## Implementation Details

### `stmtWindow` Structure

The `stmtWindow` struct in `stmtsummary.go` has been updated:

```go
type stmtWindow struct {
    begin   time.Time
    fifo    *kvcache.SimpleLRUCache // Probationary Queue (FIFO)
    lru     *kvcache.SimpleLRUCache // Main Queue (LRU)
    evicted *stmtEvicted
}
```

### Capacity Allocation

The total `MaxStmtCount` capacity is distributed as follows:
-   **Probationary Queue**: ~1/3 of total capacity.
-   **Main Queue**: ~2/3 of total capacity.

This split is enforced in `newStmtWindow` and `SetMaxStmtCount`.

### Access Logic (`Add` method)

When adding a statement summary (`Add`):
1.  **Check Main Queue (LRU)**:
    -   If found: Update the record. (Implicitly moves to MRU position in Main).
2.  **Check Probationary Queue (FIFO)**:
    -   If found: This is the second access.
        -   Remove from Probationary Queue.
        -   Insert into Main Queue (Promotion).
    -   If not found:
        -   Insert into Probationary Queue.

### Eviction Policy

Both queues share the same `onEvict` callback. Evicted items from either queue are aggregated into `stmtWindow.evicted` (the "other" category).
-   Items in Probationary Queue are evicted purely based on insertion order (FIFO behavior of the underlying cache when items are not promoted).
-   Items in Main Queue are evicted based on LRU.

## Code Changes

-   **`pkg/util/stmtsummary/v2/stmtsummary.go`**:
    -   Modified `stmtWindow` struct.
    -   Updated `NewStmtSummary`, `Add`, `ClearInternal`, `SetMaxStmtCount`, `rotate`, `flush`.
    -   Added helper methods `Size()` and `Values()` to `stmtWindow` to encapsulate the two queues.
    -   Fixed `NewStmtSummary4Test` to correctly initialize `optMaxStmtCount`.

-   **`pkg/util/stmtsummary/v2/logger.go`**:
    -   Updated `persist` to iterate over values from both queues using `w.Values()`.

-   **`pkg/util/stmtsummary/v2/reader.go`**:
    -   Updated `MemReader.Rows` to read from both queues.

-   **`pkg/util/stmtsummary/v2/stmtsummary_test.go`**:
    -   Updated `TestStmtWindow` and `TestStmtSummary` to reflect the 2Q behavior and capacity splits.
    -   Added `Test2QBehavior` to specifically verify the promotion logic from FIFO to LRU.

## Testing

Unit tests in `pkg/util/stmtsummary/v2` pass, validating:
-   Basic add/get functionality.
-   Promotion from FIFO to LRU on second access.
-   Eviction logic and counts.
-   Persistence and reading of the summary window.
