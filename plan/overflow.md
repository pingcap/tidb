# TiDB BR Module - Integer Overflow Vulnerability Tracking

**Analysis Date:** 2025-09-24
**Last Updated:** 2025-10-22
**Status:** 🟡 In Progress (2/54 Fixed)

---

## 📊 Executive Summary

This document tracks integer overflow vulnerabilities in the TiDB Backup & Restore (BR) module identified through comprehensive security analysis.

### Key Statistics

| Metric | Count |
|--------|-------|
| **Total Issues** | 54 |
| 🔴 **Critical** | 3 |
| 🟠 **High Risk** | 16 |
| 🟡 **Medium Risk** | 23 |
| 🟢 **Low Risk** | 12 |
| ✅ **Fixed** | 2 |
| 🚧 **In Progress** | 1 |
| ⏳ **Pending** | 51 |

### Most Affected Areas

| Package | Issue Count |
|---------|-------------|
| `br/pkg/metautil/` | 13 |
| `br/pkg/utils/` | 12 |
| `br/pkg/stream/` | 8 |
| `br/pkg/task/` | 8 |
| `br/pkg/storage/` | 7 |
| `br/pkg/backup/`, `br/pkg/restore/` | 6 |

---

## 🔴 Critical Issues (Immediate Action Required)

### ✅ Issue #1: Stream Processing - Byte Conversion [FIXED]

- **File:** [`br/pkg/stream/meta_kv.go:176-182`](../br/pkg/stream/meta_kv.go#L176-L182)
- **Severity:** 🔴 CRITICAL
- **Impact:** Index out of bounds panic when `len(data) == 1`
- **Status:** ✅ **FIXED** (2025-10-22)

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
case flagShortValuePrefix:
    vlen := data[1]  // ❌ No check if len(data) >= 2
    if len(data) < int(vlen)+2 {
        return errors.Annotatef(...)
    }
    v.shortValue = data[2 : int(vlen)+2]
```

**Fix Applied:**
```go
case flagShortValuePrefix:
    // Need at least 2 bytes: flag + vlen
    if len(data) < 2 {
        return errors.Annotatef(berrors.ErrInvalidArgument,
            "insufficient data for short value prefix, need at least 2 bytes but only have %d",
            len(data))
    }
    vlen := data[1]
    requiredLen := int(vlen) + 2
    if len(data) < requiredLen {
        return errors.Annotatef(...)
    }
    v.shortValue = data[2:requiredLen]
```

**Test Coverage:** Added test case in `meta_kv_test.go:TestWriteCFValueShortValueOverflow`

</details>

---

### ✅ Issue #2: Timestamp TSO Bit Shift Overflow [FIXED]

- **File:** [`br/pkg/task/backup_test.go:36,47,56`](../br/pkg/task/backup_test.go#L36)
- **Severity:** 🔴 CRITICAL
- **Impact:** Potential overflow in test code, misleading test results
- **Status:** ✅ **FIXED** (2025-10-22)

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
localTimestamp := localTime.Unix()
localTSO := uint64((localTimestamp << 18) * 1000)  // ❌ Can overflow
```

**Fix Applied:**
```go
// Use oracle.GoTimeToTS instead of manual calculation to avoid overflow
localTSO := oracle.GoTimeToTS(localTime)
```

**Changes:**
- Replaced manual TSO calculation with `oracle.GoTimeToTS()` at 3 locations
- Added import: `"github.com/tikv/client-go/v2/oracle"`

</details>

---

### 🚧 Issue #3: Storage Range Calculation Overflow [IN PROGRESS]

- **File:** [`br/pkg/storage/storage.go:307`](../br/pkg/storage/storage.go#L307)
- **Severity:** 🔴 CRITICAL
- **Impact:** Reading wrong data ranges, potential data corruption
- **Status:** 🚧 **IN PROGRESS**

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
func ReadDataInRange(
    ctx context.Context,
    storage ExternalStorage,
    name string,
    start int64,
    p []byte,
) (n int, err error) {
    end := start + int64(len(p))  // ❌ No overflow check
    rd, err := storage.Open(ctx, name, &ReaderOption{
        StartOffset: &start,
        EndOffset:   &end,
    })
```

**Proposed Fix:**
```go
func ReadDataInRange(...) (n int, err error) {
    // Check for overflow before addition
    if start > math.MaxInt64 - int64(len(p)) {
        return 0, errors.Annotate(berrors.ErrInvalidArgument,
            "range calculation overflow: start + len(p) exceeds int64 max")
    }
    end := start + int64(len(p))
    rd, err := storage.Open(ctx, name, &ReaderOption{
        StartOffset: &start,
        EndOffset:   &end,
    })
```

**Next Steps:**
- [ ] Apply fix
- [ ] Add test case for large start values
- [ ] Verify with integration tests

</details>

---

## 🟠 High Risk Issues

### ⏳ Issue #4: Binary Search Offset Arithmetic

- **File:** [`br/pkg/stream/stream_metas.go:1176`](../br/pkg/stream/stream_metas.go#L1176)
- **Severity:** 🟠 HIGH
- **Impact:** Incorrect binary search results, potential data loss
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
received, ok := slices.BinarySearchFunc(
    medit.DeleteLogicalFiles[idx].Spans,
    dfi.RangeOffset,
    func(s *pb.Span, u uint64) int {
        return int(s.Offset - u)  // ❌ uint64 difference cast to int
    })
```

**Proposed Fix:**
```go
func(s *pb.Span, u uint64) int {
    if s.Offset > u {
        return 1
    } else if s.Offset < u {
        return -1
    }
    return 0
}
```

</details>

---

### ⏳ Issue #5: S3 Multipart Buffer Pool Size

- **File:** [`br/pkg/storage/s3.go:1210`](../br/pkg/storage/s3.go#L1210)
- **Severity:** 🟠 HIGH
- **Impact:** Memory allocation failure, panic
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
u.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(
    option.Concurrency * hardcodedS3ChunkSize)  // ❌ No overflow check
```

**Proposed Fix:**
```go
const hardcodedS3ChunkSize = 5 * 1024 * 1024 // 5MB
if option.Concurrency > math.MaxInt / hardcodedS3ChunkSize {
    return nil, errors.Annotate(berrors.ErrInvalidArgument,
        "buffer pool size calculation would overflow")
}
bufferPoolSize := option.Concurrency * hardcodedS3ChunkSize
u.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(bufferPoolSize)
```

</details>

---

### ⏳ Issue #6: Bitmap Position Calculation

- **File:** [`br/pkg/restore/log_client/log_file_map.go:25`](../br/pkg/restore/log_client/log_file_map.go#L25)
- **Severity:** 🟠 HIGH
- **Impact:** Undefined behavior with negative offsets
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
func (m bitMap) pos(off int) (blockIndex int, bitOffset uint64) {
    return off >> 6, uint64(1) << (off & 63)  // ❌ Negative off not handled
}
```

**Proposed Fix:**
```go
func (m bitMap) pos(off int) (blockIndex int, bitOffset uint64) {
    if off < 0 {
        panic(fmt.Sprintf("bitMap.pos: negative offset not allowed: %d", off))
    }
    return off >> 6, uint64(1) << (off & 63)
}
```

</details>

---

### ⏳ Issue #7: Event Iterator Bounds Check

- **File:** [`br/pkg/stream/decode_kv.go:66`](../br/pkg/stream/decode_kv.go#L66)
- **Severity:** 🟠 HIGH
- **Impact:** Buffer length truncation for large buffers
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
func (ei *EventIterator) Valid() bool {
    return ei.err == nil && ei.pos < uint32(len(ei.buff))  // ❌ Truncation if len > MaxUint32
}
```

**Proposed Fix:**
```go
func (ei *EventIterator) Valid() bool {
    if ei.err != nil {
        return false
    }
    buffLen := len(ei.buff)
    if buffLen > math.MaxUint32 {
        ei.err = errors.New("buffer length exceeds uint32 max")
        return false
    }
    return ei.pos < uint32(buffLen)
}
```

</details>

---

### ⏳ Issue #8: S3 Content Length Underflow

- **File:** [`br/pkg/storage/s3.go:958`](../br/pkg/storage/s3.go#L958)
- **Severity:** 🟠 HIGH
- **Impact:** Integer underflow for empty objects
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
objectSize := *(result.ContentLength)
r = RangeInfo{
    Start: 0,
    End:   objectSize - 1,  // ❌ Underflow if objectSize == 0
    Size:  objectSize,
}
```

**Proposed Fix:**
```go
objectSize := *(result.ContentLength)
if objectSize == 0 {
    r = RangeInfo{Start: 0, End: 0, Size: 0}
} else {
    r = RangeInfo{Start: 0, End: objectSize - 1, Size: objectSize}
}
```

</details>

---

### ⏳ Issue #9: Task Rate Limit Calculation

- **File:** [`br/pkg/task/common.go:634`](../br/pkg/task/common.go#L634)
- **Severity:** 🟠 HIGH
- **Impact:** Silent overflow in rate limiting, incorrect throttling
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
cfg.RateLimit = rateLimit * rateLimitUnit  // ❌ No overflow check
```

**Proposed Fix:**
```go
if rateLimit > 0 && rateLimitUnit > 0 && rateLimit > math.MaxUint64/rateLimitUnit {
    return errors.Annotate(berrors.ErrInvalidArgument,
        "rate limit calculation would overflow")
}
cfg.RateLimit = rateLimit * rateLimitUnit
```

</details>

---

### ⏳ Issue #10: Progress Counter Overflow

- **File:** [`br/pkg/utils/progress.go:55`](../br/pkg/utils/progress.go#L55)
- **Severity:** 🟠 HIGH
- **Impact:** Progress counter wraps around, incorrect reporting
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
func (pp *ProgressPrinter) IncBy(cnt int64) {
    atomic.AddInt64(&pp.progress, cnt)  // ❌ No overflow check
}
```

**Proposed Fix:**
```go
func (pp *ProgressPrinter) IncBy(cnt int64) {
    if cnt <= 0 {
        return
    }
    current := atomic.LoadInt64(&pp.progress)
    if current > math.MaxInt64 - cnt {
        // Cap at total if overflow would occur
        atomic.StoreInt64(&pp.progress, pp.total)
        return
    }
    atomic.AddInt64(&pp.progress, cnt)
}
```

</details>

---

### ⏳ Issue #11: Checksum Accumulation

- **File:** [`br/pkg/checksum/executor.go:333-334`](../br/pkg/checksum/executor.go#L333-L334)
- **Severity:** 🟠 HIGH
- **Status:** ⏳ PENDING

### ⏳ Issue #12: TiFlash Usage Estimation

- **File:** [`br/pkg/task/restore.go:1787`](../br/pkg/task/restore.go#L1787)
- **Severity:** 🟠 HIGH
- **Status:** ⏳ PENDING

### ⏳ Issue #13: Exponential Backoff

- **File:** [`br/pkg/utils/backoff.go:118`](../br/pkg/utils/backoff.go#L118)
- **Severity:** 🟠 HIGH
- **Status:** ⏳ PENDING

### ⏳ Issue #14: Archive Size Calculation

- **File:** [`br/pkg/metautil/metafile.go:229-234`](../br/pkg/metautil/metafile.go#L229-L234)
- **Severity:** 🟠 HIGH
- **Status:** ⏳ PENDING

### ⏳ Issue #15: Metadata Size Accumulation

- **File:** [`br/pkg/metautil/metafile.go:898-899`](../br/pkg/metautil/metafile.go#L898-L899)
- **Severity:** 🟠 HIGH
- **Status:** ⏳ PENDING

### ⏳ Issue #16: Memory Limit Calculation

- **File:** [`br/pkg/utils/memory_monitor.go:66`](../br/pkg/utils/memory_monitor.go#L66)
- **Severity:** 🟠 HIGH
- **Status:** ⏳ PENDING

---

## 🟡 Medium Risk Issues

### ⏳ Issue #17: KV Entry Encoding Size Truncation

- **File:** [`br/pkg/stream/decode_kv.go:90-91`](../br/pkg/stream/decode_kv.go#L90-L91)
- **Severity:** 🟡 MEDIUM
- **Status:** ⏳ PENDING

<details>
<summary>📝 Details</summary>

**Vulnerable Code:**
```go
binary.LittleEndian.PutUint32(length, uint32(len(k)))  // ❌ Truncates if > 4GB
binary.LittleEndian.PutUint32(length, uint32(len(v)))
```

**Proposed Fix:**
```go
if len(k) > math.MaxUint32 || len(v) > math.MaxUint32 {
    return nil, errors.New("key or value too large for encoding (max 4GB)")
}
binary.LittleEndian.PutUint32(length, uint32(len(k)))
binary.LittleEndian.PutUint32(length, uint32(len(v)))
```

</details>

---

### ⏳ Issue #18-39: Other Medium Risk Issues

<details>
<summary>View All Medium Risk Issues (22 more)</summary>

| # | File | Line | Description | Status |
|---|------|------|-------------|--------|
| 18 | `metafile.go` | 590,599,639-640 | Metadata file size accumulation | ⏳ PENDING |
| 19 | `client.go` | 1147 | Batch size calculation | ⏳ PENDING |
| 20 | `sum_sorted.go` | 116-117,130-131,165-166 | Region split division by zero | ⏳ PENDING |
| 21 | `gcs.go` | 350,356 | GCS part size conversion | ⏳ PENDING |
| 22 | `table_mapping.go` | 803 | Table mapping ID generation | ⏳ PENDING |
| 23 | `writer.go` | 201 | Buffered writer calculation | ⏳ PENDING |
| ... | ... | ... | ... | ... |

</details>

---

## 🟢 Low Risk Issues

### ⏳ Issue #40-51: Low Risk Issues

<details>
<summary>View All Low Risk Issues (12 items)</summary>

| # | File | Line | Description | Status |
|---|------|------|-------------|--------|
| 40 | `stream_status.go` | 351 | QPS counter underflow | ⏳ PENDING |
| 41 | `rewrite_meta_rawkv.go` | 639 | Delete range params allocation | ⏳ PENDING |
| 42 | `storage.go` | 287-288 | HTTP connection pool sizing | ⏳ PENDING |
| 43 | `ebs.go` | 299,744 | AWS volume size accumulation | ⏳ PENDING |
| ... | ... | ... | ... | ... |

</details>

---

## 📋 Remediation Plan

### ✅ Phase 1: Critical Fixes (Week 1) - IN PROGRESS

- [x] **Issue #1:** meta_kv.go byte conversion (✅ FIXED)
- [x] **Issue #2:** TSO timestamp bit shift (✅ FIXED)
- [ ] **Issue #3:** storage range calculation (🚧 IN PROGRESS)
- [ ] Run integration tests for all critical fixes
- [ ] Deploy hotfix to testing environment

**Progress: 66% (2/3 complete)**

---

### Phase 2: High Risk Fixes (Week 2-3)

- [ ] **Issues #4-10:** Fix all high-risk binary search, buffer, and arithmetic issues
- [ ] **Issues #11-16:** Fix accumulation and calculation overflows
- [ ] Create `br/pkg/utils/safe_math.go` utility library
- [ ] Add overflow-specific unit tests
- [ ] Update CI/CD pipeline checks

**Progress: 0% (0/13 complete)**

---

### Phase 3: Medium Risk Fixes (Week 4-5)

- [ ] Address all type conversion issues (#17-21)
- [ ] Fix size accumulation overflows (#18-19)
- [ ] Add division by zero checks (#20)
- [ ] Implement safe arithmetic utilities
- [ ] Update code review guidelines

**Progress: 0% (0/23 complete)**

---

### Phase 4: Low Risk & Prevention (Week 6-8)

- [ ] Fix all low-risk issues (#40-51)
- [ ] Integrate static analysis tools (gosec G109, G115)
- [ ] Implement fuzzing for critical parsers
- [ ] Create secure coding guidelines document
- [ ] Train team on overflow prevention

**Progress: 0% (0/12 complete)**

---

## 🛠️ Prevention & Automation

### Static Analysis Configuration

```yaml
# .golangci.yml additions
linters:
  enable:
    - gosec  # Enable G109, G115 for overflow detection
  settings:
    gosec:
      config:
        G109:
          enabled: true  # Integer type conversion overflow
        G115:
          enabled: true  # Integer overflow detection
```

### Safe Math Utilities

**Create:** `br/pkg/utils/safe_math.go`

```go
// SafeAddInt64 performs checked int64 addition
func SafeAddInt64(a, b int64) (int64, error)

// SafeMulUint64 performs checked uint64 multiplication
func SafeMulUint64(a, b uint64) (uint64, error)

// SafeInt64ToInt safely converts int64 to int
func SafeInt64ToInt(val int64) (int, error)
```

### Makefile Targets

```makefile
.PHONY: check-overflow
check-overflow: ## Check for integer overflow vulnerabilities
	tools/bin/golangci-lint run ./br/pkg/... --enable gosec | grep "G109\|G115"

.PHONY: test-overflow
test-overflow: ## Run overflow-specific tests
	cd br/pkg && go test -run ".*Overflow.*" ./...
```

---

## 📊 Progress Tracking

| Week | Target | Actual | Status |
|------|--------|--------|--------|
| Week 1 | 3 Critical | 2 | 🟡 66% |
| Week 2-3 | 13 High Risk | 0 | ⏳ Not Started |
| Week 4-5 | 23 Medium Risk | 0 | ⏳ Not Started |
| Week 6-8 | 12 Low Risk + Tooling | 0 | ⏳ Not Started |

**Overall Progress:** 3.7% (2/54 issues fixed)

---

## 📚 References

- [Go Integer Overflow Guide](https://golang.org/ref/spec#Integer_overflow)
- [gosec Security Checker](https://github.com/securego/gosec)
- [TiDB BR Documentation](https://docs.pingcap.com/tidb/stable/backup-and-restore-overview)

---

## 📝 Change Log

| Date | Changes | Author |
|------|---------|--------|
| 2025-10-22 | Fixed Issue #1 (meta_kv.go) and #2 (backup_test.go) | Claude |
| 2025-10-22 | Reformatted document with checklists and progress tracking | Claude |
| 2025-09-24 | Initial vulnerability analysis completed | Security Team |

---

**Last Updated:** 2025-10-22
**Next Review:** 2025-10-29
