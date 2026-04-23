# Issue-5 Doc vs Implementation Signature Mismatch - Resolution Summary

## Problem Statement

The design document (`docs/design/2026-02-27-column-level-masking.md`) contained incorrect function signatures for masking builtin functions that did not match the actual implementation in `pkg/expression/builtin_masking.go`.

## Signature Inconsistities Found

### 1. MASK_PARTIAL

**Original (Incorrect) Documentation:**
```
MASK_PARTIAL(col, preserve_left, preserve_right, mask_char)
```

**Actual Implementation:**
```
MASK_PARTIAL(col, pad, start, length)
```

**Implementation Details:**
- Registered in `pkg/expression/builtin.go:656` with 4 required args
- `col`: The string column to mask
- `pad`: Single character used for masking (e.g., '*', 'X')
- `start`: Starting position (0-indexed) where masking begins
- `length`: Number of characters to mask

**Example Usage:**
```sql
MASK_PARTIAL('1234567890', '*', 0, 6)  -- Returns: '******7890'
MASK_PARTIAL('hello world', 'X', 6, 5)  -- Returns: 'hello XXXXX'
```

### 2. MASK_FULL

**Original (Incorrect) Documentation:**
```
MASK_FULL(col, mask_char_or_default)
```

**Actual Implementation:**
```
MASK_FULL(col, mask_char)
```

**Implementation Details:**
- Registered in `pkg/expression/builtin.go:655` with 2 required args (no optional parameter)
- `col`: The column to mask (string, datetime, or numeric types)
- `mask_char`: Single character used for masking (e.g., '*', 'X')
- For datetime types: returns '1970-01-01' (date) or '1970-01-01 00:00:00' (datetime)

**Example Usage:**
```sql
MASK_FULL('secret', 'X')              -- Returns: 'XXXXXX'
MASK_FULL(birth_date, '*')             -- Returns: '1970-01-01'
```

### 3. MASK_DATE

**Original (Incorrect) Documentation:**
```
MASK_DATE(col, format_or_template)
```

**Actual Implementation:**
```
MASK_DATE(col, date_literal)
```

**Implementation Details:**
- Registered in `pkg/expression/builtin.go:658` with 2 required args
- `col`: The date/time column to mask
- `date_literal`: Fixed date string in 'YYYY-MM-DD' format (e.g., '1970-01-01')
- The implementation validates the date literal format strictly (must be 10 characters with proper hyphens)

**Example Usage:**
```sql
MASK_DATE(birth_date, '1970-01-01')  -- Returns: '1970-01-01'
MASK_DATE(hire_date, '2000-12-31')   -- Returns: '2000-12-31'
```

## Changes Made

### 1. Updated Design Document

**File:** `docs/design/2026-02-27-column-level-masking.md`

**Changes:**
- Corrected all four masking builtin function signatures
- Added detailed parameter descriptions for each function
- Added example usage for each function
- Added a comprehensive "Example Usage" section with practical SQL examples

### 2. Created New Integration Tests

**File:** `tests/integrationtest/t/expression/masking_builtin_signature.test`

**Purpose:**
- Validate the actual function signatures match the documentation
- Test edge cases for each function
- Ensure UTF-8 character handling works correctly
- Verify error conditions (invalid pad characters, negative start positions, etc.)

**Test Coverage:**
- `MASK_PARTIAL`: 4 test cases including UTF-8 support
- `MASK_FULL`: 4 test cases including date/datetime masking
- `MASK_DATE`: 3 test cases with different date literals
- `MASK_NULL`: 2 test cases for different data types

## Impact Assessment

### Positive Impact
- Users can now write correct masking policies based on accurate documentation
- Reduces risk of expression parsing errors
- Prevents potential fail-open scenarios (Issue-2) due to incorrect policy expressions
- Improves developer experience with clear examples

### No Breaking Changes
- This is a documentation-only fix
- No changes to actual implementation
- Existing policies continue to work as before
- No migration or compatibility concerns

## Validation

### Manual Validation
- Reviewed all function signatures in `pkg/expression/builtin_masking.go`
- Cross-referenced with `pkg/expression/builtin.go` registration
- Verified existing tests use correct signatures

### Automated Validation
- Created comprehensive integration test suite
- Tests can be run via MySQL test framework

## Verification Steps

To verify the changes:

1. **Review updated documentation:**
   ```bash
   git diff docs/design/2026-02-27-column-level-masking.md
   ```

2. **Run new integration tests:**
   ```bash
   mysql-test-run tests/integrationtest/t/expression/masking_builtin_signature.test
   ```

3. **Test policy creation with updated signatures:**
   ```sql
   CREATE MASKING POLICY p_test AS
     MASK_PARTIAL(col, '*', 0, 4) ENABLE;

   SELECT MASK_PARTIAL('test_data', '*', 0, 4);
   -- Expected: '****_data'
   ```

## Files Modified

1. `docs/design/2026-02-27-column-level-masking.md` - Updated function signatures and examples
2. `tests/integrationtest/t/expression/masking_builtin_signature.test` - New integration test file (created)
3. `task.md` - Updated checklist status

## Related Issues

- Issue-5: Doc vs Implementation Signature Mismatch (this issue)
- Issue-2: Point-get fail-open behavior (mitigated by correct documentation)
- GitHub Issue: pingcap/tidb#67046

## Next Steps

1. Code review of documentation changes
2. Integration test validation in CI environment
3. Review with documentation team
4. Merge to main branch
5. Update any external documentation or tutorials if needed