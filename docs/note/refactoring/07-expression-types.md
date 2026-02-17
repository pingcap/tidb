# Expression and Types Package Issues

## Expression God Files

| File | Lines | Contents |
|------|-------|----------|
| `builtin_time.go` | 7,260 | 214+ time function implementations |
| `builtin_string.go` | 5,622 | 143+ string function implementations |
| `builtin_compare.go` | 3,542 | 168+ comparison implementations |
| `builtin_time_vec.go` | 3,010 | Vectorized time functions |
| `builtin_string_vec.go` | 2,700+ | Vectorized string functions |
| `builtin_control_vec_generated.go` | 42,339 | Generated control flow vectorized code |

Total: 128 files in `pkg/expression/`.

## Clone Method Duplication

71+ identical Clone() patterns across builtin functions:
```go
func (b *builtinXxxSig) Clone() builtinFunc {
    newSig := &builtinXxxSig{}
    newSig.cloneFrom(&b.baseBuiltinFunc)
    return newSig
}
```

**Fix**: Auto-generate all Clone() methods. The `go:generate` directives in
`builtin.go:19-24` only cover 6 generators. Missing generators for arithmetic,
math, json, encryption, regexp.

## Vectorized String Function Performance

### []rune Allocation Problem (CRITICAL)

20+ locations in `builtin_string_vec.go` perform `[]rune(str)` inside loops:

```go
// Line 220 - builtinLeftUTF8Sig
for i := range n {
    str := buf.GetString(i)
    runes, leftLength := []rune(str), int(nums[i])  // ALLOC 1: rune slice
    result.AppendString(string(runes[:leftLength]))   // ALLOC 2: back to string
}
```

Each `[]rune()` allocates a new slice and iterates all bytes for UTF-8 decoding.
Two allocations per row in batch processing.

**Optimized approach**:
```go
// Use utf8 package for length without allocation
runeCount := utf8.RuneCountInString(str)
// Use index-based traversal for substring
byteIdx := 0
for runeIdx := 0; runeIdx < leftLength; runeIdx++ {
    _, size := utf8.DecodeRuneInString(str[byteIdx:])
    byteIdx += size
}
result.AppendString(str[:byteIdx])  // Zero-copy substring
```

### String Case Conversion in Loops
Lines 450-451, 485, 1740-1741: `strings.ToLower()` / `strings.ToUpper()` called
per row, each allocating a new string.

### String Split/Join per Row
Lines 825-846 (`builtinSubstringIndexSig`):
```go
strs := strings.Split(str, delim)           // Alloc: slice + strings
substrs := strs[start:end]
result.AppendString(strings.Join(substrs, delim))  // Alloc: new string
```

## Incomplete Vectorization

Some "vectorized" functions are pass-throughs:
- Line 37-38: `builtinLowerSig.vecEvalString` simply delegates to child
- Line 183-185: `builtinUpperSig.vecEvalString` same - no actual vectorization

Only 6 code generators exist (`builtin.go:19-24`) out of 20+ function categories.

## Datum Type Design

### Structure (72 bytes per instance)

**File**: `pkg/types/datum.go:68-82`
```go
type Datum struct {
    k         byte     // 1 byte  - kind
    decimal   uint16   // 2 bytes
    length    uint32   // 4 bytes
    i         int64    // 8 bytes - int/uint/float storage
    collation string   // 16 bytes - WASTE: could be uint16 collation ID
    b         []byte   // 24 bytes - string/byte storage
    x         any      // 16 bytes - BOXING: MyDecimal, Time, Duration
}
// Total: 72 bytes (EmptyDatumSize)
```

### Boxing Overhead
Complex types stored in `any` field require runtime type assertions:
```go
func (d *Datum) GetMysqlDecimal() *MyDecimal { return d.x.(*MyDecimal) }
func (d *Datum) GetMysqlTime() Time          { return d.x.(Time) }
```

### Duplicate Type Switches
`SetValueWithDefaultCollation()` (lines 595-642) and `SetValue()` (lines 644-689)
are nearly identical 27-case type switches. Only difference: collation source.

### Copy Overhead
`Copy()` (lines 92-105) allocates for []byte and dereferences pointer types:
```go
case KindMysqlDecimal:
    d := *d.GetMysqlDecimal()  // Deref, copy 100+ byte struct
    dst.SetMysqlDecimal(&d)    // Allocate on heap
```

## Context Overhead

### Three Context Interface Hierarchies
- `exprctx.EvalContext` (exprctx/context.go:63-95) - 20+ methods
- `exprctx.BuildContext` (exprctx/context.go:98-133) - 15+ methods
- `exprctx.ExprContext` (exprctx/context.go:137-144) - combines both

### Static Context Wrappers
- `exprstatic/exprctx.go` (368 lines) - 14-field `exprCtxState`
- `exprstatic/evalctx.go` (536 lines) - 13-field `evalCtxState`

### VecEval Overhead
Every `VecEvalXxx()` call in `scalar_function.go:56-121`:
```go
func (sf *ScalarFunction) VecEvalInt(ctx EvalContext, ...) error {
    intest.Assert(ctx != nil)           // check even when disabled
    if intest.EnableAssert {
        ctx = wrapEvalAssert(ctx, sf.Function)  // wrapping overhead
    }
    return sf.Function.vecEvalInt(ctx, input, result)
}
```
Pattern repeats for 9 different VecEval methods.

## Decimal Implementation

**File**: `pkg/types/mydecimal.go`

Large lookup tables and complex arithmetic:
- `wordBufLen = 9`, `digitsPerWord = 9`
- `dig2bytes = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}`
- `pow10off81 = [...]float64{1e-81, ..., 1e81}` - 163 pre-computed values
- Fixed-size word buffer with extensive boundary checking

Manual memory management throughout conversion functions makes operations expensive.
