# MV Index Fast Check Phase C: BIT_XOR Unification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace MV index checksum from SUM-based to BIT_XOR-based, eliminating `JSON_SUM_CRC32` and unifying aggregation with normal indexes.

**Architecture:** Add a new builtin function `JSON_ARRAY_XOR_CRC32(array, prefix)` across parser/expression/planner layers (following `JSON_SUM_CRC32`'s pattern). Then add `mvXorCheckBuilder` implementing `indexCheckBuilder`, swap it in, and delete the old code.

**Tech Stack:** Go, TiDB parser (yacc), expression framework, executor

**Spec:** `docs/superpowers/specs/2026-03-18-mv-index-fast-check-phase-c-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `pkg/parser/ast/functions.go` | `JSONArrayXorCrc32Expr` AST node |
| `pkg/parser/parser.y` | Parsing rule for `JSON_ARRAY_XOR_CRC32(expr, expr)` |
| `pkg/parser/misc.go` | Keyword token mapping |
| `pkg/expression/builtin_json.go` | Function class, builtin signature, scalar eval |
| `pkg/expression/builtin_json_vec.go` | Vectorized eval |
| `pkg/expression/builtin_json_test.go` | Unit tests for the new function |
| `pkg/planner/core/expression_rewriter.go` | Expression rewriter case |
| `pkg/executor/check_table_index.go` | `mvXorCheckBuilder` + swap in `newIndexCheckBuilder` |
| `pkg/executor/builder.go` | Remove type restriction in `indexSupportFastCheck` |
| `pkg/executor/test/admintest/admin_test.go` | Per-type admin check tests |

---

### Task 1: Parser — Add `JSON_ARRAY_XOR_CRC32` AST node and parsing rule

**Files:**
- Modify: `pkg/parser/ast/functions.go`
- Modify: `pkg/parser/parser.y`
- Modify: `pkg/parser/misc.go`

This task follows the exact same pattern as `JSON_SUM_CRC32`. Read the existing `JSONSumCrc32Expr` implementation and create a parallel `JSONArrayXorCrc32Expr`.

- [ ] **Step 1: Add AST constant and node to `functions.go`**

Add the function name constant next to `JSONSumCrc32`:
```go
JSONArrayXorCrc32 = "json_array_xor_crc32"
```

Add the AST node struct, modeled on `JSONSumCrc32Expr` (lines 619-666). Key difference: the new function takes 2 arguments (array_expr and prefix_string), whereas `JSONSumCrc32` takes 1 argument + a type. The struct should have:
```go
type JSONArrayXorCrc32Expr struct {
    node
    // Expr is the JSON array expression (first arg)
    Expr ExprNode
    // Prefix is the CONCAT_WS prefix expression (second arg)
    Prefix ExprNode
    // Tp is the target type for casting array elements (metadata, not a SQL arg)
    Tp *types.FieldType
    ExplicitCharSet bool
}
```

Implement `Restore()`, `Format()`, `Accept()` methods following the same pattern.

- [ ] **Step 2: Add parser token and rule to `misc.go` and `parser.y`**

In `misc.go`, add keyword mapping:
```go
"JSON_ARRAY_XOR_CRC32": jsonArrayXorCrc32,
```

In `parser.y`:
1. Add token declaration: `jsonArrayXorCrc32 "JSON_ARRAY_XOR_CRC32"`
2. Add parsing rule for `JSON_ARRAY_XOR_CRC32(expr AS type, expr)` syntax — the first arg is the array expression with CAST type info (same as JSON_SUM_CRC32), the second arg is the prefix expression.

Look at the existing `jsonSumCrc32` rule near line 8474 and create a parallel rule.

- [ ] **Step 3: Regenerate parser**

Run: `cd pkg/parser && make parser`
Expected: `parser.go` regenerated successfully.

- [ ] **Step 4: Run parser tests**

Run: `cd pkg/parser && go test ./... -count=1 -timeout 120s`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add pkg/parser/
git commit -m "parser: add JSON_ARRAY_XOR_CRC32 AST node and parsing rule"
```

---

### Task 2: Expression — Add function class, builtin signature, and eval

**Files:**
- Modify: `pkg/expression/builtin_json.go`
- Modify: `pkg/expression/builtin_json_vec.go`

Follow the pattern of `jsonSumCRC32FunctionClass` / `builtinJSONSumCRC32Sig` (builtin_json.go:191-343).

- [ ] **Step 1: Add function class and builtin signature**

In `builtin_json.go`, add:

```go
type jsonArrayXorCRC32FunctionClass struct {
    baseFunctionClass
}
```

With `getFunction()` that:
1. Verifies arg count = 2 (array_expr, prefix_string)
2. Returns `builtinJSONArrayXorCRC32Sig` with the target array type stored in the sig.

```go
type builtinJSONArrayXorCRC32Sig struct {
    baseBuiltinFunc
    arrayTp *types.FieldType  // target type for casting elements
}
```

- [ ] **Step 2: Implement `evalInt` for scalar evaluation**

The eval method implements the semantics from the spec:

```go
func (b *builtinJSONArrayXorCRC32Sig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
    // 1. Evaluate array_expr
    val, isNull, err := b.args[0].EvalJSON(ctx, row)
    if err != nil {
        return 0, false, err
    }

    // 2. Evaluate prefix_string
    prefix, prefixIsNull, err := b.args[1].EvalString(ctx, row)
    if err != nil {
        return 0, false, err
    }
    if prefixIsNull {
        return 0, true, nil
    }

    // 3. Handle NULL array → return CRC32(MD5(prefix))
    if isNull {
        hash := md5.Sum([]byte(prefix))
        return int64(crc32.ChecksumIEEE([]byte(fmt.Sprintf("%x", hash)))), false, nil
    }

    if val.TypeCode != types.JSONTypeCodeArray {
        return 0, false, ErrInvalidTypeForJSON.GenWithStackByArgs(1, "JSON_ARRAY_XOR_CRC32")
    }

    // 4. For each element: cast to target type, convert to string, compute CRC
    sc := ctx.GetSessionVars().StmtCtx
    ft := b.arrayTp
    separator := string([]byte{0x02})
    seen := make(map[uint32]struct{}, val.GetElemCount())
    var result uint32

    for i := range val.GetElemCount() {
        elem := val.ArrayGetElem(i)
        // Cast JSON element to target type datum
        datum, err := convertJSONToTargetDatum(sc, elem, ft)
        if err != nil {
            return 0, false, err
        }
        // Convert datum to string via standard type system
        strDatum, err := datum.ConvertTo(sc.TypeCtx(), types.NewFieldType(mysql.TypeVarString))
        if err != nil {
            return 0, false, err
        }
        str := strDatum.GetString()

        // CRC32(MD5(CONCAT_WS(0x2, prefix, str)))
        concat := prefix + separator + str
        hash := md5.Sum([]byte(concat))
        crc := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%x", hash)))

        // Dedup by CRC value, then XOR
        if _, ok := seen[crc]; !ok {
            seen[crc] = struct{}{}
            result ^= crc
        }
    }

    return int64(result), false, nil
}
```

Note: `convertJSONToTargetDatum` is a new helper function that converts a `BinaryJSON` element to a `Datum` of the target type. This can reuse logic from the existing `convertJSON2String` but returns a Datum instead of a string. You can use `types.ConvertJSONToInt`, `types.ConvertJSONToFloat`, etc. based on `ft.EvalType()`, similar to how `convertJSON2String` dispatches.

- [ ] **Step 3: Add `BuildJSONArrayXorCrc32FunctionWithCheck`**

A public builder function called from the planner, following the pattern of `BuildJSONSumCrc32FunctionWithCheck` (lines 345-366):

```go
func BuildJSONArrayXorCrc32FunctionWithCheck(ctx BuildContext, arrayExpr, prefixExpr Expression, tp *types.FieldType) (Expression, error) {
    // Similar to BuildJSONSumCrc32FunctionWithCheck but with 2 args
}
```

- [ ] **Step 4: Add vectorized eval to `builtin_json_vec.go`**

Follow the pattern of `builtinJSONSumCRC32Sig.vecEvalInt()` (builtin_json_vec.go:920-977). Same logic as scalar but processes chunks.

- [ ] **Step 5: Verify compilation**

Run: `go build ./pkg/expression/...`
Expected: Build succeeds.

- [ ] **Step 6: Commit**

```bash
git add pkg/expression/builtin_json.go pkg/expression/builtin_json_vec.go
git commit -m "expression: add JSON_ARRAY_XOR_CRC32 builtin function"
```

---

### Task 3: Planner — Add expression rewriter case

**Files:**
- Modify: `pkg/planner/core/expression_rewriter.go`

- [ ] **Step 1: Add case for `*ast.JSONArrayXorCrc32Expr`**

Find the existing case for `*ast.JSONSumCrc32Expr` (near line 1679). Add a parallel case:

```go
case *ast.JSONArrayXorCrc32Expr:
    if !er.sctx.GetSessionVars().InRestrictedSQL {
        er.err = expression.ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_ARRAY_XOR_CRC32")
        return inNode, true
    }
    // Build the array expression
    arrayExpr := er.ctxStack[len(er.ctxStack)-2]
    prefixExpr := er.ctxStack[len(er.ctxStack)-1]
    er.ctxStack = er.ctxStack[:len(er.ctxStack)-2]
    f, err := expression.BuildJSONArrayXorCrc32FunctionWithCheck(er.sctx.GetExprCtx(), arrayExpr, prefixExpr, v.Tp)
    if err != nil {
        er.err = err
        return inNode, true
    }
    er.ctxStackAppend(f, types.EmptyName)
```

Note: The exact stack manipulation depends on how the AST node's `Accept()` visits child nodes. Read the `JSONSumCrc32Expr.Accept()` pattern and make sure the `JSONArrayXorCrc32Expr.Accept()` visits both `Expr` and `Prefix`, so both are on the stack.

- [ ] **Step 2: Verify compilation**

Run: `go build ./pkg/planner/...`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add pkg/planner/core/expression_rewriter.go
git commit -m "planner: add expression rewriter for JSON_ARRAY_XOR_CRC32"
```

---

### Task 4: Unit tests for `JSON_ARRAY_XOR_CRC32`

**Files:**
- Modify: `pkg/expression/builtin_json_test.go`

- [ ] **Step 1: Add test function**

Follow the pattern of `TestJSONSumCrc32` (lines 127-231). Test cases should cover:

1. **Basic XOR**: `[1, 2, 3]` with prefix "handle" → verify CRC32(MD5(CONCAT_WS(0x2, "handle", "1"))) XOR CRC32(MD5(CONCAT_WS(0x2, "handle", "2"))) XOR CRC32(MD5(CONCAT_WS(0x2, "handle", "3")))
2. **Dedup**: `[1, 1, 2]` → same result as `[1, 2]`
3. **NULL array**: returns CRC32(MD5(prefix))
4. **Empty array `[]`**: returns 0
5. **Single element**: `[5]` → no XOR, just the single CRC
6. **Type: INT** (UNSIGNED, SIGNED)
7. **Type: STRING** (CHAR, BINARY)
8. **Type: REAL** (DOUBLE, FLOAT)

- [ ] **Step 2: Run tests**

Run: `go test ./pkg/expression/ -run TestJSONArrayXorCrc32 -v -count=1 -timeout 120s`
Expected: All PASS.

- [ ] **Step 3: Commit**

```bash
git add pkg/expression/builtin_json_test.go
git commit -m "expression: add unit tests for JSON_ARRAY_XOR_CRC32"
```

---

### Task 5: Add `mvXorCheckBuilder`

**Files:**
- Modify: `pkg/executor/check_table_index.go`

- [ ] **Step 1: Add `mvXorCheckBuilder` struct**

Add after `mvCheckBuilder` (around line 458):

```go
type mvXorCheckBuilder struct {
    tblName    string
    handleCols []string
    pkTypes    []*types.FieldType
    idxInfo    *model.IndexInfo
    tblInfo    *model.TableInfo
}
```

- [ ] **Step 2: Implement `handleChecksum()`**

Unlike `mvCheckBuilder`, this uses only handle columns (non-array cols are in the per-entry CRC now):

```go
func (b *mvXorCheckBuilder) handleChecksum() string {
    return crc32FromCols(b.handleCols)
}
```

- [ ] **Step 3: Implement `buildChecksumQuery()`**

Key: table side uses `BIT_XOR(JSON_ARRAY_XOR_CRC32(...))`, index side uses `BIT_XOR(CRC32(MD5(CONCAT_WS(...))))`.

For the table side:
- Build `CONCAT_WS(0x2, handleCols, non_array_cols)` as the prefix
- Build `JSON_ARRAY_XOR_CRC32(cast_array_expr, prefix)` as the per-row checksum
- Wrap with `BIT_XOR(...)` and GROUP BY

For the index side:
- Build `CRC32(MD5(CONCAT_WS(0x2, handleCols, non_array_cols, hidden_col)))` as per-entry checksum
- Wrap with `BIT_XOR(...)` and GROUP BY

Remember to include the JSON_LENGTH filter on the table side for empty arrays.

- [ ] **Step 4: Implement `buildCheckRowQuery()`**

Table side: per-row `JSON_ARRAY_XOR_CRC32(...)` as checksum, with handle + raw array cols for comparison.

Index side: subquery with GROUP BY handle. For each handle group:
- `BIT_XOR(CRC32(MD5(CONCAT_WS(0x2, handleCols, non_array_cols, hidden_col))))` as per-handle checksum
- `JSON_ARRAYAGG` for array values (for error reporting)
- `MIN(non_array_col)` for non-array values

Follow the existing `mvCheckBuilder.buildCheckRowQuery()` pattern but replace the SUM-based checksum with BIT_XOR-based.

- [ ] **Step 5: Implement `indexColTypes()` and `getRecords()`**

Same as `mvCheckBuilder` — array columns map to TypeJSON:

```go
func (b *mvXorCheckBuilder) indexColTypes() []*types.FieldType {
    // Same as mvCheckBuilder.indexColTypes()
}

func (b *mvXorCheckBuilder) getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error) {
    return getRecordWithChecksum(ctx, se, sql, b.tblInfo.IsCommonHandle, b.pkTypes, b.indexColTypes())
}
```

- [ ] **Step 6: Verify compilation**

Run: `go build ./pkg/executor/...`
Expected: Build succeeds.

- [ ] **Step 7: Commit**

```bash
git add pkg/executor/check_table_index.go
git commit -m "executor: add mvXorCheckBuilder with BIT_XOR-based checksums"
```

---

### Task 6: Swap builders and remove type restriction

**Files:**
- Modify: `pkg/executor/check_table_index.go` (swap in `newIndexCheckBuilder`)
- Modify: `pkg/executor/builder.go` (remove type restriction)

- [ ] **Step 1: Swap `mvXorCheckBuilder` in `newIndexCheckBuilder`**

```go
func newIndexCheckBuilder(...) indexCheckBuilder {
    if idxInfo.MVIndex {
        return &mvXorCheckBuilder{...}  // was mvCheckBuilder
    }
    return &normalCheckBuilder{...}
}
```

- [ ] **Step 2: Remove type restriction in `indexSupportFastCheck`**

In `builder.go`, find `indexSupportFastCheck` and remove the switch on `ArrayType().EvalType()`:

```go
// BEFORE:
if len(ExtractCastArrayExpr(tblCol)) > 0 {
    switch tblCol.FieldType.ArrayType().EvalType() {
    case types.ETDatetime, types.ETDuration, types.ETString, types.ETInt, types.ETReal:
    default:
        return false
    }
}

// AFTER:
// No type restriction — all CAST ARRAY types are supported.
// (just keep the ExtractCastArrayExpr check for MV index detection)
```

- [ ] **Step 3: Verify compilation**

Run: `go build ./pkg/executor/...`
Expected: Build succeeds.

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/check_table_index.go pkg/executor/builder.go
git commit -m "executor: swap to mvXorCheckBuilder, remove MV index type restriction"
```

---

### Task 7: Per-type admin check tests

**Files:**
- Modify: `pkg/executor/test/admintest/admin_test.go`

- [ ] **Step 1: Add `TestAdminCheckMVIndexAllTypes`**

Test each CAST ARRAY type with fast check enabled. For each type:
1. Create table with MV index using that type
2. Insert data with edge-case values
3. Run `admin check table` with `tidb_enable_fast_table_check = 1`
4. Verify no false positives

```go
func TestAdminCheckMVIndexAllTypes(t *testing.T) {
    store := testkit.CreateMockStore(t)
    tk := testkit.NewTestKit(t, store)
    tk.MustExec("use test")
    tk.MustExec("set tidb_enable_fast_table_check = 1")

    cases := []struct {
        name    string
        castType string
        values  []string
    }{
        {"unsigned", "UNSIGNED", []string{`'[1, 2, 3]'`, `'[0]'`, `'[]'`, `NULL`}},
        {"signed", "SIGNED", []string{`'[-1, 0, 1]'`, `'[2147483647]'`}},
        {"char", "CHAR(20)", []string{`'["hello", "world"]'`, `'[""]'`, `'[]'`}},
        {"binary", "BINARY(16)", []string{`'["abc", "def"]'`}},
        {"double", "DOUBLE", []string{`'[1.0, 2.5, 0.1]'`, `'[0.0]'`}},
        {"float", "FLOAT", []string{`'[1.5, 2.0]'`}},
        {"decimal", "DECIMAL(10,2)", []string{`'[1.10, 2.00, 3.50]'`}},
        {"date", "DATE", []string{`'["2024-01-01", "2024-12-31"]'`}},
        {"datetime", "DATETIME", []string{`'["2024-01-01 12:00:00"]'`}},
        {"time", "TIME", []string{`'["12:30:00", "23:59:59"]'`}},
        {"duplicates", "UNSIGNED", []string{`'[1, 1, 2]'`, `'[3, 3, 3]'`}},
    }

    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            tk.MustExec("DROP TABLE IF EXISTS t")
            tk.MustExec(fmt.Sprintf(
                "CREATE TABLE t (id INT PRIMARY KEY, j JSON, INDEX mvi((CAST(j AS %s ARRAY))))",
                tc.castType))
            for i, v := range tc.values {
                tk.MustExec(fmt.Sprintf("INSERT INTO t VALUES (%d, %s)", i+1, v))
            }
            tk.MustExec("ADMIN CHECK TABLE t")
        })
    }
}
```

- [ ] **Step 2: Add corruption detection test for XOR-based check**

Extend the existing `TestAdminCheckMVIndex` or add a new test that corrupts an MV index entry and verifies the fast check detects it.

- [ ] **Step 3: Run tests**

Run: `go test ./pkg/executor/test/admintest/ -run "TestAdminCheckMVIndex" -v -count=1 -timeout 300s`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/test/admintest/admin_test.go
git commit -m "executor: add per-type admin check tests for MV index fast check"
```

---

### Task 8: Delete old code

**Files:**
- Modify: `pkg/executor/check_table_index.go` — delete `mvCheckBuilder`
- Modify: `pkg/expression/builtin_json.go` — delete `JSON_SUM_CRC32` function class, builtin sig, `convertJSON2String`, `JSONCRC32Mod`
- Modify: `pkg/expression/builtin_json_vec.go` — delete `builtinJSONSumCRC32Sig.vecEvalInt`
- Modify: `pkg/planner/core/expression_rewriter.go` — delete `*ast.JSONSumCrc32Expr` case
- Modify: `pkg/parser/ast/functions.go` — delete `JSONSumCrc32Expr` and constant
- Modify: `pkg/parser/parser.y` — delete parsing rule
- Modify: `pkg/parser/misc.go` — delete keyword mapping

- [ ] **Step 1: Delete `mvCheckBuilder` from `check_table_index.go`**

Remove the entire `mvCheckBuilder` struct and all its methods. The `mvXorCheckBuilder` replaces it.

- [ ] **Step 2: Delete `JSON_SUM_CRC32` from expression layer**

In `builtin_json.go`:
- Delete `JSONCRC32Mod` constant
- Delete `jsonSumCRC32FunctionClass` and its methods
- Delete `builtinJSONSumCRC32Sig` and its methods
- Delete `convertJSON2String` function
- Delete `BuildJSONSumCrc32FunctionWithCheck` function

In `builtin_json_vec.go`:
- Delete `builtinJSONSumCRC32Sig.vecEvalInt`

- [ ] **Step 3: Delete planner case**

In `expression_rewriter.go`, delete the `*ast.JSONSumCrc32Expr` case.

- [ ] **Step 4: Delete parser definitions**

In `ast/functions.go`: delete `JSONSumCrc32` constant and `JSONSumCrc32Expr` struct.
In `parser.y`: delete the `jsonSumCrc32` token and parsing rule.
In `misc.go`: delete the keyword mapping.
Run: `cd pkg/parser && make parser` to regenerate.

- [ ] **Step 5: Remove `expression.JSONCRC32Mod` references in check_table_index.go**

After deleting `JSONCRC32Mod`, any remaining references in `mvCheckBuilder` (which should already be deleted in step 1) would cause compile errors. Verify there are no remaining references.

- [ ] **Step 6: Verify full compilation**

Run: `go build ./pkg/executor/... && go build ./pkg/expression/... && go build ./pkg/planner/...`
Expected: All build successfully.

- [ ] **Step 7: Commit**

```bash
git add pkg/executor/ pkg/expression/ pkg/planner/ pkg/parser/
git commit -m "executor,expression,planner,parser: delete JSON_SUM_CRC32 and mvCheckBuilder"
```

---

### Task 9: Full test verification

- [ ] **Step 1: Run expression tests**

Run: `go test ./pkg/expression/ -run "TestJSON" -v -count=1 -timeout 120s`
Expected: All JSON-related tests pass (including new JSON_ARRAY_XOR_CRC32 tests, minus deleted JSON_SUM_CRC32 tests).

- [ ] **Step 2: Run full admin test suite**

Run: `go test ./pkg/executor/test/admintest/ -v -count=1 -timeout 600s`
Expected: All tests pass.

- [ ] **Step 3: Run parser tests**

Run: `cd pkg/parser && go test ./... -count=1 -timeout 120s`
Expected: All pass.
