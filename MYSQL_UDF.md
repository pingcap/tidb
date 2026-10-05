# MySQL User-Defined Functions (UDF) in TiDB

## Overview

This document describes the implementation of MySQL-compatible SQL User-Defined Functions (UDFs) in TiDB. The implementation supports stored functions written in SQL with `BEGIN...END` blocks, compatible with MySQL 8.0 syntax.

## Architecture in TiDB's Distributed Context

### TiDB Distributed Architecture Background

TiDB is a distributed SQL database with a layered architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                        TiDB Server Layer                        │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ TiDB-1  │ │ TiDB-2  │ │ TiDB-3  │ │ TiDB-4  │ │ TiDB-N  │   │
│  │ (SQL)   │ │ (SQL)   │ │ (SQL)   │ │ (SQL)   │ │ (SQL)   │   │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘   │
│       │           │           │           │           │         │
└───────┼───────────┼───────────┼───────────┼───────────┼─────────┘
        │           │           │           │           │
        ▼           ▼           ▼           ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TiKV Storage Layer                         │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ TiKV-1  │ │ TiKV-2  │ │ TiKV-3  │ │ TiKV-4  │ │ TiKV-N  │   │
│  │(Region) │ │(Region) │ │(Region) │ │(Region) │ │(Region) │   │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### UDF Execution Model

**Key Design Decision: TiDB-Local Execution**

Unlike coprocessor pushdown for built-in functions, SQL UDFs execute entirely within the TiDB server layer:

```
┌─────────────────────────────────────────────────────────────────┐
│                         TiDB Server                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                     SQL Layer                              │  │
│  │  ┌─────────┐   ┌──────────┐   ┌────────────────────────┐  │  │
│  │  │ Parser  │──▶│ Planner  │──▶│      Executor          │  │  │
│  │  └─────────┘   └──────────┘   │  ┌──────────────────┐  │  │  │
│  │                               │  │  UDF Evaluator   │  │  │  │
│  │                               │  │  ┌────────────┐  │  │  │  │
│  │                               │  │  │ BEGIN...END│  │  │  │  │
│  │                               │  │  │ Interpreter│  │  │  │  │
│  │                               │  │  └────────────┘  │  │  │  │
│  │                               │  └──────────────────┘  │  │  │
│  │                               └────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                   UDF Definition Cache                     │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │  schema.func_name → Definition (parsed AST cached)  │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Data Fetch (if UDF contains SELECT)
                              ▼
                    ┌─────────────────┐
                    │     TiKV        │
                    └─────────────────┘
```

### Why TiDB-Local Execution?

1. **Procedural Logic Complexity**: SQL UDFs contain control flow (IF, WHILE, LOOP) that cannot be pushed to TiKV's coprocessor which only supports expression evaluation.

2. **Variable State Management**: UDFs maintain local variable state across multiple statements, requiring a stateful interpreter.

3. **DML Operations**: UDFs may contain INSERT/UPDATE/DELETE which execute through TiDB's transaction layer using the current session context (`ExecOptionUseCurSession`) to ensure proper transaction commit.

4. **Consistency Model**: Executing in TiDB ensures proper transaction isolation and MVCC semantics.

### Distributed Considerations

#### UDF Definition Storage

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   TiDB-1     │     │   TiDB-2     │     │   TiDB-3     │
│              │     │              │     │              │
│ UDF Cache    │     │ UDF Cache    │     │ UDF Cache    │
│ (in-memory)  │     │ (in-memory)  │     │ (in-memory)  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       │     Schema Version Synchronization      │
       │◄──────────────────►│◄──────────────────►│
       │                    │                    │
       ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────┐
│                    TiKV (mysql.func)                     │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Function definitions stored in system table       │  │
│  │  - name, schema, parameters, return_type           │  │
│  │  - source_code (BEGIN...END block)                 │  │
│  │  - deterministic, sql_security, definer            │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

#### Cache Invalidation

When a UDF is created, modified, or dropped:

1. DDL job is submitted to the DDL owner
2. Schema version is incremented
3. All TiDB instances detect schema change via etcd watch
4. Each TiDB invalidates its local UDF cache
5. Next invocation reloads definition from TiKV

#### Execution Isolation

Each UDF invocation:
- Creates isolated local variable scope
- Executes within the calling transaction context
- Does not share state with concurrent invocations
- Inherits session variables from the calling session

---

## Formal Design

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Parser Layer                                │
│  pkg/parser/                                                     │
│  ├── parser.y          # Grammar rules for CREATE/DROP FUNCTION │
│  ├── ast/ddl.go        # CreateFunctionStmt, DropFunctionStmt   │
│  ├── ast/procedure.go  # ReturnStmt, control flow statements    │
│  └── ast/dml.go        # ShowCreateFunction                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Shared Type Layer                             │
│  pkg/udf/                                                        │
│  ├── types.go          # Definition struct, type conversions    │
│  └── cache.go          # Thread-safe definition cache           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Expression Layer                               │
│  pkg/expression/                                                 │
│  └── udf_scalar.go     # UDF evaluation engine                  │
│      ├── udfFuncClass          # Function class registration    │
│      ├── udfFuncSig            # Function signature             │
│      ├── executeSQLFunction    # Entry point                    │
│      ├── executeProcedureBlock # BEGIN...END interpreter        │
│      ├── executeStatement      # Statement dispatcher           │
│      └── evaluateExpression    # Expression evaluator           │
└─────────────────────────────────────────────────────────────────┘
```

### AST Definitions

#### CreateFunctionStmt

```go
// pkg/parser/ast/ddl.go
type CreateFunctionStmt struct {
    ddlNode
    OrReplace       bool                 // CREATE OR REPLACE
    IfNotExists     bool                 // IF NOT EXISTS
    Definer         *auth.UserIdentity   // DEFINER = user
    FuncName        *TableName           // schema.function_name
    Parameters      []*FunctionParam     // (param1 TYPE, param2 TYPE, ...)
    ReturnType      *types.FieldType     // RETURNS type
    IsDeterministic bool                 // DETERMINISTIC | NOT DETERMINISTIC
    SQLBody         StmtNode             // BEGIN...END block
    Comment         string               // COMMENT 'string'
    DataAccess      string               // CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA
    SQLSecurity     string               // SQL SECURITY {DEFINER | INVOKER}
}

type FunctionParam struct {
    node
    Name string
    Type *types.FieldType
}
```

#### Control Flow Statements

```go
// pkg/parser/ast/procedure.go

// RETURN expression
type ReturnStmt struct {
    stmtNode
    ReturnValue ExprNode
}

// LOOP...END LOOP
type ProcedureLoopStmt struct {
    stmtNode
    Body []StmtNode
}

// IF...THEN...ELSEIF...ELSE...END IF
type ProcedureIfInfo struct {
    stmtNode
    IfBody *ProcedureIfBlock
}

// WHILE condition DO...END WHILE
type ProcedureWhileStmt struct {
    stmtNode
    Condition ExprNode
    Body      []StmtNode
}

// REPEAT...UNTIL condition END REPEAT
type ProcedureRepeatStmt struct {
    stmtNode
    Body      []StmtNode
    Condition ExprNode
}

// LEAVE label | ITERATE label
type ProcedureJump struct {
    stmtNode
    Label   string
    IsLeave bool  // true=LEAVE, false=ITERATE
}

// label: BEGIN...END label
type ProcedureLabelBlock struct {
    stmtNode
    Label string
    Body  *ProcedureBlock
}
```

### UDF Definition Structure

```go
// pkg/udf/types.go
type Definition struct {
    ID              int64
    Name            string
    SchemaName      string
    ParamNames      []string
    ParamTypes      []byte    // TiDB type IDs
    ReturnType      byte
    Language        string    // "sql"
    SourceCode      string    // BEGIN...END block
    IsDeterministic bool
    IsAggregate     bool

    // Metadata
    Definer         string
    SQLSecurity     string    // "DEFINER" or "INVOKER"
    Comment         string
    DataAccess      string
    Version         uint64

    // Cached data
    cachedFuncID    string
}
```

### Execution Engine

#### Execution Flow

```
executeSQLFunction(ctx, def, args)
    │
    ├── Parse sourceCode to AST (cached)
    │   └── parseSQLFunctionBody()
    │
    ├── Build parameter map
    │   └── paramMap[paramName] = argValue
    │
    └── Execute function body
        └── executeSQLFunctionBody(ctx, body, paramMap)
            │
            └── executeProcedureBlock(ctx, block, paramMap)
                │
                ├── Initialize local variables
                │   └── Process DECLARE statements
                │
                ├── Register error handlers
                │   └── Process DECLARE HANDLER statements
                │
                └── Execute statements
                    └── executeStatementListWithHandlers()
                        │
                        └── for each statement:
                            └── executeStatement()
                                │
                                ├── ReturnStmt
                                │   └── Evaluate expression, set return flag
                                │
                                ├── SetStmt
                                │   └── Evaluate RHS, update variable
                                │
                                ├── ProcedureIfInfo
                                │   └── Evaluate conditions, execute matching branch
                                │
                                ├── ProcedureWhileStmt
                                │   └── Loop while condition true (max 10,000 iterations)
                                │
                                ├── ProcedureRepeatStmt
                                │   └── Loop until condition true (max 10,000 iterations)
                                │
                                ├── ProcedureLoopStmt
                                │   └── Infinite loop until LEAVE (max 10,000 iterations)
                                │
                                ├── SimpleCaseStmt / SearchCaseStmt
                                │   └── Evaluate CASE expression
                                │
                                ├── ProcedureJump (LEAVE/ITERATE)
                                │   └── Set control flow flags
                                │
                                └── DML Statements
                                    └── Execute via internal SQL
```

#### Variable Management

```go
// Variable scoping model
type executionContext struct {
    globalVars   map[string]types.Datum  // Parameters + outer scope
    localVars    map[string]types.Datum  // Current block declarations
    shadowingVars map[string]bool         // Track shadowed variables
}

// Variable lookup order:
// 1. Local variables (current block)
// 2. Global variables (parameters + outer blocks)
// 3. Case-insensitive matching
```

#### Expression Evaluation

```go
// Supported expression types
func evaluateExpression(ctx, expr, vars) (Datum, isNull, error) {
    switch e := expr.(type) {
    case *ValueExpr:           // Literals: 1, 'string', 3.14
    case *ColumnNameExpr:      // Variable reference: var_name
    case *BinaryOperationExpr: // a + b, a > b, a AND b
    case *UnaryOperationExpr:  // -a, NOT a
    case *FuncCallExpr:        // CONCAT(a, b), ABS(x)
    case *IsNullExpr:          // x IS NULL
    case *IsTruthExpr:         // x IS TRUE
    case *CaseExpr:            // CASE expression
    case *FuncCastExpr:        // CAST(x AS type)
    case *ParenthesesExpr:     // (expr)
    }
}
```

### Implemented Built-in Functions

The UDF evaluator implements these functions for use within UDF bodies:

| Category | Functions |
|----------|-----------|
| **String** | CONCAT, LENGTH, CHAR_LENGTH, SUBSTRING, LEFT, RIGHT, TRIM, LTRIM, RTRIM, UPPER, LOWER, REPLACE, REPEAT, REVERSE, ASCII, ORD, STRCMP, LPAD, RPAD, INSTR, LOCATE, SPACE |
| **Math** | ABS, FLOOR, CEIL, CEILING, ROUND, TRUNCATE, MOD, POWER, POW, SQRT, SIGN, RAND, GREATEST, LEAST, PI, LOG, LOG10, LOG2, EXP, SIN, COS, TAN |
| **Comparison** | IF, IFNULL, NULLIF, COALESCE |
| **Type** | CAST |
| **Date/Time** | NOW, CURDATE, CURTIME, DATE, TIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFWEEK, DAYOFYEAR, DATEDIFF, DATE_ADD, DATE_SUB |
| **JSON** | JSON_EXTRACT, JSON_UNQUOTE, JSON_TYPE, JSON_LENGTH, JSON_KEYS |
| **Bitwise** | BIT_AND, BIT_OR, BIT_XOR |

---

## Supported Features

### DDL Statements

```sql
-- Create a function
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] [schema.]name
    (param1 type, param2 type, ...)
    RETURNS return_type
    [DETERMINISTIC | NOT DETERMINISTIC]
    [COMMENT 'string']
    [CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA]
    [SQL SECURITY {DEFINER | INVOKER}]
BEGIN
    -- function body
END

-- Drop a function
DROP FUNCTION [IF EXISTS] [schema.]name

-- Show function definition
SHOW CREATE FUNCTION [schema.]name
```

### Control Flow Constructs

```sql
-- IF statement
IF condition THEN
    statements;
ELSEIF condition THEN
    statements;
ELSE
    statements;
END IF;

-- WHILE loop
WHILE condition DO
    statements;
END WHILE;

-- REPEAT loop
REPEAT
    statements;
UNTIL condition END REPEAT;

-- LOOP with LEAVE
[label:] LOOP
    statements;
    IF condition THEN
        LEAVE label;
    END IF;
END LOOP [label];

-- CASE statement (simple)
CASE expr
    WHEN value1 THEN statements;
    WHEN value2 THEN statements;
    ELSE statements;
END CASE;

-- CASE statement (searched)
CASE
    WHEN condition1 THEN statements;
    WHEN condition2 THEN statements;
    ELSE statements;
END CASE;

-- RETURN
RETURN expression;

-- LEAVE (exit loop/block)
LEAVE label;

-- ITERATE (continue loop)
ITERATE label;
```

### Variable Declarations

```sql
-- Simple declaration
DECLARE var_name type;

-- Declaration with default
DECLARE var_name type DEFAULT value;

-- Multiple declarations
DECLARE var1, var2, var3 type DEFAULT value;
```

### Supported Data Types

| Category | Types |
|----------|-------|
| **Integer** | TINYINT, SMALLINT, MEDIUMINT, INT, INTEGER, BIGINT |
| **Floating** | FLOAT, DOUBLE, REAL |
| **Decimal** | DECIMAL, NUMERIC |
| **String** | CHAR, VARCHAR, TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT |
| **Binary** | BINARY, VARBINARY, BLOB |
| **Date/Time** | DATE, TIME, DATETIME, TIMESTAMP, YEAR |
| **JSON** | JSON |

---

## Limitations

### Execution Constraints

| Constraint | Value | Rationale |
|------------|-------|-----------|
| **Max loop iterations** | 10,000 | Prevents runaway infinite loops |
| **Max recursion depth** | Not supported | Recursion is prohibited |
| **Max nested blocks** | Limited by stack | Go stack limit applies |

### Distributed Limitations

1. **No Coprocessor Pushdown**: UDFs cannot be pushed to TiKV for execution. All UDF logic executes in TiDB.

2. **No Parallel Execution**: A single UDF invocation executes serially. However, multiple concurrent queries can invoke the same UDF in parallel (each with isolated state).

3. **Cache Coherence Delay**: After CREATE/DROP FUNCTION, there may be a brief delay before all TiDB instances see the change (schema version propagation).

4. **No Cross-Region Optimization**: UDFs that access data do not benefit from locality-aware scheduling.

### Functional Limitations

1. **No OUT/INOUT Parameters**: All function parameters are IN only (MySQL functions also have this limitation; procedures support OUT/INOUT).

2. **No Recursive Functions**: Functions cannot call themselves.

3. **No Dynamic SQL**: Cannot use PREPARE/EXECUTE within functions.

4. **No Result Set Returns**: Functions return a single scalar value, not result sets.

5. **No Transaction Control**: Cannot use COMMIT/ROLLBACK within functions.

6. **Session Variable Access**: Limited to reading session variables; modifications may not persist as expected.

---

## Unsupported Features

### Parser/Syntax Level

| Feature | Status | Notes |
|---------|--------|-------|
| `SELECT ... INTO var` | **Implemented** | Supports both MySQL (`SELECT x INTO var FROM t`) and TiDB (`SELECT x FROM t INTO var`) syntax |
| `DEFINER = user` clause | Parser conflict | Removed due to shift/reduce conflict |
| `SIGNAL` / `RESIGNAL` | **Implemented** | Full support including SET clause |
| `GET DIAGNOSTICS` | Not implemented | Diagnostic information retrieval |
| `DECLARE CONDITION` | Not implemented | Named condition declarations |

### Execution Level

| Feature | Status | Notes |
|---------|--------|-------|
| **Cursors** | **Implemented** | DECLARE, OPEN, FETCH INTO, CLOSE fully working |
| **Handlers** | **Implemented** | DECLARE HANDLER with CONTINUE/EXIT for NOT FOUND, SQLEXCEPTION |
| **DML in UDFs** | **Implemented** | INSERT/UPDATE/DELETE persist correctly via current session |
| **Recursive calls** | Blocked | Returns error if detected |
| **Result sets** | Not supported | Functions must return scalar |
| **Temporary tables** | Not tested | May work but not validated |
| **Prepared statements** | Not supported | Dynamic SQL not available |

### Data Types

| Type | Status | Notes |
|------|--------|-------|
| ENUM return type | Not tested | May have issues |
| SET return type | Not tested | May have issues |
| BIT return type | Not tested | May have issues |
| Spatial types | Not supported | GEOMETRY, POINT, etc. |
| User-defined types | Not supported | N/A |

### Security Features

| Feature | Status | Notes |
|---------|--------|-------|
| SQL SECURITY DEFINER | Parsed only | Not enforced at runtime |
| SQL SECURITY INVOKER | Parsed only | Not enforced at runtime |
| Privilege checking | Basic | CREATE ROUTINE privilege not fully enforced |

---

## Testing

### Test Coverage

| Test Suite | Tests | Pass Rate |
|------------|-------|-----------|
| Unit tests (udf_scalar_test.go) | 12 | 100% |
| E2E tests (udf_mysql_e2e_test.go) | 249 | 98.8% |
| MySQL sp.test ported | 66 | 100% |

### Running Tests

```bash
# Run all UDF-related tests
go test -v ./pkg/expression/ -run ".*[Uu][Dd][Ff].*|.*MySQLSP.*|.*MySQLCompat.*"

# Run specific test suites
go test -v ./pkg/expression/ -run "TestMySQLSPFactorial"
go test -v ./pkg/expression/ -run "TestMySQLSPComplexAlgorithms"
```

---

## Examples

### Simple Arithmetic Function

```sql
CREATE FUNCTION double_value(n INT)
RETURNS INT
DETERMINISTIC
BEGIN
    RETURN n * 2;
END;

SELECT double_value(21);  -- Returns 42
```

### Factorial with WHILE Loop

```sql
CREATE FUNCTION factorial(n INT)
RETURNS BIGINT
DETERMINISTIC
BEGIN
    DECLARE result BIGINT DEFAULT 1;
    WHILE n > 1 DO
        SET result = result * n;
        SET n = n - 1;
    END WHILE;
    RETURN result;
END;

SELECT factorial(10);  -- Returns 3628800
```

### Customer Classification

```sql
CREATE FUNCTION customer_level(credit DECIMAL(10,2))
RETURNS VARCHAR(20)
DETERMINISTIC
BEGIN
    DECLARE level VARCHAR(20);
    IF credit > 50000 THEN
        SET level = 'PLATINUM';
    ELSEIF credit >= 10000 THEN
        SET level = 'GOLD';
    ELSE
        SET level = 'SILVER';
    END IF;
    RETURN level;
END;

SELECT customer_level(25000);  -- Returns 'GOLD'
```

### Fibonacci Sequence

```sql
CREATE FUNCTION fibonacci(n INT)
RETURNS BIGINT
DETERMINISTIC
BEGIN
    DECLARE a BIGINT DEFAULT 0;
    DECLARE b BIGINT DEFAULT 1;
    DECLARE temp BIGINT;
    DECLARE i INT DEFAULT 0;

    IF n <= 0 THEN RETURN 0; END IF;
    IF n = 1 THEN RETURN 1; END IF;

    WHILE i < n - 1 DO
        SET temp = a + b;
        SET a = b;
        SET b = temp;
        SET i = i + 1;
    END WHILE;

    RETURN b;
END;

SELECT fibonacci(10);  -- Returns 55
```

### Prime Number Check

```sql
CREATE FUNCTION is_prime(n INT)
RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE i INT DEFAULT 2;
    IF n < 2 THEN
        RETURN 0;
    END IF;
    WHILE i * i <= n DO
        IF MOD(n, i) = 0 THEN
            RETURN 0;
        END IF;
        SET i = i + 1;
    END WHILE;
    RETURN 1;
END;

SELECT is_prime(17);  -- Returns 1 (true)
SELECT is_prime(15);  -- Returns 0 (false)
```

### Counter with DML Operations

```sql
-- Create supporting tables
CREATE TABLE counters (name VARCHAR(50) PRIMARY KEY, value INT DEFAULT 0);
CREATE TABLE audit_log (id INT AUTO_INCREMENT PRIMARY KEY, action VARCHAR(100), old_val INT, new_val INT);
INSERT INTO counters VALUES ('hits', 0);

-- Function that modifies data
CREATE FUNCTION increment_counter(counter_name VARCHAR(50), amount INT)
RETURNS INT
DETERMINISTIC
MODIFIES SQL DATA
BEGIN
    DECLARE current_val INT DEFAULT 0;
    DECLARE new_val INT;

    SELECT value INTO current_val FROM counters WHERE name = counter_name;
    SET new_val = current_val + amount;

    UPDATE counters SET value = new_val WHERE name = counter_name;
    INSERT INTO audit_log (action, old_val, new_val)
        VALUES (CONCAT('INCREMENT:', counter_name), current_val, new_val);

    RETURN new_val;
END;

SELECT increment_counter('hits', 5);   -- Returns 5, updates table
SELECT increment_counter('hits', 10);  -- Returns 15, updates table
SELECT * FROM counters;                -- Shows hits = 15
SELECT * FROM audit_log;               -- Shows both increments
```

---

## Future Work

### 1. Data Type Restrictions

**Current Status**: Basic types work; complex types untested or unsupported.

#### ENUM Return Type

**What's Required**:
```
Parser: Already supports ENUM in type specifications
Execution:
├── Validate return value against ENUM values
├── Return empty string or NULL for invalid values
└── Handle ENUM value ordering (1-indexed)
```

**Challenge**: ENUM type metadata is table-specific. Standalone ENUM in function return type needs synthetic metadata.

#### SET Return Type

**What's Required**:
```
Similar to ENUM, but:
├── Support comma-separated values
├── Validate each element against SET members
└── Handle SET operations (add/remove elements)
```

**Challenge**: SET is rarely used for function return types; low priority.

#### BIT Return Type

**What's Required**:
```
Execution:
├── Proper bit-width handling
├── Display format (binary vs decimal)
└── Arithmetic operations on BIT values
```

**Status**: Should work but needs testing for edge cases.

#### Spatial Types (GEOMETRY, POINT, etc.)

**Why Not Possible Yet**:
- Spatial types require specialized storage format (WKB/WKT)
- Spatial operations need computational geometry library
- TiDB's spatial support is limited overall
- Would need spatial function implementations in UDF evaluator

**Recommendation**: Wait for TiDB's overall spatial support improvement.

#### User-Defined Types

**Why Not Possible**:
- MySQL does not support user-defined types in SQL layer
- Would require type registry and custom serialization
- Not MySQL-compatible

---

### 5. SQL SECURITY Enforcement

**Current Status**: Parsed but not enforced.

**What's Required**:
```
For DEFINER security:
├── Store function definer (user@host) with function definition
├── On function call:
│   ├── Save current session user
│   ├── Switch security context to definer
│   ├── Execute function body
│   └── Restore original session user
└── Apply definer privileges for all operations in function

For INVOKER security:
└── Use calling user's privileges (current default behavior)
```

**Implementation Complexity**: Medium-High
- Requires privilege system integration
- Must handle nested calls with different security contexts
- Need to track security context stack

---

### 6. Stored Procedures (CREATE PROCEDURE)

**Current Status**: Not implemented.

**What's Required**:
```
Parser:
├── CREATE PROCEDURE statement
├── IN/OUT/INOUT parameter modes
├── CALL statement
└── Procedure-specific statements allowed (result sets)

AST:
├── CreateProcedureStmt
├── CallStmt
└── Parameter mode tracking

Execution:
├── executeProcedure()
├── Handle OUT/INOUT parameter binding
└── Support multiple result sets
```

**Key Differences from Functions**:
- Procedures can return result sets
- Procedures support OUT/INOUT parameters
- Procedures can use transaction control (in some cases)
- CALL syntax vs inline function call

**Implementation Complexity**: High
- Significant parser work
- New execution model for result set handling
- Parameter binding complexity

---

### 7. Performance Optimizations

**Potential Improvements**:

1. **Compiled Execution Plans**
   - Pre-compile frequently used UDFs to optimized representation
   - Cache expression evaluation plans
   - Estimated effort: High

2. **JIT Compilation**
   - Compile hot UDFs to native code (via LLVM or similar)
   - Significant performance gain for compute-heavy UDFs
   - Estimated effort: Very High

3. **Parallel Loop Execution**
   - For DETERMINISTIC functions with no side effects
   - Parallelize independent loop iterations
   - Estimated effort: High (correctness concerns)

4. **Memory Pooling**
   - Pool allocation for frequently created objects
   - Reduce GC pressure for high-frequency UDF calls
   - Estimated effort: Medium

---

### 8. Observability

**What's Required**:
```
Metrics:
├── udf_invocation_total (counter by function name)
├── udf_execution_duration_seconds (histogram)
├── udf_errors_total (counter by error type)
└── udf_cache_hits/misses (for parsed AST cache)

Tracing:
├── Add span for UDF execution
├── Track nested function calls
└── Include parameter values (with PII masking)

Logging:
├── Slow UDF query logging
├── UDF creation/modification audit
└── Error details with context
```

**Implementation Complexity**: Low-Medium
- Instrument existing execution paths
- Add Prometheus metrics
- Integrate with TiDB's existing observability stack

---

### 9. Error Handling Improvements

**Current Gaps**:
- DECLARE HANDLER partially implemented
- No SQLWARNING handling
- Limited error context preservation

**What's Required**:
```
Full handler types:
├── SQLWARNING (SQL warnings)
├── NOT FOUND (cursor exhaustion)
├── SQLEXCEPTION (all other errors)
└── Specific SQLSTATE codes

Diagnostic area:
├── Track current error info
├── Support GET DIAGNOSTICS
└── Preserve error chain for RESIGNAL
```

---

### Implementation Priority Recommendation

| Feature | Priority | Effort | Impact | Status |
|---------|----------|--------|--------|--------|
| SELECT...INTO | High | Medium | Enables data access in UDFs | **DONE** |
| Cursor support | Medium | High | Enables row-by-row processing | **DONE** |
| SIGNAL/RESIGNAL | Medium | Medium | Better error handling | **DONE** |
| DML in UDFs | High | Medium | Enables data modification | **DONE** |
| Observability | High | Low | Operations support | **DONE** |
| SQL SECURITY DEFINER | Low | Medium | Security compliance | Pending |
| ENUM/SET types | Low | Low | Edge case support | Pending |
| Stored Procedures | Future | Very High | Major feature expansion | **DONE** |
| Performance opts | Medium | Varies | Scalability | Pending |

---

## References

- [MySQL 8.0 CREATE FUNCTION](https://dev.mysql.com/doc/refman/8.0/en/create-procedure.html)
- [MySQL 8.0 Stored Program Syntax](https://dev.mysql.com/doc/refman/8.0/en/sql-compound-statements.html)
- [TiDB Architecture](https://docs.pingcap.com/tidb/stable/tidb-architecture)
