// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/udf"
	"github.com/stretchr/testify/require"
)

func TestUDFFuncClassBasics(t *testing.T) {
	// Test creating a UDF function class
	def := &udf.Definition{
		ID:              1,
		Name:            "test_add",
		SchemaName:      "test",
		ParamNames:      []string{"a", "b"},
		ParamTypes:      []byte{byte(udf.TypeIDInt), byte(udf.TypeIDInt)},
		ReturnType:      byte(udf.TypeIDInt),
		Language:        "javascript",
		SourceCode:      "return a + b;",
		IsDeterministic: true,
		IsAggregate:     false,
	}

	fc := newUDFFuncClass(def)
	require.NotNil(t, fc)
	require.Equal(t, "test_add", fc.funcName)
	require.Equal(t, 2, fc.minArgs)
	require.Equal(t, 2, fc.maxArgs)
}

func TestUDFTypeIDToEvalType(t *testing.T) {
	tests := []struct {
		typeID   int32
		expected string
	}{
		{udf.TypeIDInt, "Int"},
		{udf.TypeIDUint, "Int"},
		{udf.TypeIDFloat, "Real"},
		{udf.TypeIDString, "String"},
		{udf.TypeIDDecimal, "Decimal"},
		{udf.TypeIDDatetime, "Datetime"},
		{udf.TypeIDJSON, "Json"},
		{99, "String"}, // Unknown type defaults to string
	}

	for _, tc := range tests {
		got := udf.TypeIDToEvalType(tc.typeID)
		require.Equal(t, tc.expected, got.String(), "TypeID %d", tc.typeID)
	}
}

func TestUDFCacheClear(t *testing.T) {
	// Add an entry to the cache
	def := &udf.Definition{
		ID:         1,
		Name:       "cached_func",
		SchemaName: "test",
	}
	fc := newUDFFuncClass(def)
	udfFuncs.Store("test.cached_func", fc)

	// Verify it's in the cache
	_, ok := udfFuncs.Load("test.cached_func")
	require.True(t, ok)

	// Clear the specific entry
	ClearUDFCacheEntry("test", "cached_func")
	_, ok = udfFuncs.Load("test.cached_func")
	require.False(t, ok)

	// Add it back and test full cache clear
	udfFuncs.Store("test.cached_func", fc)
	udfFuncs.Store("test.another_func", fc)

	ClearUDFCache()
	_, ok = udfFuncs.Load("test.cached_func")
	require.False(t, ok)
	_, ok = udfFuncs.Load("test.another_func")
	require.False(t, ok)
}

func TestProcedureCache(t *testing.T) {
	// Create a procedure definition
	procDef := &udf.ProcedureDefinition{
		ID:         1,
		Name:       "test_proc",
		SchemaName: "test",
		Params: []udf.ProcedureParam{
			{Name: "p_in", Mode: udf.ParamModeIn},
			{Name: "p_out", Mode: udf.ParamModeOut},
		},
		SourceCode: "BEGIN SET p_out = p_in * 2; END",
	}

	// Register the procedure
	RegisterProcedure(procDef)

	// Verify it's in the cache
	retrieved := GetProcedure("test", "test_proc")
	require.NotNil(t, retrieved)
	require.Equal(t, "test_proc", retrieved.Name)
	require.Equal(t, 2, len(retrieved.Params))

	// Verify case-insensitive lookup
	retrieved = GetProcedure("TEST", "TEST_PROC")
	require.NotNil(t, retrieved)

	// Clear the procedure
	ClearProcedureCacheEntry("test", "test_proc")
	retrieved = GetProcedure("test", "test_proc")
	require.Nil(t, retrieved)
}

// TestParseSQLFunctionBody tests parsing of SQL function bodies.
func TestParseSQLFunctionBody(t *testing.T) {
	p := parser.New()

	tests := []struct {
		name       string
		sourceCode string
		wantErr    bool
	}{
		{
			name:       "simple return",
			sourceCode: "BEGIN RETURN n * 2; END",
			wantErr:    false,
		},
		{
			name:       "return with variables",
			sourceCode: "BEGIN DECLARE result INT DEFAULT 0; RETURN result; END",
			wantErr:    false,
		},
		{
			name:       "return with expression",
			sourceCode: "BEGIN RETURN a + b; END",
			wantErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.sourceCode)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, body)
			}
		})
	}
}

// TestEvaluateBinaryOp tests binary operation evaluation.
func TestEvaluateBinaryOp(t *testing.T) {
	tests := []struct {
		name     string
		op       opcode.Op
		left     types.Datum
		right    types.Datum
		expected float64
		isNull   bool
	}{
		{
			name:     "addition",
			op:       opcode.Plus,
			left:     types.NewIntDatum(10),
			right:    types.NewIntDatum(5),
			expected: 15,
		},
		{
			name:     "subtraction",
			op:       opcode.Minus,
			left:     types.NewIntDatum(10),
			right:    types.NewIntDatum(3),
			expected: 7,
		},
		{
			name:     "multiplication",
			op:       opcode.Mul,
			left:     types.NewIntDatum(4),
			right:    types.NewIntDatum(3),
			expected: 12,
		},
		{
			name:     "division",
			op:       opcode.Div,
			left:     types.NewIntDatum(10),
			right:    types.NewIntDatum(2),
			expected: 5,
		},
		{
			name:   "division by zero returns NULL",
			op:     opcode.Div,
			left:   types.NewIntDatum(10),
			right:  types.NewIntDatum(0),
			isNull: true,
		},
		{
			name:   "null left operand",
			op:     opcode.Plus,
			left:   types.Datum{},
			right:  types.NewIntDatum(5),
			isNull: true,
		},
		{
			name:     "greater than true",
			op:       opcode.GT,
			left:     types.NewIntDatum(10),
			right:    types.NewIntDatum(5),
			expected: 1,
		},
		{
			name:     "greater than false",
			op:       opcode.GT,
			left:     types.NewIntDatum(3),
			right:    types.NewIntDatum(5),
			expected: 0,
		},
		{
			name:     "equal true",
			op:       opcode.EQ,
			left:     types.NewIntDatum(5),
			right:    types.NewIntDatum(5),
			expected: 1,
		},
		{
			name:     "equal false",
			op:       opcode.EQ,
			left:     types.NewIntDatum(5),
			right:    types.NewIntDatum(10),
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, isNull, err := evaluateBinaryOp(tc.op, tc.left, tc.right)
			require.NoError(t, err)
			if tc.isNull {
				require.True(t, isNull || result.IsNull())
			} else {
				floatVal, err := result.ToFloat64(types.DefaultStmtNoWarningContext)
				require.NoError(t, err)
				require.Equal(t, tc.expected, floatVal)
			}
		})
	}
}

// TestEvaluateUnaryOp tests unary operation evaluation.
func TestEvaluateUnaryOp(t *testing.T) {
	tests := []struct {
		name     string
		op       opcode.Op
		val      types.Datum
		expected float64
		isNull   bool
	}{
		{
			name:     "negate positive",
			op:       opcode.Minus,
			val:      types.NewIntDatum(5),
			expected: -5,
		},
		{
			name:     "negate negative",
			op:       opcode.Minus,
			val:      types.NewIntDatum(-3),
			expected: 3,
		},
		{
			name:     "not zero",
			op:       opcode.Not,
			val:      types.NewIntDatum(0),
			expected: 1,
		},
		{
			name:     "not non-zero",
			op:       opcode.Not,
			val:      types.NewIntDatum(1),
			expected: 0,
		},
		{
			name:   "null value",
			op:     opcode.Minus,
			val:    types.Datum{},
			isNull: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, isNull, err := evaluateUnaryOp(tc.op, tc.val)
			require.NoError(t, err)
			if tc.isNull {
				require.True(t, isNull || result.IsNull())
			} else {
				floatVal, err := result.ToFloat64(types.DefaultStmtNoWarningContext)
				require.NoError(t, err)
				require.Equal(t, tc.expected, floatVal)
			}
		})
	}
}

// TestSQLFunctionExecution tests end-to-end SQL function execution.
func TestSQLFunctionExecution(t *testing.T) {
	p := parser.New()

	tests := []struct {
		name       string
		sourceCode string
		params     map[string]types.Datum
		expected   float64
	}{
		{
			name:       "simple return constant",
			sourceCode: "BEGIN RETURN 42; END",
			params:     map[string]types.Datum{},
			expected:   42,
		},
		{
			name:       "return parameter",
			sourceCode: "BEGIN RETURN n; END",
			params:     map[string]types.Datum{"n": types.NewIntDatum(10)},
			expected:   10,
		},
		{
			name:       "multiply parameter",
			sourceCode: "BEGIN RETURN n * 2; END",
			params:     map[string]types.Datum{"n": types.NewIntDatum(5)},
			expected:   10,
		},
		{
			name:       "add two parameters",
			sourceCode: "BEGIN RETURN a + b; END",
			params: map[string]types.Datum{
				"a": types.NewIntDatum(3),
				"b": types.NewIntDatum(7),
			},
			expected: 10,
		},
		{
			name:       "complex expression",
			sourceCode: "BEGIN RETURN (a + b) * 2; END",
			params: map[string]types.Datum{
				"a": types.NewIntDatum(3),
				"b": types.NewIntDatum(7),
			},
			expected: 20,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.sourceCode)
			require.NoError(t, err)
			require.NotNil(t, body)

			result, isNull, err := executeSQLFunctionBody(nil, body, tc.params)
			require.NoError(t, err)
			require.False(t, isNull, "Result should not be null")

			floatVal, err := result.ToFloat64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.Equal(t, tc.expected, floatVal)
		})
	}
}

// TestLogicalOperators tests AND, OR logical operators.
func TestLogicalOperators(t *testing.T) {
	tests := []struct {
		name     string
		op       opcode.Op
		left     types.Datum
		right    types.Datum
		expected int64
	}{
		{
			name:     "AND true and true",
			op:       opcode.LogicAnd,
			left:     types.NewIntDatum(1),
			right:    types.NewIntDatum(1),
			expected: 1,
		},
		{
			name:     "AND true and false",
			op:       opcode.LogicAnd,
			left:     types.NewIntDatum(1),
			right:    types.NewIntDatum(0),
			expected: 0,
		},
		{
			name:     "AND false and false",
			op:       opcode.LogicAnd,
			left:     types.NewIntDatum(0),
			right:    types.NewIntDatum(0),
			expected: 0,
		},
		{
			name:     "OR true or true",
			op:       opcode.LogicOr,
			left:     types.NewIntDatum(1),
			right:    types.NewIntDatum(1),
			expected: 1,
		},
		{
			name:     "OR true or false",
			op:       opcode.LogicOr,
			left:     types.NewIntDatum(1),
			right:    types.NewIntDatum(0),
			expected: 1,
		},
		{
			name:     "OR false or false",
			op:       opcode.LogicOr,
			left:     types.NewIntDatum(0),
			right:    types.NewIntDatum(0),
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, isNull, err := evaluateBinaryOp(tc.op, tc.left, tc.right)
			require.NoError(t, err)
			require.False(t, isNull)
			intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intVal)
		})
	}
}

// TestIntegerDivisionAndModulo tests DIV and MOD operators.
func TestIntegerDivisionAndModulo(t *testing.T) {
	tests := []struct {
		name     string
		op       opcode.Op
		left     types.Datum
		right    types.Datum
		expected int64
		isNull   bool
	}{
		{
			name:     "integer division",
			op:       opcode.IntDiv,
			left:     types.NewIntDatum(10),
			right:    types.NewIntDatum(3),
			expected: 3,
		},
		{
			name:     "integer division exact",
			op:       opcode.IntDiv,
			left:     types.NewIntDatum(12),
			right:    types.NewIntDatum(4),
			expected: 3,
		},
		{
			name:   "integer division by zero",
			op:     opcode.IntDiv,
			left:   types.NewIntDatum(10),
			right:  types.NewIntDatum(0),
			isNull: true,
		},
		{
			name:     "modulo",
			op:       opcode.Mod,
			left:     types.NewIntDatum(10),
			right:    types.NewIntDatum(3),
			expected: 1,
		},
		{
			name:     "modulo exact",
			op:       opcode.Mod,
			left:     types.NewIntDatum(12),
			right:    types.NewIntDatum(4),
			expected: 0,
		},
		{
			name:   "modulo by zero",
			op:     opcode.Mod,
			left:   types.NewIntDatum(10),
			right:  types.NewIntDatum(0),
			isNull: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, isNull, err := evaluateBinaryOp(tc.op, tc.left, tc.right)
			require.NoError(t, err)
			if tc.isNull {
				require.True(t, isNull || result.IsNull())
			} else {
				intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
				require.NoError(t, err)
				require.Equal(t, tc.expected, intVal)
			}
		})
	}
}

// NOTE: TestBuiltinFunctions removed - covered by TestE2EBuiltinFunctions and TestMySQLCompatAllStringFunctions

// TestCoalesceAndIfNull tests COALESCE and IFNULL functions.
func TestCoalesceAndIfNull(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// Test COALESCE - first non-null value
	body, err := parseSQLFunctionBody(p, "BEGIN RETURN COALESCE(NULL, NULL, 5); END")
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(5), intVal)

	// Test IFNULL with non-null first value
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN IFNULL(10, 20); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err = result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(10), intVal)
}

// TestIFFunction tests the IF() function.
func TestIFFunction(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// Test IF with true condition
	body, err := parseSQLFunctionBody(p, "BEGIN RETURN IF(1, 'yes', 'no'); END")
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	strVal, err := result.ToString()
	require.NoError(t, err)
	require.Equal(t, "yes", strVal)

	// Test IF with false condition
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN IF(0, 'yes', 'no'); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	strVal, err = result.ToString()
	require.NoError(t, err)
	require.Equal(t, "no", strVal)
}

// TestGreatestLeast tests GREATEST and LEAST functions.
func TestGreatestLeast(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// Test GREATEST
	body, err := parseSQLFunctionBody(p, "BEGIN RETURN GREATEST(1, 5, 3, 2); END")
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(5), intVal)

	// Test LEAST
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN LEAST(1, 5, 3, 2); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err = result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(1), intVal)
}

// NOTE: TestNewStringFunctions removed - covered by TestMySQLCompatAllStringFunctions and TestMySQLCompatInstrLocateFunctions

// NOTE: TestNewMathFunctions removed - covered by TestMySQLCompatAllMathFunctions

// TestCursorContext tests the cursor context management.
func TestCursorContext(t *testing.T) {
	// Test newCursorContext
	ctx := newCursorContext()
	require.NotNil(t, ctx)
	require.NotNil(t, ctx.cursors)
	require.Len(t, ctx.cursors, 0)

	// Test declareCursor
	ctx.declareCursor("test_cursor", nil)
	require.Len(t, ctx.cursors, 1)

	// Test getCursor
	cursor := ctx.getCursor("test_cursor")
	require.NotNil(t, cursor)
	require.Equal(t, "test_cursor", cursor.def.name)
	require.False(t, cursor.isOpen)
	require.Nil(t, cursor.rows)
	require.Equal(t, 0, cursor.position)

	// Test getCursor case insensitivity
	cursor = ctx.getCursor("TEST_CURSOR")
	require.NotNil(t, cursor)

	// Test getCursor not found
	cursor = ctx.getCursor("nonexistent")
	require.Nil(t, cursor)
}

// TestCursorStateManagement tests cursor open/close state management.
func TestCursorStateManagement(t *testing.T) {
	ctx := newCursorContext()
	ctx.declareCursor("my_cursor", nil)

	cursor := ctx.getCursor("my_cursor")
	require.NotNil(t, cursor)

	// Initially closed
	require.False(t, cursor.isOpen)

	// Simulate opening
	cursor.isOpen = true
	cursor.rows = [][]types.Datum{
		{types.NewIntDatum(1), types.NewStringDatum("a")},
		{types.NewIntDatum(2), types.NewStringDatum("b")},
		{types.NewIntDatum(3), types.NewStringDatum("c")},
	}
	cursor.position = 0

	// Verify open state
	require.True(t, cursor.isOpen)
	require.Len(t, cursor.rows, 3)

	// Simulate fetching
	require.Equal(t, 0, cursor.position)
	cursor.position++
	require.Equal(t, 1, cursor.position)

	// Simulate closing
	cursor.isOpen = false
	cursor.rows = nil
	cursor.position = 0

	require.False(t, cursor.isOpen)
	require.Nil(t, cursor.rows)
}

// TestCursorContextInVars tests storing cursor context in vars map.
func TestCursorContextInVars(t *testing.T) {
	vars := make(map[string]types.Datum)

	// Initially no cursor context
	ctx := getCursorContext(vars)
	require.Nil(t, ctx)

	// Create and store cursor context
	cursorCtx := newCursorContext()
	cursorCtx.declareCursor("test", nil)

	var datum types.Datum
	datum.SetInterface(cursorCtx)
	vars[cursorContextKey] = datum

	// Retrieve cursor context
	ctx = getCursorContext(vars)
	require.NotNil(t, ctx)
	require.NotNil(t, ctx.getCursor("test"))
}

// TestHandlerContext tests the error handler context.
func TestHandlerContext(t *testing.T) {
	// Test newHandlerContext
	ctx := newHandlerContext()
	require.NotNil(t, ctx)
	require.NotNil(t, ctx.handlers)
	require.Len(t, ctx.handlers, 0)

	// Test addHandler
	handler := &handlerDef{
		controlType: 1,
		conditions:  nil,
		statement:   nil,
	}
	ctx.addHandler(handler)
	require.Len(t, ctx.handlers, 1)

	// Add another handler
	handler2 := &handlerDef{
		controlType: 2,
		conditions:  nil,
		statement:   nil,
	}
	ctx.addHandler(handler2)
	require.Len(t, ctx.handlers, 2)
}

// NOTE: TestWhileLoopStatement removed - covered by TestE2EWhileLoop

// NOTE: TestRepeatLoopStatement removed - covered by TestE2ERepeatLoop

// NOTE: TestNullHandlingInNewFunctions removed - covered by TestMySQLCompatNullHandling

// TestLogDomainErrors tests LOG functions with invalid domain values.
func TestLogDomainErrors(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// LOG of negative number should return NULL
	body, err := parseSQLFunctionBody(p, "BEGIN RETURN LOG(-1); END")
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.True(t, isNull || result.IsNull())

	// LOG of zero should return NULL
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN LOG(0); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.True(t, isNull || result.IsNull())

	// ASIN out of range should return NULL
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN ASIN(2); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.True(t, isNull || result.IsNull())

	// ACOS out of range should return NULL
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN ACOS(2); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.True(t, isNull || result.IsNull())
}

// TestConcatWsWithNulls tests CONCAT_WS skipping NULL values.
func TestConcatWsWithNulls(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// CONCAT_WS should skip NULLs
	body, err := parseSQLFunctionBody(p, "BEGIN RETURN CONCAT_WS(',', 'a', NULL, 'b', NULL, 'c'); END")
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	strVal, err := result.ToString()
	require.NoError(t, err)
	require.Equal(t, "a,b,c", strVal)
}

// TestEltOutOfRange tests ELT with out of range index.
func TestEltOutOfRange(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// ELT with index 0 should return NULL
	body, err := parseSQLFunctionBody(p, "BEGIN RETURN ELT(0, 'a', 'b', 'c'); END")
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.True(t, isNull || result.IsNull())

	// ELT with index beyond list should return NULL
	body, err = parseSQLFunctionBody(p, "BEGIN RETURN ELT(10, 'a', 'b', 'c'); END")
	require.NoError(t, err)
	result, isNull, err = executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.True(t, isNull || result.IsNull())
}

// NOTE: TestSimpleCaseStatement removed - covered by TestE2ECaseStatement

// NOTE: TestSearchedCaseStatement removed - covered by TestE2ECaseStatement

// NOTE: TestLoopWithLeave removed - covered by TestE2ELoopWithLeaveIterate

// NOTE: TestLoopWithIterate removed - covered by TestE2ELoopWithLeaveIterate

// TestNestedControlFlow tests nested IF/WHILE/LOOP statements.
func TestNestedControlFlow(t *testing.T) {
	p := parser.New()

	// Nested loops counting
	body := `BEGIN
		DECLARE i INT DEFAULT 0;
		DECLARE j INT DEFAULT 0;
		DECLARE count INT DEFAULT 0;
		WHILE i < 3 DO
			SET j = 0;
			WHILE j < 4 DO
				SET count = count + 1;
				SET j = j + 1;
			END WHILE;
			SET i = i + 1;
		END WHILE;
		RETURN count;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	// 3 * 4 = 12 iterations
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(12), intVal)
}

// TestLabeledBlock tests BEGIN...END with labels.
func TestLabeledBlock(t *testing.T) {
	p := parser.New()

	body := `BEGIN
		DECLARE result INT DEFAULT 0;
		myblock: BEGIN
			DECLARE x INT DEFAULT 10;
			SET result = x * 2;
		END myblock;
		RETURN result;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(20), intVal)
}

// TestErrorHandlerMatching tests the error handler condition matching.
func TestErrorHandlerMatching(t *testing.T) {
	handlers := newHandlerContext()

	// Add handlers for different conditions
	handlers.addHandler(&handlerDef{
		controlType: ast.PROCEDUR_CONTINUE,
		conditions:  []ast.ErrNode{&ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_NOT_FOUND}},
		statement:   nil,
	})
	handlers.addHandler(&handlerDef{
		controlType: ast.PROCEDUR_CONTINUE,
		conditions:  []ast.ErrNode{&ast.ProcedureErrorCon{ErrorCon: ast.PROCEDUR_SQLEXCEPTION}},
		statement:   nil,
	})
	handlers.addHandler(&handlerDef{
		controlType: ast.PROCEDUR_CONTINUE,
		conditions:  []ast.ErrNode{&ast.ProcedureErrorVal{ErrorNum: 1062}},
		statement:   nil,
	})
	handlers.addHandler(&handlerDef{
		controlType: ast.PROCEDUR_CONTINUE,
		conditions:  []ast.ErrNode{&ast.ProcedureErrorState{CodeStatus: "23000"}},
		statement:   nil,
	})

	// Test NOT FOUND matching
	notFoundErr := errors.New("no data found")
	handler := handlers.findHandler(notFoundErr)
	require.NotNil(t, handler)

	// Test SQLEXCEPTION matching - any error that's not warning or not found
	genericErr := errors.New("some generic error")
	handler = handlers.findHandler(genericErr)
	require.NotNil(t, handler)
}

// NOTE: TestIfElseIfElse removed - covered by TestE2EIfStatement

// TestDecimalArithmetic tests arithmetic with DECIMAL types.
func TestDecimalArithmetic(t *testing.T) {
	p := parser.New()

	body := `BEGIN
		DECLARE price DECIMAL(10,2) DEFAULT 100.50;
		DECLARE rate DECIMAL(5,4) DEFAULT 0.15;
		DECLARE result DECIMAL(10,2);
		SET result = price * rate;
		RETURN result;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	// 100.50 * 0.15 = 15.075
	floatVal, err := result.ToFloat64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.InDelta(t, 15.075, floatVal, 0.001)
}

// TestComplexExpression tests complex nested expressions.
func TestComplexExpression(t *testing.T) {
	p := parser.New()

	body := `BEGIN
		DECLARE a INT DEFAULT 10;
		DECLARE b INT DEFAULT 5;
		DECLARE c INT DEFAULT 3;
		RETURN (a + b) * c - (a - b);
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	// (10 + 5) * 3 - (10 - 5) = 15 * 3 - 5 = 45 - 5 = 40
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(40), intVal)
}

// TestVariableScope tests that variables are properly scoped.
func TestVariableScope(t *testing.T) {
	p := parser.New()

	// Inner block variable should not shadow outer if different name
	body := `BEGIN
		DECLARE outer_var INT DEFAULT 10;
		BEGIN
			DECLARE inner_var INT DEFAULT 20;
			SET outer_var = outer_var + inner_var;
		END;
		RETURN outer_var;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(30), intVal)
}

// TestFunctionWithParameters tests function execution with parameters.
func TestFunctionWithParameters(t *testing.T) {
	p := parser.New()

	body := `BEGIN
		DECLARE result INT;
		SET result = a * b + c;
		RETURN result;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)

	// Create parameter map
	vars := map[string]types.Datum{}
	vars["a"] = types.NewIntDatum(5)
	vars["b"] = types.NewIntDatum(4)
	vars["c"] = types.NewIntDatum(3)

	result, isNull, err := executeSQLFunctionBody(nil, parsed, vars)
	require.NoError(t, err)
	require.False(t, isNull)
	// 5 * 4 + 3 = 23
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(23), intVal)
}

// TestReturnFromNestedBlock tests RETURN from within nested control flow.
func TestReturnFromNestedBlock(t *testing.T) {
	p := parser.New()

	body := `BEGIN
		DECLARE i INT DEFAULT 0;
		WHILE i < 100 DO
			SET i = i + 1;
			IF i = 5 THEN
				RETURN i;
			END IF;
		END WHILE;
		RETURN -1;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(5), intVal)
}

// TestDebugLabeledLoop is a debug test to inspect the AST structure of labeled loops.
func TestDebugLabeledLoop(t *testing.T) {
	p := parser.New()

	bodyStr := `BEGIN
		DECLARE i INT DEFAULT 0;
		exitloop: LOOP
			LEAVE exitloop;
		END LOOP exitloop;
		RETURN i;
	END`

	stmtNode, err := parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)

	block, ok := stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok, "expected ProcedureBlock but got %T", stmtNode)

	t.Logf("Block statements count: %d", len(block.ProcedureProcStmts))
	for i, stmt := range block.ProcedureProcStmts {
		t.Logf("Statement %d: %T", i, stmt)
		if labelLoop, ok := stmt.(*ast.ProcedureLabelLoop); ok {
			t.Logf("  LabelName: %s", labelLoop.LabelName)
			t.Logf("  Block type: %T", labelLoop.Block)
			if loop, ok := labelLoop.Block.(*ast.ProcedureLoopStmt); ok {
				t.Logf("  Loop body count: %d", len(loop.Body))
				for j, bs := range loop.Body {
					t.Logf("    Body stmt %d: %T", j, bs)
					if jump, ok := bs.(*ast.ProcedureJump); ok {
						t.Logf("      IsLeave: %v, Name: %s", jump.IsLeave, jump.Name)
					}
				}
			}
		}
	}
}

func TestSignalResignalParsing(t *testing.T) {
	p := parser.New()

	// Test SIGNAL with SQLSTATE
	bodyStr := `BEGIN
		SIGNAL SQLSTATE '45000';
	END`
	stmtNode, err := parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)
	block, ok := stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok)
	require.Len(t, block.ProcedureProcStmts, 1)
	signalStmt, ok := block.ProcedureProcStmts[0].(*ast.SignalStmt)
	require.True(t, ok, "expected SignalStmt but got %T", block.ProcedureProcStmts[0])
	require.Equal(t, "45000", signalStmt.SQLState)

	// Test SIGNAL with SQLSTATE VALUE
	bodyStr = `BEGIN
		SIGNAL SQLSTATE VALUE '45001';
	END`
	stmtNode, err = parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)
	block, ok = stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok)
	signalStmt, ok = block.ProcedureProcStmts[0].(*ast.SignalStmt)
	require.True(t, ok)
	require.Equal(t, "45001", signalStmt.SQLState)

	// Test simple RESIGNAL
	bodyStr = `BEGIN
		RESIGNAL;
	END`
	stmtNode, err = parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)
	block, ok = stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok)
	resignalStmt, ok := block.ProcedureProcStmts[0].(*ast.ResignalStmt)
	require.True(t, ok, "expected ResignalStmt but got %T", block.ProcedureProcStmts[0])

	// Test RESIGNAL with SQLSTATE
	bodyStr = `BEGIN
		RESIGNAL SQLSTATE '45002';
	END`
	stmtNode, err = parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)
	block, ok = stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok)
	resignalStmt, ok = block.ProcedureProcStmts[0].(*ast.ResignalStmt)
	require.True(t, ok)
	require.Equal(t, "45002", resignalStmt.SQLState)

	// Test SIGNAL with SET MESSAGE_TEXT
	bodyStr = `BEGIN
		SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Custom error';
	END`
	stmtNode, err = parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)
	block, ok = stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok)
	signalStmt, ok = block.ProcedureProcStmts[0].(*ast.SignalStmt)
	require.True(t, ok)
	require.Equal(t, "45000", signalStmt.SQLState)
	require.Len(t, signalStmt.InfoItems, 1)
	require.Equal(t, "MESSAGE_TEXT", signalStmt.InfoItems[0].ItemName)

	// Test RESIGNAL with SET
	bodyStr = `BEGIN
		RESIGNAL SET MESSAGE_TEXT = 'Modified error';
	END`
	stmtNode, err = parseSQLFunctionBody(p, bodyStr)
	require.NoError(t, err)
	block, ok = stmtNode.(*ast.ProcedureBlock)
	require.True(t, ok)
	resignalStmt, ok = block.ProcedureProcStmts[0].(*ast.ResignalStmt)
	require.True(t, ok)
	require.Len(t, resignalStmt.InfoItems, 1)
	require.Equal(t, "MESSAGE_TEXT", resignalStmt.InfoItems[0].ItemName)
}
