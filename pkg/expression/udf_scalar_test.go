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

// TestBuiltinFunctions tests built-in SQL function evaluation.
func TestBuiltinFunctions(t *testing.T) {
	vars := map[string]types.Datum{}

	tests := []struct {
		name       string
		sourceCode string
		expected   string
		isInt      bool
		intVal     int64
	}{
		{
			name:       "UPPER",
			sourceCode: "BEGIN RETURN UPPER('hello'); END",
			expected:   "HELLO",
		},
		{
			name:       "LOWER",
			sourceCode: "BEGIN RETURN LOWER('HELLO'); END",
			expected:   "hello",
		},
		{
			name:       "LENGTH",
			sourceCode: "BEGIN RETURN LENGTH('hello'); END",
			isInt:      true,
			intVal:     5,
		},
		{
			name:       "CHAR_LENGTH",
			sourceCode: "BEGIN RETURN CHAR_LENGTH('hello'); END",
			isInt:      true,
			intVal:     5,
		},
		{
			name:       "CONCAT two",
			sourceCode: "BEGIN RETURN CONCAT('hello', ' world'); END",
			expected:   "hello world",
		},
		{
			name:       "CONCAT three",
			sourceCode: "BEGIN RETURN CONCAT('a', 'b', 'c'); END",
			expected:   "abc",
		},
		{
			name:       "LEFT",
			sourceCode: "BEGIN RETURN LEFT('hello', 3); END",
			expected:   "hel",
		},
		{
			name:       "RIGHT",
			sourceCode: "BEGIN RETURN RIGHT('hello', 3); END",
			expected:   "llo",
		},
		{
			name:       "TRIM",
			sourceCode: "BEGIN RETURN TRIM('  hello  '); END",
			expected:   "hello",
		},
		{
			name:       "ABS positive",
			sourceCode: "BEGIN RETURN ABS(5); END",
			isInt:      true,
			intVal:     5,
		},
		{
			name:       "ABS negative",
			sourceCode: "BEGIN RETURN ABS(-5); END",
			isInt:      true,
			intVal:     5,
		},
		{
			name:       "SIGN positive",
			sourceCode: "BEGIN RETURN SIGN(5); END",
			isInt:      true,
			intVal:     1,
		},
		{
			name:       "SIGN negative",
			sourceCode: "BEGIN RETURN SIGN(-5); END",
			isInt:      true,
			intVal:     -1,
		},
		{
			name:       "SIGN zero",
			sourceCode: "BEGIN RETURN SIGN(0); END",
			isInt:      true,
			intVal:     0,
		},
	}

	p := parser.New()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.sourceCode)
			require.NoError(t, err)
			require.NotNil(t, body)

			result, isNull, err := executeSQLFunctionBody(nil, body, vars)
			require.NoError(t, err)
			require.False(t, isNull, "Result should not be null")

			if tc.isInt {
				intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
				require.NoError(t, err)
				require.Equal(t, tc.intVal, intVal)
			} else {
				strVal, err := result.ToString()
				require.NoError(t, err)
				require.Equal(t, tc.expected, strVal)
			}
		})
	}
}

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

// TestNewStringFunctions tests the newly added string functions.
func TestNewStringFunctions(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	tests := []struct {
		name       string
		sourceCode string
		expected   string
		isInt      bool
		intVal     int64
		isFloat    bool
		floatVal   float64
	}{
		// INSTR tests
		{
			name:       "INSTR found",
			sourceCode: "BEGIN RETURN INSTR('hello world', 'world'); END",
			isInt:      true,
			intVal:     7,
		},
		{
			name:       "INSTR not found",
			sourceCode: "BEGIN RETURN INSTR('hello', 'xyz'); END",
			isInt:      true,
			intVal:     0,
		},
		{
			name:       "INSTR at start",
			sourceCode: "BEGIN RETURN INSTR('hello', 'hel'); END",
			isInt:      true,
			intVal:     1,
		},

		// LOCATE tests
		{
			name:       "LOCATE found",
			sourceCode: "BEGIN RETURN LOCATE('o', 'hello'); END",
			isInt:      true,
			intVal:     5,
		},
		{
			name:       "LOCATE with start position",
			sourceCode: "BEGIN RETURN LOCATE('o', 'hello world', 6); END",
			isInt:      true,
			intVal:     8,
		},
		{
			name:       "LOCATE not found",
			sourceCode: "BEGIN RETURN LOCATE('xyz', 'hello'); END",
			isInt:      true,
			intVal:     0,
		},

		// LPAD tests
		{
			name:       "LPAD basic",
			sourceCode: "BEGIN RETURN LPAD('hi', 5, '*'); END",
			expected:   "***hi",
		},
		{
			name:       "LPAD no padding needed",
			sourceCode: "BEGIN RETURN LPAD('hello', 5, '*'); END",
			expected:   "hello",
		},
		{
			name:       "LPAD truncate",
			sourceCode: "BEGIN RETURN LPAD('hello', 3, '*'); END",
			expected:   "hel",
		},
		{
			name:       "LPAD with multi-char pad",
			sourceCode: "BEGIN RETURN LPAD('hi', 7, 'ab'); END",
			expected:   "ababahi",
		},

		// RPAD tests
		{
			name:       "RPAD basic",
			sourceCode: "BEGIN RETURN RPAD('hi', 5, '*'); END",
			expected:   "hi***",
		},
		{
			name:       "RPAD no padding needed",
			sourceCode: "BEGIN RETURN RPAD('hello', 5, '*'); END",
			expected:   "hello",
		},
		{
			name:       "RPAD truncate",
			sourceCode: "BEGIN RETURN RPAD('hello', 3, '*'); END",
			expected:   "hel",
		},

		// SPACE tests
		{
			name:       "SPACE basic",
			sourceCode: "BEGIN RETURN CONCAT('a', SPACE(3), 'b'); END",
			expected:   "a   b",
		},
		{
			name:       "SPACE zero",
			sourceCode: "BEGIN RETURN SPACE(0); END",
			expected:   "",
		},

		// CONCAT_WS tests
		{
			name:       "CONCAT_WS basic",
			sourceCode: "BEGIN RETURN CONCAT_WS(',', 'a', 'b', 'c'); END",
			expected:   "a,b,c",
		},
		{
			name:       "CONCAT_WS single",
			sourceCode: "BEGIN RETURN CONCAT_WS('-', 'hello'); END",
			expected:   "hello",
		},

		// ELT tests
		{
			name:       "ELT first",
			sourceCode: "BEGIN RETURN ELT(1, 'a', 'b', 'c'); END",
			expected:   "a",
		},
		{
			name:       "ELT second",
			sourceCode: "BEGIN RETURN ELT(2, 'a', 'b', 'c'); END",
			expected:   "b",
		},
		{
			name:       "ELT third",
			sourceCode: "BEGIN RETURN ELT(3, 'a', 'b', 'c'); END",
			expected:   "c",
		},

		// FIELD tests
		{
			name:       "FIELD found first",
			sourceCode: "BEGIN RETURN FIELD('a', 'a', 'b', 'c'); END",
			isInt:      true,
			intVal:     1,
		},
		{
			name:       "FIELD found second",
			sourceCode: "BEGIN RETURN FIELD('b', 'a', 'b', 'c'); END",
			isInt:      true,
			intVal:     2,
		},
		{
			name:       "FIELD not found",
			sourceCode: "BEGIN RETURN FIELD('x', 'a', 'b', 'c'); END",
			isInt:      true,
			intVal:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.sourceCode)
			require.NoError(t, err)
			require.NotNil(t, body)

			result, isNull, err := executeSQLFunctionBody(nil, body, vars)
			require.NoError(t, err)
			require.False(t, isNull, "Result should not be null")

			if tc.isInt {
				intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
				require.NoError(t, err)
				require.Equal(t, tc.intVal, intVal)
			} else if tc.isFloat {
				floatVal, err := result.ToFloat64(types.DefaultStmtNoWarningContext)
				require.NoError(t, err)
				require.InDelta(t, tc.floatVal, floatVal, 0.0001)
			} else {
				strVal, err := result.ToString()
				require.NoError(t, err)
				require.Equal(t, tc.expected, strVal)
			}
		})
	}
}

// TestNewMathFunctions tests the newly added math functions.
func TestNewMathFunctions(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	tests := []struct {
		name       string
		sourceCode string
		expected   float64
		delta      float64 // For floating point comparison
	}{
		// LOG tests
		{
			name:       "LOG natural",
			sourceCode: "BEGIN RETURN LOG(2.718281828); END",
			expected:   1.0,
			delta:      0.0001,
		},
		{
			name:       "LOG with base",
			sourceCode: "BEGIN RETURN LOG(10, 100); END",
			expected:   2.0,
			delta:      0.0001,
		},

		// LOG10 tests
		{
			name:       "LOG10",
			sourceCode: "BEGIN RETURN LOG10(100); END",
			expected:   2.0,
			delta:      0.0001,
		},

		// LOG2 tests
		{
			name:       "LOG2",
			sourceCode: "BEGIN RETURN LOG2(8); END",
			expected:   3.0,
			delta:      0.0001,
		},

		// LN tests
		{
			name:       "LN",
			sourceCode: "BEGIN RETURN LN(2.718281828); END",
			expected:   1.0,
			delta:      0.0001,
		},

		// EXP tests
		{
			name:       "EXP zero",
			sourceCode: "BEGIN RETURN EXP(0); END",
			expected:   1.0,
			delta:      0.0001,
		},
		{
			name:       "EXP one",
			sourceCode: "BEGIN RETURN EXP(1); END",
			expected:   2.718281828,
			delta:      0.0001,
		},

		// Trigonometric functions
		{
			name:       "SIN zero",
			sourceCode: "BEGIN RETURN SIN(0); END",
			expected:   0.0,
			delta:      0.0001,
		},
		{
			name:       "COS zero",
			sourceCode: "BEGIN RETURN COS(0); END",
			expected:   1.0,
			delta:      0.0001,
		},
		{
			name:       "TAN zero",
			sourceCode: "BEGIN RETURN TAN(0); END",
			expected:   0.0,
			delta:      0.0001,
		},

		// Inverse trig functions
		{
			name:       "ASIN zero",
			sourceCode: "BEGIN RETURN ASIN(0); END",
			expected:   0.0,
			delta:      0.0001,
		},
		{
			name:       "ACOS one",
			sourceCode: "BEGIN RETURN ACOS(1); END",
			expected:   0.0,
			delta:      0.0001,
		},
		{
			name:       "ATAN zero",
			sourceCode: "BEGIN RETURN ATAN(0); END",
			expected:   0.0,
			delta:      0.0001,
		},
		{
			name:       "ATAN2",
			sourceCode: "BEGIN RETURN ATAN(1, 1); END",
			expected:   0.7853981634, // PI/4
			delta:      0.0001,
		},

		// PI
		{
			name:       "PI",
			sourceCode: "BEGIN RETURN PI(); END",
			expected:   3.141592653589793,
			delta:      0.0000001,
		},

		// DEGREES and RADIANS
		{
			name:       "DEGREES",
			sourceCode: "BEGIN RETURN DEGREES(3.141592653589793); END",
			expected:   180.0,
			delta:      0.0001,
		},
		{
			name:       "RADIANS",
			sourceCode: "BEGIN RETURN RADIANS(180); END",
			expected:   3.141592653589793,
			delta:      0.0001,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.sourceCode)
			require.NoError(t, err)
			require.NotNil(t, body)

			result, isNull, err := executeSQLFunctionBody(nil, body, vars)
			require.NoError(t, err)
			require.False(t, isNull, "Result should not be null")

			floatVal, err := result.ToFloat64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.InDelta(t, tc.expected, floatVal, tc.delta)
		})
	}
}

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

// TestWhileLoopStatement tests the WHILE loop statement execution.
func TestWhileLoopStatement(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// Test WHILE loop for summing 1 to 5
	sourceCode := `BEGIN
		DECLARE counter INT DEFAULT 0;
		DECLARE result INT DEFAULT 0;
		WHILE counter < 5 DO
			SET counter = counter + 1;
			SET result = result + counter;
		END WHILE;
		RETURN result;
	END`

	body, err := parseSQLFunctionBody(p, sourceCode)
	require.NoError(t, err)
	require.NotNil(t, body)

	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)

	// Sum of 1+2+3+4+5 = 15
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(15), intVal)
}

// TestRepeatLoopStatement tests the REPEAT loop statement.
func TestRepeatLoopStatement(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	// Test REPEAT loop for summing 1 to 5
	sourceCode := `BEGIN
		DECLARE counter INT DEFAULT 0;
		DECLARE result INT DEFAULT 0;
		REPEAT
			SET counter = counter + 1;
			SET result = result + counter;
		UNTIL counter >= 5
		END REPEAT;
		RETURN result;
	END`

	body, err := parseSQLFunctionBody(p, sourceCode)
	require.NoError(t, err)
	require.NotNil(t, body)

	result, isNull, err := executeSQLFunctionBody(nil, body, vars)
	require.NoError(t, err)
	require.False(t, isNull)

	// Sum of 1+2+3+4+5 = 15
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(15), intVal)
}

// TestNullHandlingInNewFunctions tests NULL handling in new functions.
func TestNullHandlingInNewFunctions(t *testing.T) {
	p := parser.New()
	vars := map[string]types.Datum{}

	nullTests := []struct {
		name       string
		sourceCode string
	}{
		{"INSTR with NULL", "BEGIN RETURN INSTR(NULL, 'test'); END"},
		{"LOCATE with NULL", "BEGIN RETURN LOCATE(NULL, 'test'); END"},
		{"LPAD with NULL", "BEGIN RETURN LPAD(NULL, 5, '*'); END"},
		{"RPAD with NULL", "BEGIN RETURN RPAD(NULL, 5, '*'); END"},
		{"SPACE with NULL", "BEGIN RETURN SPACE(NULL); END"},
		{"LOG with NULL", "BEGIN RETURN LOG(NULL); END"},
		{"LOG10 with NULL", "BEGIN RETURN LOG10(NULL); END"},
		{"LOG2 with NULL", "BEGIN RETURN LOG2(NULL); END"},
		{"LN with NULL", "BEGIN RETURN LN(NULL); END"},
		{"EXP with NULL", "BEGIN RETURN EXP(NULL); END"},
		{"SIN with NULL", "BEGIN RETURN SIN(NULL); END"},
		{"COS with NULL", "BEGIN RETURN COS(NULL); END"},
		{"TAN with NULL", "BEGIN RETURN TAN(NULL); END"},
		{"ASIN with NULL", "BEGIN RETURN ASIN(NULL); END"},
		{"ACOS with NULL", "BEGIN RETURN ACOS(NULL); END"},
		{"ATAN with NULL", "BEGIN RETURN ATAN(NULL); END"},
		{"DEGREES with NULL", "BEGIN RETURN DEGREES(NULL); END"},
		{"RADIANS with NULL", "BEGIN RETURN RADIANS(NULL); END"},
	}

	for _, tc := range nullTests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.sourceCode)
			require.NoError(t, err)
			require.NotNil(t, body)

			result, isNull, err := executeSQLFunctionBody(nil, body, vars)
			require.NoError(t, err)
			require.True(t, isNull || result.IsNull(), "Expected NULL result for %s", tc.name)
		})
	}
}

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

// TestSimpleCaseStatement tests simple CASE val WHEN ... END CASE.
func TestSimpleCaseStatement(t *testing.T) {
	p := parser.New()

	tests := []struct {
		name     string
		body     string
		vars     map[string]types.Datum
		expected int64
	}{
		{
			name: "case_first_match",
			body: `BEGIN
				DECLARE grade VARCHAR(10);
				DECLARE x INT DEFAULT 1;
				CASE x
					WHEN 1 THEN SET grade = 'A';
					WHEN 2 THEN SET grade = 'B';
					ELSE SET grade = 'C';
				END CASE;
				RETURN x;
			END`,
			vars:     map[string]types.Datum{},
			expected: 1,
		},
		{
			name: "case_second_match",
			body: `BEGIN
				DECLARE result INT DEFAULT 0;
				DECLARE val INT DEFAULT 2;
				CASE val
					WHEN 1 THEN SET result = 10;
					WHEN 2 THEN SET result = 20;
					WHEN 3 THEN SET result = 30;
				END CASE;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 20,
		},
		{
			name: "case_else_branch",
			body: `BEGIN
				DECLARE result INT DEFAULT 0;
				DECLARE val INT DEFAULT 99;
				CASE val
					WHEN 1 THEN SET result = 10;
					WHEN 2 THEN SET result = 20;
					ELSE SET result = 100;
				END CASE;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 100,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.body)
			require.NoError(t, err)
			result, isNull, err := executeSQLFunctionBody(nil, body, tc.vars)
			require.NoError(t, err)
			require.False(t, isNull)
			intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intVal)
		})
	}
}

// TestSearchedCaseStatement tests searched CASE WHEN condition THEN ... END CASE.
func TestSearchedCaseStatement(t *testing.T) {
	p := parser.New()

	tests := []struct {
		name     string
		body     string
		vars     map[string]types.Datum
		expected int64
	}{
		{
			name: "searched_case_first_true",
			body: `BEGIN
				DECLARE result INT DEFAULT 0;
				DECLARE x INT DEFAULT 10;
				CASE
					WHEN x > 5 THEN SET result = 1;
					WHEN x > 3 THEN SET result = 2;
					ELSE SET result = 3;
				END CASE;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 1,
		},
		{
			name: "searched_case_second_true",
			body: `BEGIN
				DECLARE result INT DEFAULT 0;
				DECLARE x INT DEFAULT 4;
				CASE
					WHEN x > 10 THEN SET result = 1;
					WHEN x > 3 THEN SET result = 2;
					ELSE SET result = 3;
				END CASE;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 2,
		},
		{
			name: "searched_case_else",
			body: `BEGIN
				DECLARE result INT DEFAULT 0;
				DECLARE x INT DEFAULT 1;
				CASE
					WHEN x > 10 THEN SET result = 1;
					WHEN x > 5 THEN SET result = 2;
					ELSE SET result = 3;
				END CASE;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.body)
			require.NoError(t, err)
			result, isNull, err := executeSQLFunctionBody(nil, body, tc.vars)
			require.NoError(t, err)
			require.False(t, isNull)
			intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intVal)
		})
	}
}

// TestLoopWithLeave tests LOOP with LEAVE statement.
func TestLoopWithLeave(t *testing.T) {
	p := parser.New()

	tests := []struct {
		name     string
		body     string
		vars     map[string]types.Datum
		expected int64
	}{
		{
			name: "simple_loop_with_leave",
			body: `BEGIN
				DECLARE i INT DEFAULT 0;
				myloop: LOOP
					SET i = i + 1;
					IF i >= 5 THEN
						LEAVE myloop;
					END IF;
				END LOOP myloop;
				RETURN i;
			END`,
			vars:     map[string]types.Datum{},
			expected: 5,
		},
		{
			name: "loop_immediate_leave",
			body: `BEGIN
				DECLARE i INT DEFAULT 100;
				exitloop: LOOP
					LEAVE exitloop;
					SET i = i + 1;
				END LOOP exitloop;
				RETURN i;
			END`,
			vars:     map[string]types.Datum{},
			expected: 100,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.body)
			require.NoError(t, err)
			result, isNull, err := executeSQLFunctionBody(nil, body, tc.vars)
			require.NoError(t, err)
			require.False(t, isNull)
			intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intVal)
		})
	}
}

// TestLoopWithIterate tests LOOP with ITERATE statement.
func TestLoopWithIterate(t *testing.T) {
	p := parser.New()

	// Test ITERATE to skip even numbers, sum only odd
	body := `BEGIN
		DECLARE i INT DEFAULT 0;
		DECLARE total INT DEFAULT 0;
		sumloop: LOOP
			SET i = i + 1;
			IF i > 10 THEN
				LEAVE sumloop;
			END IF;
			IF i % 2 = 0 THEN
				ITERATE sumloop;
			END IF;
			SET total = total + i;
		END LOOP sumloop;
		RETURN total;
	END`

	parsed, err := parseSQLFunctionBody(p, body)
	require.NoError(t, err)
	result, isNull, err := executeSQLFunctionBody(nil, parsed, map[string]types.Datum{})
	require.NoError(t, err)
	require.False(t, isNull)
	// Sum of odd numbers 1-10: 1+3+5+7+9 = 25
	intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
	require.NoError(t, err)
	require.Equal(t, int64(25), intVal)
}

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

// TestIfElseIfElse tests comprehensive IF/ELSEIF/ELSE chains.
func TestIfElseIfElse(t *testing.T) {
	p := parser.New()

	tests := []struct {
		name     string
		body     string
		vars     map[string]types.Datum
		expected int64
	}{
		{
			name: "if_branch",
			body: `BEGIN
				DECLARE x INT DEFAULT 100;
				DECLARE result INT DEFAULT 0;
				IF x > 50 THEN
					SET result = 1;
				ELSEIF x > 25 THEN
					SET result = 2;
				ELSE
					SET result = 3;
				END IF;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 1,
		},
		{
			name: "elseif_branch",
			body: `BEGIN
				DECLARE x INT DEFAULT 30;
				DECLARE result INT DEFAULT 0;
				IF x > 50 THEN
					SET result = 1;
				ELSEIF x > 25 THEN
					SET result = 2;
				ELSE
					SET result = 3;
				END IF;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 2,
		},
		{
			name: "else_branch",
			body: `BEGIN
				DECLARE x INT DEFAULT 10;
				DECLARE result INT DEFAULT 0;
				IF x > 50 THEN
					SET result = 1;
				ELSEIF x > 25 THEN
					SET result = 2;
				ELSE
					SET result = 3;
				END IF;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 3,
		},
		{
			name: "multiple_elseif",
			body: `BEGIN
				DECLARE grade INT DEFAULT 75;
				DECLARE result INT DEFAULT 0;
				IF grade >= 90 THEN
					SET result = 4;
				ELSEIF grade >= 80 THEN
					SET result = 3;
				ELSEIF grade >= 70 THEN
					SET result = 2;
				ELSEIF grade >= 60 THEN
					SET result = 1;
				ELSE
					SET result = 0;
				END IF;
				RETURN result;
			END`,
			vars:     map[string]types.Datum{},
			expected: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := parseSQLFunctionBody(p, tc.body)
			require.NoError(t, err)
			result, isNull, err := executeSQLFunctionBody(nil, body, tc.vars)
			require.NoError(t, err)
			require.False(t, isNull)
			intVal, err := result.ToInt64(types.DefaultStmtNoWarningContext)
			require.NoError(t, err)
			require.Equal(t, tc.expected, intVal)
		})
	}
}

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
