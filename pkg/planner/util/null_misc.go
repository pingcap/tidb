// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// SQL three-valued logic (3VL) quick reference
// --------------------------------------------
// 3VL has three truth values: TRUE, FALSE, UNKNOWN (result from any NULL participation).
// Basic rules:
//   - Comparisons/pattern/regex with a NULL operand → UNKNOWN (i.e., result is NULL), except NULL-safe <=>.
//   - AND: FALSE dominates; TRUE ∧ UNKNOWN = UNKNOWN; UNKNOWN ∧ UNKNOWN = UNKNOWN; UNKNOWN ∧ FALSE = FALSE.
//   - OR:  TRUE dominates; FALSE ∨ UNKNOWN = UNKNOWN; UNKNOWN ∨ UNKNOWN = UNKNOWN; TRUE ∨ UNKNOWN = TRUE.
//   - NOT: NOT UNKNOWN = UNKNOWN; NOT FALSE = TRUE.
//
// In this file, "NULL propagation" means: if any relevant argument is NULL, the function yields NULL (UNKNOWN).
// We call such functions NullPreserving.
//
// This implementation follows a two-set framework for null-rejection analysis:
//   - NonTrue(E): variables x where x=U ⇒ E ∈ {F,U} (null-rejected)
//   - MustNull(E): variables x where x=U ⇒ E = U (must-NULL evidence)
// Key rules:
//   - NonTrue(A AND B) = NonTrue(A) ∪ NonTrue(B)
//   - NonTrue(A OR B) = NonTrue(A) ∩ NonTrue(B)
//   - NonTrue(NOT A) = MustNull(A)
//   - MustNull(A AND B) = MustNull(A) ∩ MustNull(B)
//   - MustNull(A OR B) = MustNull(A) ∩ MustNull(B)
//   - MustNull(NULL Preserving F(args...)) = ⋃ MustNull(arg_i)

// NullBehavior describes how a function interacts with SQL 3VL and NULL propagation.
// Function null-behavior traits (centralized, easy to extend):
//   - NullPreserving:   any NULL input (relevant arg) yields NULL output (e.g., +, -, *, comparisons, LIKE, REGEXP)
//   - NullHiding:       may turn NULL into non-NULL or short-circuit to a non-NULL truth value
//     (IF/CASE/COALESCE/IS .../AND/OR)
//   - NullSafeEq:       NULL-safe equality (<=>)
//   - NullTransparentWrapper: unary wrapper that is NULL-transparent and "structurally" transparent for our checks
//     (e.g., CAST/CONVERT/WEIGHT_STRING、UNARY + / UNARY -). We peel these before reasoning.
type NullBehavior uint8

// NullBehavior constants define how functions interact with NULL values
const (
	// NullPreserving indicates any NULL input yields NULL output
	NullPreserving NullBehavior = 1 << iota
	// NullHiding indicates may turn NULL into non-NULL
	NullHiding
	// NullSafeEq indicates NULL-safe equality
	NullSafeEq
	// NullTransparentWrapper indicates unary wrapper that is NULL-transparent
	NullTransparentWrapper
)

// fnTraits maps ast func name → trait flags.
// NOTE: keep names in lower-case (ast constants already are).
var fnTraits = map[string]NullBehavior{
	// Comparisons / pattern / regexp: 3VL → NULL when any side is NULL
	ast.EQ:         NullPreserving,
	ast.NE:         NullPreserving,
	ast.GT:         NullPreserving,
	ast.GE:         NullPreserving,
	ast.LT:         NullPreserving,
	ast.LE:         NullPreserving,
	ast.Like:       NullPreserving,
	ast.Ilike:      NullPreserving,
	ast.RegexpLike: NullPreserving,
	ast.Regexp:     NullPreserving,
	ast.NullEQ:     NullSafeEq,

	// Boolean connectives (may hide NULL via short-circuit)
	// Note: AND/OR can hide NULL due to short-circuit evaluation (F AND U = F, T OR U = T)
	ast.LogicAnd: NullHiding,
	ast.LogicOr:  NullHiding,
	// XOR is NULL-preserving: NULL XOR anything = NULL (no short-circuit)
	ast.LogicXor: NullPreserving,

	// Truth predicates & NULL tests – they *handle* NULL explicitly
	ast.IsFalsity:          NullHiding,
	ast.IsTruthWithNull:    NullHiding,
	ast.IsTruthWithoutNull: NullHiding,
	ast.IsNull:             NullHiding,

	// Control flow / NULL-bridging
	ast.If:       NullHiding,
	ast.Case:     NullHiding,
	ast.Coalesce: NullHiding,
	ast.Ifnull:   NullHiding,
	ast.Nullif:   NullHiding,

	// Unary wrappers we want to peel structurally
	ast.Cast:         NullTransparentWrapper,
	ast.Convert:      NullTransparentWrapper,
	ast.WeightString: NullTransparentWrapper,
	ast.UnaryPlus:    NullTransparentWrapper,
	ast.UnaryMinus:   NullTransparentWrapper,

	// Arithmetic / bit ops (all NULL-preserving)
	ast.Plus:       NullPreserving,
	ast.Minus:      NullPreserving,
	ast.Mul:        NullPreserving,
	ast.Div:        NullPreserving,
	ast.IntDiv:     NullPreserving,
	ast.Mod:        NullPreserving,
	ast.And:        NullPreserving, // bitand
	ast.Or:         NullPreserving, // bitor
	ast.Xor:        NullPreserving, // bitxor
	ast.LeftShift:  NullPreserving,
	ast.RightShift: NullPreserving,
	ast.BitNeg:     NullPreserving,

	// N-ary math helpers that propagate NULL if any arg is NULL
	ast.Greatest: NullPreserving,
	ast.Least:    NullPreserving,

	// Time/date functions that propagate NULL if any arg is NULL
	ast.FromUnixTime:  NullPreserving,
	ast.TimestampDiff: NullPreserving,

	// JSON functions that propagate NULL if any arg is NULL
	ast.JSONExtract: NullPreserving,
}

// traitOf returns the NullBehavior trait for a function name.
// Uses switch for hot-path functions to avoid map lookup overhead.
func traitOf(name string) NullBehavior {
	// Fast path: check frequently used functions via switch (avoid map allocation)
	switch name {
	case ast.EQ, ast.NE, ast.GT, ast.GE, ast.LT, ast.LE:
		return NullPreserving
	case ast.Like, ast.Ilike, ast.RegexpLike, ast.Regexp:
		return NullPreserving
	case ast.LogicAnd, ast.LogicOr:
		return NullHiding
	case ast.LogicXor:
		return NullPreserving
	case ast.NullEQ:
		return NullSafeEq
	case ast.IsNull, ast.IsFalsity, ast.IsTruthWithNull, ast.IsTruthWithoutNull:
		return NullHiding
	case ast.Cast, ast.Convert, ast.UnaryPlus, ast.UnaryMinus:
		return NullTransparentWrapper
	case ast.UnaryNot:
		return 0 // NOT has special handling, not in fnTraits
	default:
		return fnTraits[name]
	}
}

func has(t NullBehavior, flag NullBehavior) bool { return t&flag != 0 }

// allConstants returns true if `expr` is composed only of constants (and
// scalar functions whose arguments are all constants). It also guards against
// expressions that are unsafe to treat as constants for plan cache.
// Optimized: early type check before recursive calls.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	switch v := expr.(type) {
	case *expression.Constant:
		return true
	case *expression.ScalarFunction:
		// Check plan cache safety first (cheaper than recursion)
		if expression.MaybeOverOptimized4PlanCache(ctx, expr) {
			return false
		}
		for _, arg := range v.GetArgs() {
			if !allConstants(ctx, arg) {
				return false
			}
		}
		return true
	case *expression.Column, *expression.CorrelatedColumn:
		return false
	default:
		// For unknown types, check plan cache safety
		return !expression.MaybeOverOptimized4PlanCache(ctx, expr)
	}
}

// isNullRejectedInList checks null filter for IN list using OR logic.
// Reason is that null filtering through evaluation by isNullRejectedExpr
// has problems with IN list. For example, constant in (outer-table.col1, inner-table.col2)
// is not null rejecting since constant in (outer-table.col1, NULL) is not false/unknown.
func isNullRejectedInList(ctx base.PlanContext, expr *expression.ScalarFunction,
	innerSchema *expression.Schema, skipPlanCacheCheck bool,
) bool {
	for i, arg := range expr.GetArgs() {
		if i > 0 {
			newArgs := make([]expression.Expression, 0, 2)
			newArgs = append(newArgs, expr.GetArgs()[0])
			newArgs = append(newArgs, arg)
			eQCondition, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ,
				expr.GetType(ctx.GetExprCtx().GetEvalCtx()), newArgs...)
			if err != nil {
				return false
			}
			if !(isNullRejectedExpr(ctx, innerSchema, eQCondition, skipPlanCacheCheck)) {
				return false
			}
		}
	}
	return true
}

// IsNullRejected takes care of complex predicates like this:
// IsNullRejected(A OR B) = IsNullRejected(A) AND IsNullRejected(B)
// IsNullRejected(A AND B) = IsNullRejected(A) OR IsNullRejected(B)
func IsNullRejected(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	predicate expression.Expression,
	skipPlanCacheCheck bool,
) bool {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	if expression.ContainOuterNot(predicate) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		switch expr.FuncName.L {
		case ast.LogicAnd:
			if IsNullRejected(ctx, innerSchema, expr.GetArgs()[0], skipPlanCacheCheck) {
				return true
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1], skipPlanCacheCheck)
		case ast.LogicOr:
			if !IsNullRejected(ctx, innerSchema, expr.GetArgs()[0], skipPlanCacheCheck) {
				return false
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1], skipPlanCacheCheck)
		case ast.In:
			return isNullRejectedInList(ctx, expr, innerSchema, skipPlanCacheCheck)
		default:
			return isNullRejectedExpr(ctx, innerSchema, expr, skipPlanCacheCheck)
		}
	default:
		return isNullRejectedExpr(ctx, innerSchema, predicate, skipPlanCacheCheck)
	}
}

// referencesAnyInner checks whether the predicate references at least one inner-side column.
// Optimized: avoids slice allocation by using visitor pattern.
func referencesAnyInner(e expression.Expression, inner *expression.Schema) bool {
	return containsColumnFromSchema(e, inner)
}

// containsColumnFromSchema recursively checks if expression contains any column from schema.
// This avoids the allocation overhead of ExtractColumns when we only need a boolean check.
func containsColumnFromSchema(e expression.Expression, sch *expression.Schema) bool {
	switch x := e.(type) {
	case *expression.Column:
		return sch.Contains(x)
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			if containsColumnFromSchema(arg, sch) {
				return true
			}
		}
		return false
	case *expression.CorrelatedColumn:
		return sch.Contains(&x.Column)
	default:
		return false
	}
}

// judgeFoldedConstant examines an already-folded expression `e`.
// Decision rule (SQL 3-valued logic):
//   - NULL (UNKNOWN)  → null-rejecting (return true, true)
//   - FALSE           → null-rejecting (return true, true)
//   - TRUE            → NOT null-rejecting (return true, false)
//   - non-constant    → undecided (return false, false)
func judgeFoldedConstant(ctx base.PlanContext, e expression.Expression) (decided bool, reject bool) {
	sc := ctx.GetSessionVars().StmtCtx
	if c, ok := e.(*expression.Constant); ok {
		if c.Value.IsNull() { // NULL / UNKNOWN
			return true, true
		}
		if b, err := c.Value.ToBool(sc.TypeCtxOrDefault()); err == nil {
			return true, b == 0 // FALSE ⇒ reject, TRUE ⇒ not reject
		}
	}
	return false, false
}

// tryConstTree handles the case where the whole predicate is a constant expression tree
// (safe for plan cache). We just FoldConstant and apply 3VL on the folded result.
// No "inner-columns-as-NULL" substitution is needed because there are no columns.
func tryConstTree(ctx base.PlanContext, pred expression.Expression) (decided bool, reject bool) {
	folded := expression.FoldConstant(ctx.GetExprCtx(), pred)
	return judgeFoldedConstant(ctx, folded)
}

// -------- Opaque info/metadata functions ------------------------------------
// These functions (like collation/charset/coercibility) should not be used as "NULL propagation" evidence
// to drive outer join elimination, otherwise expressions like `where collation(t2.c) = 'utf8mb4_bin'`
// would be incorrectly judged as allowing LEFT JOIN to be converted to INNER JOIN.
// Approach: The structural judgment phase in isNullPropagatingWRTInner treats them as
// "potentially masking NULL" (returning false), which is the conservative choice.
var opaqueInfoFuncSet = map[string]struct{}{
	ast.Collation:    {},
	ast.Charset:      {},
	ast.Coercibility: {},
}

func isOpaqueInfoFuncName(name string) bool {
	_, ok := opaqueInfoFuncSet[name]
	return ok
}

// Decision flow for isNullRejectedExpr:
//
// +------------------------+
// | Constant?             |--Yes--> NULL/FALSE? → true (null-rejecting)
// +------------------------+         TRUE? → false (not null-rejecting)
//            |
//            No
//            v
// +------------------------+
// | All-constant tree?     |--Yes--> Fold → NULL/FALSE? → true
// +------------------------+                 TRUE? → false
//            |
//            No
//            v
// +------------------------+
// | Ref inner columns?     |--No--> false (not null-rejecting)
// +------------------------+
//            |
//            Yes
//            v
// +------------------------+
// | Structural analysis    |--Preserving--> true (null-rejecting)
// | (NonTrue/MustNull)     |--Hiding/Undecided--> false (conservative)
// +------------------------+

// isNullRejectedExpr decides whether `predicate` is null-rejecting w.r.t. the `innerSchema`.
// Strategy (fast → slow):
//  1. Literal constant fast-path: WHERE NULL / 0 / 1.
//  2. Whole constant tree: FoldConstant, then judge under 3VL (TRUE/FALSE/UNKNOWN).
//     (Must run before the inner-column check; constant predicates don't depend on columns.)
//  3. Quick bailout: if the predicate does NOT reference any inner column → false.
//  4. Structural decision: syntactic rules using NonTrue/MustNull framework
//     (comparisons/LIKE/REGEXP/<=>/AND/OR/NOT/IS NULL/IS NOT NULL ...).
//     Key insight: Preserving + MustNull(inner) = null-rejecting.
//     If Hiding or Undecided, return false conservatively.
func isNullRejectedExpr(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	predicate expression.Expression,
	_ bool, // skipPlanCacheCheck - kept for API compatibility
) bool {
	// (1) Literal constant fast-path - check type first to avoid method calls
	if c, ok := predicate.(*expression.Constant); ok {
		return evaluateConstantPredicate(ctx, c)
	}

	// (2) Whole constant expression tree (no columns at all).
	//     Handle upfront to avoid misclassifying expressions like (1=0).
	if allConstants(ctx.GetExprCtx(), predicate) {
		if decided, reject := tryConstTree(ctx, predicate); decided {
			return reject
		}
		return false
	}

	// (3) Quick bailout: no inner columns ⇒ not null-rejecting WRT inner
	if !referencesAnyInner(predicate, innerSchema) {
		return false
	}

	// (4) Structural (syntactic) decision
	// The switch is exhaustive, so the result is always determined here.
	// Preserving means NULL is preserved, which combined with MustNull(inner) makes it null-rejecting.
	result := isNullRejectingByStructure(predicate, innerSchema)
	return result == Preserving
}

// evaluateConstantPredicate evaluates a constant predicate under 3VL.
// Returns true if null-rejecting (NULL or FALSE), false otherwise.
func evaluateConstantPredicate(ctx base.PlanContext, c *expression.Constant) bool {
	if c.Value.IsNull() {
		return true // WHERE NULL → always null-rejecting
	}
	sc := ctx.GetSessionVars().StmtCtx
	if b, err := c.Value.ToBool(sc.TypeCtxOrDefault()); err == nil {
		return b == 0 // WHERE 0 ⇒ reject, WHERE 1 ⇒ not reject
	}
	return false
}

// ResetNotNullFlag resets the not-null flag of [start, end) columns in the schema.
func ResetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.DelFlag(mysql.NotNullFlag)
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}

// -----------------------------------------
// Transparent wrapper peeling (trait-driven)
// -----------------------------------------

// isTransparentWrapper checks if a function name is a transparent wrapper.
// Inlined check for hot path to avoid map lookup.
func isTransparentWrapper(name string) bool {
	switch name {
	case ast.Cast, ast.Convert, ast.WeightString, ast.UnaryPlus, ast.UnaryMinus:
		return true
	default:
		return false
	}
}

// peelTransparentWrappers removes unary shells that are both NULL-transparent and
// structurally transparent for our purposes (e.g., CAST/CONVERT/WEIGHT_STRING).
// For CAST/CONVERT that carry more than one arg, we still peel the first argument.
func peelTransparentWrappers(e expression.Expression) expression.Expression {
	for {
		sf, ok := e.(*expression.ScalarFunction)
		if !ok {
			return e
		}
		if !isTransparentWrapper(sf.FuncName.L) {
			return e
		}
		args := sf.GetArgs()
		if len(args) == 0 {
			return e
		}
		// Peel the primary payload (arg0). This is safe because the wrapper is declared transparent.
		e = args[0]
	}
}

// isNullPropagatingWRTInner (MustNull evidence) reports whether expression `e` has at least one
// evaluation path where a NULL coming from the `inner` schema *must* propagate
// to the result of `e` (i.e., no guard like IF/IFNULL/COALESCE catches it).
//
// This implements MustNull(E) semantics: returns true if there exists a variable x in inner
// such that x=U ⇒ E=U.
//
// WRT = "with respect to": here it means we only consider NULLs coming from
// columns in `inner` (not from outer columns or constants).
//
// Framework rules:
//   - MustNull(column from inner) = {that column}
//   - MustNull(constant) = ∅
//   - MustNull(NULL Preserving F(args...)) = ⋃ MustNull(arg_i)  [union: any arg propagates]
//   - MustNull(NULL Hiding F(...)) = ∅  [conservative: cannot prove propagation]
//   - MustNull(A AND B) = MustNull(A) ∩ MustNull(B)  [both must be U]
//   - MustNull(A OR B) = MustNull(A) ∩ MustNull(B)  [both must be U]
//   - MustNull(NOT A) = MustNull(A)  [NOT preserves U]
//
// Examples:
//   - inner.a                 → true  (x ∈ MustNull(inner.a))
//   - ifnull(inner.a, 0)      → false (NULL is hidden, x ∉ MustNull)
//   - inner.a + 1             → true  (NULL Preserving: union of args)
//   - if(inner.a > 0, 1, 2)   → false (NULL Hiding)
//   - outer.b + 1             → false (no inner cols)
func isNullPropagatingWRTInner(e expression.Expression, inner *expression.Schema) bool {
	e = peelTransparentWrappers(e)
	switch x := e.(type) {
	case *expression.Column:
		// Base case: column from inner schema → x ∈ MustNull(column)
		return inner.Contains(x)

	case *expression.Constant:
		// Constants never propagate inner NULL → ∅
		return false

	case *expression.ScalarFunction:
		t := traitOf(x.FuncName.L)
		// Info/meta functions are *not* evidence structurally (stay conservative).
		if isOpaqueInfoFuncName(x.FuncName.L) {
			return false
		}

		// Handle boolean connectives with intersection semantics
		switch x.FuncName.L {
		case ast.LogicAnd:
			// MustNull(A AND B) = MustNull(A) ∩ MustNull(B)
			// Both sides must propagate NULL (U AND F = F, so both must be U)
			return isNullPropagatingWRTInner(x.GetArgs()[0], inner) &&
				isNullPropagatingWRTInner(x.GetArgs()[1], inner)

		case ast.LogicOr:
			// MustNull(A OR B) = MustNull(A) ∩ MustNull(B)
			// Both sides must propagate NULL (T OR U = T, so both must be U)
			return isNullPropagatingWRTInner(x.GetArgs()[0], inner) &&
				isNullPropagatingWRTInner(x.GetArgs()[1], inner)

		case ast.UnaryNot:
			// MustNull(NOT A) = MustNull(A)  [NOT preserves U]
			return isNullPropagatingWRTInner(x.GetArgs()[0], inner)
		}

		// NullHiding / NullSafeEq can potentially "consume/handle" NULL:
		// - IF/CASE/COALESCE/IS... may mask NULL values
		// - `<=>` is NULL-safe (NULL <=> NULL returns TRUE), should not be treated as "always propagating NULL"
		if has(t, NullHiding) || has(t, NullSafeEq) {
			return false
		}

		// NULL Preserving functions: MustNull(F(args...)) = ⋃ MustNull(arg_i)
		// Union semantics: if ANY argument propagates NULL, the result propagates NULL
		if has(t, NullPreserving) {
			for _, a := range x.GetArgs() {
				if isNullPropagatingWRTInner(a, inner) {
					return true
				}
			}
			return false
		}

		// Unknown function: conservatively assume it does not propagate NULL.
		// If a function is NULL-preserving, it should be explicitly added to fnTraits.
		return false

	default:
		// Unknown node kinds → assume not propagating to stay conservative.
		return false
	}
}

// NullRejectionResult represents the result of structural null-rejection analysis.
// Following winoros's suggestion, we use a simplified 2-state + undecided model:
//   - Preserving: NULL values are preserved/propagated (may be null-rejecting)
//   - Hiding: NULL values are hidden/transformed to non-NULL (not null-rejecting)
//   - Undecided: cannot determine from structure (conservative: treat as not null-rejecting)
type NullRejectionResult int

const (
	// Undecided means we cannot determine from structure alone.
	// The caller treats this as not null-rejecting (conservative default).
	Undecided NullRejectionResult = iota

	// Preserving means NULL inputs produce NULL outputs.
	// This is a necessary condition for null-rejection.
	// Examples: column, NOT, comparisons (=, >, <), arithmetic (+, -, *), LIKE, REGEXP
	// Combined with MustNull(inner), this becomes null-rejecting.
	Preserving

	// Hiding means NULL inputs may produce non-NULL outputs.
	// This makes the predicate NOT null-rejecting.
	// Examples: istrue_with_null, coalesce, AND/OR (short-circuit), IS NULL, <=>, CASE/IF
	Hiding
)

// checkNullPreservingArgs checks if a NULL-preserving function is null-rejecting
// based on whether any of its arguments propagate inner NULL.
// This is the unified logic for: NonTrue(F(args...)) ⊇ MustNull(F(args...)) = ⋃ MustNull(arg_i)
//
// Parameters:
//   - args: function arguments to check
//   - inner: inner schema to check against
//   - maxArgsToCheck: maximum number of arguments to check (use -1 for all)
//
// Returns Preserving if any arg propagates NULL, Undecided otherwise.
func checkNullPreservingArgs(
	args []expression.Expression, inner *expression.Schema, maxArgsToCheck int,
) NullRejectionResult {
	if maxArgsToCheck < 0 || maxArgsToCheck > len(args) {
		maxArgsToCheck = len(args)
	}
	for i := range maxArgsToCheck {
		if isNullPropagatingWRTInner(args[i], inner) {
			return Preserving
		}
	}
	return Undecided
}

// isNullRejectingByStructure tries to decide *syntactically* whether predicate `p`
// is NULL-rejecting w.r.t. `inner`. It returns:
//
//	Preserving   → NULL is preserved (may be null-rejecting if combined with MustNull)
//	Hiding       → NULL is hidden (NOT null-rejecting)
//	Undecided    → cannot determine structurally (treated as not null-rejecting by caller)
//
// This implements the two-set framework (NonTrue/MustNull) for sound null-rejection analysis.
// The rules are deliberately conservative: if we cannot be *certain* from the
// structure, we return Undecided (which the caller treats as not null-rejecting).
//
// Framework rules applied:
//   - NonTrue(A AND B) = NonTrue(A) ∪ NonTrue(B)  [union: either side null-rejecting]
//   - NonTrue(A OR B) = NonTrue(A) ∩ NonTrue(B)   [intersection: both must be null-rejecting]
//   - NonTrue(NOT A) = MustNull(A)                [NOT requires MustNull evidence]
//   - NonTrue(NULL Preserving F(args...)) ⊇ MustNull(F(args...)) = ⋃ MustNull(arg_i)
//   - NonTrue(IS NOT NULL / IS TRUE / IS FALSE) ⊇ MustNull(arg)  [reject-NULL tests]
//   - NonTrue(IS NULL / IS UNKNOWN) = ∅                           [accept-NULL tests]
func isNullRejectingByStructure(p expression.Expression, inner *expression.Schema) NullRejectionResult {
	sf, ok := p.(*expression.ScalarFunction)
	if !ok {
		// Non-scalar (pure column/constant) rarely decides anything structurally.
		return Undecided
	}

	args := sf.GetArgs()
	funcName := sf.FuncName.L

	switch funcName {
	// ---- 1) Comparisons / LIKE / REGEXP ------------------------------------
	// NULL Preserving functions with explicit handling for common comparison operators.
	case ast.EQ, ast.NE, ast.GT, ast.GE, ast.LT, ast.LE, ast.Like, ast.Ilike, ast.RegexpLike, ast.Regexp:
		return handleNullPreservingComparison(sf, args, inner)

	// ---- 2) Null-safe equal `<=>` -------------------------------------------
	// By definition it does not reject NULLs (NULL <=> NULL is TRUE).
	// This is Hiding because it explicitly handles NULL.
	case ast.NullEQ:
		return Hiding

	// ---- 3) IN / NOT IN -----------------------------------------------------
	// Defer IN to higher-level isNullRejectedInList() in IsNullRejected.
	case ast.In:
		return Undecided

	// ---- 4) IS NULL (accept-NULL test) --------------------------------------
	// Framework: NonTrue(IS NULL) = ∅. When A=U, IS NULL(A) = T.
	// This is Hiding because it explicitly handles NULL (returns TRUE for NULL).
	case ast.IsNull:
		return Hiding

	// ---- 5) IS TRUE / IS FALSE / IS NOT NULL (reject-NULL tests) -----------
	// Framework: NonTrue(Test_reject_NULL(A)) ⊇ MustNull(A)
	case ast.IsFalsity, ast.IsTruthWithoutNull, ast.IsTruthWithNull:
		if len(args) > 0 && isNullPropagatingWRTInner(args[0], inner) {
			return Preserving
		}
		return Undecided

	// ---- 6) Boolean composition ---------------------------------------------
	case ast.LogicAnd:
		return handleLogicAnd(args, inner)

	case ast.LogicOr:
		return handleLogicOr(args, inner)

	// ---- 7) XOR (NULL-preserving: NULL XOR anything = NULL) -----------------
	case ast.LogicXor:
		return handleLogicXor(args, inner)

	// ---- 8) NOT … -----------------------------------------------------------
	case ast.UnaryNot:
		return handleUnaryNot(args, inner)

	default:
		// Generic handler for other NullPreserving functions
		return handleGenericNullPreserving(sf, args, inner)
	}
}

// handleNullPreservingComparison handles comparison operators (=, <>, <, >, <=, >=, LIKE, REGEXP).
// Note: caller (isNullRejectedExpr) has already verified that predicate references inner columns.
func handleNullPreservingComparison(
	_ *expression.ScalarFunction, args []expression.Expression, inner *expression.Schema,
) NullRejectionResult {
	// Check if any argument propagates NULL (union semantics)
	// For LIKE/ILIKE, only check first 2 args (escape arg is usually constant)
	maxArgs := 2
	return checkNullPreservingArgs(args, inner, maxArgs)
}

// handleLogicAnd handles AND with union semantics: NonTrue(A AND B) = NonTrue(A) ∪ NonTrue(B)
// AND is Hiding due to short-circuit: FALSE AND NULL = FALSE (hides NULL)
// But if either operand is null-preserving w.r.t. inner, the whole can be null-rejecting.
func handleLogicAnd(args []expression.Expression, inner *expression.Schema) NullRejectionResult {
	if len(args) < 2 {
		return Undecided
	}

	left := isNullRejectingByStructure(args[0], inner)
	// Short-circuit: if left is Preserving, the whole is Preserving (union)
	if left == Preserving {
		return Preserving
	}

	right := isNullRejectingByStructure(args[1], inner)
	if right == Preserving {
		return Preserving
	}

	// Both sides determined but neither is Preserving
	// AND can hide NULL (FALSE AND NULL = FALSE), so it's Hiding
	if left != Undecided && right != Undecided {
		return Hiding
	}

	return Undecided
}

// handleLogicXor handles XOR with union semantics (NULL-preserving: NULL XOR anything = NULL).
// NonTrue(A XOR B) ⊇ MustNull(A) ∪ MustNull(B)
func handleLogicXor(args []expression.Expression, inner *expression.Schema) NullRejectionResult {
	if len(args) < 2 {
		return Undecided
	}
	// XOR is NULL-preserving: if ANY side propagates NULL, result is NULL
	return checkNullPreservingArgs(args, inner, -1)
}

// handleLogicOr handles OR with intersection semantics: NonTrue(A OR B) = NonTrue(A) ∩ NonTrue(B)
// OR is Hiding due to short-circuit: TRUE OR NULL = TRUE (hides NULL)
// Only if BOTH operands are null-preserving w.r.t. inner, the whole is null-rejecting.
func handleLogicOr(args []expression.Expression, inner *expression.Schema) NullRejectionResult {
	if len(args) < 2 {
		return Undecided
	}

	left := isNullRejectingByStructure(args[0], inner)
	// Short-circuit: if left is Hiding, the whole cannot be Preserving (intersection fails)
	if left == Hiding {
		return Hiding
	}

	right := isNullRejectingByStructure(args[1], inner)
	if right == Hiding {
		return Hiding
	}

	// Both sides must be Preserving for intersection
	if left == Preserving && right == Preserving {
		return Preserving
	}

	return Undecided
}

// handleUnaryNot handles NOT with MustNull semantics: NonTrue(NOT A) = MustNull(A)
// NOT is Preserving: NOT(NULL) = NULL
func handleUnaryNot(args []expression.Expression, inner *expression.Schema) NullRejectionResult {
	if len(args) == 0 {
		return Undecided
	}

	child := args[0]
	childPeeled := peelTransparentWrappers(child)

	// CRITICAL: Unwrap istrue_with_null introduced by PushDownNot
	// PushDownNot transforms: not(X) → not(istrue_with_null(X))
	// For null-rejection detection, we need to check the original X because:
	// - not(X) when X=NULL: not(NULL) = NULL (null-rejecting) ✓
	// - not(istrue_with_null(X)) when X=NULL: not(istrue_with_null(NULL)) = not(FALSE) = TRUE (not null-rejecting) ✗
	// By unwrapping, we restore the correct null-rejection semantics.
	if childSF, ok := childPeeled.(*expression.ScalarFunction); ok {
		if childSF.FuncName.L == ast.IsTruthWithNull {
			// Unwrap istrue_with_null and check the inner expression directly
			if len(childSF.GetArgs()) > 0 {
				unwrapped := childSF.GetArgs()[0]
				if isNullPropagatingWRTInner(unwrapped, inner) {
					return Preserving
				}
			}
			return Undecided
		}

		// Special handling for NOT (IS NULL) and NOT (IN)
		switch childSF.FuncName.L {
		case ast.IsNull:
			// NOT (X IS NULL) ≡ X IS NOT NULL (reject-NULL test)
			if len(childSF.GetArgs()) > 0 && isNullPropagatingWRTInner(childSF.GetArgs()[0], inner) {
				return Preserving
			}
			return Hiding
		case ast.In:
			// NOT (IN(...)) → defer to fallback
			return Undecided
		}
	}

	// General NOT: NonTrue(NOT A) = MustNull(A)
	// NOT is Preserving, so if the child propagates NULL, NOT propagates it too
	if isNullPropagatingWRTInner(child, inner) {
		return Preserving
	}
	return Undecided
}

// handleGenericNullPreserving handles other NullPreserving functions not explicitly listed.
// Uses the generic rule: NonTrue(F(args...)) ⊇ MustNull(F(args...)) = ⋃ MustNull(arg_i)
// Note: caller (isNullRejectedExpr) has already verified that predicate references inner columns.
func handleGenericNullPreserving(
	sf *expression.ScalarFunction, args []expression.Expression, inner *expression.Schema,
) NullRejectionResult {
	t := traitOf(sf.FuncName.L)
	if !has(t, NullPreserving) {
		return Undecided
	}

	// Check all arguments for NULL propagation
	return checkNullPreservingArgs(args, inner, -1)
}
